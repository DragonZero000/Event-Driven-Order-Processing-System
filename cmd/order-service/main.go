package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	order "github.com/DragonZero000/Event-Driven-Order-Processing-System/internal/order"
	"github.com/DragonZero000/Event-Driven-Order-Processing-System/pkg/kafka"
	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/order"
	"go.uber.org/zap"
	"go.uber.org/zap/zapgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	grpclog.SetLoggerV2(zapgrpc.NewLogger(logger))
	sugar := logger.Sugar()
	sugar.Info("Start Order Service")
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}
	defer lis.Close()
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}
	brokers := []string{kafkaBroker}
	// Топики создаются заранее, чтобы потребители не стартовали раньше них.
	for _, t := range []string{"orders", "stock-reserved", "stock-rejected", "payment"} {
		if err := kafka.EnsureTopic(kafkaBroker, t, 3, 1); err != nil {
			sugar.Warnw("failed to ensure kafka topic (may already exist)", "topic", t, "error", err)
		}
	}
	producer := kafka.NewProducer(brokers, "orders")
	defer producer.Close()
	serv := order.NewServer(producer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumers := map[string]struct {
		c *kafka.Consumer
		h kafka.Handler
	}{
		"stock-reserved": {kafka.NewConsumer(brokers, "stock-reserved", "order-service-reserved"), serv.HandleStockReserved},
		"stock-rejected": {kafka.NewConsumer(brokers, "stock-rejected", "order-service-rejected"), serv.HandleStockRejected},
		"payment":        {kafka.NewConsumer(brokers, "payment", "order-service-payment"), serv.HandlePayment},
	}
	var wg sync.WaitGroup
	consErrCh := make(chan error, len(consumers))
	for topic, cs := range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cs.c.Consume(ctx, cs.h); err != nil && ctx.Err() == nil {
				sugar.Errorw("consumer failed", "topic", topic, "error", err)
				consErrCh <- err
			}
		}()
	}

	grpcServer := grpc.NewServer()
	pb.RegisterOrderServiceServer(grpcServer, serv)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	serveErrCh := make(chan error, 1)
	go func() { serveErrCh <- grpcServer.Serve(lis) }()
	select {
	case sig := <-sigCh:
		sugar.Infow("received signal, shutting down", "signal", sig.String())
	case err := <-consErrCh:
		sugar.Errorw("shutting down: consumer error", "error", err)
	case err := <-serveErrCh:
		if err != nil {
			sugar.Errorw("grpc server failed unexpectedly", "error", err)
		}
	}
	grpcServer.GracefulStop()
	cancel()
	wg.Wait()
	for _, cs := range consumers {
		cs.c.Close()
	}
	sugar.Info("Order Service stopped")
}
