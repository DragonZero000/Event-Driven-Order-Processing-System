package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	order "github.com/DragonZero000/Event-Driven-Order-Processing-System/internal/order"
	"github.com/DragonZero000/Event-Driven-Order-Processing-System/pkg/kafka"
	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto"
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
	if err := kafka.EnsureTopic(kafkaBroker, "orders", 1, 1); err != nil {
		sugar.Warnw("failed to ensure kafka topic (may already exist, this is often OK)", "topic", "orders", "error", err)
	} else {
		sugar.Info("topic 'orders' ensured")
	}
	producer := kafka.NewProducer([]string{kafkaBroker}, "orders")
	defer producer.Close()
	serv := order.NewServer(producer)
	grpcServer := grpc.NewServer()
	pb.RegisterOrderServiceServer(grpcServer, serv)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()
	select {
	case sig := <-sigCh:
		sugar.Infow("received signal, shutting down", "signal", sig.String())
		grpcServer.GracefulStop()
	case serveErr := <-serveErrCh:
		if serveErr != nil {
			sugar.Errorw("grpc server failed unexpectedly", "error", serveErr)
		}
	}
	sugar.Info("Order Service stopped")
}
