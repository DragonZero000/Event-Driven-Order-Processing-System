package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/DragonZero000/Event-Driven-Order-Processing-System/internal/inventory"
	"github.com/DragonZero000/Event-Driven-Order-Processing-System/pkg/kafka"
	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/inventory"
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
	sugar.Info("Starting inventory service...")
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		sugar.Fatal(err)
	}
	defer lis.Close()
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}
	consumer := kafka.NewConsumer([]string{kafkaBroker}, "orders", "inventory-service")
	defer consumer.Close()
	sugar.Infow("kafka connection established", "broker", kafkaBroker, "topic", "orders", "group", "inventory-service")
	grpcServer := grpc.NewServer()
	handler := inventory.NewHandler(startInventory())
	pb.RegisterInventoryServiceServer(grpcServer, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	consErrCh := make(chan error, 1)
	grpcServErrCh := make(chan error, 1)
	go func() {
		consErrCh <- consumer.Consume(ctx, handler.Handle)
	}()
	go func() {
		grpcServErrCh <- grpcServer.Serve(lis)
	}()
	select {
	case sig := <-sigCh:
		sugar.Infof("Received signal %v", sig)
		cancel()
	case err := <-consErrCh:
		if err != nil {
			sugar.Errorf("Error consuming: %s", err.Error())
		}
	case err := <-grpcServErrCh:
		if err != nil {
			sugar.Errorw("Error serving gRPC: %s", "error", err)
		}
	}
	sugar.Info("Inventory service is stopped")
}

func startInventory() map[string]int64 {
	return map[string]int64{
		"soup":  10,
		"bread": 25,
	}
}
