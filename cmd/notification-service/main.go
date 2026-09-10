package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/DragonZero000/Event-Driven-Order-Processing-System/internal/notification"
	"github.com/DragonZero000/Event-Driven-Order-Processing-System/pkg/kafka"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()
	sugar.Info("Starting notification service...")

	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	orderConsumer := kafka.NewConsumer([]string{kafkaBroker}, "orders", "notification-service-order")
	paymentConsumer := kafka.NewConsumer([]string{kafkaBroker}, "payment", "notification-service-payment")
	defer orderConsumer.Close()
	defer paymentConsumer.Close()
	handler := notification.NewHandler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	orderConsErrCh := make(chan error, 1)
	paymentConsErrCh := make(chan error, 1)
	go func() {
		orderConsErrCh <- orderConsumer.Consume(ctx, handler.HandleOrderEvent)
	}()
	go func() {
		paymentConsErrCh <- paymentConsumer.Consume(ctx, handler.HandlePaymentEvent)
	}()
	select {
	case sig := <-sigCh:
		sugar.Infof("Received signal: %v", sig)
		cancel()
	case err := <-orderConsErrCh:
		if err != nil {
			sugar.Errorf("Error consuming order events: %s", err.Error())
		}
		cancel()
	case err := <-paymentConsErrCh:
		if err != nil {
			sugar.Errorf("Error consuming payments events: %s", err.Error())
		}
		cancel()
	}
	sugar.Info("notification service is stopped")
}
