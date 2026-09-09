package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/DragonZero000/Event-Driven-Order-Processing-System/internal/payment"
	"github.com/DragonZero000/Event-Driven-Order-Processing-System/pkg/kafka"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()
	sugar.Info("Starting payment service...")

	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	consumer := kafka.NewConsumer([]string{kafkaBroker}, "orders", "payment-service")
	producer := kafka.NewProducer([]string{kafkaBroker}, "payment")
	defer consumer.Close()
	defer producer.Close()
	sugar.Infow("kafka connection established", "broker", kafkaBroker, "topic", "orders", "group", "payment-service")
	paymentGateway := payment.NewSimulatedPaymentGateway()
	handler := payment.NewHandler(producer, paymentGateway)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	consErrCh := make(chan error, 1)
	go func() {
		consErrCh <- consumer.Consume(ctx, handler.Handle)
	}()
	select {
	case sig := <-sigCh:
		sugar.Infof("Received signal %v", sig)
		cancel()
	case err := <-consErrCh:
		if err != nil {
			sugar.Errorf("Error consuming: %s", err.Error())
		}
	}
	sugar.Info("Payment service is stopped")
}
