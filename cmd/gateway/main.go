package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/DragonZero000/Event-Driven-Order-Processing-System/internal/gateway"
	invpb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/inventory"
	orderpb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/order"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	orderConn, err := grpc.NewClient("passthrough:///"+env("ORDER_SERVICE_ADDR", "localhost:50051"), creds)
	if err != nil {
		sugar.Fatal(err)
	}
	defer orderConn.Close()
	orderConn.Connect() // не ждать ленивого подключения на первом запросе
	invConn, err := grpc.NewClient("passthrough:///"+env("INVENTORY_SERVICE_ADDR", "localhost:50052"), creds)
	if err != nil {
		sugar.Fatal(err)
	}
	defer invConn.Close()
	invConn.Connect()

	gw := gateway.New(orderpb.NewOrderServiceClient(orderConn), invpb.NewInventoryServiceClient(invConn))
	srv := &http.Server{Addr: ":" + env("PORT", "8080"), Handler: gw.Handler(), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		sugar.Infow("API gateway listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			sugar.Fatal(err)
		}
	}()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	sugar.Info("API gateway stopped")
}
