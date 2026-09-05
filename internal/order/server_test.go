package order

import (
	"context"
	"sync"
	"testing"

	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateOrder_Success(t *testing.T) {
	s := NewServer()
	resp, err := s.CreateOrder(context.Background(), &pb.CreateOrderRequest{
		CustomerId: "123",
		Items: []*pb.OrderItem{
			{ProductId: "fsee", Quantity: 50, Price: 4.9},
		},
	})
	if err != nil {
		t.Fatalf("expected no error but got %v", err)
	}
	if resp.OrderId == "" {
		t.Errorf("expected non empty id")
	}
}

func TestGetOrder_Success(t *testing.T) {
	s := NewServer()
	resp, err := s.CreateOrder(context.Background(), &pb.CreateOrderRequest{
		CustomerId: "123",
		Items: []*pb.OrderItem{
			{ProductId: "fsee", Quantity: 50, Price: 4.9},
		},
	})
	if err != nil {
		t.Fatalf("expected no error but got %v", err)
	}
	if resp.OrderId == "" {
		t.Errorf("expected non empty id")
	}
	resp2, err := s.GetOrder(context.Background(), &pb.GetOrderRequest{
		OrderId: resp.OrderId,
	})
	if err != nil {
		t.Fatalf("expected no error but got %v", err)
	}
	if resp2.Order == nil {
		t.Errorf("expected non empty order")
	}
	if resp2.Order.CustomerId != "123" {
		t.Errorf("expected customer id to be 123")
	}
	if len(resp2.Order.Items) != 1 {
		t.Errorf("expected one item in the order")
	}
	if resp2.Order.Items[0].ProductId != "fsee" {
		t.Errorf("expected product id to be fsee")
	}
	if resp2.Order.Items[0].Quantity != 50 {
		t.Errorf("expected quantity to be 5")
	}
	if resp2.Order.Items[0].Price != 4.9 {
		t.Errorf("expected price to be 4.9")
	}
	if resp2.Order.TotalPrice != 245.0 {
		t.Errorf("expected total price to be 245, but found %f", resp2.Order.TotalPrice)
	}
	if resp2.Order.Status != pb.OrderStatus_Order_Status_PENDING {
		t.Errorf("expected status to be completed")
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	s := NewServer()
	resp, err := s.GetOrder(context.Background(), &pb.GetOrderRequest{
		OrderId: "123",
	})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("expected to be a gRPC error")
		} else if st.Code() != codes.NotFound {
			t.Errorf("expected NotFound but got %v", st)
		}
		return
	}
	if resp.Order != nil {
		t.Errorf("expected empty order")
	}
}

func TestCreateOrder_ConcurrentAccess(t *testing.T) {
	s := NewServer()
	const numRequests = 100

	var wg sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.CreateOrder(context.Background(), &pb.CreateOrderRequest{
				CustomerId: "concurrent-customer",
				Items: []*pb.OrderItem{
					{ProductId: "sku-1", Quantity: 1, Price: 10.0},
				},
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.orders) != numRequests {
		t.Errorf("expected %d orders, got %d", numRequests, len(s.orders))
	}
}
