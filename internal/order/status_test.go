package order

import (
	"context"
	"testing"

	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/order"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateOrder_DuplicateProductRejected(t *testing.T) {
	s := NewServer(NewFakePublisher())
	_, err := s.CreateOrder(context.Background(), &pb.CreateOrderRequest{
		CustomerId: "c1",
		Items: []*pb.OrderItem{
			{ProductId: "soup", Quantity: 6, Price: 1},
			{ProductId: "soup", Quantity: 6, Price: 1},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for duplicate product_id, got %v", err)
	}
}

func TestSetStatus_ProcessingDoesNotOverrideFinal(t *testing.T) {
	s := NewServer(NewFakePublisher())
	s.orders["o1"] = &Order{ID: "o1", Status: Pending}
	// payment пришёл раньше stock-reserved: запоздавшее событие не откатывает финальный статус
	_ = s.HandlePayment(context.Background(), nil, []byte(`{"order_id":"o1","status":1}`))
	_ = s.HandleStockReserved(context.Background(), nil, []byte(`{"id":"o1"}`))
	if s.orders["o1"].Status != Completed {
		t.Fatalf("status = %d, want COMPLETED", s.orders["o1"].Status)
	}
}
