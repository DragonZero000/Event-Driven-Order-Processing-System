package order

import (
	"context"
	"testing"

	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto"
)

func newOrder(t *testing.T, s *Server) string {
	t.Helper()
	r, err := s.CreateOrder(context.Background(), &pb.CreateOrderRequest{
		CustomerId: "c", Items: []*pb.OrderItem{{ProductId: "soup", Quantity: 1, Price: 5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return r.OrderId
}

func statusOf(t *testing.T, s *Server, id string) pb.OrderStatus {
	t.Helper()
	r, err := s.GetOrder(context.Background(), &pb.GetOrderRequest{OrderId: id})
	if err != nil {
		t.Fatal(err)
	}
	return r.Order.Status
}

func TestSaga_PaymentSuccessCompletesOrder(t *testing.T) {
	s := NewServer(NewFakePublisher())
	id := newOrder(t, s)
	_ = s.HandlePayment(context.Background(), nil, []byte(`{"order_id":"`+id+`","status":1}`))
	if got := statusOf(t, s, id); got != pb.OrderStatus_Order_Status_COMPLETED {
		t.Fatalf("status = %v", got)
	}
}

func TestSaga_StockRejectedCancelsOrder(t *testing.T) {
	s := NewServer(NewFakePublisher())
	id := newOrder(t, s)
	_ = s.HandleStockRejected(context.Background(), nil, []byte(`{"id":"`+id+`"}`))
	// финальное состояние не перезаписывается поздним событием
	_ = s.HandlePayment(context.Background(), nil, []byte(`{"order_id":"`+id+`","status":1}`))
	if got := statusOf(t, s, id); got != pb.OrderStatus_Order_Status_CANCELLED {
		t.Fatalf("status = %v", got)
	}
}
