package order

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/order"
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

func TestCreateOrder_Validation(t *testing.T) {
	good := &pb.OrderItem{ProductId: "soup", Quantity: 1, Price: 5}
	cases := map[string]*pb.CreateOrderRequest{
		"no customer":       {Items: []*pb.OrderItem{good}},
		"no items":          {CustomerId: "c"},
		"negative quantity": {CustomerId: "c", Items: []*pb.OrderItem{{ProductId: "soup", Quantity: -3, Price: 5}}},
		"zero price":        {CustomerId: "c", Items: []*pb.OrderItem{{ProductId: "soup", Quantity: 1}}},
		"no product id":     {CustomerId: "c", Items: []*pb.OrderItem{{Quantity: 1, Price: 5}}},
	}
	for name, req := range cases {
		pub := NewFakePublisher()
		_, err := NewServer(pub).CreateOrder(context.Background(), req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: code = %v, want InvalidArgument", name, status.Code(err))
		}
		if len(pub.events) != 0 {
			t.Errorf("%s: event must not be published", name)
		}
	}
}

type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, string, []byte) error {
	return errors.New("kafka down")
}

func TestCreateOrder_PublishFailureRollsBack(t *testing.T) {
	s := NewServer(failingPublisher{})
	_, err := s.CreateOrder(context.Background(), &pb.CreateOrderRequest{
		CustomerId: "c", Items: []*pb.OrderItem{{ProductId: "soup", Quantity: 1, Price: 5}},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", status.Code(err))
	}
	if len(s.orders) != 0 {
		t.Fatal("order must be rolled back")
	}
}

func TestSaga_StockReservedMovesToProcessing(t *testing.T) {
	s := NewServer(NewFakePublisher())
	id := newOrder(t, s)
	_ = s.HandleStockReserved(context.Background(), nil, []byte(`{"id":"`+id+`"}`))
	if got := statusOf(t, s, id); got != pb.OrderStatus_Order_Status_PROCESSING {
		t.Fatalf("status = %v", got)
	}
	_ = s.HandlePayment(context.Background(), nil, []byte(`{"order_id":"`+id+`","status":1}`))
	_ = s.HandleStockReserved(context.Background(), nil, []byte(`{"id":"`+id+`"}`)) // поздний дубль
	if got := statusOf(t, s, id); got != pb.OrderStatus_Order_Status_COMPLETED {
		t.Fatalf("status = %v", got)
	}
}
