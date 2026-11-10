package inventory

import (
	"context"
	"testing"
)

type recPub struct{ keys []string }

func (r *recPub) Publish(_ context.Context, key string, _ []byte) error {
	r.keys = append(r.keys, key)
	return nil
}

func TestSaga_ReservedEventPublished(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 5})
	ok, bad := &recPub{}, &recPub{}
	h.SetPublishers(ok, bad)
	_ = h.Handle(context.Background(), nil, []byte(`{"id":"o1","total_price":10,"items":[{"product_id":"soup","quantity":2}]}`))
	if len(ok.keys) != 1 || len(bad.keys) != 0 {
		t.Fatalf("reserved=%d rejected=%d", len(ok.keys), len(bad.keys))
	}
}

func TestSaga_RejectedEventPublished(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 1})
	ok, bad := &recPub{}, &recPub{}
	h.SetPublishers(ok, bad)
	_ = h.Handle(context.Background(), nil, []byte(`{"id":"o1","items":[{"product_id":"soup","quantity":2}]}`))
	if len(ok.keys) != 0 || len(bad.keys) != 1 {
		t.Fatalf("reserved=%d rejected=%d", len(ok.keys), len(bad.keys))
	}
}

func TestSaga_PaymentFailureReleasesStock(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 5})
	_ = h.Handle(context.Background(), nil, []byte(`{"id":"o1","items":[{"product_id":"soup","quantity":2}]}`))
	for i := 0; i < 2; i++ { // повторная доставка не должна вернуть товар дважды
		_ = h.HandlePayment(context.Background(), nil, []byte(`{"order_id":"o1","status":0}`))
	}
	if h.stock["soup"] != 5 {
		t.Fatalf("stock = %d, want 5", h.stock["soup"])
	}
}

func TestSaga_PaymentSuccessKeepsReservation(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 5})
	_ = h.Handle(context.Background(), nil, []byte(`{"id":"o1","items":[{"product_id":"soup","quantity":2}]}`))
	_ = h.HandlePayment(context.Background(), nil, []byte(`{"order_id":"o1","status":1}`))
	if h.stock["soup"] != 3 {
		t.Fatalf("stock = %d, want 3", h.stock["soup"])
	}
}

func TestHandle_NegativeQuantityRejected(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 5})
	ok, bad := &recPub{}, &recPub{}
	h.SetPublishers(ok, bad)
	_ = h.Handle(context.Background(), nil, []byte(`{"id":"o1","items":[{"product_id":"soup","quantity":-4}]}`))
	if h.stock["soup"] != 5 || len(bad.keys) != 1 || len(ok.keys) != 0 {
		t.Fatalf("stock=%d rejected=%d reserved=%d", h.stock["soup"], len(bad.keys), len(ok.keys))
	}
}
