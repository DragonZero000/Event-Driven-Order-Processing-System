package payment

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakePaymentGateway struct {
	correctPayment map[string]bool
}

func NewFakePaymentGateway() *fakePaymentGateway {
	return &fakePaymentGateway{
		correctPayment: make(map[string]bool),
	}
}

func (g *fakePaymentGateway) Charge(ctx context.Context, orderID string, amount float32) (approved bool, err error) {
	if approved, ok := g.correctPayment[orderID]; !ok || amount < 0.0 {
		return false, errors.New("invalid payment")
	} else if approved {
		return true, nil
	}
	return false, nil
}

type fakePublisher struct {
	events    map[string][]byte
	mu        sync.Mutex
	callCount int
}

func NewFakePublisher() *fakePublisher {
	return &fakePublisher{
		events: make(map[string][]byte),
	}
}
func (p *fakePublisher) Publish(ctx context.Context, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events[key] = value
	p.callCount++
	return nil
}

func TestHandle_ApprovedPay(t *testing.T) {
	fakePaymentGateway := NewFakePaymentGateway()
	fakePaymentGateway.correctPayment["1"] = true
	fakePubl := NewFakePublisher()
	h := NewHandler(fakePubl, fakePaymentGateway)
	order := OrderCreateEvent{
		ID:         "1",
		CustomerID: "1",
		OrderItems: []OrderItem{},
		Status:     0,
		CreatedAt:  time.Now().Unix(),
		TotalPrice: 2.5,
	}
	data, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("failed to marshal order: %v", err)
	}
	if err = h.Handle(context.Background(), nil, data); err != nil {
		t.Errorf("expected no error but got: %v", err)
	}
	if fakePubl.events["1"] == nil {
		t.Error("payment event not published")
	}
	var event PaymentEvent
	if err := json.Unmarshal(fakePubl.events["1"], &event); err != nil {
		t.Fatalf("failed to unmarshal published event: %v", err)
	}
	if event.Status != PaymentStatusProcessed {
		t.Errorf("expected status %v, got %v", PaymentStatusProcessed, event.Status)
	}
	if event.OrderID != "1" {
		t.Errorf("expected order id %q, got %q", "1", event.OrderID)
	}
}

func TestHandle_DeclinedPayment(t *testing.T) {
	fakePaymentGateway := NewFakePaymentGateway()
	fakePaymentGateway.correctPayment["1"] = false
	fakePubl := NewFakePublisher()
	h := NewHandler(fakePubl, fakePaymentGateway)
	order := OrderCreateEvent{
		ID:         "1",
		CustomerID: "1",
		OrderItems: []OrderItem{},
		Status:     0,
		CreatedAt:  time.Now().Unix(),
		TotalPrice: 2.5,
	}
	data, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("failed to marshal order: %v", err)
	}
	if err = h.Handle(context.Background(), nil, data); err != nil {
		t.Errorf("expected no error but got: %v", err)
	}
	if fakePubl.events["1"] == nil {
		t.Errorf("payment event not published")
	}
	var event PaymentEvent
	if err := json.Unmarshal(fakePubl.events["1"], &event); err != nil {
		t.Fatalf("failed to unmarshal published event: %v", err)
	}
	if event.Status != PaymentStatusFailed {
		t.Errorf("expected status %v, got %v", PaymentStatusFailed, event.Status)
	}
	if event.OrderID != "1" {
		t.Errorf("expected order id %q, got %q", "1", event.OrderID)
	}
}

func TestHandle_GatewayError(t *testing.T) {
	fakePaymentGateway := NewFakePaymentGateway()
	fakePubl := NewFakePublisher()
	h := NewHandler(fakePubl, fakePaymentGateway)
	order := OrderCreateEvent{
		ID:         "1",
		CustomerID: "1",
		OrderItems: []OrderItem{},
		Status:     0,
		CreatedAt:  time.Now().Unix(),
		TotalPrice: 2.5,
	}
	data, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("failed to marshal order: %v", err)
	}
	if err = h.Handle(context.Background(), nil, data); err == nil {
		t.Errorf("expected error but got: %v", err)
	}
	if fakePubl.events["1"] != nil {
		t.Errorf("payment event published, but expected not to be")
	}
}

func TestHandle_Idempotent(t *testing.T) {
	fakePaymentGateway := NewFakePaymentGateway()
	fakePaymentGateway.correctPayment["1"] = true
	fakePubl := NewFakePublisher()
	h := NewHandler(fakePubl, fakePaymentGateway)
	order := OrderCreateEvent{
		ID:         "1",
		CustomerID: "1",
		OrderItems: []OrderItem{},
		Status:     0,
		CreatedAt:  time.Now().Unix(),
		TotalPrice: 2.5,
	}
	data, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("failed to marshal order: %v", err)
	}
	if err = h.Handle(context.Background(), nil, data); err != nil {
		t.Errorf("unexpected error while handling payment event for first time: %v", err)
	}
	if err = h.Handle(context.Background(), nil, data); err != nil {
		t.Errorf("unexpected error while handling payment event for second time: %v", err)
	}
	if fakePubl.events["1"] == nil {
		t.Error("payment event not published")
	}
	if fakePubl.callCount != 1 {
		t.Errorf("expected to publish only once, but got %d times", fakePubl.callCount)
	}
}
