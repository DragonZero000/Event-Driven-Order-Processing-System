package inventory

import (
	"context"
	"encoding/json"
	"testing"
)

func TestHandle_Idempotent(t *testing.T) {
	h := NewHandler()
	event := OrderCreateEvent{
		ID:         "1",
		CustomerID: "customer-1",
		OrderItems: []OrderItem{
			{
				ProductID: "item-1",
				Quantity:  2,
			},
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	if err := h.Handle(context.Background(), nil, data); err != nil {
		t.Fatalf("first handle call failed: %v", err)
	}
	if err := h.Handle(context.Background(), nil, data); err != nil {
		t.Fatalf("second handle call failed: %v", err)
	}
	if h.reservarions != 1 {
		t.Errorf("expected reservation count to be 1 but got %d", h.reservarions)
	}
}

func TestHandle_Success(t *testing.T) {
	h := NewHandler()
	event := OrderCreateEvent{
		ID:         "2",
		CustomerID: "customer-2",
		OrderItems: []OrderItem{
			{
				ProductID: "item-1",
				Quantity:  3,
			},
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	if err := h.Handle(context.Background(), nil, data); err != nil {
		t.Fatalf("handle call failed: %v", err)
	}
	if h.processed["2"] == false {
		t.Errorf("order not processed")
	}
	if h.reservarions != 1 {
		t.Errorf("expected reservation count to be 1 but got %d", h.reservarions)
	}
}

func TestHandle_InvalidJSON(t *testing.T) {
	h := NewHandler()

	badData := []byte(`{"id": "broken`) // незакрытая строка/скобка — реально невалидный JSON

	if err := h.Handle(context.Background(), nil, badData); err == nil {
		t.Errorf("expected error but got none")
	}
	if h.reservarions != 0 {
		t.Errorf("reservation count should be zero but is not")
	}
	if len(h.processed) != 0 {
		t.Errorf("processed map should be empty but it's not")
	}
}
