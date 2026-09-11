package inventory

import (
	"context"
	"encoding/json"
	"testing"

	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/inventory"
)

func TestGetStockLevel_InitialStock(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 10})
	resp, err := h.GetStockLevel(context.Background(), &pb.GetStockLevelRequest{ProductId: "soup"})
	if err != nil {
		t.Fatalf("expected no error but got: %v", err)
	}
	if resp.StockQuantity != 10 {
		t.Errorf("initial stock quantity for soup is not 10: got %d instead", resp.StockQuantity)
	}
	respUnknown, err := h.GetStockLevel(context.Background(), &pb.GetStockLevelRequest{ProductId: "unknown-product"})
	if err != nil {
		t.Fatalf("expected no error but got: %v", err)
	}
	if respUnknown.StockQuantity != 0 {
		t.Errorf("initial stock quantity for unknown-product is not 0: got %d instead", respUnknown.StockQuantity)
	}
}

func TestHandle_ReducesStock(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 10})
	event := OrderCreateEvent{
		ID:         "test-reduce",
		CustomerID: "customer-test",
		OrderItems: []OrderItem{
			{ProductID: "soup", Quantity: 3},
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	if err := h.Handle(context.Background(), nil, data); err != nil {
		t.Fatalf("expected no error but got: %v", err)
	}
	resp, err := h.GetStockLevel(context.Background(), &pb.GetStockLevelRequest{ProductId: "soup"})
	if err != nil {
		t.Fatalf("expected no error but got: %v", err)
	}
	expectedRemaining := int64(10 - 3)
	if resp.StockQuantity != expectedRemaining {
		t.Errorf("expected stock quantity of %d but got %d", expectedRemaining, resp.StockQuantity)
	}
}

func TestHandle_InsufficientStock(t *testing.T) {
	h := NewHandler(map[string]int64{"item": 5})
	event := OrderCreateEvent{
		ID:         "test-insufficient",
		CustomerID: "customer-test",
		OrderItems: []OrderItem{
			{ProductID: "item", Quantity: 10},
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	err = h.Handle(context.Background(), nil, data)
	if err != nil {
		t.Errorf("failed to handle event: %v", err)
	}
	resp, _ := h.GetStockLevel(context.Background(), &pb.GetStockLevelRequest{ProductId: "item"})
	if resp.StockQuantity != 5 {
		t.Errorf("expected stock level to be 5, got %d", resp.StockQuantity)
	}
}

func TestHandle_MultiItem_AllOrNothing(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 10, "bread": 2})
	event := OrderCreateEvent{
		ID:         "test-multi",
		CustomerID: "customer-test",
		OrderItems: []OrderItem{
			{ProductID: "soup", Quantity: 3},
			{ProductID: "bread", Quantity: 5},
		},
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	err = h.Handle(context.Background(), nil, data)
	if err != nil {
		t.Errorf("failed to handle event: %v", err)
	}
	respSoup, _ := h.GetStockLevel(context.Background(), &pb.GetStockLevelRequest{ProductId: "soup"})
	if respSoup.StockQuantity != 10 {
		t.Errorf("expecting StockQuantity(soup) == 10 (unchanged), but got %d", respSoup.StockQuantity)
	}

	respBread, _ := h.GetStockLevel(context.Background(), &pb.GetStockLevelRequest{ProductId: "bread"})
	if respBread.StockQuantity != 2 {
		t.Errorf("expecting StockQuantity(bread) == 2 (unchanged), but got %d", respBread.StockQuantity)
	}
}

func TestHandle_Idempotent(t *testing.T) {
	h := NewHandler(map[string]int64{"item-1": 10})
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
	if h.processed["1"] == false {
		t.Errorf("expected order to be marked as processed")
	}
	if len(h.processed) != 1 {
		t.Errorf("expected 1 processed order but got %d", len(h.processed))
	}
}

func TestHandle_Success(t *testing.T) {
	h := NewHandler(map[string]int64{"item-1": 3})
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
	if h.stock["item-1"] != 0 {
		t.Errorf("expected stock to be 0 but got %d", h.stock["item-1"])
	}
}

func TestHandle_InvalidJSON(t *testing.T) {
	h := NewHandler(map[string]int64{"item-1": 10})
	badData := []byte(`{"id": "broken`)
	if err := h.Handle(context.Background(), nil, badData); err == nil {
		t.Errorf("expected error but got none")
	}
	if len(h.processed) != 0 {
		t.Errorf("processed map should be empty but it's not")
	}
}
