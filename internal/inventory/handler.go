package inventory

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/inventory"
)

type Handler struct {
	mu        sync.RWMutex
	processed map[string]bool
	stock     map[string]int64
	pb.UnimplementedInventoryServiceServer
}

func NewHandler(initialStock map[string]int64) *Handler {
	return &Handler{
		processed: make(map[string]bool),
		stock:     initialStock,
	}
}

func (h *Handler) Handle(ctx context.Context, key, value []byte) error {
	var event OrderCreateEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.processed[event.ID] {
		log.Printf("order %s already processed, skipping", event.ID)
		return nil
	}
	for _, item := range event.OrderItems {
		if h.stock[item.ProductID] < int64(item.Quantity) {
			log.Printf("order %s cannot be fulfilled: insufficient stock for %s", event.ID, item.ProductID)
			h.processed[event.ID] = true
			return nil
		}
	}
	for _, item := range event.OrderItems {
		h.stock[item.ProductID] -= int64(item.Quantity)
		log.Printf("reserved %d item(s) of product %s for order %s", item.Quantity, item.ProductID, event.ID)
	}
	log.Printf("all items for order %s have been reserved", event.ID)
	h.processed[event.ID] = true
	return nil
}

func (h *Handler) GetStockLevel(ctx context.Context, req *pb.GetStockLevelRequest) (*pb.GetStockLevelResponse, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	quantity := h.stock[req.ProductId]
	return &pb.GetStockLevelResponse{StockQuantity: quantity}, nil
}
