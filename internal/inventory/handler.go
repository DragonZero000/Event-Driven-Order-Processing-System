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
	reserved  map[string][]OrderItem // резервы по заказам — нужны для компенсации
	reservedP eventPublisher
	rejectedP eventPublisher
	pb.UnimplementedInventoryServiceServer
}

func NewHandler(initialStock map[string]int64) *Handler {
	return &Handler{
		processed: make(map[string]bool),
		stock:     initialStock,
		reserved:  make(map[string][]OrderItem),
	}
}

// SetPublishers подключает публикацию результатов саги.
// Без вызова (например, в unit-тестах) события не отправляются.
func (h *Handler) SetPublishers(reserved, rejected eventPublisher) {
	h.reservedP = reserved
	h.rejectedP = rejected
}

// Handle обрабатывает OrderCreated: резервирует товары (всё или ничего)
// и публикует stock-reserved либо stock-rejected.
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
			if err := h.emit(ctx, h.rejectedP, event, "insufficient stock: "+item.ProductID); err != nil {
				return err // не помечаем обработанным — сообщение будет повторено
			}
			h.processed[event.ID] = true
			return nil
		}
	}
	for _, item := range event.OrderItems {
		h.stock[item.ProductID] -= int64(item.Quantity)
		log.Printf("reserved %d item(s) of product %s for order %s", item.Quantity, item.ProductID, event.ID)
	}
	h.reserved[event.ID] = event.OrderItems
	if err := h.emit(ctx, h.reservedP, event, ""); err != nil {
		// откатываем резерв, чтобы повторная обработка была корректной
		h.release(event.ID)
		return err
	}
	log.Printf("all items for order %s have been reserved", event.ID)
	h.processed[event.ID] = true
	return nil
}

// HandlePayment — компенсирующее действие саги: при отказе в оплате резерв снимается.
func (h *Handler) HandlePayment(ctx context.Context, key, value []byte) error {
	var res PaymentResult
	if err := json.Unmarshal(value, &res); err != nil {
		return err
	}
	if res.Status != 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.release(res.OrderID) {
		log.Printf("payment for order %s failed: reservation released", res.OrderID)
	}
	return nil
}

// release возвращает товары на склад; идемпотентна. Вызывать под h.mu.
func (h *Handler) release(orderID string) bool {
	items, ok := h.reserved[orderID]
	if !ok {
		return false
	}
	for _, item := range items {
		h.stock[item.ProductID] += int64(item.Quantity)
	}
	delete(h.reserved, orderID)
	return true
}

func (h *Handler) emit(ctx context.Context, p eventPublisher, e OrderCreateEvent, reason string) error {
	if p == nil {
		return nil
	}
	data, err := json.Marshal(StockEvent{ID: e.ID, CustomerID: e.CustomerID, TotalPrice: e.TotalPrice, Reason: reason})
	if err != nil {
		return err
	}
	return p.Publish(ctx, e.ID, data)
}

func (h *Handler) GetStockLevel(ctx context.Context, req *pb.GetStockLevelRequest) (*pb.GetStockLevelResponse, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	quantity := h.stock[req.ProductId]
	return &pb.GetStockLevelResponse{StockQuantity: quantity}, nil
}
