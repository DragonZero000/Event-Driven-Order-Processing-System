package payment

import (
	"context"
	"encoding/json"
	"log"
	"sync"
)

type Handler struct {
	eventPublisher eventPublisher
	paymentGateway PaymentGateway
	paymentEvents  map[string]bool
	mu             sync.Mutex
}

type eventPublisher interface {
	Publish(ctx context.Context, key string, value []byte) error
}

func NewHandler(eventPublisher eventPublisher, paymentGateway PaymentGateway) *Handler {
	return &Handler{
		eventPublisher: eventPublisher,
		paymentGateway: paymentGateway,
		paymentEvents:  make(map[string]bool),
	}
}

func (h *Handler) Handle(ctx context.Context, key, value []byte) error {
	var order OrderCreateEvent
	if err := json.Unmarshal(value, &order); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.paymentEvents[order.ID] {
		log.Printf("payment for order %s already processed, skipping", order.ID)
		return nil
	}
	payment, err := h.paymentGateway.Charge(ctx, order.ID, order.TotalPrice)
	if err != nil {
		return err
	}
	paymentStatus := PaymentStatusFailed
	if payment {
		paymentStatus = PaymentStatusProcessed
	}
	log.Printf("sending payment event for order %s", order.ID)
	jsonData, err := json.Marshal(PaymentEvent{
		OrderID: order.ID,
		Status:  paymentStatus,
		Amount:  order.TotalPrice,
	})
	if err != nil {
		return err
	}
	err = h.eventPublisher.Publish(ctx, order.ID, jsonData)
	if err != nil {
		log.Printf("faild to publish payment event for order %s: %v", order.ID, err)
		return err
	}
	h.paymentEvents[order.ID] = true
	log.Printf("payment event published for order %s (approved=%v)", order.ID, paymentStatus)
	return nil
}
