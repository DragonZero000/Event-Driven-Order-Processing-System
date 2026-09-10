package notification

import (
	"context"
	"encoding/json"
	"log"
	"sync"
)

type Handler struct {
	mu                        sync.Mutex
	notificationEvents        map[string]*notificationState
	orderNotificationsCount   int
	paymentNotificationsCount int
}

func NewHandler() *Handler {
	return &Handler{
		notificationEvents: make(map[string]*notificationState),
	}
}
func (h *Handler) HandleOrderEvent(ctx context.Context, key, value []byte) error {
	var event OrderEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	state := h.stateFor(event.ID)
	if state.orderStatus {
		log.Printf("order notification for %s already sent, skipping", event.ID)
		return nil
	}

	log.Printf("sending order-created notification for %s", event.ID)
	state.orderStatus = true
	h.orderNotificationsCount++
	return nil
}

func (h *Handler) HandlePaymentEvent(ctx context.Context, key, value []byte) error {
	var event PaymentEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	state := h.stateFor(event.ID)
	if state.paymentStatus {
		log.Printf("payment notification for %s already sent, skipping", event.ID)
		return nil
	}
	log.Printf("sending payment notification for %s (status=%v)", event.ID, event.Status)
	state.paymentStatus = true
	h.paymentNotificationsCount++
	return nil
}

func (h *Handler) stateFor(orderID string) *notificationState {
	s, ok := h.notificationEvents[orderID]
	if !ok {
		s = &notificationState{}
		h.notificationEvents[orderID] = s
	}
	return s
}
