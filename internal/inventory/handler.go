package inventory

import (
	"context"
	"encoding/json"
	"log"
	"sync"
)

type Handler struct {
	mu           sync.Mutex
	processed    map[string]bool
	reservarions int
}

func NewHandler() *Handler {
	return &Handler{
		processed: make(map[string]bool),
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
	log.Printf("reserving items for order %s", event.ID)
	h.processed[event.ID] = true
	h.reservarions++
	return nil
}
