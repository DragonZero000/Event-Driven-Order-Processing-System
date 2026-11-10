package notification

import (
	"context"
	"testing"
)

func TestHandleStockRejectedEvent_Idempotent(t *testing.T) {
	h := NewHandler()
	data := []byte(`{"id":"order-1","reason":"insufficient stock: soup"}`)
	for i := 0; i < 2; i++ {
		if err := h.HandleStockRejectedEvent(context.Background(), nil, data); err != nil {
			t.Fatalf("HandleStockRejectedEvent failed: %v", err)
		}
	}
	if h.rejectedNotificationsCount != 1 {
		t.Errorf("expected 1 cancellation notification, got %d", h.rejectedNotificationsCount)
	}
}
