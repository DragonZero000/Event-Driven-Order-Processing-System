package notification

import (
	"context"
	"encoding/json"
	"testing"
)

func TestHandle_OrderAndPaymentDoNotCollide(t *testing.T) {
	h := NewHandler()
	orderData, err := json.Marshal(OrderEvent{ID: "order-1", CustomerID: "cust-1"})
	if err != nil {
		t.Fatalf("failed to marshal order event: %v", err)
	}
	paymentData, err := json.Marshal(PaymentEvent{ID: "order-1", Status: PaymentStatusProcessed})
	if err != nil {
		t.Fatalf("failed to marshal payment event: %v", err)
	}
	if err := h.HandleOrderEvent(context.Background(), nil, orderData); err != nil {
		t.Fatalf("HandleOrderEvent failed: %v", err)
	}
	if err := h.HandlePaymentEvent(context.Background(), nil, paymentData); err != nil {
		t.Fatalf("HandlePaymentEvent failed: %v", err)
	}
	if h.orderNotificationsCount != 1 {
		t.Errorf("expected 1 order notification, got %d", h.orderNotificationsCount)
	}
	if h.paymentNotificationsCount != 1 {
		t.Errorf("expected 1 payment notification, got %d (payment was skipped due to shared-key collision!)", h.paymentNotificationsCount)
	}
}

func TestHandleOrderEvent_Idempotent(t *testing.T) {
	h := NewHandler()
	orderData, err := json.Marshal(OrderEvent{ID: "order-1", CustomerID: "cust-1"})
	if err != nil {
		t.Fatalf("failed to marshal order event: %v", err)
	}
	if err := h.HandleOrderEvent(context.Background(), nil, orderData); err != nil {
		t.Fatalf("first HandleOrderEvent failed: %v", err)
	}
	if err := h.HandleOrderEvent(context.Background(), nil, orderData); err != nil {
		t.Fatalf("second HandleOrderEvent failed: %v", err)
	}
	if h.orderNotificationsCount != 1 {
		t.Errorf("expected orderNotifications == 1 (idempotent), got %d", h.orderNotificationsCount)
	}
}

func TestHandlePaymentEvent_Idempotent(t *testing.T) {
	h := NewHandler()
	paymentData, err := json.Marshal(PaymentEvent{ID: "pay-1", Status: PaymentStatusProcessed})
	if err != nil {
		t.Fatalf("failed to marshal payment event: %v", err)
	}
	if err := h.HandlePaymentEvent(context.Background(), nil, paymentData); err != nil {
		t.Fatalf("first HandlePaymentEvent failed: %v", err)
	}
	if err := h.HandlePaymentEvent(context.Background(), nil, paymentData); err != nil {
		t.Fatalf("second HandlePaymentEvent failed: %v", err)
	}
	if h.paymentNotificationsCount != 1 {
		t.Errorf("expected paymentNotifications == 1 (idempotent), got %d", h.paymentNotificationsCount)
	}
}

func TestHandleOrderEvent_InvalidJSON(t *testing.T) {
	h := NewHandler()
	badData := []byte("{broken")
	if err := h.HandleOrderEvent(context.Background(), nil, badData); err == nil {
		t.Error("expected error for invalid JSON but got none")
	}
	if h.orderNotificationsCount != 0 {
		t.Errorf("expected orderNotifications == 0 after invalid JSON, got %d", h.orderNotificationsCount)
	}
}
