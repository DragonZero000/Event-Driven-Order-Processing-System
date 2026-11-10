package inventory

import "context"

type OrderCreateEvent struct {
	ID         string      `json:"id"`
	CustomerID string      `json:"customer_id"`
	TotalPrice float32     `json:"total_price"`
	OrderItems []OrderItem `json:"items"`
	Status     int         `json:"status"`
	CreatedAt  int64       `json:"created_at"`
}

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float32 `json:"price"`
}

// Топики Kafka, в которые склад публикует результат резервирования (шаг саги).
const (
	TopicStockReserved = "stock-reserved"
	TopicStockRejected = "stock-rejected"
)

// StockEvent — результат резервирования товаров.
// Поля id/customer_id/total_price совпадают с событием заказа, поэтому
// payment-service читает stock-reserved тем же форматом, что и orders.
type StockEvent struct {
	ID         string  `json:"id"`
	CustomerID string  `json:"customer_id"`
	TotalPrice float32 `json:"total_price"`
	Reason     string  `json:"reason,omitempty"`
}

// PaymentResult — срез события payment, достаточный для компенсации.
type PaymentResult struct {
	OrderID string `json:"order_id"`
	Status  int    `json:"status"` // 0 — отказ, 1 — успех
}

type eventPublisher interface {
	Publish(ctx context.Context, key string, value []byte) error
}
