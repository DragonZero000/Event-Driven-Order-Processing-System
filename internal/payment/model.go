package payment

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int64   `json:"quantity"`
	Price     float32 `json:"price"`
}

type OrderCreateEvent struct {
	ID         string      `json:"id"`
	CustomerID string      `json:"customer_id"`
	OrderItems []OrderItem `json:"order_items"`
	Status     int         `json:"status"`
	CreatedAt  int64       `json:"created_at"`
	TotalPrice float32     `json:"total_price"`
}

type PaymentEvent struct {
	OrderID string        `json:"order_id"`
	Status  paymentStatus `json:"status"`
	Amount  float32       `json:"amount"`
}

type paymentStatus int

const (
	PaymentStatusFailed paymentStatus = iota
	PaymentStatusProcessed
)
