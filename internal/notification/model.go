package notification

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type OrderEvent struct {
	ID         string      `json:"id"`
	CustomerID string      `json:"customer_id"`
	OrderItems []OrderItem `json:"items"`
	CreatedAt  int         `json:"created_at"`
	TotalPrice float64     `json:"total_price"`
}

type PaymentEvent struct {
	ID     string        `json:"order_id"`
	Status paymentStatus `json:"status"`
	Amount float64       `json:"amount"`
}

type paymentStatus int

const (
	PaymentStatusFailed paymentStatus = iota
	PaymentStatusProcessed
)

type notificationState struct {
	paymentStatus bool
	orderStatus   bool
}
