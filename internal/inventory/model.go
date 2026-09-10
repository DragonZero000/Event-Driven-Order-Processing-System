package inventory

type OrderCreateEvent struct {
	ID         string      `json:"id"`
	CustomerID string      `json:"customer_id"`
	OrderItems []OrderItem `json:"items"`
	Status     int         `json:"status"`
	CreatedAt  int64       `json:"created_at"`
}

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float32 `json:"price"`
}
