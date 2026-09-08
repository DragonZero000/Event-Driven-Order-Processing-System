package order

import (
	"time"
)

type Order struct {
	ID         string
	CustomerID string
	Items      []OrderItem
	Status     OrderStatus
	TotalPrice float64
	CreatedAt  time.Time
}

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int64   `json:"quantity"`
	Price     float32 `json:"price"`
}

type OrderStatus int8

const (
	Pending = iota
	Processing
	Completed
	Cancelled
)
