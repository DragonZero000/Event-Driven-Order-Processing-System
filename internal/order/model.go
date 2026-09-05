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
	ProductID string
	Quantity  int64
	Price     float32
}

type OrderStatus int8

const (
	Pending = iota
	Processing
	Completed
	Cancelled
)
