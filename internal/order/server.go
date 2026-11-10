package order

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/order"
)

type Server struct {
	pb.UnimplementedOrderServiceServer
	orders       map[string]*Order
	mu           sync.RWMutex
	eventPublish eventPublisher
}

func NewServer(eventPublish eventPublisher) *Server {
	return &Server{
		orders:       make(map[string]*Order),
		eventPublish: eventPublish,
	}
}

func validateCreateOrder(req *pb.CreateOrderRequest) error {
	if req.CustomerId == "" {
		return status.Error(codes.InvalidArgument, "customer_id is required")
	}
	if len(req.Items) == 0 {
		return status.Error(codes.InvalidArgument, "order must contain at least one item")
	}
	for _, it := range req.Items {
		switch {
		case it == nil || it.ProductId == "":
			return status.Error(codes.InvalidArgument, "item product_id is required")
		case it.Quantity <= 0:
			return status.Errorf(codes.InvalidArgument, "item %s: quantity must be positive", it.ProductId)
		case it.Price <= 0:
			return status.Errorf(codes.InvalidArgument, "item %s: price must be positive", it.ProductId)
		}
	}
	return nil
}

func (s *Server) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	if err := validateCreateOrder(req); err != nil {
		return nil, err
	}
	order := &Order{
		ID:         uuid.New().String(),
		CustomerID: req.CustomerId,
		Items:      toDomainItems(req.Items),
		Status:     Pending,
		CreatedAt:  time.Now(),
	}
	order.TotalPrice = TotalOrderPrice(order)
	event := OrderCreatedEvent{
		ID:         order.ID,
		CustomerID: order.CustomerID,
		OrderItems: order.Items,
		TotalPrice: float32(order.TotalPrice),
		Status:     int(order.Status),
		CreatedAt:  order.CreatedAt.Unix(),
	}
	jsonData, err := json.Marshal(event)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to encode order event")
	}
	// Заказ сохраняется до публикации (ответные события не опередят запись), а сама
	// публикация идёт без глобальной блокировки. При сбое Kafka заказ откатывается,
	// клиент получает Unavailable и может повторить запрос. Атомарность с реальной БД
	// обеспечивает паттерн Transactional Outbox (см. README).
	s.mu.Lock()
	s.orders[order.ID] = order
	s.mu.Unlock()
	if err := s.eventPublish.Publish(ctx, order.ID, jsonData); err != nil {
		slog.Error("failed to publish order created event", "order_id", order.ID, "error", err)
		s.mu.Lock()
		delete(s.orders, order.ID)
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "order was not accepted, please retry")
	}
	return &pb.CreateOrderResponse{OrderId: order.ID}, nil
}

func (s *Server) GetOrder(ctx context.Context, req *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	order := s.orders[req.OrderId]
	if order == nil {
		return &pb.GetOrderResponse{}, status.Errorf(codes.NotFound, "order not found")
	}
	return &pb.GetOrderResponse{Order: toPBOrder(order)}, nil
}

func toDomainItems(items []*pb.OrderItem) []OrderItem {
	domainItems := make([]OrderItem, len(items))
	for i, item := range items {
		domainItems[i] = OrderItem{
			ProductID: item.ProductId,
			Quantity:  int64(item.Quantity),
			Price:     float32(item.Price),
		}
	}
	return domainItems
}

func toPBOrder(o *Order) *pb.Order {
	pbOrder := &pb.Order{
		Id:         o.ID,
		CustomerId: o.CustomerID,
		Status:     pb.OrderStatus(o.Status),
		TotalPrice: float32(o.TotalPrice),
		CreatedAt:  int64(o.CreatedAt.Unix()),
	}
	for _, item := range o.Items {
		pbItem := &pb.OrderItem{
			ProductId: item.ProductID,
			Quantity:  int64(item.Quantity),
			Price:     float32(item.Price),
		}
		pbOrder.Items = append(pbOrder.Items, pbItem)
	}
	return pbOrder
}

func TotalOrderPrice(o *Order) float64 {
	var total float64
	for _, item := range o.Items {
		total += (float64(item.Price) * float64(item.Quantity))
	}
	return total
}

type OrderCreatedEvent struct {
	ID         string      `json:"id"`
	CustomerID string      `json:"customer_id"`
	Status     int         `json:"status"`
	TotalPrice float32     `json:"total_price"`
	CreatedAt  int64       `json:"created_at"`
	OrderItems []OrderItem `json:"items"`
}

type eventPublisher interface {
	Publish(ctx context.Context, key string, value []byte) error
}

// HandleStockRejected — событие stock-rejected от склада: заказ отменяется.
func (s *Server) HandleStockRejected(ctx context.Context, key, value []byte) error {
	var ev struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(value, &ev); err != nil {
		return err
	}
	s.setStatus(ev.ID, Cancelled)
	return nil
}

// HandlePayment — событие payment: заказ завершается либо отменяется.
func (s *Server) HandlePayment(ctx context.Context, key, value []byte) error {
	var ev struct {
		OrderID string `json:"order_id"`
		Status  int    `json:"status"` // 1 — оплачен
	}
	if err := json.Unmarshal(value, &ev); err != nil {
		return err
	}
	if ev.Status == 1 {
		s.setStatus(ev.OrderID, Completed)
	} else {
		s.setStatus(ev.OrderID, Cancelled)
	}
	return nil
}

// setStatus идемпотентен и не меняет заказ, уже находящийся в финальном состоянии.
func (s *Server) setStatus(id string, st OrderStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.orders[id]
	if o == nil || o.Status == Completed || o.Status == Cancelled {
		return
	}
	o.Status = st
}

// HandleStockReserved — событие stock-reserved: заказ переходит в PROCESSING (ожидает оплаты).
func (s *Server) HandleStockReserved(ctx context.Context, key, value []byte) error {
	var ev struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(value, &ev); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if o := s.orders[ev.ID]; o != nil && o.Status == Pending {
		o.Status = Processing
	}
	return nil
}
