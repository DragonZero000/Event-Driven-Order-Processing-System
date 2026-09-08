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

	pb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto"
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

func (s *Server) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	order := &Order{
		ID:         uuid.New().String(),
		CustomerID: req.CustomerId,
		Items:      toDomainItems(req.Items),
		Status:     Pending,
		CreatedAt:  time.Now(),
	}
	order.TotalPrice = TotalOrderPrice(order)
	s.orders[order.ID] = order
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
		slog.Error("failed to marshal event", "order_id", order.ID, "error", err)
	} else if err = s.eventPublish.Publish(ctx, order.ID, jsonData); err != nil {
		slog.Error("failed to publish order creatd event", "order_id", order.ID, "error", err)
	}
	return &pb.CreateOrderResponse{
		OrderId: order.ID,
	}, nil
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
