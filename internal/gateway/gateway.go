// Package gateway — тонкий REST/JSON-шлюз перед gRPC-сервисами для браузерного клиента.
package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	invpb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/inventory"
	orderpb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/order"
)

// OrderClient и InventoryClient — узкие интерфейсы, которые удобно подменять в тестах.
type OrderClient interface {
	CreateOrder(ctx context.Context, req *orderpb.CreateOrderRequest) (*orderpb.CreateOrderResponse, error)
	GetOrder(ctx context.Context, req *orderpb.GetOrderRequest) (*orderpb.GetOrderResponse, error)
}

type InventoryClient interface {
	GetStockLevel(ctx context.Context, req *invpb.GetStockLevelRequest) (*invpb.GetStockLevelResponse, error)
}

type Gateway struct {
	orders    OrderClient
	inventory InventoryClient
}

// Адаптеры скрывают grpc.CallOption сгенерированных клиентов.
type orderAdapter struct{ c orderpb.OrderServiceClient }

func (a orderAdapter) CreateOrder(ctx context.Context, r *orderpb.CreateOrderRequest) (*orderpb.CreateOrderResponse, error) {
	return a.c.CreateOrder(ctx, r)
}

func (a orderAdapter) GetOrder(ctx context.Context, r *orderpb.GetOrderRequest) (*orderpb.GetOrderResponse, error) {
	return a.c.GetOrder(ctx, r)
}

type inventoryAdapter struct{ c invpb.InventoryServiceClient }

func (a inventoryAdapter) GetStockLevel(ctx context.Context, r *invpb.GetStockLevelRequest) (*invpb.GetStockLevelResponse, error) {
	return a.c.GetStockLevel(ctx, r)
}

func New(o orderpb.OrderServiceClient, i invpb.InventoryServiceClient) *Gateway {
	return &Gateway{orders: orderAdapter{o}, inventory: inventoryAdapter{i}}
}

type itemDTO struct {
	ProductID string  `json:"product_id"`
	Quantity  int64   `json:"quantity"`
	Price     float32 `json:"price"`
}

type createOrderDTO struct {
	CustomerID string    `json:"customer_id"`
	Items      []itemDTO `json:"items"`
}

type orderDTO struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	Items      []itemDTO `json:"items"`
	Status     string    `json:"status"`
	TotalPrice float32   `json:"total_price"`
	CreatedAt  int64     `json:"created_at"`
}

var statusNames = map[orderpb.OrderStatus]string{
	orderpb.OrderStatus_Order_Status_PENDING:    "PENDING",
	orderpb.OrderStatus_Order_Status_PROCESSING: "PROCESSING",
	orderpb.OrderStatus_Order_Status_COMPLETED:  "COMPLETED",
	orderpb.OrderStatus_Order_Status_CANCELLED:  "CANCELLED",
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/orders", g.createOrder)
	mux.HandleFunc("GET /api/orders/{id}", g.getOrder)
	mux.HandleFunc("GET /api/stock/{product}", g.getStock)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return cors(mux)
}

func (g *Gateway) createOrder(w http.ResponseWriter, r *http.Request) {
	var dto createOrderDTO
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req := &orderpb.CreateOrderRequest{CustomerId: dto.CustomerID}
	for _, it := range dto.Items {
		req.Items = append(req.Items, &orderpb.OrderItem{ProductId: it.ProductID, Quantity: it.Quantity, Price: it.Price})
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := g.orders.CreateOrder(ctx, req)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"order_id": resp.OrderId})
}

func (g *Gateway) getOrder(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := g.orders.GetOrder(ctx, &orderpb.GetOrderRequest{OrderId: r.PathValue("id")})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	o := resp.Order
	dto := orderDTO{
		ID: o.Id, CustomerID: o.CustomerId, Status: statusNames[o.Status],
		TotalPrice: o.TotalPrice, CreatedAt: o.CreatedAt, Items: []itemDTO{},
	}
	for _, it := range o.Items {
		dto.Items = append(dto.Items, itemDTO{ProductID: it.ProductId, Quantity: it.Quantity, Price: it.Price})
	}
	writeJSON(w, http.StatusOK, dto)
}

func (g *Gateway) getStock(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	product := r.PathValue("product")
	resp, err := g.inventory.GetStockLevel(ctx, &invpb.GetStockLevelRequest{ProductId: product})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"product_id": product, "stock_quantity": resp.StockQuantity})
}

func writeGRPCError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	code := http.StatusInternalServerError
	switch st.Code() {
	case codes.InvalidArgument:
		code = http.StatusBadRequest
	case codes.NotFound:
		code = http.StatusNotFound
	case codes.Unavailable, codes.DeadlineExceeded:
		code = http.StatusServiceUnavailable
	}
	writeError(w, code, st.Message())
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// cors разрешает запросы с dev-сервера Vite; в Docker фронтенд ходит через nginx на тот же origin.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
