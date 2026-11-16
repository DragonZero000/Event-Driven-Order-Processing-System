package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	invpb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/inventory"
	orderpb "github.com/DragonZero000/Event-Driven-Order-Processing-System/proto/order"
)

type fakeOrders struct{}

func (fakeOrders) CreateOrder(_ context.Context, r *orderpb.CreateOrderRequest) (*orderpb.CreateOrderResponse, error) {
	if len(r.Items) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no items")
	}
	return &orderpb.CreateOrderResponse{OrderId: "o1"}, nil
}

func (fakeOrders) GetOrder(_ context.Context, r *orderpb.GetOrderRequest) (*orderpb.GetOrderResponse, error) {
	if r.OrderId != "o1" {
		return nil, status.Error(codes.NotFound, "order not found")
	}
	return &orderpb.GetOrderResponse{Order: &orderpb.Order{Id: "o1", Status: orderpb.OrderStatus_Order_Status_COMPLETED}}, nil
}

type fakeInv struct{}

func (fakeInv) GetStockLevel(context.Context, *invpb.GetStockLevelRequest) (*invpb.GetStockLevelResponse, error) {
	return &invpb.GetStockLevelResponse{StockQuantity: 7}, nil
}

func do(method, path, body string) *httptest.ResponseRecorder {
	g := &Gateway{orders: fakeOrders{}, inventory: fakeInv{}}
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func TestCreateOrder(t *testing.T) {
	rec := do("POST", "/api/orders", `{"customer_id":"c","items":[{"product_id":"soup","quantity":1,"price":5}]}`)
	if rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), `"o1"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestCreateOrder_ErrorsMapped(t *testing.T) {
	if rec := do("POST", "/api/orders", `{"customer_id":"c"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("grpc InvalidArgument: code = %d", rec.Code)
	}
	if rec := do("POST", "/api/orders", `not json`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json: code = %d", rec.Code)
	}
}

func TestGetOrder(t *testing.T) {
	rec := do("GET", "/api/orders/o1", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"COMPLETED"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if rec := do("GET", "/api/orders/zzz", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestGetStock(t *testing.T) {
	rec := do("GET", "/api/stock/soup", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"stock_quantity":7`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}
