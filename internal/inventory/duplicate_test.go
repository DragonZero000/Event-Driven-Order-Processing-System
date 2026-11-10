package inventory

import (
	"context"
	"testing"
)

func TestHandle_DuplicateItemsCheckedBySum(t *testing.T) {
	h := NewHandler(map[string]int64{"soup": 10})
	ok, bad := &recPub{}, &recPub{}
	h.SetPublishers(ok, bad)
	// 6 + 6 > 10: по отдельности каждая позиция прошла бы проверку
	_ = h.Handle(context.Background(), nil, []byte(`{"id":"o1","items":[{"product_id":"soup","quantity":6},{"product_id":"soup","quantity":6}]}`))
	if len(bad.keys) != 1 || h.stock["soup"] != 10 {
		t.Fatalf("rejected=%d stock=%d, want rejected=1 stock=10", len(bad.keys), h.stock["soup"])
	}
}
