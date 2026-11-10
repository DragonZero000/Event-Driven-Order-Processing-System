package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestHandleWithRetry_SucceedsAfterTransientError(t *testing.T) {
	calls := 0
	h := func(ctx context.Context, key, value []byte) error {
		calls++
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	}
	if err := handleWithRetry(context.Background(), h, nil, nil); err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestHandleWithRetry_GivesUpAfterMaxAttempts(t *testing.T) {
	calls := 0
	h := func(ctx context.Context, key, value []byte) error {
		calls++
		return errors.New("poison message")
	}
	if err := handleWithRetry(context.Background(), h, nil, nil); err == nil {
		t.Fatal("expected error for poison message")
	}
	if calls != maxAttempts {
		t.Fatalf("calls = %d, want %d", calls, maxAttempts)
	}
}

func TestHandleWithRetry_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h := func(ctx context.Context, key, value []byte) error { return errors.New("fail") }
	if err := handleWithRetry(ctx, h, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
