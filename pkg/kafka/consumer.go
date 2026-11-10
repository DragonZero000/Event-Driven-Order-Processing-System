package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

type Handler func(ctx context.Context, key, value []byte) error

const (
	maxAttempts  = 3
	retryBackoff = 200 * time.Millisecond
)

func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := handleWithRetry(ctx, handler, msg.Key, msg.Value); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// "Ядовитое" сообщение: коммитим смещение, чтобы не блокировать партицию.
			// В production его следует отправлять в Dead Letter Topic.
			log.Printf("kafka: dropping message key=%s after %d attempts: %v", msg.Key, maxAttempts, err)
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// handleWithRetry вызывает обработчик до maxAttempts раз с линейной задержкой.
// Обработчики идемпотентны, поэтому повтор безопасен (семантика at-least-once).
func handleWithRetry(ctx context.Context, h Handler, key, value []byte) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = h(ctx, key, value); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * retryBackoff):
		}
	}
	return err
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
