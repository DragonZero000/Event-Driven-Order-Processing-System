package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	dlq    *kafka.Writer // Dead Letter Topic: <topic>.dlq
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	// Dead Letter Topic создаётся заранее: kafka.Writer сам топики не создаёт,
	// и без этого первое же «ядовитое» сообщение остановило бы потребителя.
	if err := EnsureTopic(brokers[0], topic+".dlq", 1, 1); err != nil {
		log.Printf("kafka: failed to ensure DLT %s.dlq: %v", topic, err)
	}
	return &Consumer{
		dlq: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic + ".dlq",
			BatchTimeout: batchTimeout(),
			RequiredAcks: kafka.RequireAll,
		},
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
			// "Ядовитое" сообщение: отправляем в Dead Letter Topic и коммитим смещение,
			// чтобы не блокировать партицию. Разбор DLT — вручную/отдельным сервисом.
			log.Printf("kafka: message key=%s failed after %d attempts, sending to DLT: %v", msg.Key, maxAttempts, err)
			if dlqErr := c.dlq.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value}); dlqErr != nil {
				return dlqErr // без DLT сообщение терять нельзя — не коммитим
			}
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
	_ = c.dlq.Close()
	return c.reader.Close()
}
