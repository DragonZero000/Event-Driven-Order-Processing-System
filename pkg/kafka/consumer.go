package kafka

import (
	"context"

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

func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := handler(ctx, msg.Key, msg.Value); err != nil {
			continue // не коммитим — Kafka повторно доставит это сообщение
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
