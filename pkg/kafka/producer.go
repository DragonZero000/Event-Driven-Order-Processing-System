package kafka

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

// defaultBatchTimeout — сколько продюсер ждёт заполнения пакета. Значение kafka-go
// по умолчанию (1 с) добавляет секунду к каждой синхронной публикации, поэтому
// для саги используется малая задержка; переопределяется KAFKA_BATCH_TIMEOUT.
const defaultBatchTimeout = 10 * time.Millisecond

func batchTimeout() time.Duration {
	if v := os.Getenv("KAFKA_BATCH_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("kafka: invalid KAFKA_BATCH_TIMEOUT=%q, using %v", v, defaultBatchTimeout)
	}
	return defaultBatchTimeout
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
			// Hash: сообщения с одинаковым ключом (order_id) попадают в одну
			// партицию, что сохраняет порядок событий одного заказа.
			Balancer:     &kafka.Hash{},
			BatchTimeout: batchTimeout(),
			// RequireAll: публикация считается успешной только после подтверждения
			// всеми синхронными репликами (у kafka.Writer по умолчанию RequireNone —
			// запись без подтверждения, при которой ошибки брокера не возвращаются).
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func EnsureTopic(brokerAddr, topic string, partitions, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	if errors.Is(err, kafka.TopicAlreadyExists) {
		return nil
	}
	return err
}
