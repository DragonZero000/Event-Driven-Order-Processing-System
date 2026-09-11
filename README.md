# Event-Driven Order Processing System

A microservices-based order processing pipeline written in Go, demonstrating event-driven architecture with gRPC APIs and Apache Kafka for asynchronous communication between services.

## Architecture

Four independent services communicate through Kafka topics and, where a synchronous
response is actually needed, gRPC:

```
                 gRPC                     Kafka: "orders"
   Client  ───────────────▶  order-service ───────────────▶ inventory-service ◀── gRPC: GetStockLevel
                                    │                          (reserves stock,        (sync query,
                                    │ Kafka: "orders"            also answers           e.g. from
                                    ▼                            stock queries)          another client)
                            payment-service ──── Kafka: "payment" ────▶
                                                                        │
                                                                        ▼
                                                            notification-service
                                                       (consumes "orders" + "payment")
```

- **order-service** exposes a gRPC API (`CreateOrder`, `GetOrder`), stores orders in memory, and publishes an `OrderCreated` event to the `orders` Kafka topic.
- **inventory-service** consumes the `orders` topic and reserves stock for each order item; if stock is insufficient the reservation is skipped and logged, though this outcome is not currently propagated to other services (see Known Limitations). It also exposes a synchronous gRPC `GetStockLevel` API, demonstrating a hybrid design: Kafka for asynchronous reactions, gRPC for callers that need an immediate answer.
- **payment-service** consumes the `orders` topic, simulates charging the customer, and publishes the outcome to the `payment` Kafka topic.
- **notification-service** consumes both the `orders` and `payment` topics and logs notifications for order creation and payment outcomes.

Each service deduplicates events by order ID, so replayed/retried Kafka messages are processed at most once. Messages are only committed after successful handling, so a failed handler causes Kafka to redeliver the message.

## Tech stack

- Go 1.26.4
- gRPC / Protocol Buffers
- Apache Kafka (via [segmentio/kafka-go](https://github.com/segmentio/kafka-go)), Zookeeper
- [zap](https://github.com/uber-go/zap) for structured logging
- Docker / Docker Compose

## Project layout

```
cmd/                    Service entrypoints (main.go per service)
  order-service/
  inventory-service/
  payment-service/
  notification-service/
internal/               Business logic per service
  order/                gRPC server, in-memory order store, OrderCreated event
  inventory/            Kafka handler + gRPC stock-level API
  payment/              Kafka handler + simulated payment gateway
  notification/         Kafka handlers for order/payment events
pkg/kafka/               Shared Kafka producer/consumer wrappers
proto/                  Protobuf definitions and generated gRPC code
docker/                 Per-service Dockerfiles
docker-compose.yml      Zookeeper, Kafka, and all four services
```

## Running locally

### Prerequisites

- Go 1.26+
- Docker and Docker Compose

### With Docker Compose (recommended)

Builds and starts Zookeeper, Kafka, and all four services:

```bash
docker compose up --build
```

Service gRPC ports (mapped to the host):

| Service              | Host port |
|----------------------|-----------|
| order-service        | 50051     |
| inventory-service    | 50052     |
| payment-service      | 50053     |
| notification-service | 50054     |

Kafka is reachable at `localhost:9092` from the host and `kafka:29092` from within the Docker network.

### Running a service directly

Each service reads its Kafka broker address from the `KAFKA_BROKERS` environment variable (defaults to `localhost:9092`):

```bash
go run ./cmd/order-service
```

## Event flow

1. A client calls `CreateOrder` on **order-service** via gRPC.
2. order-service stores the order, publishes an `OrderCreated` event (JSON) to the `orders` topic, keyed by order ID.
3. **inventory-service** consumes `orders`, checks and decrements stock per item. At any time, a client can also call `GetStockLevel` directly on inventory-service via gRPC to read current stock synchronously, independent of the event flow.
4. **payment-service** consumes `orders`, simulates a charge (orders over $200 are declined), and publishes a `PaymentEvent` to the `payment` topic.
5. **notification-service** consumes both `orders` and `payment` and logs a notification for each event.

## Known Limitations

These are deliberate simplifications for a learning project, not oversights:

- **No persistence**: all state (orders, stock, processed-event dedup maps) lives in memory and is lost on restart. A production system would use a real database.
- **Dual-write problem**: order-service writes to its in-memory store and publishes to Kafka as two separate steps. If the publish fails, the order still exists but no event is emitted. A transactional outbox would solve this properly.
- **No compensation/saga rollback**: if inventory reservation fails after payment succeeds (or vice versa), nothing currently reverses the other side effect.
- **Simulated payment gateway**: no real payment provider integration; charges are approved/declined based on a simple amount threshold.

## Testing

Run the test suite for all services, with the race detector enabled:

```bash
go test ./... -race
```
