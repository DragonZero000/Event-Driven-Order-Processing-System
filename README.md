# Распределённая система обработки заказов (Event-Driven Architecture, Kafka, gRPC)

Прототип (MVP) к курсовой работе «Архитектура клиент-серверных приложений».
Тема: *Разработка и обоснование архитектуры распределённой системы обработки заказов на основе событийно-ориентированного подхода с использованием брокера сообщений Kafka и межсервисного взаимодействия по протоколу gRPC.*

## 1. Предметная область и требования

Интернет-магазин принимает заказы; для каждого нужно зарезервировать товар, провести оплату и уведомить клиента. Шаги выполняются разными сервисами, которые должны развиваться и масштабироваться независимо.

**Функциональные требования**
- FR1. Создание заказа и получение его статуса (gRPC, `OrderService`).
- FR2. Резервирование товаров на складе по принципу «всё или ничего».
- FR3. Оплата заказа после успешного резервирования.
- FR4. Отмена заказа и снятие резерва при отказе в оплате или нехватке товара (компенсация).
- FR5. Уведомление клиента о создании заказа и результате оплаты.
- FR6. Запрос остатка товара (gRPC, `InventoryService`).

**Нефункциональные требования**
- NFR1. Слабая связанность: сбой сервиса не блокирует приём заказов.
- NFR2. Горизонтальное масштабирование потребителей (consumer groups, партиции).
- NFR3. Идемпотентность обработчиков (доставка at-least-once).
- NFR4. Низкая задержка синхронных вызовов клиента (бинарный gRPC/Protobuf).
- NFR5. Воспроизводимое развёртывание (Docker Compose).

**Ограничения.** Разработчик один, стек — Go; ресурсы — одна машина; нагрузка в прототипе — десятки заказов/с; хранилища in-memory (см. «Ограничения прототипа»).

**Этапы и сроки (ориентир).** Анализ и требования — 1 нед.; выбор архитектуры и диаграммы — 1 нед.; реализация сервисов и саги — 3 нед.; тестирование и развёртывание — 1 нед.; оформление отчёта — 2 нед.

## 2. Выбор архитектурного стиля

Оценка по 5-балльной шкале (5 — лучше). Добавлен проектный критерий «отказоустойчивость».

| Стиль | Масштабируемость | Производительность | Простота разработки | Совместимость со стеком (Go, Kafka, gRPC) | Отказоустойчивость | Итого |
|---|---|---|---|---|---|---|
| Монолит | 2 | 4 | 5 | 3 | 1 | 15 |
| Многоуровневая (N-tier) | 2 | 3 | 4 | 3 | 2 | 14 |
| Микросервисы + синхронный REST/gRPC | 4 | 3 | 3 | 4 | 2 | 16 |
| SOA (ESB) | 3 | 2 | 2 | 2 | 3 | 12 |
| **Микросервисы + Event-Driven (Kafka) + gRPC на границе** | **5** | **4** | **3** | **5** | **5** | **22** |

**Обоснование.** Событийная модель развязывает сервисы по времени: заказ принимается, даже если склад или оплата временно недоступны, а события сохраняются в журнале Kafka и обрабатываются позже. Потребители масштабируются через consumer groups. gRPC выбран для синхронного клиентского API (строгий контракт, Protobuf, малая задержка), Kafka — для асинхронного обмена между сервисами.

**Компромиссы.** Выигрыш в отказоустойчивости и масштабируемости оплачивается eventual consistency (статус заказа сходится не мгновенно), сложностью отладки и необходимостью идемпотентности и компенсаций.

## 3. Архитектура

### 3.1 Компоненты и взаимодействие

```mermaid
flowchart LR
    C[Клиент] -- "gRPC CreateOrder / GetOrder" --> O[order-service]
    C -- "gRPC GetStockLevel" --> I[inventory-service]
    O -- "orders" --> K[(Kafka)]
    K -- "orders" --> I
    K -- "orders" --> N[notification-service]
    I -- "stock-reserved" --> K
    I -- "stock-rejected" --> K
    K -- "stock-reserved" --> P[payment-service]
    P -- "payment" --> K
    K -- "stock-rejected, payment" --> O
    K -- "payment (компенсация)" --> I
    K -- "payment" --> N
```

### 3.2 Сага (хореография): основной сценарий и компенсации

```mermaid
sequenceDiagram
    participant C as Клиент
    participant O as order-service
    participant K as Kafka
    participant I as inventory
    participant P as payment
    C->>O: CreateOrder (gRPC)
    O-->>C: order_id (PENDING)
    O->>K: orders (OrderCreated)
    K->>I: OrderCreated
    alt товара достаточно
        I->>K: stock-reserved
        K->>P: stock-reserved
        P->>K: payment (status=1)
        K->>O: payment → COMPLETED
    else нехватка товара
        I->>K: stock-rejected
        K->>O: stock-rejected → CANCELLED
    end
    Note over P,I: Отказ оплаты (status=0): payment → order (CANCELLED) и payment → inventory (снятие резерва)
```

Оплата запускается только после резервирования, поэтому возвратов средств не требуется: компенсация нужна лишь для склада.

### 3.3 Контракты

**gRPC** (`proto/order.proto`, `proto/inventory.proto`):

| Сервис | Метод | Запрос → ответ |
|---|---|---|
| OrderService | CreateOrder | `customer_id, items[]` → `order_id` |
| OrderService | GetOrder | `order_id` → `Order{status: PENDING/PROCESSING/COMPLETED/CANCELLED}` |
| InventoryService | GetStockLevel | `product_id` → `stock_quantity` |

**События Kafka (JSON, ключ = `order_id`, что гарантирует порядок событий одного заказа в партиции):**

| Топик | Производитель | Потребители (consumer group) | Поля |
|---|---|---|---|
| `orders` | order | inventory, notification | `id, customer_id, items[], total_price, status, created_at` |
| `stock-reserved` | inventory | payment, order (→ PROCESSING) | `id, customer_id, total_price` |
| `stock-rejected` | inventory | order | `id, customer_id, total_price, reason` |
| `payment` | payment | order, inventory, notification | `order_id, status (0/1), amount` |

### 3.4 Модель данных (in-memory в прототипе)

```mermaid
erDiagram
    ORDER ||--|{ ORDER_ITEM : contains
    ORDER { string id PK  string customer_id  int status  float total_price  int created_at }
    ORDER_ITEM { string product_id  int quantity  float price }
    STOCK { string product_id PK  int quantity }
    RESERVATION { string order_id PK  json items }
    ORDER_ITEM }o--|| STOCK : "reserves"
    ORDER ||--o| RESERVATION : "has"
```

### 3.5 Физическая инфраструктура

```mermaid
flowchart TB
    subgraph Docker Compose / сеть kafka-network
        ZK[zookeeper :2181] --- KF[kafka :9092/29092]
        OS[order-service :50051]
        IS[inventory-service :50052]
        PS[payment-service]
        NS[notification-service]
    end
    OS & IS & PS & NS -- KAFKA_BROKERS=kafka:29092 --> KF
```

CI/CD в проекте не настроен; рекомендуемый минимум — GitHub Actions: `go vet`, `go test ./...`, сборка образов из `docker/`.

## 4. Применённые паттерны

| Паттерн | Где применён |
|---|---|
| Event-Driven / Pub-Sub | обмен между сервисами через топики Kafka |
| Saga (choreography) + компенсирующая транзакция | `stock-reserved → payment`, снятие резерва в `inventory.HandlePayment` |
| Idempotent Consumer | `processed`/`paymentEvents`/статусы финальны в `order.setStatus` |
| Database per service (логически) | у каждого сервиса собственное in-memory состояние; физически БД нет — см. ограничения |
| Retry + poison message | `pkg/kafka/consumer.go` (3 попытки, затем Dead Letter Topic `<topic>.dlq`) |
| Ports & Adapters | интерфейсы `eventPublisher`, `PaymentGateway` позволяют подменять инфраструктуру в тестах |
| CQRS (применимо) | запись — события, чтение — `GetOrder`/`GetStockLevel`; полноценное разделение — направление развития |
| DDD | границы сервисов совпадают с ограниченными контекстами: Orders, Inventory, Payment, Notification |

MVC/MVVM неприменимы: у системы нет пользовательского интерфейса.

## 5. Обоснование технологий

| Технология | Зачем |
|---|---|
| Go | лёгкие горутины, быстрые статические бинарники, зрелая экосистема gRPC/Kafka |
| Kafka (`segmentio/kafka-go`) | долговечный журнал, партиционирование по ключу, consumer groups, повторное чтение |
| gRPC + Protobuf | строгий контракт, кодогенерация, бинарный формат |
| Docker / Compose | воспроизводимое окружение; для продакшена — Kubernetes |
| zap / slog | структурированное логирование |

## 6. Запуск и проверка

```bash
docker compose up --build
go test ./...
```

Создание заказа (нужен `grpcurl`, рефлексия не включена — используйте proto-файл):

```bash
grpcurl -plaintext -import-path proto -proto order.proto \
  -d '{"customer_id":"c1","items":[{"product_id":"soup","quantity":2,"price":50}]}' \
  localhost:50051 order.OrderService/CreateOrder
grpcurl -plaintext -import-path proto -proto order.proto \
  -d '{"order_id":"<id>"}' localhost:50051 order.OrderService/GetOrder
```

Сценарии: сумма ≤ 200 — заказ `COMPLETED`; сумма > 200 — оплата отклонена, заказ `CANCELLED`, резерв снят; товара нет на складе (`soup`: 10, `bread`: 25) — `CANCELLED`.

Порты хоста: `order-service` — 50051, `inventory-service` — 50052 (gRPC); Kafka — `localhost:9092` с хоста и `kafka:29092` внутри сети Docker. `payment-service` и `notification-service` — только потребители/производители Kafka, gRPC-портов у них нет. Отдельный сервис можно запустить локально: `KAFKA_BROKERS=localhost:9092 go run ./cmd/order-service`.

Тестирование: `go test ./... -race`.

## Структура проекта

```
cmd/<service>/main.go   точки входа сервисов
internal/order          gRPC-сервер, хранилище заказов, обработчики событий саги
internal/inventory      резервирование, компенсация, gRPC GetStockLevel
internal/payment        обработчик stock-reserved, эмулятор платёжного шлюза
internal/notification   обработчики событий orders/payment
pkg/kafka               общие producer/consumer (retry, Dead Letter Topic)
proto/                  .proto и сгенерированный код (proto/order, proto/inventory)
docker/                 Dockerfile каждого сервиса
```

## Допущения и ограничения прототипа

Их следует прямо указать в отчёте как осознанные допущения MVP.

- **Состояние в памяти.** Принцип «database per service» соблюдён лишь логически: у каждого сервиса своё хранилище, но оно in-memory и теряется при перезапуске. Следующий шаг — PostgreSQL на сервис и **Transactional Outbox**: сейчас при сбое публикации заказ откатывается, а клиент получает `Unavailable`, но с реальной БД запись и публикация должны быть атомарны.
- **Валидация.** `CreateOrder` отклоняет пустой заказ, неположительные количество/цену и пустые идентификаторы (`InvalidArgument`); склад дополнительно защищён от некорректного количества.
- **Сообщения, не прошедшие обработку.** После 3 попыток сообщение отправляется в Dead Letter Topic `<topic>.dlq`. Автоматической обработки DLT и таймаутов саги нет — застрявший заказ остаётся в `PENDING`/`PROCESSING`.
- **Деньги в `float32`.** Допущение MVP; в production — целые минимальные единицы (копейки) или decimal.
- **Масштабирование.** Топики создаются с 3 партициями, но брокер один (replication factor 1); экземпляры сервисов хранят состояние в памяти, поэтому для горизонтального масштабирования нужна внешняя БД.
- **События — JSON**, без Schema Registry и версионирования.
- **Нет e2e-теста** с реальным брокером (тесты модульные, Kafka подменена); не реализованы healthcheck сервисов, метрики (Prometheus), трассировка (OpenTelemetry), TLS/JWT для gRPC.
- `notification-service` только логирует уведомления.
