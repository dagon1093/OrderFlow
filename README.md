# OrderFlow

Pet project for learning Kafka, Outbox pattern, consumers, retries, DLQ and performance testing with .NET.

## Learning goal

The goal of this project is not just to write code, but to understand how event-driven systems work step by step:

- ASP.NET Core Web API as a producer
- Kafka topics, keys, partitions and consumer groups
- PostgreSQL as a business database
- Outbox pattern for reliable event publishing
- Worker services as consumers
- Retry and DLQ patterns
- Idempotent message processing
- Metrics, logs and performance testing

## Planned services

```text
OrderFlow.OrderService       - creates orders and writes events
OrderFlow.PaymentService     - consumes order events and simulates payments
OrderFlow.NotificationService - reacts to payment results
OrderFlow.AnalyticsService   - collects simple event statistics
OrderFlow.Contracts          - shared event contracts
```

## First milestone

The first milestone is intentionally small:

1. Create .NET solution structure
2. Run Kafka locally with Docker Compose
3. Send the first test event from OrderService
4. Read the event in PaymentService

After that the project will gradually grow into a small performance lab for Kafka and Outbox.

## Local infrastructure

The project uses Kafka as a local event broker.

### Start infrastructure

```bash
docker compose up -d
```
### Stop infrastructure
```bash
docker compose down
```
### Kafka UI

Kafka UI is available at:
```text
http://localhost:8080
```
### Kafka broker

For applications running on the host machine, Kafka is available at:
```text
localhost:9092
```
Inside Docker Compose network, services can use:

```text
kafka:29092
```

