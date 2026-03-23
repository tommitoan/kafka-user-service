# Current state of the repo

This section summarizes what the current codebase already implements.

## Repo identity

- Current Go module: `github.com/example/user-kafka-go`
- Current repo folder from zip: `user-kafka-go`
- Intended future repo name: `kafka-user-service`

## Infrastructure currently present

### External compose file uploaded separately
The uploaded external `docker-compose.yml` has exactly these services:
- `broker`
- `schema-registry`
- `kafka-ui`

Ports:
- Kafka broker: `9092`
- Schema Registry: `8081`
- Kafka UI: `8080`

This matches the mentor's preference for a minimal Kafka learning stack.

### Repo-local compose file
The repo also contains its own `docker-compose.yml` with:
- `broker`
- `schema-registry`
- `kafka-ui`
- `postgres`

Important note: there are now **two compose sources** in play. The orchestrator should not silently mix them up.

## HTTP/API layer

Already implemented:
- REST API using Gin
- CRUD for users
- health endpoint
- Swagger UI/docs

Main files:
- `cmd/server/main.go`
- `internal/api/handler.go`
- `internal/service/user_service.go`

## Database layer

Already implemented:
- PostgreSQL via GORM
- migrations directory exists
- users table migration exists
- repository pattern for users

Main files:
- `internal/db/postgres.go`
- `internal/db/migrate.go`
- `internal/repository/user_repository.go`
- `migrations/000001_create_users_table.up.sql`

## Kafka layer

Already implemented:
- topic definitions in config
- startup topic creation via `EnsureTopics()`
- producer
- consumer
- dual topics for Avro and Proto

Topic names:
- `com.br4.user.core.event.avro`
- `com.br4.user.core.event.proto`

Main files:
- `internal/kafka/admin.go`
- `internal/kafka/producer.go`
- `internal/kafka/consumer.go`
- `config.yaml`

## Schema Registry usage

Already implemented:
- connect to Schema Registry using `srclient`
- register Avro schema at producer startup
- register Protobuf schema at producer startup
- Avro consumer fetches schema by schema ID and uses it to decode payload

Important nuance:
- Avro path is closer to proper Schema Registry usage.
- Proto path registers schema, but serializes the actual payload as JSON with Confluent-style prefix.

## Current producer behavior

Current producer behavior:
- publish the same logical user event to two topics
- one Avro topic
- one Proto topic
- key is `event.UserID`

Current durability/consistency characteristics:
- no transactional producer flow
- no idempotent producer flow
- no outbox pattern
- no delivery guarantee handling beyond basic write

## Current consumer behavior

Current consumer behavior:
- creates two consumers/readers
- one for Avro topic
- one for Proto topic
- uses consumer groups:
  - `<group_id>-avro`
  - `<group_id>-proto`
- loops with `ReadMessage()`
- deserializes and calls a handler

Current limitations:
- no explicit `FetchMessage()` / `CommitMessages()` logic
- no manual offset control
- no retry policy
- no DLQ
- no idempotency
- no persistence of processed event IDs

## Current business flow

For create/update/delete user:
1. write to DB first
2. then call `publishEvent(...)`
3. `publishEvent(...)` builds a `UserEvent`
4. calls producer in fire-and-forget style

Important limitation:
- DB write and Kafka publish are **not atomic**.
- If DB succeeds and Kafka publish fails, the system can become inconsistent.

## Tests currently present

Already implemented:
- unit tests for API, config, service
- integration tests using embedded Postgres

Current limitation:
- integration tests mock Kafka producer
- they do **not** verify real Kafka publish/consume behavior
- they do **not** verify offsets, consumer group behavior, DLQ, or Schema Registry lifecycle under failure

## What the repo already does well

The current codebase is already a solid base for:
- minimal Kafka local learning setup
- Schema Registry basics
- self-produce and self-consume flow
- topic creation from config
- user CRUD API integrated with event emission

It is a good foundation for the next stage: reliability and correctness under failure.
