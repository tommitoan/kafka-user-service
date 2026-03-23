# user-kafka-go

A Go REST API service for User CRUD with dual-format (Avro + Protobuf) Kafka event publishing,
Schema Registry validation, and a dual-format consumer — all wired with Viper config.

## Stack

| Layer        | Library                          |
|--------------|----------------------------------|
| HTTP         | `gin-gonic/gin`                  |
| ORM          | `gorm.io/gorm` + postgres driver |
| Migrations   | `golang-migrate/migrate`         |
| Kafka        | `segmentio/kafka-go`             |
| Schema Reg.  | `riferrei/srclient`              |
| Avro         | `hamba/avro`                     |
| Protobuf     | `google.golang.org/protobuf`     |
| Config       | `spf13/viper`                    |
| Testing      | `stretchr/testify`               |

## Project Structure

```
.
├── cmd/server/main.go               # Entry point
├── config.yaml                      # Default config (Viper)
├── docker-compose.yml               # Kafka + Schema Registry + Postgres
├── migrations/                      # go-migrate SQL files
├── proto/                           # Protobuf definition + generated Go
├── schemas/avro/                    # Avro schema (.avsc)
└── internal/
    ├── api/
    │   ├── handler.go               # Gin HTTP handlers
    │   └── handler_test.go          # Unit tests (black-box, httptest)
    ├── config/
    │   ├── config.go                # Viper config loader
    │   └── config_test.go           # Unit tests
    ├── db/
    │   ├── postgres.go              # GORM connection
    │   └── migrate.go               # go-migrate runner
    ├── kafka/
    │   ├── producer.go              # Avro + Protobuf producer
    │   └── consumer.go              # Avro + Protobuf consumer
    ├── mocks/
    │   ├── user_repository_mock.go  # testify/mock for repo
    │   └── producer_mock.go         # testify/mock for producer
    ├── models/user.go               # GORM model + event types
    ├── repository/user_repository.go
    └── service/
        ├── user_service.go          # Business logic
        └── user_service_test.go     # Unit tests (white-box)
└── test/integration/
    └── user_integration_test.go     # Integration tests (build tag: integration)
```

## Configuration

Config is loaded by Viper in this priority order (highest wins):

1. **Environment variables** — prefixed `APP_`, dots replaced by `_`
   e.g. `APP_DATABASE_HOST=mydb` overrides `database.host`
2. **`config.yaml`** in the working directory
3. **Defaults** baked into the loader

### config.yaml

```yaml
server:
  host: "0.0.0.0"
  port: 8080

database:
  host: "localhost"
  port: 5432
  user: "postgres"
  password: "postgres"
  name: "userdb"
  sslmode: "disable"

kafka:
  brokers:
    - "localhost:9092"
  group_id: "user-service"
  schema_registry: "http://localhost:8081"
```

## Running

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Run the server (reads config.yaml from cwd)
go run ./cmd/server

# Override via env:
APP_DATABASE_PASSWORD=secret go run ./cmd/server
```

## API Endpoints

| Method | Path               | Description      |
|--------|--------------------|------------------|
| POST   | /api/v1/users      | Create user      |
| GET    | /api/v1/users      | List users       |
| GET    | /api/v1/users/:id  | Get user by ID   |
| PUT    | /api/v1/users/:id  | Update user      |
| DELETE | /api/v1/users/:id  | Delete user      |
| GET    | /health            | Health check     |

## Kafka Topics & Schemas

| Topic                | Format   | Schema Subject                   |
|----------------------|----------|----------------------------------|
| `user-events-avro`   | Avro     | `user-events-avro-value`         |
| `user-events-proto`  | Protobuf | `user-events-proto-value`        |

Both use the **Confluent wire format**: `[0x00][4-byte schema ID][payload]`

## Testing

```bash
# Unit tests (no infrastructure required)
go test ./internal/...

# Integration tests (requires docker compose stack)
go test -tags=integration ./test/integration/...
```

### Test placement rationale

| Package                    | Type        | Reason                                              |
|----------------------------|-------------|-----------------------------------------------------|
| `internal/service`         | Unit        | White-box: tests business logic with mocked deps    |
| `internal/api`             | Unit        | Black-box via `httptest`: tests HTTP contract only  |
| `internal/config`          | Unit        | Validates Viper loading, env overrides, DSN builder |
| `internal/mocks`           | Helpers     | Shared testify mocks — not a test package itself    |
| `test/integration`         | Integration | Requires full stack; gated behind `integration` tag |
