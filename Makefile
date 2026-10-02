.PHONY: help up down demo run build test test-integration cover vet fmt lint swagger proto-gen

GO ?= go
export GOTOOLCHAIN ?= auto

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "} {printf "  make %-18s %s\n", $$1, $$2}'

# ── infrastructure ───────────────────────────────────────────────────────────
up: ## Start Kafka, Schema Registry, Kafka UI and Postgres
	docker compose up -d --wait

down: ## Stop the stack and delete its volumes
	docker compose down -v

demo: ## Build and start the whole stack, service included
	docker compose --profile app up -d --build --wait

# ── app ──────────────────────────────────────────────────────────────────────
run: ## Run the service against the local stack (make up first)
	$(GO) run ./cmd/server

build: ## Compile the server binary into bin/
	$(GO) build -o bin/server ./cmd/server

# ── quality ──────────────────────────────────────────────────────────────────
test: ## Unit tests with the race detector
	$(GO) test -race ./...

test-integration: ## HTTP + idempotency tests against embedded Postgres (no Docker needed)
	$(GO) test -tags=integration -timeout 300s ./test/integration/...

cover: ## Unit test coverage summary
	$(GO) test -coverprofile=coverage.out ./internal/...
	$(GO) tool cover -func=coverage.out | tail -1

vet: ## go vet, including the integration-tagged files
	$(GO) vet ./...
	$(GO) vet -tags=integration ./...

fmt: ## Format all Go files
	gofmt -w .

lint: vet ## vet + staticcheck (if installed) + gofmt check
	@command -v staticcheck >/dev/null && staticcheck ./... || echo "staticcheck not installed, skipped"
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)

# ── code generation ──────────────────────────────────────────────────────────
swagger: ## Regenerate docs/ (OpenAPI) from handler annotations
	$(GO) run github.com/swaggo/swag/cmd/swag@v1.16.3 init -g internal/api/docs.go -o docs --parseInternal

proto-gen: ## Regenerate proto/user_event.pb.go (needs protoc + protoc-gen-go)
	@command -v protoc >/dev/null || (echo "protoc not found: brew install protobuf | apt install protobuf-compiler" && exit 1)
	@command -v protoc-gen-go >/dev/null || (echo "protoc-gen-go not found: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" && exit 1)
	protoc --proto_path=proto --go_out=proto --go_opt=paths=source_relative proto/user_event.proto
