# Orchestrator handoff brief

Use this brief as the top-level context for the repo.

## Repo baseline

This repo already has:
- user CRUD API in Go
- PostgreSQL persistence
- Kafka producer and consumer
- Schema Registry integration
- two business topics:
  - `com.br4.user.core.event.avro`
  - `com.br4.user.core.event.proto`
- topic auto-creation on startup
- self-consume flow inside the same service

## Current limitations

The current repo does **not** yet provide the reliability features the mentor asked for.
Specifically:
- consumer still uses `ReadMessage()` instead of explicit fetch plus commit flow
- no manual offset control
- no idempotency
- no dead letter queue
- no outbox pattern
- no real Kafka transaction demo
- no failure simulation runbooks for duplicate handling, missed processing, or offset recovery
- current integration tests mock Kafka producer and therefore do not verify real broker behavior

## Implementation priorities

1. Manual commit consumer flow
2. Idempotency with processed-events table
3. DLQ
4. Outbox for the main HTTP -> DB -> Kafka flow
5. Optional separate Kafka transaction demo
6. Real end-to-end testing and failure runbooks

## Important repo facts to preserve

- Keep current CRUD API behavior stable unless a change is required for reliability.
- Preserve dual topic flow unless there is a strong reason to simplify.
- Be explicit about the difference between:
  - the external minimal compose file with 3 services
  - the repo-local compose file with Postgres included
- Do not claim exactly-once semantics for the whole system without clearly defining scope.

## Output expected from the orchestrator

The orchestrator should produce:
- a stepwise implementation plan
- a minimal file-by-file change plan
- acceptance criteria per feature
- a list of new migrations, tables, config fields, and tests/runbooks needed

The orchestrator should not start by refactoring the whole project structure.
