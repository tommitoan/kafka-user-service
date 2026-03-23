# AGENTS.md

## Mission
Work like a careful backend engineer:
plan -> implement -> validate -> review -> summarize.

## Hard Guards
- Keep scope narrow to the requested feature.
- Do not refactor the whole project structure unless required.
- Do not change API contracts unless necessary for the feature.
- Do not claim full exactly-once semantics unless scope is explicitly defined.
- For risky edits, show a file-by-file plan first.

## Project Priorities
The implementation order for this repo is:
1. manual commit consumer flow
2. idempotency
3. DLQ
4. outbox for HTTP -> DB -> Kafka
5. separate Kafka transaction demo
6. real Kafka end-to-end integration tests

## Repo Facts
- `internal/kafka/consumer.go` currently uses `ReadMessage()`.
- `internal/service/user_service.go` currently does DB write then fire-and-forget publish.
- `test/integration/user_integration_test.go` currently mocks Kafka producer.
- Preserve dual topic flow unless there is a strong reason to simplify.
- Keep external minimal compose vs repo-local compose distinction explicit.

## Default Workflow
1. Restate goal and non-goals.
2. Read relevant docs in `docs/orchestrator/` and `docs/ai/`.
3. Produce a file-by-file plan.
4. Make the smallest coherent change.
5. Run relevant tests / checks.
6. Review diff for correctness and scope control.
7. Summarize what changed, what remains, and manual verification steps.

## Validation Commands
- `go test ./...`
- `go test -tags=integration ./test/integration/...`
- `docker compose up -d`
- `docker compose down -v`

## Instruction Files
Read when relevant:
- `docs/orchestrator/08-orchestrator-handoff.md`
- `docs/orchestrator/03-implementation-order.md`
- `docs/orchestrator/04-manual-commit-spec.md`
- `docs/orchestrator/05-idempotency-spec.md`
- `docs/orchestrator/06-dlq-spec.md`
- `docs/orchestrator/07-outbox-vs-transaction-demo.md`