# Task prompts for the orchestrator

These prompts are written to keep the work focused.

## Prompt 1: manual commit

Read the repo and the spec files in `docs/orchestrator/`.
Focus only on implementing manual commit consumer flow.

Requirements:
- Replace `ReadMessage()` with explicit `FetchMessage()` plus `CommitMessages()` in the consumer.
- Commit only after successful handler execution.
- Add clear structured logs for topic, partition, offset, key, and consumer group.
- Keep scope narrow. Do not implement idempotency, DLQ, outbox, or transaction demo in this change.
- Produce a file-by-file plan first, then the code changes.
- Add a short manual runbook showing how to verify crash-before-commit behavior.

## Prompt 2: idempotency

Read the repo and the spec files in `docs/orchestrator/`.
Focus only on implementing idempotency.

Requirements:
- Add stable `event_id` to the event model and serialization paths.
- Add a `processed_events` table via migration.
- Add repository and service logic so duplicate delivery of the same event does not repeat the business side effect.
- Use a DB transaction around dedupe marker plus business side effect.
- Keep scope narrow. Do not implement DLQ or outbox in this change.
- Produce a file-by-file plan first, then the code changes.
- Add a short manual runbook showing duplicate-safe behavior after restart.

## Prompt 3: DLQ

Read the repo and the spec files in `docs/orchestrator/`.
Focus only on implementing dead letter queue handling.

Requirements:
- Add DLQ topic configuration.
- Add code to publish malformed or unrecoverable messages to DLQ.
- Include metadata: original topic, partition, offset, key, error message, and timestamp.
- Commit original offset only after successful DLQ publish.
- Keep scope narrow. Do not implement retry framework or outbox in this change.
- Produce a file-by-file plan first, then the code changes.
- Add a short manual runbook showing how to trigger and inspect DLQ behavior.

## Prompt 4: outbox for the main service

Read the repo and the spec files in `docs/orchestrator/`.
Focus only on implementing outbox for the HTTP -> DB -> Kafka flow.

Requirements:
- Add an `outbox_events` table via migration.
- When user create/update/delete succeeds, write the outbox row in the same DB transaction as the user mutation.
- Add a background publisher that publishes pending outbox rows to Kafka and marks them as published.
- Preserve current API behavior as much as possible.
- Keep scope narrow. Do not implement Kafka transaction demo in this change.
- Produce a file-by-file plan first, then the code changes.
- Add a short manual runbook showing recovery of pending outbox rows after restart.

## Prompt 5: separate Kafka transaction demo

Read the repo and the spec files in `docs/orchestrator/`.
Focus only on a standalone Kafka transaction demo.

Requirements:
- Keep this separate from the main HTTP -> DB -> Kafka service path.
- Build a Kafka-only demonstration of consume -> produce -> commit offsets transactionally.
- Add clear documentation of what guarantees are demonstrated and what are not.
- Do not modify the main user service flow unless absolutely necessary.
- Produce a file-by-file plan first, then the code changes.
