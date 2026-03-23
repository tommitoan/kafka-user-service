# Feature spec: idempotency

## Goal

Make consumer side effects safe when the same message is delivered more than once.

## Why this is needed

After manual commit is introduced, a crash after business logic but before offset commit can cause a message to be consumed again.
Without idempotency, the same business effect may happen twice.

## Target state

Each event should carry an `event_id`.
The consumer should persist a deduplication marker before or alongside business side effects so the same event is not applied twice.

## Scope

Likely affected files:
- event model definitions
- Avro schema
- Proto schema / payload mapping
- `internal/kafka/producer.go`
- `internal/kafka/consumer.go`
- service or processor layer for consumer-side business handling
- repository layer for processed events
- new migration for `processed_events`

## Data model suggestion

Table: `processed_events`

Suggested fields:
- `event_id` primary key or unique key
- `consumer_group`
- `topic`
- `partition`
- `offset`
- `processed_at`

## Processing contract

For each message:
1. parse event and obtain `event_id`
2. start DB transaction
3. attempt to record event in `processed_events`
4. if duplicate key exists, treat as already processed
5. if not duplicate, execute business side effect
6. commit DB transaction
7. commit Kafka offset

## Acceptance criteria

- Events have a stable `event_id`.
- Duplicate delivery of the same event does not repeat the business side effect.
- Consumer can restart after a crash and safely re-read the same message.
- There is a manual lab or test that proves duplicate delivery results in one business effect only.

## Non-goals

- Do not implement full global exactly-once semantics.
- Do not combine this with outbox in the same change unless necessary.
