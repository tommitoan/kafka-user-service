# Outbox in the main service vs a separate Kafka transaction demo

## The architectural problem in the current repo

Current user mutation flow is:
1. write to PostgreSQL
2. publish Kafka event afterward

This means DB and Kafka are not atomic.

## What should solve the main service problem

For the main service, prefer **outbox pattern**.

### Why outbox fits this repo

Because the main entrypoint is HTTP -> DB -> Kafka.
Kafka transaction does not automatically make a PostgreSQL commit and a Kafka write become a single atomic unit.

### Target outbox flow

For create/update/delete user:
1. start DB transaction
2. write user change
3. insert outbox event row in same transaction
4. commit DB transaction
5. background publisher reads pending outbox rows
6. publish to Kafka
7. mark outbox row as published

### Suggested outbox table

`outbox_events`

Suggested columns:
- `id`
- `aggregate_type`
- `aggregate_id`
- `event_type`
- `event_id`
- `payload`
- `status`
- `published_at`
- `created_at`
- optional retry/error fields

## What a separate Kafka transaction demo should do

A transaction demo is still useful, but keep it separate from the main service.

### Good demo scope

Kafka-only pipeline such as:
1. consume from topic A
2. transform event
3. produce to topic B
4. commit consumed offsets transactionally

### Why keep it separate

This teaches Kafka transaction semantics clearly without pretending that PostgreSQL and Kafka are now atomic together.

## Recommendation

- In the main service: implement outbox.
- Separately: add a transaction demo command or folder for learning and explanation.

## Acceptance criteria for outbox

- user mutation and outbox row creation happen in one DB transaction
- if HTTP request succeeds, there is always either a persisted user change plus pending outbox row, or neither
- publisher can recover pending outbox rows after restart

## Acceptance criteria for transaction demo

- there is a clearly separate demo entrypoint or package
- docs explain what guarantees it demonstrates
- docs explain what it does not solve for PostgreSQL-backed business flows
