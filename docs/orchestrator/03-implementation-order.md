# Implementation order and why

This section defines the recommended implementation sequence.

## Guiding principle

Do not implement all missing features in one pass.
Implement them in the order that exposes correctness clearly.

## Recommended order

## Step 1: Manual commit consumer flow

Why first:
- it is the clearest way to make offset behavior visible
- it lays the groundwork for controlled failure handling
- it is necessary before idempotency and DLQ become meaningful

Deliverables:
- replace `ReadMessage()` with `FetchMessage()`
- commit offsets only after handler success
- add structured logs with topic / partition / offset / key / group
- document crash windows

## Step 2: Add event ID and idempotency table

Why second:
- once manual commit exists, duplicates will appear naturally in crash scenarios
- idempotency is how the consumer remains safe when a message is re-read

Deliverables:
- event ID added to event schema / payload
- `processed_events` table
- transactional dedupe check and business side effect handling
- skip duplicate events safely

## Step 3: Add DLQ

Why third:
- once manual commit exists, poison messages need somewhere safe to go
- this prevents a bad record from blocking the main flow

Deliverables:
- DLQ topic
- DLQ publish path
- policy for deserialization failure and validation failure
- offset committed after DLQ publish succeeds

## Step 4: Solve DB plus Kafka consistency

Preferred path for this repo:
- implement **outbox pattern** in the main service

Why:
- current flow is HTTP -> DB -> Kafka
- Kafka transaction alone does not make PostgreSQL and Kafka one atomic transaction
- outbox is the right fit for this architecture

Deliverables:
- `outbox_events` table
- create/update/delete user and outbox row in one DB transaction
- background publisher marks outbox rows as published

## Step 5: Optional Kafka transaction demo

Why separate:
- it is useful for learning and explaining Kafka internals
- but it should not be confused with solving HTTP plus PostgreSQL plus Kafka atomicity

Deliverables:
- standalone transaction demo command or package
- consume -> produce -> commit offsets transactionally in Kafka-only flow
- documentation of what it does and does not solve

## Step 6: Real Kafka end-to-end testing and failure runbooks

Deliverables:
- real Kafka integration test or scripted lab flow
- duplicate replay demo
- crash-before-commit demo
- bad payload to DLQ demo
- outbox recovery demo

## Non-goals during this phase

Avoid broad refactors that are not needed for the above goals, such as:
- changing API contract unnecessarily
n- redesigning the entire package structure before features are working
- switching serialization approach for every topic at once
- attempting full platform-level exactly-once guarantees without defining scope
