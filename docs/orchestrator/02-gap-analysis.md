# Gap analysis: what is still missing

This compares the current repo against the mentor's desired learning outcomes.

## Mentor goals mapped to current status

## 1. Small BE connected to Kafka with producers and consumers
Status: **done**

Reason:
- brokers configured through `config.yaml`
- producer and consumer exist
- service connects to Kafka and starts both consumers

## 2. Schema Registry integration for validation and schema fetch
Status: **partially done**

Already present:
- Schema Registry client
- schema registration
- Avro consumer fetches schema by ID

Still missing:
- explicit compatibility mode management
- schema evolution lab with v1/v2/v3
- backward/forward compatibility demonstrations
- deliberate incompatible schema failure case

## 3. Simple web server CRUD + self-consume
Status: **done**

Already present:
- CRUD user API
- producer publishes on user actions
- same service runs consumers

## 4. Transactional producer / atomic write
Status: **not done**

Current problem:
- DB write and Kafka publish are separate steps
- no atomic boundary between them

Consequence:
- user row can be committed without corresponding Kafka event
- Kafka event could be attempted after DB mutation and still fail

## 5. Understand consume commit offset and delivery mode
Status: **concept touched, implementation incomplete**

Current problem:
- consumer uses `ReadMessage()` only
- there is no explicit manual commit flow
- no controlled demonstration of at-most-once / at-least-once / exactly-once semantics

## 6. Simulate missed consuming, lag, missing offsets
Status: **not done**

Current repo does not include:
- a consumer lag simulation
- a crash-before-commit simulation
- an offset reset or offset-not-found simulation
- recovery runbooks

## 7. Idempotency
Status: **not done**

Missing pieces:
- event ID in event payload
- processed-events table
- deduplication logic in consumer
- transactional DB handling around dedupe marker and business side effect

## 8. Dead letter queue
Status: **not done**

Missing pieces:
- DLQ topic(s)
- DLQ producer
- policy for poison messages
- structured error metadata for DLQ records
- tests or runbooks for bad payloads / validation failures

## 9. Real Kafka end-to-end testing
Status: **not done**

Current integration tests:
- use embedded Postgres
- mock Kafka producer

Still needed:
- real broker-based publish
- real consume verification
- visible offset behavior
- failure-path tests for duplicate and DLQ cases

## Practical summary

The current repo already covers the **basic lab**:
- infrastructure
- CRUD API
- producer/consumer
- schema registry basics
- topic creation

The current repo does **not yet cover the reliability stage**:
- manual offset control
- idempotency
- DLQ
- atomic DB plus event publishing pattern
- transaction demo or outbox solution
- failure simulation and recovery behavior

That reliability stage is the next implementation target.
