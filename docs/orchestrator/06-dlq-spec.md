# Feature spec: dead letter queue

## Goal

Route poison messages or unrecoverable failures away from the main topic so consumption can continue.

## Current state

Current behavior is mostly log-and-continue.
There is no DLQ topic and no structured failure path.

## Target state

The service should publish a structured DLQ record when a message cannot be safely processed.

## Scope

Likely affected files:
- `internal/kafka/consumer.go`
- new DLQ producer helper(s)
- config for DLQ topic names
- optional new models for DLQ payload
- docs/runbook for DLQ verification

## Suggested initial policy

Send to DLQ when:
- payload is malformed
- schema/prefix is invalid
- deserialization fails
- business validation fails and retry is not appropriate

Potentially later:
- retry transient failures before DLQ

## Suggested DLQ payload fields

- original topic
- original partition
- original offset
- original key
- original payload bytes or a safe representation
- error message
- error type
- timestamp
- consumer group

## Commit behavior

For messages sent to DLQ successfully:
- commit the original topic offset after DLQ publish succeeds

If DLQ publish fails:
- do not commit the original offset yet
- log clearly

## Acceptance criteria

- DLQ topic exists or is auto-created by config.
- Known bad payloads can be routed to DLQ.
- Original offset is committed only after successful DLQ publish.
- A manual runbook shows how to trigger and inspect DLQ records in Kafka UI.

## Non-goals

- Do not build a full retry framework yet unless explicitly required.
