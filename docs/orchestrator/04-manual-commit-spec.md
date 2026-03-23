# Feature spec: manual commit consumer flow

## Goal

Make consumer offset handling explicit and observable.

## Current state

The current consumer uses `ReadMessage()` in both Avro and Proto loops.
That hides fetch and commit behavior behind the reader abstraction.

## Target state

The consumer should:
1. fetch a message explicitly
2. deserialize it
3. run handler logic
4. commit the offset only after successful handling

## Scope

Likely affected files:
- `internal/kafka/consumer.go`
- possibly new helper files under `internal/kafka/`
- docs/runbook for manual-commit testing

## Requirements

- Use `FetchMessage()` instead of `ReadMessage()`.
- Call `CommitMessages()` only after the message has been handled successfully.
- Log these fields on fetch and commit:
  - topic
  - partition
  - offset
  - key
  - consumer group
- Keep Avro and Proto flows symmetrical unless there is a clear reason not to.

## Failure behavior to document

Need a runbook for at least these cases:
- fetched, then crash before handler finishes
- handled successfully, then crash before commit
- deserialization failure

## Acceptance criteria

- Consumer no longer uses `ReadMessage()`.
- Successful handler execution leads to explicit `CommitMessages()`.
- Failed handler execution does not commit the offset.
- Logs make it easy to correlate fetched offset vs committed offset.
- A manual test can demonstrate that crash-before-commit causes the same message to be re-read after restart.

## Non-goals

- Do not add idempotency in this step.
- Do not add DLQ in this step.
- Do not change API behavior in this step.
