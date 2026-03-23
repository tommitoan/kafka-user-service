# Runbook: manual commit consumer — crash behavior testing

## Purpose

Verify that the manual-commit consumer re-reads a message after a crash that
happens between fetching a message and committing its offset.

This runbook covers three crash scenarios.  
**Idempotency is not yet implemented.** Re-reads will cause duplicate handler
executions.  That is expected at this step and will be solved in Step 2
(idempotency table).

---

## Prerequisites

```
docker compose up -d        # starts Kafka, Schema Registry, PostgreSQL
go build ./...              # confirm clean build
```

Keep a second terminal open for log tailing:

```
go run ./cmd/server/...  2>&1 | tee /tmp/svc.log
```

---

## Scenario 1 — normal flow (baseline)

**Steps**

1. Start the service.
2. Create a user:
   ```
   curl -s -X POST http://localhost:8080/users \
     -H 'Content-Type: application/json' \
     -d '{"name":"Alice","email":"alice@example.com","age":30}'
   ```
3. Observe logs.

**Expected log sequence (for both Avro and Proto consumers)**

```
level=INFO msg="kafka fetch"  format=avro  group=...  topic=...  partition=0  offset=0  key=...
level=INFO msg="kafka event received" ...
level=INFO msg="kafka commit" format=avro  group=...  topic=...  partition=0  offset=0  key=...
```

The `kafka fetch` offset and `kafka commit` offset must match.  
No `kafka handler error` or `kafka commit error` lines should appear.

---

## Scenario 2 — crash after fetch, before handler completes

This simulates a process kill that happens after the message was fetched but
before the handler finished (and therefore before the offset was committed).

**Steps**

1. Add a temporary sleep inside `LoggingHandler` in `consumer.go` (revert
   after testing):
   ```go
   func LoggingHandler(format string) EventHandler {
       return func(ctx context.Context, event *models.UserEvent) error {
           time.Sleep(10 * time.Second) // TEMPORARY — gives time to kill
           b, _ := json.MarshalIndent(event, "", "  ")
           slog.Info("kafka event received", "format", format, "event", string(b))
           return nil
       }
   }
   ```
2. Rebuild and start the service.
3. Create a user (same `curl` as above).
4. Watch for the `kafka fetch` log line.
5. **Immediately kill the service** with `kill -9 <pid>` (or `Ctrl+C` is not
   sufficient — use SIGKILL to prevent graceful shutdown).
6. Restart the service (`go run ./cmd/server/...`).

**Expected behavior after restart**

The same `offset` appears again in a `kafka fetch` log line:

```
level=INFO msg="kafka fetch"  format=avro  offset=0  ...
```

This confirms the broker did not receive a commit for that offset and
re-delivered the message to the same consumer group.

**Revert** the temporary sleep before committing.

---

## Scenario 3 — crash after handler success, before CommitMessages

This is the tightest crash window: the handler returned without error but the
process dies before `CommitMessages` runs.

**Steps**

1. Add a temporary sleep **between** the handler call and `CommitMessages` in
   `StartAvro` (and `StartProto`) in `consumer.go` (revert after testing):
   ```go
   if err := handler(ctx, event); err != nil {
       // ... existing error path
   }

   time.Sleep(10 * time.Second) // TEMPORARY — window to kill before commit

   if err := c.avroReader.CommitMessages(ctx, msg); err != nil {
       // ...
   }
   ```
2. Rebuild and start the service.
3. Create a user.
4. Watch logs — wait until `kafka event received` appears (handler done) but
   **before** `kafka commit` appears.
5. Kill the service with `kill -9 <pid>`.
6. Restart.

**Expected behavior after restart**

```
level=INFO msg="kafka fetch"  format=avro  offset=0  ...   ← same offset again
level=INFO msg="kafka event received" ...                   ← handler runs again
level=INFO msg="kafka commit" format=avro  offset=0  ...
```

The handler runs **twice** for the same message.  This is the at-least-once
delivery guarantee.  Duplicate handling will be solved by the idempotency
table in Step 2.

**Revert** the temporary sleep before committing.

---

## Scenario 4 — deserialization failure

A corrupt or schema-incompatible message is on the topic.

**Expected behavior**

```
level=ERROR msg="kafka deserialize error"  format=avro  offset=N  error="..."
```

- The handler is **never called**.
- `CommitMessages` is **never called**.
- The service continues reading subsequent messages.
- **The bad message blocks re-reads until the service restarts**, at which
  point it will be fetched and fail again.

> This is the poison-pill problem.  It will be resolved in Step 3 (DLQ).
> At this step, document the blocked offset in the `kafka deserialize error`
> log and handle it manually by resetting the consumer group offset past the
> bad message if needed:
>
> ```
> kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
>   --group <group-id> --topic <topic> \
>   --reset-offsets --to-offset <N+1> --execute
> ```

---

## Scenario 5 — CommitMessages failure within a live session

This is a subtle edge case: the handler succeeds but `CommitMessages` returns
an error (e.g., broker temporarily unreachable).

**Behavior within the same running process**

The broker's committed offset has **not moved**.  However, kafka-go's internal
fetch position has already advanced past this message.  Subsequent
`FetchMessage` calls will return the *next* message.  The failed message is
effectively skipped in this process run.

**Behavior after restart**

Because the broker offset was never moved, the message **will** be re-delivered
to the same consumer group.  The handler will run again.

**Expected log sequence**

```
level=INFO  msg="kafka fetch"   offset=N  ...
level=INFO  msg="kafka event received" ...
level=ERROR msg="kafka commit error"  offset=N  error="<broker error>"
level=INFO  msg="kafka fetch"   offset=N+1  ...   ← live session skips to next
```

After restart:

```
level=INFO  msg="kafka fetch"   offset=N  ...     ← same message re-delivered
level=INFO  msg="kafka event received" ...
level=INFO  msg="kafka commit"  offset=N  ...
```

> This is a known gap in Step 1.  The handler may run once (live skip) and then
> once more (restart re-delivery), producing two side effects for one message.
> This will be addressed by the idempotency table in Step 2.

---

## How to correlate fetch vs commit in logs

Each `kafka fetch` and `kafka commit` log line shares the same
`topic`, `partition`, `offset`, and `key` values.  To confirm a commit
happened for a specific fetch:

```
grep '"kafka fetch"'  /tmp/svc.log | grep '"offset":0'
grep '"kafka commit"' /tmp/svc.log | grep '"offset":0'
```

A missing `kafka commit` line for a given offset means the offset was not
acknowledged to the broker.

---

## What is still missing after this step

| Gap | Planned step |
|-----|--------------|
| Duplicate handler execution on re-read | Step 2: idempotency table |
| Poison-pill message blocks the partition | Step 3: DLQ |
| DB write and Kafka publish not atomic | Step 4: outbox pattern |
