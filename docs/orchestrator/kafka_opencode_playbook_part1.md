# Kafka User Service + OpenCode Playbook (Part 1)

This playbook is tailored to the uploaded repo and orchestrator pack.

## What I verified from your uploads

- `internal/kafka/consumer.go` still uses `ReadMessage()` for both Avro and Proto flows.
- `internal/service/user_service.go` still writes DB first and then fire-and-forget publishes Kafka events.
- `test/integration/user_integration_test.go` uses embedded Postgres and a mocked Kafka producer, so it is not real Kafka end-to-end.
- The orchestrator pack recommends this implementation order:
  1. manual commit consumer flow
  2. idempotency
  3. DLQ
  4. outbox
  5. separate Kafka transaction demo
  6. real end-to-end tests

That means your first OpenCode implementation slice should be **manual commit only**.

---

## Goal of Part 1

Turn this repo into a clean OpenCode-controlled project and finish the first reliability feature:

- repo guidance in `AGENTS.md`
- OpenCode project config in `opencode.json`
- reusable skills in `.agents/skills/`
- one reviewer subagent in `.opencode/agents/`
- two repeatable commands in `.opencode/commands/`
- implement **manual commit consumer flow only**

Do **not** mix in idempotency, DLQ, outbox, or transaction demo yet.

---

## Step 0 — Prepare the repo once

Inside your repo root, add this structure:

```text
kafka-user-service/
├─ AGENTS.md
├─ opencode.json
├─ docs/
│  ├─ ai/
│  │  ├─ architecture-map.md
│  │  ├─ coding-standards.md
│  │  └─ testing-playbook.md
│  └─ orchestrator/
│     ├─ README.md
│     ├─ 01-current-state.md
│     ├─ 02-gap-analysis.md
│     ├─ 03-implementation-order.md
│     ├─ 04-manual-commit-spec.md
│     ├─ 05-idempotency-spec.md
│     ├─ 06-dlq-spec.md
│     ├─ 07-outbox-vs-transaction-demo.md
│     ├─ 08-orchestrator-handoff.md
│     └─ 09-task-prompts.md
├─ .agents/
│  └─ skills/
│     ├─ feature-plan/
│     │  └─ SKILL.md
│     ├─ refactor-safe/
│     │  └─ SKILL.md
│     └─ test-fix/
│        └─ SKILL.md
└─ .opencode/
   ├─ agents/
   │  └─ reviewer.md
   └─ commands/
      ├─ resume.md
      └─ ship-check.md
```

Copy the uploaded orchestrator-pack docs into `docs/orchestrator/`.

---

## Step 1 — Add `AGENTS.md`

Create `AGENTS.md` in the repo root:

```md
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
```

---

## Step 2 — Add `opencode.json`

Create `opencode.json`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "instructions": [
    "docs/orchestrator/08-orchestrator-handoff.md",
    "docs/orchestrator/03-implementation-order.md"
  ],
  "permission": {
    "read": {
      "*": "allow",
      "*.env": "deny",
      "*.env.*": "deny",
      "*.env.example": "allow"
    },
    "edit": "ask",
    "bash": {
      "*": "ask",
      "git status*": "allow",
      "git diff*": "allow",
      "git log*": "allow",
      "rg *": "allow",
      "grep *": "allow",
      "find *": "allow",
      "go test*": "allow",
      "docker compose ps*": "allow"
    }
  },
  "agent": {
    "plan": {
      "mode": "primary"
    },
    "build": {
      "mode": "primary"
    }
  }
}
```

After this is stable, you can add per-agent model overrides.

---

## Step 3 — Add shared skills

### `.agents/skills/feature-plan/SKILL.md`

```md
---
name: feature-plan
description: Use when the task needs a narrow implementation plan before code changes.
---

1. Restate the feature, constraints, and non-goals.
2. Read the relevant docs under `docs/orchestrator/`.
3. Identify touched files and data flows.
4. Propose the smallest implementation path.
5. List validation steps and manual verification steps.
6. Do not edit code until the plan is approved.
```

### `.agents/skills/refactor-safe/SKILL.md`

```md
---
name: refactor-safe
description: Use when making internal changes while preserving behavior.
---

1. Preserve public API behavior unless explicitly asked otherwise.
2. Keep changes narrow and file-local when possible.
3. Avoid broad package restructuring.
4. Update only the minimum tests needed for the current feature.
5. Summarize risks and remaining gaps after edits.
```

### `.agents/skills/test-fix/SKILL.md`

```md
---
name: test-fix
description: Use when tests fail or when adding a feature requires tight validation.
---

1. Reproduce with the narrowest possible command.
2. Classify failure: compile, logic, integration, environment, or flaky behavior.
3. Fix the smallest root cause first.
4. Re-run targeted checks before broad checks.
5. Summarize cause, fix, and remaining risk.
```

---

## Step 4 — Add reviewer subagent

Create `.opencode/agents/reviewer.md`:

```md
---
description: Reviews diffs for correctness, scope control, missing tests, and risky Kafka behavior
mode: subagent
temperature: 0.1
permission:
  edit: deny
  bash:
    "*": ask
    "git status*": allow
    "git diff*": allow
    "git log*": allow
    "rg *": allow
---

You are a strict reviewer for this Kafka learning project.

Focus on:
- correctness of Kafka offset flow
- whether commit happens only after success
- whether scope stays narrow
- missing logs, runbooks, or tests
- regressions in current CRUD behavior

Output format:
1. Critical issues
2. Important improvements
3. Nice-to-have cleanup
4. Verdict: ready / not ready
```

---

## Step 5 — Add two commands

### `.opencode/commands/resume.md`

```md
---
description: Rebuild current context for this repo
agent: plan
---

Summarize the current work state for this repository.

Return:
1. current objective
2. touched files
3. what is already done
4. blockers or open risks
5. next best step
```

### `.opencode/commands/ship-check.md`

```md
---
description: Run a pre-commit quality pass
agent: build
---

For the current change:
1. inspect git diff
2. run relevant Go tests if appropriate
3. identify missing tests or scope creep
4. produce a release-risk summary
```

---

## Step 6 — Choose agent roles in OpenCode

Suggested model split from the models you showed:

- **Plan**: Claude Sonnet 4.6
- **Build**: GPT-5.4
- **Reviewer**: Claude Opus 4.6
- **Fast/light tasks**: GPT-5.4 mini or Claude Haiku 4.5

Reason:
- use one model family for implementation
- use a different family for review to reduce same-model blind spots

---

## Step 7 — First real OpenCode task: manual commit only

Create a new branch first:

```bash
git checkout -b feat/manual-commit-consumer
```

Open the repo in OpenCode Desktop.

### 7.1 Start with Plan agent

Paste this into **Plan**:

```text
Read these files first:
- docs/orchestrator/08-orchestrator-handoff.md
- docs/orchestrator/03-implementation-order.md
- docs/orchestrator/04-manual-commit-spec.md
- internal/kafka/consumer.go
- cmd/server/main.go

Task: implement manual commit consumer flow only.

Requirements:
- Replace ReadMessage() with FetchMessage() + CommitMessages() in both Avro and Proto loops.
- Commit only after successful handler execution.
- Add clear structured logs for topic, partition, offset, key, and consumer group.
- Keep scope narrow.
- Do not implement idempotency, DLQ, outbox, or transaction demo.
- Produce a file-by-file plan first.
- Include manual verification steps for crash-before-commit behavior.
```

### 7.2 What a good plan should say

A good plan should likely mention only a small set of files, such as:

- `internal/kafka/consumer.go`
- maybe one new helper file for logging or message processing
- maybe one new docs file for a manual runbook
- maybe tests if they are easy and narrow

If Plan wants to modify service, repository, DB, or producer code for this step, push back.

---

## Step 8 — Switch to Build agent

After the plan looks clean, paste this into **Build**:

```text
Implement the approved manual-commit plan now.

Constraints:
- scope must stay limited to manual commit behavior
- keep Avro and Proto flows symmetrical where reasonable
- commit offset only after successful handler execution
- no commit on deserialize failure or handler failure
- add logs for fetch and commit with topic, partition, offset, key, and consumer group
- add a short manual runbook under docs/orchestrator/manual-commit-runbook.md

Before editing, restate the files you will change.
After editing, summarize the exact behavior changes.
```

---

## Step 9 — Review with reviewer agent

After Build finishes, ask `@reviewer`:

```text
Review this change specifically for manual offset control.

Check:
- does the consumer still use ReadMessage anywhere?
- are offsets committed only after handler success?
- are deserialize and handler failures intentionally not committed?
- are the logs enough to verify fetched vs committed offsets?
- is scope still narrow?
```

Fix only high-value review findings.

---

## Step 10 — Run the quality pass

Use `/ship-check` and then manually verify:

```bash
go test ./...
```

If local infra is already running:

```bash
docker compose up -d
go run ./cmd/server
```

Then trigger a user event through the API and verify logs show:

- fetch log with topic / partition / offset / key / group
- successful handler log
- explicit commit log for the same offset

---

## Step 11 — Manual crash-before-commit demo

Add this runbook to `docs/orchestrator/manual-commit-runbook.md`.

Suggested content:

```md
# Manual Commit Runbook

## Goal
Show that a message is re-read if the service crashes after handling but before offset commit.

## Steps
1. Start infra with `docker compose up -d`.
2. Run the service.
3. Send a create-user request.
4. Temporarily add a forced process exit after handler success but before commit.
5. Restart the service.
6. Observe the same message being fetched again.

## Expected outcome
- fetched offset appears in logs before crash
- no committed-offset log exists for that message before crash
- after restart, the same offset is fetched again
```

You do not need a polished chaos harness yet. A simple manual demonstration is enough for this step.

---

## Step 12 — Commit message

When done, use a narrow commit message:

```bash
git add .
git commit -m "feat(kafka): add manual consumer commit flow"
```

---

## What you should NOT do in Part 1

Do not let OpenCode expand into these yet:

- adding `event_id`
- creating `processed_events`
- creating DLQ topics
- adding outbox tables
- refactoring producer for transactions
- replacing the integration test strategy

Those belong to later slices.

---

## Success criteria for Part 1

You are done with Part 1 when all of these are true:

- OpenCode project files exist and are committed.
- The repo has `docs/orchestrator/` copied in.
- Consumer no longer uses `ReadMessage()`.
- Consumer uses explicit fetch then commit.
- Commit only happens after successful handling.
- Logs clearly show fetched vs committed offsets.
- There is a short manual runbook proving crash-before-commit replay.

---

## Next part after this

Only after Part 1 is stable, move to:

**Part 2: idempotency**

That will add:
- `event_id`
- `processed_events` migration
- duplicate-safe consumer-side effect flow
- duplicate replay demonstration
