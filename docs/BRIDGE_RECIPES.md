# Bridge recipes and operator workflows

This page collects producer-side patterns after Hermes creates or reuses a signed async-thread route. The happy path is agent-first: a user asks Hermes to watch work and report here, Hermes uses model-facing ATH tools, and `/ath` remains the manual admin/debug surface. The receiver still treats every event body as untrusted data; these recipes are about shaping safe state facts, not issuing instructions to Hermes.

## Model-facing producer handoffs

The model-facing `ath_generate_producer_handoff` tool turns an existing listener into a producer handoff without pasting the raw HMAC secret into normal chat/tool output. It can return:

- a generic `async-thread-event/v1` contract;
- local `producer_handoff.json` + `emit_async_thread_event.py` helper files;
- a GitHub Actions recipe/step;
- a debug emitter shape that only returns literal secret material behind an explicit sensitive flag.

Default handoffs include endpoint URL, thread key, producer id, allowed event types, a schema-valid example event, `secretFile`/`contractFile` references, retry/de-dupe guidance, and listener lifecycle guidance. Local helper files are written under the configured handoff root with restrictive permissions and read the HMAC key from `ATH_SECRET_FILE`; they do not embed the raw secret.

Use this path for the happy-case agent workflow: create/reuse a listener, generate a producer handoff, give the producer the helper file path or contract, then verify delivery with `ath_trace_event` or `/ath trace`.

## Complete agent-first workflow

User ask:

```text
watch this PR review lane and report back here when it is ready or blocked
```

Agent actions:

1. Call `ath_create_listener` from the current gateway conversation:

   ```json
   {
     "purpose": "watch this PR review lane and report readiness or blockers back here",
     "producer_hint": "repo-review",
     "event_kinds": ["ready", "blocked"],
     "delivery": "agent_queue",
     "max_turns": 1,
     "max_tool_calls": 0
   }
   ```

2. Call `ath_generate_producer_handoff` for the returned `threadKey`:

   ```json
   {"thread_key": "ath_...", "mode": "generic_contract"}
   ```

3. Give the producer the generated `contractFile` or helper path and the `ATH_SECRET_FILE` reference. Do not paste the raw secret.

4. Producer sends a signed event when review state changes:

   ```json
   {
     "version": "async-thread-event/v1",
     "eventId": "repo-review-37-head-abcd-ready",
     "eventType": "repo-review.ready",
     "producer": {"id": "repo-review"},
     "occurredAt": "2026-06-20T19:00:00Z",
     "asyncThread": {"threadKey": "ath_..."},
     "summary": "PR #37 is ready for review",
     "tailMode": "compact",
     "subject": {"repo": "example/repo", "pr": 37, "url": "https://example.invalid/repo/pull/37"},
     "payload": {"status": "ready", "head_sha": "abcd1234"}
   }
   ```

5. Verify with `ath_trace_event` or `/ath trace repo-review-37-head-abcd-ready`.

## Local script wrapper

User ask:

```text
run this long script and ping this thread when it finishes or fails
```

Agent defaults:

- `producer_hint: "local-job"`
- `event_kinds: ["finished", "failed"]`
- `delivery: "agent_queue"` unless the user only wants a direct notification
- handoff mode: `local_script`

Producer guidance:

- write full stdout/stderr to a local log file;
- send compact `status`, `verification`, `duration`, and `log_path` fields;
- use `tailMode: "compact"` or `"none"` for routine events;
- retry transport/`502` failures with the same `eventId`.

## PR/review lane

Use this for repository automation, code review lanes, or CI review gates.

Recommended event types:

- `repo-review.ready`
- `repo-review.blocked`
- `repo-review.failed`
- `repo-review.finished`

Keep one event id per immutable review artifact, for example `<repo>:pr-<n>:<head_sha>:ready`. Before acting on a stale event, the agent should re-check the current PR head.

## Manual `/ath` admin/debug path

Use `/ath` when the user explicitly asks to administer or debug listeners:

- `/ath status` — receiver/config state.
- `/ath list` / `/ath inspect` — listener inventory.
- `/ath trace <event_id>` / `/ath events <thread_key>` — delivery diagnostics.
- `/ath pause` / `/ath resume` / `/ath retire` / `/ath revoke` — lifecycle.
- `/ath rotate-secret` — active-listener secret rotation.
- `/ath prune --dry-run` — retention cleanup preview.

## Sandbox-safe emit command

For one-off producers or sandboxed worker lanes, prefer `/ath emit-command` over pasting secrets into prompts:

```text
/ath emit-command <thread_key> --event job.ready --summary "job is ready"
```

The command prints a shell/Python template with the receiver URL, thread key, producer id, event type, and summary filled in. It deliberately does not print the listener HMAC secret. Put the secret in a local `0600` file or environment variable outside the worker prompt/log, then run the generated command from the worker environment.

This is safer than giving a worker direct SQLite access. If the emit fails, the generated script exits with an HTTP/transport error that the worker can report as blocked.

## Dynamic Workflows loop emitter

Use this when Dynamic Workflows is the controller and ATH is only the signal/visibility surface. Create or reuse a listener with a narrow allowlist such as `loop.started`, `loop.waiting_for_event`, `loop.step_completed`, `loop.waiting_for_approval`, `loop.stalled`, `loop.halted`, and `loop.converged`, then generate a handoff with `mode: dynamic_workflows`.

If you generate the handoff from an existing listener whose allowlist is missing the core loop events, treat `listenerCompatibility.warning` as a blocker and recreate/reuse a listener with the recommended event types. Otherwise signed loop examples can be perfectly formed but still rejected by ATH authorization.

The handoff gives Dynamic Workflows endpoint URL, `ATH_THREAD_KEY`, `ATH_PRODUCER_ID`, `ATH_CONTRACT_FILE`, and `ATH_SECRET_FILE` references. It never returns the raw HMAC secret in ordinary output. The controller should read the secret from the file or its own secret manager, set `occurredAt` to the current UTC emission time, build the JSON body once, sign the exact UTF-8 bytes, and reuse the same `eventId` on retry. Do not sign stale copied JSON; receiver replay protection rejects events outside the freshness window.

Copy/paste sequence:

1. emit `loop.started` with `loop.runId`, `loop.specId`, `correlation.correlationKey`, `correlation.idempotencyKey`, `correlation.signalKey`, `refs`, `evidence`, and `nextExpectedSignal`;
2. emit `loop.waiting_for_event` when the controller parks for GitHub/Relay/external state;
3. let the external signed producer emit the matching signal; ATH wakes/renders but Dynamic Workflows verifies live state and decides the next transition;
4. emit `loop.step_completed` with compact evidence handles after a bounded step;
5. emit either `loop.converged` or `loop.halted` as the terminal loop result, then stop the producer loop for single-goal runs.

ATH does not decide convergence, retry, halt, or approval. Dynamic Workflows owns those state transitions. ATH authenticates, de-dupes, wakes, replies, renders, and records events. Public comments and producer payload text remain untrusted data.

For GitHub PR/check/review/comment/head-change/deployment signals and Relay step-completion signals, use [`LOOP_SIGNAL_INGESTION.md`](LOOP_SIGNAL_INGESTION.md). It defines the producer event types, refs/evidence handles, idempotency keys, trusted-action gate, and stale-head checks expected by loop controllers.

## Task-board-to-ATH bridge recipe

Use this shape when a durable task board already records task events and you want material transitions to wake the original Hermes thread.

Recommended defaults:

- emit only material transitions by default: `completed`, `blocked`, `gave_up`, `crashed`, `needs_attention`;
- do not wake for every comment, heartbeat, or progress tick;
- use a stable idempotency key such as `<board>:<task_id>:<task_event_id>`;
- include `workflowId` as `<board>:<task_id>`;
- include `seriesKey` as `task-board:<board>:<task_id>`;
- put lane, issue/PR URL, and task id in `subject`;
- put compact status facts in `payload`;
- use `tailMode: "compact"` unless explicitly debugging.

Example event:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "board-a:TASK-42:event-1009",
  "eventType": "task_board.task.blocked",
  "producer": {"id": "example-task-board"},
  "occurredAt": "2026-06-20T19:00:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "TASK-42 is blocked waiting for review evidence",
  "tailMode": "compact",
  "seriesKey": "task-board:board-a:TASK-42",
  "workflowId": "board-a:TASK-42",
  "stage": "blocked",
  "subject": {
    "board": "board-a",
    "task": "TASK-42",
    "lane": "review",
    "url": "https://example.invalid/tasks/TASK-42"
  },
  "payload": {
    "status": "blocked",
    "reason": "review evidence missing"
  }
}
```

Bridge checklist:

1. Persist the last emitted upstream event id so restarts do not duplicate wakeups.
2. Apply a transition allowlist before signing, not after delivery.
3. Coalesce noisy comments/progress into a single digest event when needed.
4. Treat `duplicate` responses as success for the same upstream transition.
5. Retry `502`/transport failures with the same `eventId`.
6. Never include raw board comments, credentials, cookies, terminal transcripts, or prompt text as instructions.

For native board integrations, persist this routing as a source binding instead of a cron poller. Bind `source=kanban` plus the board ref to an existing listener with `/ath bind-source kanban <thread_key> --board <board> --events kanban.task.blocked,kanban.task.unblocked,kanban.task.completed,...`, or use the model-facing `ath_create_source_binding` tool. Listing/inspection redacts secret-shaped material and reports listener compatibility fail-closed; pausing or retiring a binding never retires the underlying listener.

Dogfood binding for the shared `ath` board:

```text
/ath bind-source kanban ath_mg3BQeDs15Gm4DnF --board ath --producer ath-kanban-bridge --events kanban.task.blocked,kanban.task.unblocked,kanban.task.completed,kanban.task.crashed,kanban.task.gave_up,kanban.task.timed_out,kanban.task.ready_for_review
```

Natural-language equivalent from a mapped conversation:

```text
bind board ath to listener ath_mg3BQeDs15Gm4DnF with the ath-kanban-bridge producer; wake this thread only for blocked, unblocked, completed, crashed, gave_up, timed_out, and ready_for_review transitions; dry-run before enabling the native runner
```

Model-facing equivalent:

```json
{
  "source": "kanban",
  "board_ref": "ath",
  "listener_thread_key": "ath_mg3BQeDs15Gm4DnF",
  "producer_id": "ath-kanban-bridge",
  "event_filter": {
    "eventTypes": [
      "kanban.task.blocked",
      "kanban.task.unblocked",
      "kanban.task.completed",
      "kanban.task.crashed",
      "kanban.task.gave_up",
      "kanban.task.timed_out",
      "kanban.task.ready_for_review"
    ]
  },
  "delivery_policy": "agent_queue"
}
```

The command/tool response contains a `bindingId`, source ref, producer id, filter, status, and compatibility verdict. It must not contain the listener secret, `secret.txt` contents, task bodies, raw comments, or raw logs. If compatibility fails, fix the listener producer/event allowlist or source ref; do not work around it by broadening the event payload.

Before enabling a runner, preview the durable `task_events` cursor with `/ath dry-run-binding <binding_id> --db /path/to/kanban.db --since <event_id> --json` or the model-facing `ath_dry_run_source_binding` tool. Dry-run reports `would_emit`, `suppressed`, `would_coalesce`, and `invalid_binding`, does not POST signed events, and does not advance the binding cursor. The transform uses `eventId=<board>:<task_id>:<task_event_id>`, `seriesKey=kanban:<board>:<task_id>`, and `workflowId=kanban:<board>:<task_id>`; it omits task bodies, raw comments, transcripts, logs, and secret-shaped fields.

Use `kanban.task.unblocked` for native blocked-resolved wakeups instead of maintaining bespoke blocked/resolved pollers.

Trace one emitted event with `/ath trace <event_id> --json` or `ath_trace_event`. For Kanban, the event id is deterministic, for example `ath:t_4361a7a9:12345`. Use `/ath inspect-binding <binding_id>` or `ath_get_source_binding` for cursor, compatibility, lag, and outbox status. Do not paste board comment text or logs back into the trace prompt; trace output is diagnostics, not an instruction channel.

The native gateway runner is opt-in, not a Hermes cron job. Enable it on the `async_threads` platform only after the dry-run cursor looks correct:

```yaml
platforms:
  async_threads:
    extra:
      source_binding_runner_enabled: true
      source_binding_runner_interval_seconds: 30
      source_binding_runner_limit: 100
```

For each upstream row, the runner inserts a durable outbox row before emitting. It advances the binding cursor only after terminal-safe outcomes: `succeeded`, receiver `duplicate`, `suppressed`, or `coalesced`. Transport and `502` failures keep the row pending and retry the same ATH event id on the next runner pass; a crash after send but before mark is reconciled by the receiver's duplicate response. Listener revocation, producer mismatch, disallowed events, and strict `agent_queue` continuation fail-closed states remain diagnosable through `/ath inspect-binding` / `ath_get_source_binding` runner status instead of silently skipping work.

Rejected source-binding examples:

- Do not put raw Kanban comments, task bodies, full result logs, terminal transcripts, or secrets into `payload` as text for the agent to follow.
- Do not encode agent directives inside `summary`, `subject`, or `payload`.
- Do not create a Hermes cron job as the normal bridge path when the native source-binding runner or a future source push hook can own the cursor.
- Do not retire or rotate the listener while pausing a binding; binding lifecycle and listener lifecycle are separate.

## Repeated artifact revisions

When several events describe one logical artifact, use a stable `seriesKey` plus a revision field:

```json
{
  "seriesKey": "github-pr:example/repo:37",
  "supersedesEventId": "pr-37-head-a-review-requested",
  "artifact": {
    "kind": "pull_request",
    "id": "37",
    "url": "https://example.invalid/repo/pull/37",
    "revision": "bbbbbbbb"
  }
}
```

This does not replace live verification. An agent should still check the current PR/build/deploy head before merging or announcing a final result. The convention makes stale-event detection boring instead of bespoke per bridge.

## Scoped lane lifecycle

For temporary agent lanes or work contexts, use workflow stages consistently:

1. `started` — lane/resource created.
2. `progress` — routine heartbeat/progress updates; safe to debounce/coalesce.
3. `blocked` / `needs_attention` — priority wakeup.
4. `ready_for_review` / `review_passed` / `review_failed` — review gate state.
5. `qa_passed` / `qa_failed` — QA gate state.
6. `released` or `cancelled` — terminal state.

Terminal cleanup convention:

- Treat event types like `*.goal.finished`, `*.phase.finished`, `*.session.finished`, and `*.run.finished` as terminal workflow events by default.
- For single-goal listeners, declare terminal event types and enable auto-retire (`ath_create_listener(..., terminal_event_types=[...], auto_retire_on_terminal=true)` or `/ath listen ... --terminal-events ... --auto-retire-terminal`). ATH disables the listener after the authenticated terminal event is successfully delivered, and duplicate retries of the same event remain idempotent.
- For shared listeners, set `shared_listener=true` or `/ath listen ... --shared-listener`; ATH records the terminal event and reports the listener as stale, but it does not retire it automatically.
- Producer loops should self-exit after emitting a terminal event. Do not keep polling GitHub/session state forever after `goal.finished`, `run.finished`, etc.; ATH is meant to remove that cleanup-prone polling loop.

Use `/ath lifecycle` to surface enabled listeners with terminal events, `/ath workflows <thread_key>` for current state, `/ath trace <event-id>` for one event's delivery path, and `/ath retire <thread_key>` when a temporary listener should stop accepting events after merge or abandonment.

## Retention and pruning

Diagnostics and replay/de-dupe markers should not grow forever. `/ath prune` is owner-scoped and dry-run by default:

```text
/ath prune --dry-run --event-log-days 30 --seen-days 7
/ath prune --force --event-log-days 30 --seen-days 7
```

Platform config can set defaults:

```yaml
platforms:
  async_threads:
    extra:
      event_log_retention_days: 30
      seen_event_retention_days: 7
```

Keep `seen_event_retention_days` longer than the replay/de-dupe window you need. Pruning old seen rows means a very old producer retry can be accepted again; that is usually fine after the operational retry window has expired.
