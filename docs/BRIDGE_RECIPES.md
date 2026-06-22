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

1. `started` — lane/work context created.
2. `progress` — routine update; usually debounce/coalesce.
3. `blocked` or `needs_attention` — priority wakeup.
4. `ready_for_review` / `review_passed` / `review_failed` — review gate state.
5. `qa_passed` / `qa_failed` — QA gate state.
6. `released` or `cancelled` — terminal state.

Use `/ath workflows <thread_key>` for current state, `/ath trace <event-id>` for one event's delivery path, and `/ath retire <thread_key>` when a temporary listener should stop accepting events after merge or abandonment.

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
