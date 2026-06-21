# Bridge recipes and operator workflows

This page collects producer-side patterns that are useful after `/ath listen` creates a signed route. The receiver still treats every event body as untrusted data; these recipes are about shaping safe state facts, not issuing instructions to Hermes.

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
- include `seriesKey` as `kanban-task:<board>:<task_id>`;
- put lane, issue/PR URL, and task id in `subject`;
- put compact status facts in `payload`;
- use `tailMode: "compact"` unless explicitly debugging.

Example event:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "board-a:TASK-42:event-1009",
  "eventType": "kanban.task.blocked",
  "producer": {"id": "example-kanban"},
  "occurredAt": "2026-06-20T19:00:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "TASK-42 is blocked waiting for review evidence",
  "tailMode": "compact",
  "seriesKey": "kanban-task:board-a:TASK-42",
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
