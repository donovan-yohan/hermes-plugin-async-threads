# Kanban source-binding example

This example binds a Hermes Kanban board to an existing ATH listener so material task transitions wake the same Hermes conversation. It uses source bindings, not a cron job.

The values below are placeholders. Do not paste raw listener HMAC secrets into prompts, issues, or docs.

## What this proves

- A durable source binding can point at an upstream Kanban board.
- Dry-run previews which `task_events` would emit and where the cursor would advance.
- The native runner signs compact `async-thread-event/v1` events and records outbox/cursor diagnostics.
- Comments, heartbeats, and progress noise are suppressed.
- Raw board comments, logs, terminal transcripts, and secrets stay out of model-visible payloads.

## Prerequisites

- Hermes gateway is running with the `async_threads` platform enabled.
- You already created an ATH listener from the conversation to wake.
- The listener allowlist includes the Kanban events you plan to emit.
- The Kanban board exists on the same host/profile as the runner.

Example listener/event scope:

```text
thread key: ath_...
producer: ath-kanban-bridge
events: kanban.task.blocked,kanban.task.completed,kanban.task.crashed,kanban.task.gave_up,kanban.task.timed_out,kanban.task.ready_for_review
```

## 1. Create the binding

Model-facing tool shape:

```json
{
  "source": "kanban",
  "board_ref": "ath",
  "listener_thread_key": "ath_...",
  "producer_id": "ath-kanban-bridge",
  "event_filter": {
    "eventTypes": [
      "kanban.task.blocked",
      "kanban.task.completed",
      "kanban.task.crashed",
      "kanban.task.gave_up",
      "kanban.task.timed_out",
      "kanban.task.ready_for_review"
    ]
  }
}
```

Manual admin equivalent:

```text
/ath bind-source kanban ath_... \
  --board ath \
  --producer ath-kanban-bridge \
  --events kanban.task.blocked,kanban.task.completed,kanban.task.crashed,kanban.task.gave_up,kanban.task.timed_out,kanban.task.ready_for_review
```

Expected result: a binding id such as `athb_...` plus compatibility status. The command must not print listener secrets.

## 2. Dry-run before emitting

Model-facing tool shape:

```json
{
  "binding_id": "athb_...",
  "board_db_path": "/absolute/path/to/.hermes/kanban/boards/ath/kanban.db",
  "limit": 100
}
```

Manual admin equivalent:

```text
/ath dry-run-binding athb_... --db /absolute/path/to/.hermes/kanban/boards/ath/kanban.db --json
```

Check:

- `would_emit` only contains material transitions.
- `suppressed` contains comments, heartbeats, creates, claims, and other noise.
- `advanced` is false for dry-run.
- Any compatibility failure is fixed before enabling the runner.

## 3. Enable the native runner

Add the runner config to the profile that owns the gateway:

```yaml
platforms:
  async_threads:
    enabled: true
    extra:
      source_binding_runner_enabled: true
      source_binding_runner_interval_seconds: 30
      source_binding_runner_limit: 100
```

Restart the gateway after config or plugin changes. The runner is in-process with the `async_threads` platform; it is not a Hermes cron job.

Verify health:

```bash
curl -fsS http://127.0.0.1:8765/async-threads/v1/health
```

The health JSON should include source-binding runner status when the platform is connected.

## 4. Trigger one safe material transition

Use a disposable test card. Complete or block it with a summary that contains only compact state, not raw logs or secrets.

Expected event id shape:

```text
ath:<task_id>:<task_event_id>
```

Example trace target:

```text
/ath trace ath:t_example:123 --json
```

A good trace shows:

- `eventType`: `kanban.task.completed` or another allowed Kanban event;
- `producerId`: `ath-kanban-bridge`;
- `outcome`: `agent_started` or `queued_active_session`;
- `ack_success`: true when acknowledgements are enabled;
- target platform and workflow metadata match the listener destination.

## 5. Inspect binding diagnostics

Model-facing tool shape:

```json
{"binding_id": "athb_..."}
```

Manual equivalent:

```text
/ath inspect-binding athb_...
```

Healthy steady state:

- compatibility valid;
- binding status active;
- cursor advanced through handled upstream events;
- lag is zero or shrinking;
- outbox has `succeeded` rows for material events and `suppressed` rows for noise;
- `lastError` is empty or classified/redacted.

## Failure handling

- `duplicate` receiver responses are success for the same upstream transition.
- Retryable transport failures should keep the event pending and cursor pinned.
- Nonretryable receiver failures should become terminal error rows and advance the cursor so one poison event cannot spam forever.
- Pause the binding instead of retiring the listener when the source is noisy or misconfigured.

## Cleanup

Retire disposable test cards. Keep the binding active only if the board should continue waking the mapped conversation. Otherwise pause or retire the source binding; that does not retire the underlying ATH listener.
