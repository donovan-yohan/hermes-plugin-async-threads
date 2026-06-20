# hermes-plugin-async-threads

![Async Threads banner showing signed producer events waking one mapped Hermes conversation](docs/assets/async-threads-banner.png)

> Event-driven wakeups for existing Hermes gateway conversations, without cron polling.

`hermes-plugin-async-threads` lets an external producer send a signed event to Hermes and target an existing conversation handle. Hermes validates the event, de-dupes it, resolves the registered async-thread handle, and either posts a direct notification or queues a bounded continuation into the same gateway session.

## Current status

This repository is an MVP. It is useful, but it is not a blanket promise that every Hermes runtime can be resumed from every producer yet.

| Surface | Status |
| --- | --- |
| Discord gateway sessions | Tested MVP path |
| Telegram gateway sessions | Intended, needs compatibility tests |
| Slack and other gateway adapters | Intended, unverified |
| CLI | Producer helper only; no `listen here` listener UX |
| Hermes Desktop/API server | Unverified |
| Multi-gateway or multi-profile routing | Unsupported in the MVP; receiver assumes the target adapter is connected in the same gateway process/profile |

Known technical debt is tracked in the public-release readiness epic: https://github.com/donovan-yohan/hermes-plugin-async-threads/issues/33

## What problem does this solve?

Hermes can already run in gateway conversations and scheduled jobs can deliver back to an origin. The awkward workaround for long-running external work is a watcher or cron job that repeatedly polls until something changes.

Async threads invert that. The external system emits a signed event only when something meaningful happens, and Hermes wakes the mapped conversation.

Good fits:

- CI or deploy jobs reporting completion;
- long-running local scripts or background agents;
- GitHub or repository automation;
- home automation alerts;
- workflow/control-plane systems that should notify or resume a Hermes conversation without learning chat-platform APIs.

## How it works

![Baoyu infographic showing producer event validation, registry lookup, policy routing, and same gateway conversation delivery](docs/assets/baoyu-async-thread-flow.png)

The diagram above is intentionally scoped to the current MVP: gateway-local dispatch, Discord-shaped path tested first, and producer payload boxed as untrusted data.

1. A user creates a listener from an existing Hermes gateway conversation with `/ath listen`.
2. The plugin stores a durable `threadKey`, the captured Hermes `SessionSource`, allowed producer/event scope, policy, and a per-handle HMAC secret.
3. A producer sends `async-thread-event/v1` JSON to `POST /async-threads/v1/events` and signs the exact request body.
4. The receiver validates timestamp, route scope, HMAC, and de-dupe state.
5. Policy chooses either direct delivery or `agent_queue` continuation.
6. Event summary, subject, and payload are rendered as untrusted data before entering the agent session.

## Quickstart

See [`docs/QUICKSTART.md`](docs/QUICKSTART.md) for install/config and a complete signed demo event. See [`docs/EVENT_CONTRACT.md`](docs/EVENT_CONTRACT.md) for the producer-facing event contract and JSON Schema.

Minimal listener example from a supported Hermes gateway conversation:

```text
/ath listen demo --events demo.job.finished --ack brief
```

Minimal event envelope, matching the [`async-thread-event/v1` contract](docs/EVENT_CONTRACT.md):

```json
{
  "version": "async-thread-event/v1",
  "eventId": "demo-001",
  "eventType": "demo.job.finished",
  "producer": {"id": "demo"},
  "occurredAt": "2026-06-20T19:00:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "demo job finished",
  "payload": {"status": "passed", "artifact": "build-123"}
}
```

Sign the exact JSON request body with:

```text
X-Hermes-Signature-256: sha256=<hmac_sha256_hex(body, secret)>
```

## Security model

- Producers must authenticate with per-handle HMAC-SHA256 secrets.
- Events include timestamps and are rejected outside the replay window.
- Events are de-duped by producer/event id.
- Producers are scoped by listener handle and optional allowed event types.
- Payload text is data, not a user instruction.
- Raw logs/transcripts should not be placed in event payloads; use compact state and log paths.
- The MVP stores per-handle HMAC secrets in plugin-local SQLite because the receiver needs to validate inbound events.
- The generated secret is shown once when the listener is created; treat that chat surface as sensitive.

See [`docs/SECURITY.md`](docs/SECURITY.md) for more detail.

## Current implementation features

- plugin-local SQLite async-thread registry;
- `/ath listen/list/inspect/status/events/workflows/pause/resume/revoke` gateway commands;
- `async_threads` gateway platform receiver;
- signed `async-thread-event/v1` HTTP endpoint;
- de-dupe by producer/event id;
- idle-session wake and active-session queue behavior;
- privacy-safe dispatch diagnostics;
- opt-in `agent_queue` acknowledgements;
- compact long-running event rendering with `tailMode: none | compact | debug`;
- optional debounce/coalescing for routine same-thread updates;
- generic workflow-stage/candidate/evidence tracking with serial/parallel gate policy;
- producer helper script for compact background-lane events.

## Known limitations

- Gateway-local MVP: dispatch assumes the target platform adapter is connected in the same gateway process/profile.
- Non-Discord gateway routing is intended but not yet backed by compatibility tests.
- Direct delivery and acknowledgement metadata currently use a small metadata shape and need a stable platform-aware continuation helper.
- Active-session queueing currently relies on Hermes gateway/adapter internals.
- CLI and Hermes Desktop cannot create a listener from “here” yet.
- Coalesced event retry semantics and unauthenticated diagnostic persistence are tracked for hardening before public shareout.

## Development

The repo is a Hermes plugin, not a standalone bot. Tests need Hermes gateway modules available. The local test harness auto-detects a sibling or profile-local Hermes checkout when present; set `HERMES_AGENT_PATH=/path/to/hermes-agent` if needed.

```bash
uv run pytest -q
```

## License

MIT — see [`LICENSE`](LICENSE).
