# hermes-plugin-async-threads

![Async Threads banner showing signed events routed through Hermes back to one mapped conversation](docs/assets/async-threads-banner.png)

> Agent-created signed callbacks for existing Hermes gateway conversations, without cron polling.

`hermes-plugin-async-threads` lets a Hermes agent create a durable callback route for the conversation it is already in, hand a producer a safe contract/secret-file reference, and later wake that same mapped conversation when a signed event arrives. The receiver authenticates the producer, rejects stale or duplicate events, resolves the async-thread handle, and either posts a direct notification or queues an agent continuation with explicit policy metadata.

The default public UX is **agent-first**: users ask naturally, agents call `ath_create_listener` and `ath_generate_producer_handoff`, and `/ath` commands remain the admin/debug escape hatch. Current Hermes core does not expose plugin-local hard caps for individual synthetic gateway continuations, so strict hard-bound requirements should opt into fail-closed mode until that core seam exists.

## Current status

This repository is an MVP. It is useful, but it is not a blanket promise that every Hermes runtime can be resumed from every producer yet.

| Surface | Status |
| --- | --- |
| Discord gateway sessions | Unit-tested dispatch path with mock adapter; live gateway smoke pending |
| Telegram gateway sessions | Metadata helper covered for DM/topic routing; live gateway smoke pending |
| Slack gateway sessions | Generic thread metadata covered; live gateway smoke pending |
| Other gateway adapters | Intended, unverified |
| CLI | Producer helper only; no `listen here` listener UX |
| Hermes Desktop/API server | Unverified |
| Multi-gateway or multi-profile routing | Unsupported in the MVP; receiver assumes the target adapter is connected in the same gateway process/profile |

Known technical debt is tracked in the public GitHub issue queue.

## What problem does this solve?

Hermes can already run in gateway conversations, and scheduled jobs can deliver back to an origin. The awkward workaround for long-running external work is a watcher or cron job that repeatedly polls until something changes.

Async threads invert that. The agent sets up a route once, the producer emits a signed event only when something meaningful happens, and Hermes wakes the mapped conversation.

Good fits:

- CI or deploy jobs reporting completion;
- long-running local scripts or background agents;
- GitHub or repository automation;
- home automation alerts;
- workflow/control-plane systems that should notify or resume a Hermes conversation without learning chat-platform APIs.

## Start here

- [`docs/QUICKSTART.md`](docs/QUICKSTART.md) — install/configure the plugin and run an agent-first signed demo.
- [`skills/async-thread-agent-tools/SKILL.md`](skills/async-thread-agent-tools/SKILL.md) — reusable guidance for Hermes agents using ATH tools.
- [`docs/EVENT_CONTRACT.md`](docs/EVENT_CONTRACT.md) — producer-facing `async-thread-event/v1` contract and JSON Schema.
- [`docs/BRIDGE_RECIPES.md`](docs/BRIDGE_RECIPES.md) — local jobs, repo/review lanes, task-board bridges, emit-command, lifecycle, trace, and prune recipes.
- [`docs/LOOP_EVENTS.md`](docs/LOOP_EVENTS.md), [`docs/LOOP_SIGNAL_INGESTION.md`](docs/LOOP_SIGNAL_INGESTION.md), and [`docs/LOOP_SCENARIO_HARNESS.md`](docs/LOOP_SCENARIO_HARNESS.md) — feedback-controller event shapes, signal-ingestion recipes, and CI-runnable loop scenarios.

## How the agent-first flow works

![Infographic showing the agent-first ATH workflow: user ask, model-facing tools, safe producer handoff, signed event validation, registry policy routing, and same conversation delivery](docs/assets/baoyu-async-thread-flow.png)

The diagram above is intentionally scoped to the current MVP: gateway-local dispatch, dispatch paths covered with mock adapter tests, and producer payload boxed as untrusted data.

1. A user asks Hermes from an existing gateway conversation to watch or report on long-running work.
2. Hermes uses model-facing ATH tools to create or reuse a listener and generate a safe producer handoff.
3. The plugin stores a durable `threadKey`, the captured Hermes `SessionSource`, allowed producer/event scope, policy, and a per-handle HMAC secret.
4. The producer receives paths/contracts such as `contract.json`, helper files, and `ATH_SECRET_FILE`; normal output does not expose the raw HMAC secret.
5. A producer sends `async-thread-event/v1` JSON to `POST /async-threads/v1/events` and signs the exact request body.
6. The receiver validates timestamp, route scope, HMAC, and de-dupe state.
7. Policy chooses either direct delivery or `agent_queue` continuation metadata, and the event is rendered back into the same mapped gateway conversation.
8. Event summary, subject, and payload are rendered as untrusted data before entering the agent session.

## Quick example

Normal user ask:

```text
watch this demo job and report back here when it finishes
```

Expected agent path:

1. call `ath_create_listener` for the current conversation;
2. call `ath_generate_producer_handoff` for the producer;
3. give the producer `ATH_SECRET_FILE`/contract paths, not the raw secret;
4. verify the signed event with `ath_trace_event` or `/ath trace`.

Manual `/ath listen` remains available for power users and debugging, but it is not the primary getting-started path.

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
- Listener creation writes a producer-facing `secret.txt` and `contract.json` under the Hermes profile data directory with restrictive permissions where supported; command/tool output shows paths, not the raw secret.

See [`docs/SECURITY.md`](docs/SECURITY.md) for more detail.

## Current implementation features

- plugin-local SQLite async-thread registry;
- model-facing tools for listener creation, inspection, retirement, tracing, and producer handoff generation;
- `/ath listen/list/inspect/status/events/trace/workflows/emit-command/rotate-secret/lifecycle/prune/pause/resume/retire/revoke` gateway commands for manual admin/debug;
- `async_threads` gateway platform receiver;
- signed `async-thread-event/v1` HTTP endpoint;
- de-dupe by producer/event id;
- idle-session wake and active-session queue behavior;
- privacy-safe dispatch diagnostics;
- opt-in `agent_queue` acknowledgements;
- compact long-running event rendering with `tailMode: none | compact | debug`;
- optional debounce/coalescing for routine same-thread updates;
- generic workflow-stage/candidate/evidence tracking with serial/parallel gate policy;
- terminal-event lifecycle policy for warning on stale enabled listeners or auto-retiring single-goal listeners after successful terminal delivery;
- producer-agnostic source-binding registry, inspection tools, board `task_events` dry-run transforms, and a config-gated native runner with durable outbox/cursor diagnostics for binding external workflow boards to existing listeners without Hermes cron or listener retargeting;
- Dynamic Workflows finalizer adapter helpers for registering `ath.listener.retire` cleanup handlers without coupling Dynamic Workflows core to ATH internals;
- explicit agent-queue continuation policy metadata, with fail-closed mode when hard Hermes core bounds are required;
- producer helper script for compact background-lane events;
- model-facing producer handoff generation for generic contracts, local emitter files, GitHub Actions recipes, and explicit debug emitters;
- benchmarkable synthetic agent-tool scenarios for PR review lanes, local long jobs, external producers, and debug/admin workflows;
- `loop.*` event contract, signal-ingestion recipes, approval/timeout/watchdog conventions, and a CI-runnable end-to-end loop-signal scenario harness for feedback-controller loops.

## Known limitations

- Gateway-local MVP: dispatch assumes the target platform adapter is connected in the same gateway process/profile.
- Non-Discord routing has unit coverage for shared send metadata, Telegram DM/topic metadata, and Slack-style generic thread metadata; live gateway smokes are still pending.
- Direct delivery, acknowledgement, and command notices share a centralized send-metadata helper, but the helper still wraps a private Hermes gateway function until the [stable continuation API](docs/design/STABLE_CONTINUATION_API.md) lands.
- Active-session queueing currently relies on Hermes gateway/adapter internals; the continuation API spike names the smallest core seam to remove that coupling.
- CLI and Hermes Desktop cannot create a listener from “here” yet; listener creation needs a live gateway origin, and no-source contexts fail closed.

## Dynamic Workflows finalizer adapter

Dynamic Workflows owns the backend-neutral resource/finalizer contract. ATH owns the concrete listener cleanup action. Use `async_threads.finalizers` to register the ATH handler with a Dynamic Workflows `ResourceFinalizerRegistry`:

```python
from async_threads.finalizers import register_ath_finalizers
from hermes_workflows import ResourceFinalizerRegistry

finalizers = ResourceFinalizerRegistry()
register_ath_finalizers(finalizers, registry=async_thread_registry, secret_root=secret_root)
```

The registered action is `ath.listener.retire`. It expects the workflow resource handle to contain `threadKey` or `thread_key`, disables the listener through the ATH registry, removes producer-facing secret artifacts, and returns bounded evidence without raw HMAC secrets. The adapter is idempotent for already-retired or absent listeners and can optionally enforce an `owner_user_id` match.

## Development

The repo is a Hermes plugin, not a standalone bot. Tests need Hermes gateway modules available. The local test harness auto-detects a sibling or profile-local Hermes checkout when present; set `HERMES_AGENT_PATH=/path/to/hermes-agent` if needed.

```bash
uv run pytest -q
```

## License

MIT — see [`LICENSE`](LICENSE).
