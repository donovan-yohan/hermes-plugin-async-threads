# Stable Hermes continuation API spike

Status: design spike for public-release phase 5
Issue: [#32](https://github.com/donovan-yohan/hermes-plugin-async-threads/issues/32)

## Recommendation

Add a small public continuation service to Hermes gateway core and migrate Async Threads to call that service instead of reading adapter internals.

The smallest useful seam is a gateway-owned object, exposed as `gateway.continuations`, with two async operations:

```python
await gateway.continuations.deliver(
    source,
    content,
    reply_to_message_id=None,
    notify=False,
)

await gateway.continuations.inject(
    source,
    event,
    active_policy="queue",
    merge_text=True,
)
```

The service should own:

- profile-aware target adapter resolution;
- platform-aware send metadata;
- session-key derivation;
- active-session queue/drain policy;
- safe conversion from runtime events into internal `MessageEvent` instances.

Async Threads should keep owning:

- HTTP listener lifecycle;
- HMAC authentication and replay/idempotency;
- producer/event-type authorization;
- event rendering/redaction;
- workflow state and event logging;
- coalescing policy.

Do **not** make producers call Hermes agent internals directly. Producers should keep emitting signed async-thread events; the plugin should be the boundary that validates and translates those events into a core continuation call.

## Current private coupling to remove

The plugin currently needs a gateway back-reference and then reaches through it into platform adapters. The exact coupling is:

| Current access | Why it exists | Proposed owner |
| --- | --- | --- |
| `adapter.gateway_runner` injected by Hermes plugin adapter creation | locate connected target adapters | gateway continuation service |
| `runner.adapters.get(source.platform)` | find the destination platform adapter | gateway continuation service, profile-aware |
| `target_adapter.send(source.chat_id, ..., metadata=...)` | visible direct delivery and acknowledgement notices | `continuations.deliver()` |
| `send_metadata_for_source()` wrapping Hermes `_thread_metadata_for_source()` | route Discord threads, Slack threads, Telegram DM topics | gateway continuation service |
| `build_session_key(source, group_sessions_per_user=..., thread_sessions_per_user=...)` | resolve the session that should receive an injected event | gateway continuation service |
| `target_adapter._active_sessions` | decide whether an agent is already running for the session | gateway continuation service or adapter public method |
| `target_adapter._pending_messages` | queue runtime follow-ups while the session is active | gateway continuation service or adapter public method |
| `merge_pending_message_event(...)` | merge queued text/media follow-ups safely | gateway continuation service or adapter public method |
| `target_adapter.handle_message(event)` | start/resume the agent turn when idle | gateway continuation service |
| `commands.py` direct `gateway.adapters.get(source.platform)` / `adapter.send(...)` | send `/ath` command notices back to the invoking thread | `continuations.deliver()` |
| `commands.py` direct `build_session_key(...)` plus `gateway.config.group_sessions_per_user/thread_sessions_per_user` | persist the listener's target session identity from `/ath listen` | gateway continuation service |
| plugin imports of `BasePlatformAdapter`, `MessageEvent`, `MessageType`, `SendResult`, and `SessionSource.from_dict(...)` | adapter subclassing, synthetic runtime events, and stored source hydration | stable gateway/plugin type surface |

The phase-4 routing helper reduced platform-metadata duplication, but it still wraps a private Hermes helper. That is a cleanup, not the final public API.

## Proposed API shape

### Types

```python
@dataclass(frozen=True)
class ContinuationDeliveryResult:
    outcome: Literal["delivered", "target_unavailable", "delivery_failed"]
    target_platform: str
    session_key: str | None = None
    message_id: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class ContinuationInjectResult:
    outcome: Literal[
        "agent_started",
        "queued_active_session",
        "rejected_active_session",
        "target_unavailable",
        "dispatch_failed",
    ]
    target_platform: str
    session_key: str | None
    active_session: bool
    queued: bool
    error: str | None = None
```

### `deliver(...)`

Use for user-visible output that does **not** start an agent turn: direct async-thread policy, acknowledgement notices, status notices, and future admin messages.

```python
async def deliver(
    self,
    source: SessionSource,
    content: str,
    *,
    reply_to_message_id: str | None = None,
    notify: bool = False,
) -> ContinuationDeliveryResult:
    ...
```

Required behavior:

1. Resolve the target adapter from `source.platform` and `source.profile`.
2. Build platform metadata from the same logic Hermes uses for normal replies.
3. Call the adapter send path.
4. Return a structured result; do not force plugins to parse `SendResult` internals.

This removes direct plugin use of `runner.adapters`, adapter `.send(...)`, and `_thread_metadata_for_source`.

### `inject(...)`

Use for an authenticated runtime event that should enter an existing conversation as an internal message.

```python
async def inject(
    self,
    source: SessionSource,
    event: MessageEvent,
    *,
    active_policy: Literal["queue", "reject"] = "queue",
    merge_text: bool = True,
) -> ContinuationInjectResult:
    ...
```

Required behavior:

1. Validate that `event.internal is True` for runtime injections.
2. Resolve the target adapter from `source.platform` and `source.profile`.
3. Apply topic recovery, if the adapter uses it, before session-key derivation.
4. Derive the session key using the gateway/adapter config.
5. If the session is active and `active_policy == "queue"`, queue/merge the event through the same mechanism normal inbound messages use.
6. If the session is idle, call the adapter's message-processing path.
7. Return a structured result including the session key hash/safe key metadata, active/queued state, and failure reason.

This removes plugin use of `build_session_key`, `_active_sessions`, `_pending_messages`, `merge_pending_message_event`, and direct `handle_message(...)` calls.

## Runtime event trust boundary

A runtime async-thread event is not a user message. Even with a valid HMAC signature:

- envelope fields are authenticated for producer identity and replay/idempotency only;
- `summary`, `subject`, and `payload` remain untrusted data;
- injected event text must preserve the existing warning that payload content is untrusted;
- `raw_message` should be compact allowlisted metadata, not the full original payload;
- runtime injections should not bypass user authorization as if they were slash commands;
- runtime injections should default to `active_policy="queue"`, not interrupt.

The continuation API should make this hard to misuse by requiring `MessageEvent.internal=True` or an explicit runtime-origin wrapper for `inject(...)`.

## Active-session policy

The default for async runtime events should be `queue`:

- If the target session is busy, append or merge the internal event into the pending slot.
- Do not interrupt a user-initiated agent turn by default.
- Do not route runtime events through active-session command bypass paths (`/stop`, `/approve`, clarify responses, etc.).
- Preserve existing queue/drain invariants: one active agent task per session key, pending follow-ups drain after the current turn, and text coalescing is owned by the adapter/core.

A future `reject` policy is useful for callers that only want idle-session dispatch, but Async Threads does not need interrupt semantics for public release.

## Platform and surface implications

### Discord, Telegram, Slack, and other gateway adapters

The continuation service should use the same metadata builder as normal gateway replies. That keeps details such as Telegram DM-topic `direct_messages_topic_id` and reply anchors out of plugins.

Phase 4 added unit evidence for metadata shape. A stable core API should replace that plugin-local wrapper with a public gateway method.

### CLI

The CLI has no always-on platform adapter in the same sense as gateway sessions. A local source can be represented as `Platform.LOCAL`, but a background HTTP receiver cannot assume there is a live terminal to write to.

For public release, the stable API should either:

- return `target_unavailable` for local/CLI sources when no live local adapter exists; or
- require an explicit local-session bridge owned by Hermes core.

Do not claim CLI end-to-end continuation until that bridge exists.

### API server and Hermes Desktop

API-server and desktop sessions need the same source/session-key abstraction, but they also need ownership rules:

- which process owns the live adapter/session;
- whether a continuation can cross process boundaries;
- how delivery results are surfaced to the UI;
- how profile-specific config and credentials are selected.

The MVP should stay gateway-local. Cross-process continuation should be a later delivery-router/outbox design, not hidden inside this plugin.

### Multi-gateway and multi-profile routing

`SessionSource.profile` already exists in Hermes core. The continuation service should use it when resolving target adapters. If the profile-specific adapter is not live in this process, return `target_unavailable` with a reason that the caller can log.

A future distributed/multi-gateway implementation can build on the same result shape by adding an outbox or remote delivery backend behind the service.

## Migration path for Async Threads

1. Add `GatewayContinuationService` in Hermes core with `deliver(...)` and `inject(...)`.
2. Expose it from `GatewayRunner` as `self.continuations`.
3. Move platform send metadata building behind `deliver(...)`.
4. Move session-key resolution and active-session queueing behind `inject(...)`.
5. Add Hermes core tests for:
   - Discord/thread metadata delivery;
   - Telegram DM-topic metadata delivery;
   - inactive-session injection calls adapter message handling;
   - active-session injection queues without interrupting;
   - `source.profile` adapter resolution;
   - target-unavailable results.
6. Migrate Async Threads `dispatch_event(...)`:
   - direct policy calls `gateway.continuations.deliver(...)`;
   - ack notices call `gateway.continuations.deliver(...)`;
   - agent-queue policy builds a sanitized internal `MessageEvent` and calls `gateway.continuations.inject(...)`.
7. Delete the plugin-local `send_metadata_for_source()` wrapper when Hermes exposes a public metadata path.

## Follow-up implementation issues

Recommended follow-ups after this spike:

1. **Hermes core:** add gateway continuation service with `deliver(...)` and `inject(...)`.
2. **Hermes core:** add continuation tests for active-session queueing and Telegram DM-topic metadata.
3. **Async Threads:** migrate `dispatch_event(...)` and ack/direct send paths to the public continuation service.
4. **Async Threads:** add live gateway smoke tests for at least Discord and one non-Discord adapter before claiming end-to-end support beyond metadata coverage.
5. **Async Threads:** keep CLI/API-server/Desktop marked unverified until Hermes has a live local/desktop/API continuation bridge or explicit `target_unavailable` contract.

## Non-goals

- No producer contract change. Producers still emit signed `async-thread-event/v1` JSON.
- No cross-process outbox in this issue.
- No new model tool.
- No broad gateway rewrite.
- No claim that Telegram, Slack, CLI, desktop, or API-server end-to-end continuation is fully supported by this spike.

## Acceptance check

This spike recommends a narrow seam: two continuation operations plus structured results. It removes the current private coupling without changing producer behavior or widening Hermes' model-facing tool surface.
