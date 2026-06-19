# MVP usage: async-thread listener slice

This repo currently ships a first MVP slice for gateway-local async-thread continuation.

## What works

- `/ath listen <producer>` from an existing Hermes gateway conversation captures the current `SessionSource` as an async-thread handle.
- `POST /async-threads/v1/events` accepts `async-thread-event/v1` JSON.
- Events are HMAC-SHA256 authenticated with the per-handle secret.
- Events are de-duped by `(producerId, eventId)`.
- Idle target sessions are woken by synthetic internal `MessageEvent` injection into the stored source.
- Active target sessions are queued into the target adapter's pending-message queue instead of interrupting the running turn.
- `direct` policy can send a notification without invoking the agent, but the intended dogfood path is `agent_queue`.

## Install/config shape

Install/enable the plugin in the Hermes profile, then enable the platform adapter:

```yaml
plugins:
  enabled:
    - async-threads

platforms:
  async_threads:
    enabled: true
    extra:
      host: "127.0.0.1"
      port: 8765
      # Optional for reverse-proxy/public test setups:
      # public_url: "https://example.com"
      # registry_path: "/absolute/path/to/registry.sqlite3"
```

Restart the gateway after config/plugin changes.

## Discord/gateway command

In the thread/channel to wake later:

```text
/ath listen relay --events relay.session.pr_opened,relay.session.needs_attention --label "relay dogfood"
```

Acknowledgements are opt-in for `agent_queue` listeners:

```text
/ath listen relay --events relay.session.pr_opened --ack none   # default, silent
/ath listen relay --events relay.session.pr_opened --ack brief  # one compact visible notice
/ath listen relay --events relay.session.pr_opened --ack debug  # safe diagnostic notice
```

`--ack` is ignored for `--policy direct`; direct delivery is already visible when it succeeds.

The command replies with:

- `threadKey`
- receiver URL
- generated HMAC secret, shown once

Management and diagnostics commands:

```text
/ath status
/ath list
/ath events [thread_key] [--limit N]
/ath inspect <thread_key>
/ath pause <thread_key>
/ath resume <thread_key>
/ath revoke <thread_key>
```

`/ath status` prints the receiver URL, live registry path, listener count, and recent event count for the current user. `/ath events` shows compact recent event rows with redacted summaries for authenticated events; rejected events do not echo producer-supplied summaries. Secrets, HMACs, tokens, cookies, raw credentials, and full payload bodies are not printed.

The registry schema stores optional `event_log.detail_json` for structured diagnostics. Details are allowlisted and sanitized before persistence; unsafe keys such as `secret`, `token`, `authorization`, `cookie`, `signature`, `payload`, `body`, `raw`, and credential-like fields are omitted or redacted.

Dispatch diagnostics currently include only privacy-safe operator metadata: target platform, gateway-runner/target-adapter presence, policy, acknowledgement mode/success/failure, whether a session key was present, a short hash of the resolved session key, active-session/queue state, whether `handle_message` was called/returned, direct-send success, and sanitized exception class/message. These fields say what the plugin knows about initial dispatch; they do not prove final user-visible delivery.

Current operator-facing outcomes:

- `agent_started`: idle `agent_queue` handed an internal event to the target platform adapter; this is not final visible delivery.
- `queued_active_session`: `agent_queue` event merged into the target adapter pending-message queue because the session was already active.
- `direct_delivered`: `direct` policy send returned success from the target platform adapter.
- `dispatch_failed`, `duplicate`, and `rejected_*`: failure/de-dupe/auth scope states.

For producer compatibility, HTTP response bodies still use the older initial status strings for the three successful handoff states: `accepted`, `queued`, and `delivered`.

## Event shape

```json
{
  "version": "async-thread-event/v1",
  "eventId": "evt_001",
  "eventType": "relay.session.pr_opened",
  "producer": {"id": "relay"},
  "occurredAt": "2026-06-18T12:34:56Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "Relay session opened a PR and is ready for review.",
  "subject": {"repo": "donovan-yohan/relay-ide", "pr": 123},
  "payload": {"safe": "untrusted data only"}
}
```

Sign the exact request body:

```text
X-Hermes-Signature-256: sha256=<hmac_sha256_hex(body, secret)>
```

## Security notes

- Payload text is rendered under an untrusted-data boundary before entering the agent session.
- Secret-shaped text in event summaries, subjects, payloads, visible acknowledgements, event diagnostics, and stored event summaries is redacted before those surfaces are rendered or persisted. HMAC verification still signs the exact raw request body; redaction only applies after validation to derived operator/agent-facing text.
- The MVP stores per-handle HMAC secrets in plugin-local SQLite because the receiver needs to validate inbound events.
- `/ath listen` uses the `pre_gateway_dispatch` hook to capture the current thread source. That hook runs before normal gateway auth, so the implementation explicitly defers to `_is_user_authorized(source)` when available.
- This is gateway-local: the receiver assumes the target platform adapter is connected in the same gateway process/profile.
