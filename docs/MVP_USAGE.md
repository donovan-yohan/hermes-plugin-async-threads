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
/ath listen relay --events relay.lane.started,relay.lane.progress,relay.lane.finished,relay.lane.failed --debounce 45  # coalesce routine lane noise while letting terminal states through
```

`--ack` is ignored for `--policy direct`; direct delivery is already visible when it succeeds.

`--debounce` is optional and only applies to `agent_queue` listeners. Routine `started`/`progress` events for the same thread are held for the debounce window and delivered as one compact digest. Terminal/priority events bypass the window and flush any pending digest immediately: `finished`/completed/succeeded states, failures/errors, `blocked`, `needs_attention`, payload states/verdicts with those values, or events that explicitly request `tailMode: debug`.

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
- `coalesced_pending`: a routine event was accepted and is waiting in a debounce window before a compact digest wake.

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
  "tailMode": "compact",
  "payload": {"safe": "untrusted data only"}
}
```

### Long-running lane event shape

For profile/background-agent producers, prefer compact state-transition payloads instead of raw command tails:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "lane-issue17-finished-001",
  "eventType": "relay.lane.finished",
  "producer": {"id": "relay-ath-dev"},
  "occurredAt": "2026-06-18T12:34:56Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "issue #17 implementation finished and is ready for review",
  "tailMode": "compact",
  "subject": {"repo": "donovan-yohan/hermes-plugin-async-threads", "issue": 17},
  "payload": {
    "profile": "ebi",
    "lane": "issue17-compact-events",
    "verdict": "passed",
    "head_sha": "13df23b",
    "pr_url": "https://github.com/donovan-yohan/hermes-plugin-async-threads/pull/20",
    "changed_files": ["async_threads/rendering.py", "tests/test_adapter.py"],
    "verification": "39 passed",
    "log_path": "/path/to/lane.log"
  }
}
```

Recommended phase payloads:

- `relay.lane.started`: subject fields `profile`, `lane`, `issue`/`pr`, optional `pid` or `delegation_id`, and `log_path`.
- `relay.lane.progress`: one meaningful milestone in payload; avoid heartbeat spam. Producers should cap this to 1–2 routine progress events per lane before a terminal state.
- `relay.lane.finished`: subject fields `head_sha`/PR/comment URLs when relevant, plus payload fields such as `verdict`, `changed_files`, `verification`, and telemetry.
- `relay.lane.failed`: subject `log_path` plus payload fields such as failure class, sanitized error summary, and retryability hint.

For listeners with `--debounce`, multiple same-thread `started`/routine `progress` events become one `async_threads.coalesced` digest payload. Terminal/high-priority events (`finished`, `failed`, `blocked`, `needs_attention`, explicit `tailMode: debug`) bypass debounce and flush any pending routine events immediately so failures and completion states are not hidden behind the timer.

`tailMode` controls raw tail handling for payload keys such as `tail`, `stdout`, `stderr`, `output`, and `transcript`:

- `compact` (default): omit raw tails and replace them with char/line counts plus a log-path hint.
- `none`: omit raw tails entirely.
- `debug`: include a redacted tail capped to a hard debug limit; use only for explicit debugging.

Large transcripts and oversized payload strings should be saved to logs and referenced by `log_path`, not injected into the conversation. In compact/none modes, oversized string fields are summarized with counts; `debug` includes only capped redacted text.

### Profile-lane helper

For background profile lanes, use the producer helper instead of hand-rolled JSON/HMAC glue when possible. The recommended `run profile lane -> emit ATH` flow is:

1. create or reuse an `/ath listen` handle for the origin thread;
2. start the background lane under the intended Hermes/profile process;
3. emit `relay.lane.started`, sparse meaningful `relay.lane.progress`, and one terminal `relay.lane.finished` or `relay.lane.failed` event;
4. keep bulky transcripts in the lane log referenced by `log_path`, not in ATH payload text.

Use `/ath status` to copy the live registry path for the profile you are running in. Example CLI usage:

```bash
python scripts/ath-profile-lane.py \
  --registry ~/.hermes/profiles/ebi/data/async-threads/registry.sqlite3 \
  --thread-key ath_... \
  --type relay.lane.started \
  --profile ebi \
  --lane issue19-docs \
  --issue '#19' \
  --log-path /tmp/ath/issue19.log \
  --summary 'issue #19 lane started'

python scripts/ath-profile-lane.py \
  --registry ~/.hermes/profiles/ebi/data/async-threads/registry.sqlite3 \
  --thread-key ath_... \
  --type relay.lane.finished \
  --profile ebi \
  --lane issue19-docs \
  --issue '#19' \
  --pr '23' \
  --head abc1234 \
  --log-path /tmp/ath/issue19.log \
  --telemetry-json '{"runtime_seconds": 123, "tokens": 4567}' \
  --payload-json '{"verification": "tests passed"}' \
  --summary 'issue #19 lane finished'
```

The Python API is the same seam for non-shell producers:

```python
from async_threads.profile_lane import build_profile_lane_event, emit_signed_event

event = build_profile_lane_event(
    thread_key="ath_...",
    producer_id="relay-ath-dev",
    event_type="relay.lane.progress",
    profile="ebi",
    lane="issue19-docs",
    summary="tests are running",
    telemetry={"runtime_seconds": 42, "tokens": 1200},
    payload={"verification": "pytest in progress"},
    log_path="/tmp/ath/issue19.log",
)
emit_signed_event(url="http://127.0.0.1:8765/async-threads/v1/events", event=event, secret=secret)
```

Sample phase payloads:

```json
{"eventType":"relay.lane.started","tailMode":"compact","subject":{"profile":"ebi","lane":"issue19-docs","issue":"#19","log_path":"/tmp/ath/issue19.log"},"payload":{"phase":"started"}}
```

```json
{"eventType":"relay.lane.progress","tailMode":"compact","subject":{"profile":"ebi","lane":"issue19-docs","issue":"#19","log_path":"/tmp/ath/issue19.log"},"payload":{"phase":"progress","milestone":"tests-running","telemetry":{"runtime_seconds":42,"tokens":1200}}}
```

```json
{"eventType":"relay.lane.finished","tailMode":"compact","subject":{"profile":"ebi","lane":"issue19-docs","issue":"#19","pr":"23","head":"abc1234","log_path":"/tmp/ath/issue19.log","status":"passed"},"payload":{"phase":"finished","verification":"49 passed","telemetry":{"runtime_seconds":123,"tokens":4567}}}
```

```json
{"eventType":"relay.lane.failed","tailMode":"compact","subject":{"profile":"ebi","lane":"issue19-docs","issue":"#19","log_path":"/tmp/ath/issue19.log","status":"failed"},"payload":{"phase":"failed","failure_class":"test_failure","retryable":true,"telemetry":{"runtime_seconds":80,"tokens":2300}}}
```

The helper can also run without registry access by passing `--producer-id` and setting `ATH_SECRET` (or another env var via `--secret-env`). It prints the receiver response, never the HMAC secret. Cron/watchers are fallback plumbing only; the happy path is profile/background-lane execution plus compact ATH events.

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
