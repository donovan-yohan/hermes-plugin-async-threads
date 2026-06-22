# Async-thread event contract

This is the producer-facing contract for `async-thread-event/v1`. Use it when building a bridge, webhook adapter, local job reporter, CI notifier, home-automation bridge, or any other generator that wakes an existing Hermes conversation.

The event can wake a mapped Hermes thread because the route is authenticated. The event content is still untrusted data. Never put instructions for the agent in `summary`, `subject`, or `payload`; put state facts there.

## Endpoint

```text
POST /async-threads/v1/events
Content-Type: application/json
X-Hermes-Signature-256: sha256=<hex_hmac_sha256>
```

Accepted signature header names, in lookup order:

1. `X-Hermes-Signature-256`
2. `X-Hermes-Signature`
3. `X-Hub-Signature-256`

Sign the exact UTF-8 request body bytes with the HMAC secret referenced by the listener's generated `secret.txt` file. The generated file is written without a trailing newline; use its exact file text as the HMAC key:

```text
hex_hmac_sha256 = HMAC-SHA256(secret, raw_request_body)
```

Do not reformat the JSON after signing. Even a whitespace or key-order change changes the signature.

## Minimal event

```json
{
  "version": "async-thread-event/v1",
  "eventId": "build-123-finished",
  "eventType": "ci.build.finished",
  "producer": {"id": "example-ci"},
  "occurredAt": "2026-06-20T19:00:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "build 123 finished",
  "tailMode": "compact",
  "subject": {"repo": "example/repo", "build": 123},
  "payload": {"status": "passed", "artifact": "build-123"}
}
```

## Required fields

| Field | Type | Contract |
| --- | --- | --- |
| `version` | string | Must be exactly `async-thread-event/v1`. |
| `eventId` | string | Stable id from the producer. The same producer must reuse the same id when retrying the same event. Keep it non-secret and under 200 characters. |
| `eventType` | string | Producer-defined event type, for example `ci.build.finished`. Keep it non-secret and under 200 characters. Listener allowlists match this exact string. |
| `producer.id` | string | Stable producer id that matches the `/ath listen <producer>` handle. Keep it non-secret and under 200 characters. |
| `occurredAt` | ISO-8601 string, epoch seconds string, or number | Event timestamp. The receiver rejects events outside the replay window, currently defaulting to 5 minutes. |
| `asyncThread.threadKey` | string | Thread key returned by `/ath listen`, for example `ath_...`. |

## Recommended fields

| Field | Type | Contract |
| --- | --- | --- |
| `summary` | string | One short human-readable fact about what changed. The receiver caps this at 2000 characters before internal use. |
| `subject` | object | Compact metadata about what the event is about: repo, job id, PR number, host, device, build id, etc. Use values that help route human attention. |
| `payload` | object | Compact state facts for the producer-specific event. Prefer status, verdict, artifact ids, URLs, paths, counts, and short messages. |
| `tailMode` | `compact`, `none`, or `debug` | Controls how large output-like fields are rendered before entering the agent context. Defaults to `compact`. |

`subject` and `payload` are intentionally flexible. They are for arbitrary bridges and generators, but they must stay JSON-safe, compact, and non-secret.

## Optional workflow fields

Use these when a producer wants Hermes to track a long-running workflow or gate sequence, not just wake a thread once.

| Field | Type | Contract |
| --- | --- | --- |
| `workflowId` | string | Stable workflow/run id. May also be provided as `workflow.id`, `workflow.workflow_id`, `subject.workflow_id`, or `subject.workflowId`. |
| `stage` | string | Current workflow stage. Common stages: `started`, `progress`, `ready_for_review`, `review_requested`, `review_passed`, `review_failed`, `candidate_ready`, `qa_requested`, `qa_passed`, `qa_failed`, `blocked`, `needs_attention`, `released`, `cancelled`. |
| `artifact` | object | The thing being moved through gates, for example `{ "kind": "pull_request", "id": "37", "url": "...", "revision": "abc123" }`. |
| `candidate` | object | Readiness candidate state, for example `{ "id": "pr-37", "kind": "pull_request", "readiness": "forming" }`. |
| `evidence` | object | Gate evidence. Include `kind` and `status`; status is normalized to `passed`, `failed`, `stale`, or `unknown`. |
| `seriesKey` | string | Optional stable series key for repeated events about the same logical artifact, for example `github-pr:owner/repo:37`. |
| `supersedesEventId` | string | Optional previous event id in the same series that this event supersedes. |

Workflow fields are sanitized before persistence. They are still producer-controlled data, not agent instructions.

## Repeated artifact and supersession convention

Use a stable `seriesKey` when multiple event ids describe revisions of the same logical artifact. Put the current revision on the artifact itself, usually as `artifact.revision` or `subject.artifact.revision`.

```json
{
  "version": "async-thread-event/v1",
  "eventId": "pr-37-head-b-review-passed",
  "eventType": "code.review.passed",
  "producer": {"id": "example-ci"},
  "occurredAt": "2026-06-20T19:00:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "review passed for PR 37 at head b",
  "seriesKey": "github-pr:example/repo:37",
  "supersedesEventId": "pr-37-head-a-review-requested",
  "workflowId": "pr-37",
  "stage": "review_passed",
  "artifact": {"kind": "pull_request", "id": "37", "url": "https://example.invalid/repo/pull/37", "revision": "bbbbbbbb"},
  "candidate": {"id": "pr-37", "readiness": "ready"},
  "evidence": {"kind": "review", "status": "passed"}
}
```

Consumers should still verify the live artifact revision before taking irreversible action. The series fields are a producer/consumer convention for stale-event handling; they do not make stale payload text trustworthy.

## Tail modes and large output fields

The renderer treats these payload keys as output/tail-like fields:

```text
tail, rawtail, raw_tail, stdout, stderr, output, fulloutput, full_output,
commandoutput, command_output, transcript, rawtranscript, raw_transcript
```

Behavior:

| `tailMode` | Behavior |
| --- | --- |
| `compact` | Default. Raw output-like fields are omitted and replaced with a compact summary plus a hint to include a log path. |
| `none` | Output-like fields are omitted. Use for routine state transitions. |
| `debug` | Includes a capped, redacted tail for explicit debugging. Do not use for routine high-volume events. |

For normal bridges, prefer:

```json
{
  "payload": {
    "status": "failed",
    "log_path": "/var/log/jobs/build-123.log",
    "tail": "...large output omitted by compact mode..."
  }
}
```

Raw logs, terminal bytes, transcripts, secrets, tokens, cookies, and authorization headers do not belong in routine events.

## Agent-queue continuation bounds

Model-created `agent_queue` listeners store a continuation policy with intended `maxTurns`, `maxToolCalls`, `timeoutSeconds`, and optional toolsets. Current Hermes core does not expose a plugin-local hard cap for an individual synthetic gateway event, so runtime diagnostics and `MessageEvent.raw_message` mark this as `coreEnforced: false` until that core seam exists. If a listener is configured with `failClosedWithoutCoreBounds`, the receiver rejects the signed event with a retryable dispatch failure instead of starting an unbounded continuation.

This is deliberately visible in trace/event diagnostics; producers should treat a `502` from strict bounded mode as retryable after operator/config remediation, not as an auth failure.

## Idempotency and retries

Idempotency means the producer can retry safely without creating duplicate visible work.

- The de-dupe key is `(producer.id, eventId)`.
- Reuse the same `eventId` when retrying the same real-world event.
- Use a new `eventId` when the real-world event changes.
- If immediate dispatch fails, the receiver clears the seen marker so the producer can retry.
- If the event is coalesced, the current MVP accepts it into a debounce bucket before final digest dispatch. Retry semantics for failed coalesced digest dispatch are being hardened in issue #30.
- After successful delivery or queueing, a retry returns `duplicate`.

## Response shapes

| HTTP status | Body shape | Meaning |
| --- | --- | --- |
| `200` | `{ "status": "delivered", "threadKey": "ath_..." }` | Direct-delivery policy succeeded. |
| `202` | `{ "status": "accepted", "threadKey": "ath_..." }` | Agent continuation started or was accepted. |
| `202` | `{ "status": "queued", "threadKey": "ath_..." }` | Event was queued behind an active session or debounce/coalescing window. |
| `200` | `{ "status": "duplicate", "threadKey": "ath_..." }` | The producer/event id was already accepted after final handling. |
| `400` | `{ "error": "..." }` | Invalid JSON, unsupported version, missing fields, or bad timestamp. |
| `401` | `{ "error": "invalid signature" }` | Missing handle, disabled handle, invalid HMAC, wrong producer, or disallowed event type. Auth/scope errors intentionally use the same public error. |
| `413` | server-generated body-too-large response | The request exceeded the configured receiver body limit before event parsing. |
| `502` | `{ "error": "event dispatch failed" }` | Auth passed, but delivery/queueing failed. Retry with the same `eventId` unless your bridge has stronger recovery logic. |

## Bridge/generator checklist

A producer bridge should do all of this:

- Store `threadKey`, receiver URL, producer id, allowed event types, and HMAC secret outside the event payload.
- Build the JSON body once, encode it as UTF-8 bytes, and sign those exact bytes.
- Generate stable `eventId` values from the upstream event id, job id, commit sha, run id, or attempt id.
- For repeated events about one artifact, include `seriesKey` plus `artifact.revision` or `subject.artifact.revision`; use `supersedesEventId` when the producer knows the previous event id.
- Keep `summary` under one or two sentences.
- Put compact routing metadata in `subject`.
- Put compact state facts in `payload`.
- Use `tailMode: compact` by default.
- Retry `502` and transport failures with the same `eventId`.
- Treat `duplicate` as success for the same real-world event.
- Never include secrets, credentials, cookies, raw headers, long logs, or prompt text telling Hermes what to do.

## JSON Schema

A permissive JSON Schema for producer-side validation lives at [`schemas/async-thread-event-v1.schema.json`](schemas/async-thread-event-v1.schema.json).

The schema validates the stable envelope. It intentionally allows additional producer-specific keys so bridges can model arbitrary workflows without waiting for plugin changes.

## Related docs

- [`QUICKSTART.md`](QUICKSTART.md): install the plugin, create a listener, and send a signed demo event.
- [`SECURITY.md`](SECURITY.md): trust boundary, HMAC auth, replay/de-dupe, and payload safety model.
