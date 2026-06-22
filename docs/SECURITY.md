# Security model

`hermes-plugin-async-threads` accepts events from external producers and can wake existing Hermes gateway conversations. Treat every producer payload as hostile even when the route is authenticated.

## Trust boundary

A valid HMAC proves that a registered producer knew the listener secret. It does not make event summary, subject, payload, logs, or public issue/comment text trustworthy instructions.

Agent-facing messages render event data under an explicit untrusted-data boundary. Route policy and prompt framing must come from trusted plugin code/config, not producer-controlled payload text.

## Event authentication

- Producers sign the exact request body with HMAC-SHA256.
- Header shape: `X-Hermes-Signature-256: sha256=<hex>`.
- Each listener has a per-handle secret generated when the listener is created.
- Listener creation writes producer-facing secret material to a profile data path such as `~/.hermes/profiles/<profile>/data/async-threads/emitters/<threadKey>/secret.txt`; normal command/tool output shows the file/contract paths, not the raw secret. The generated `secret.txt` is written without a trailing newline.
- On POSIX systems the generated secret and contract files are written with `0600` file mode, and parent directories are made private where supported.
- Rotating an active listener secret updates SQLite and overwrites the producer-facing secret file. Disabled listeners cannot be rotated. Revoked/retired listeners reject future events with the generic auth error, and inspection does not recreate removed producer-facing secret artifacts.

## Replay and de-dupe

- Events must include `occurredAt`.
- Non-standard JSON constants such as `NaN` and non-finite timestamps are rejected.
- Events outside the replay window are rejected.
- Accepted events are de-duped by producer and event id.

## Route scope

Listeners bind a generated `threadKey` to a captured Hermes `SessionSource`, producer id, optional allowed event types, policy, owner, and secret.

A producer should not be able to choose arbitrary platform/chat/thread ids. It targets a handle that Hermes created or that an operator explicitly configured.

## Payload handling

Do not send:

- API keys, bearer tokens, cookies, signatures, passwords, credentials;
- raw terminal output or transcripts;
- browser cookies or environment dumps;
- public issue/comment body text as instructions;
- unbounded logs.

Prefer compact state fields and external log paths. Use `tailMode: compact` or `tailMode: none` for routine events. Use `tailMode: debug` only when explicitly debugging; debug output is still capped and redacted.

Redaction is defense-in-depth, not permission to send secrets. The plugin redacts common secret-bearing keys plus common bare token shapes such as AWS access keys, GitHub tokens, OpenAI keys, Slack tokens, JWT-like values, HMAC signatures, session keys, and PEM private-key blocks before producer text reaches prompts, logs, or command output. Producers must still keep secrets out of event payloads.

## Local persistence

The MVP stores per-handle HMAC secrets in plugin-local SQLite because the receiver needs to validate inbound events. Listener creation also writes a producer-facing `secret.txt`/`contract.json` pair under the Hermes profile data directory; operator-facing output should pass those paths to producers instead of pasting raw secrets into chat, prompts, logs, or issue comments.

Authenticated accepted events, duplicates, dispatch failures, and authenticated scope or disabled-handle rejections may write sanitized event-log rows. Unauthenticated probes are rejected with generic responses and are not persisted as event-log rows.

Use `/ath prune` to dry-run or apply owner-scoped cleanup for old event-log rows and replay/de-dupe markers. Defaults can be set with `event_log_retention_days` and `seen_event_retention_days` in the `async_threads` platform config. Keep seen-event retention longer than the operational retry window you want to protect.

## Current hardening work

The public-release readiness epic tracks remaining hardening before broad shareout:

- add live non-Discord gateway smoke evidence before claiming end-to-end support beyond metadata routing coverage.
- replace private gateway/adapter coupling with the stable continuation API proposed in [`docs/design/STABLE_CONTINUATION_API.md`](design/STABLE_CONTINUATION_API.md).
- expand retention automation beyond the explicit owner-scoped `/ath prune` command if real-world usage requires background cleanup.

See https://github.com/donovan-yohan/hermes-plugin-async-threads/issues/33
