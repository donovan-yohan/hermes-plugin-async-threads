# Quickstart

This quickstart exercises the current gateway-local MVP. It assumes a Hermes gateway process is running with the target platform adapter connected in the same profile/process as the `async_threads` receiver.

## Prerequisites

- Hermes Agent with gateway plugin support.
- This plugin installed in the active Hermes profile.
- `aiohttp` available in the Hermes runtime environment.
- A supported gateway conversation where you can send `/ath` commands. The tested MVP path is Discord-shaped gateway sessions.

## Enable the plugin

For a source checkout/directory install, copy or clone this repository into the active Hermes profile's plugin directory so the plugin directory contains both `plugin.yaml` and root `__init__.py`. Then enable the plugin and platform adapter in the profile config.

The project also declares a `hermes_agent.plugins` entry point for packaged installs, but source checkout/copy is the most explicit path until a published package exists.

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
      # Optional when exposing through a reverse proxy:
      # public_url: "https://example.com"
      # Optional explicit registry path:
      # registry_path: "/absolute/path/to/registry.sqlite3"
```

Restart the Hermes gateway after plugin/config changes. Plugins are loaded during gateway startup.

Verify the receiver is reachable:

```bash
curl -fsS http://127.0.0.1:8765/async-threads/v1/health
```

Expected shape:

```json
{"ok": true, "platform": "async_threads"}
```

## Create a listener

From the gateway conversation you want to wake later:

```text
/ath listen demo --events demo.job.finished --ack brief --label "demo async job"
```

The command response includes:

- `threadKey`
- receiver URL
- `secretFile` path
- `contractFile` path

Save the `threadKey` and pass the `secretFile` path to the producer through a local secret manager, mounted file, or `ATH_SECRET_FILE`. The raw secret is not printed in normal command/tool output.

Useful management commands:

```text
/ath status
/ath list
/ath events [thread_key] [--limit N]
/ath trace <event_id> [--json]
/ath workflows [thread_key] [--limit N]
/ath inspect <thread_key>
/ath emit-command <thread_key> --event event.type [--summary text]
/ath rotate-secret <thread_key>
/ath lifecycle [thread_key]
/ath prune [--dry-run|--force] [--event-log-days N] [--seen-days N]
/ath pause <thread_key>
/ath resume <thread_key>
/ath retire <thread_key>
/ath revoke <thread_key>
```

## Send a signed demo event

Replace the environment variables with values from `/ath listen`.

```bash
export ATH_URL="http://127.0.0.1:8765/async-threads/v1/events"
export ATH_THREAD_KEY="ath_..."
export ATH_SECRET_FILE="/path/from/ath-listen/secret.txt"

python3 - <<'PY'
import hashlib
import hmac
import json
import os
import time
import urllib.request

url = os.environ["ATH_URL"]
thread_key = os.environ["ATH_THREAD_KEY"]
secret = open(os.environ["ATH_SECRET_FILE"], encoding="utf-8").read().strip()

body = {
    "version": "async-thread-event/v1",
    "eventId": f"demo-{int(time.time())}",
    "eventType": "demo.job.finished",
    "producer": {"id": "demo"},
    "occurredAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "asyncThread": {"threadKey": thread_key},
    "summary": "demo job finished",
    "payload": {"status": "passed", "artifact": "build-123"},
}
raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
signature = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
request = urllib.request.Request(
    url,
    data=raw,
    method="POST",
    headers={
        "Content-Type": "application/json",
        "X-Hermes-Signature-256": f"sha256={signature}",
    },
)
with urllib.request.urlopen(request, timeout=20) as response:
    print(response.status)
    print(response.read().decode("utf-8", "replace"))
PY
```

Expected successful response:

- `202` with `{"status":"accepted", ...}` for idle `agent_queue` continuation;
- `202` with `{"status":"queued", ...}` if the target session is currently active;
- `200` with `{"status":"delivered", ...}` for `--policy direct` listeners.

With `--ack brief`, the mapped gateway conversation should also receive a compact visible acknowledgement before the continuation is handed off or queued.

## Event fields

This section shows the short version. The complete producer-facing contract is [`EVENT_CONTRACT.md`](EVENT_CONTRACT.md), with a permissive JSON Schema at [`schemas/async-thread-event-v1.schema.json`](schemas/async-thread-event-v1.schema.json). Bridge and operator recipes live in [`BRIDGE_RECIPES.md`](BRIDGE_RECIPES.md).

Required fields:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "stable-id-from-producer",
  "eventType": "demo.job.finished",
  "producer": {"id": "demo"},
  "occurredAt": "2026-06-20T19:00:00Z",
  "asyncThread": {"threadKey": "ath_..."}
}
```

Recommended fields:

- `summary`: short human-readable status.
- `subject`: compact routing/subject metadata.
- `payload`: compact producer-specific data.
- `tailMode`: `compact`, `none`, or `debug`.
- `workflowId`, `stage`, `artifact`, `candidate`, `evidence`: optional workflow-state fields.
- `seriesKey`, `supersedesEventId`, and `artifact.revision`: optional repeated-artifact/stale-event convention fields.

Payload text is untrusted data. Do not put raw logs, transcripts, secrets, terminal bytes, or user-authored instructions in event payloads. Use the bridge/generator checklist in [`EVENT_CONTRACT.md`](EVENT_CONTRACT.md) when building a producer.

## Compact long-running event pattern

For long-running jobs or background agent lanes, prefer sparse state transitions:

```json
{
  "version": "async-thread-event/v1",
  "eventId": "job-123-finished",
  "eventType": "job.finished",
  "producer": {"id": "demo"},
  "occurredAt": "2026-06-20T19:00:00Z",
  "asyncThread": {"threadKey": "ath_..."},
  "summary": "job finished and is ready for review",
  "tailMode": "compact",
  "subject": {"job": "job-123", "log_path": "/tmp/job-123.log"},
  "payload": {"status": "passed", "verification": "tests passed"}
}
```

Use `tailMode: debug` only for explicit debugging. Debug tails are redacted and capped, but compact state plus a log path is safer for routine events.

## Current limitations

- Listener creation is a gateway chat command, not a generic CLI/Desktop command.
- The receiver assumes the target platform adapter is connected in the same gateway process/profile.
- Non-Discord gateway support is intended but needs compatibility tests.
- Direct-delivery routing metadata still needs a stable platform-aware continuation helper.
