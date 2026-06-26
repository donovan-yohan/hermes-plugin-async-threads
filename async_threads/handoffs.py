"""Producer handoff artifacts for signed async-thread event emitters."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .privacy import redact_metadata_text
from .registry import AsyncThreadHandle
from .secrets import describe_secret_artifact


HANDOFF_VERSION = "async-thread-producer-handoff/v1"
DYNAMIC_WORKFLOW_EVENT_TYPES = ["loop.started", "loop.waiting_for_event", "loop.step_completed", "loop.converged", "loop.halted"]
DYNAMIC_WORKFLOW_OPTIONAL_EVENT_TYPES = ["loop.waiting_for_approval", "loop.stalled"]


def build_producer_handoff(
    handle: AsyncThreadHandle,
    *,
    event_url: str,
    secret_root: str | Path | None = None,
    handoff_root: str | Path | None = None,
    mode: str = "generic_contract",
    event_type: str = "",
    create_files: bool = True,
    include_sensitive_secret: bool = False,
) -> dict[str, Any]:
    """Build a producer-facing handoff for one active listener.

    The default result is safe for normal model/tool output: it contains file
    references and copyable snippets, but not the literal HMAC secret.
    """

    normalized_mode = _normalize_mode(mode)
    default_event_type = _select_event_type(handle, event_type)
    secret_ref = describe_secret_artifact(handle, event_url=event_url, root=secret_root, ensure=True)
    minimal_event = _minimal_event(handle, event_type=default_event_type)
    payload: dict[str, Any] = {
        "ok": True,
        "version": HANDOFF_VERSION,
        "mode": normalized_mode,
        "threadKey": handle.thread_key,
        "producerId": redact_metadata_text(handle.producer_id),
        "allowedEventTypes": [redact_metadata_text(item) for item in handle.allowed_event_types],
        "defaultEventType": redact_metadata_text(default_event_type),
        "endpoint": {"url": event_url, "method": "POST"},
        "secretRef": secret_ref,
        "exampleEvent": minimal_event,
        "contract": _contract_summary(handle, event_url=event_url, event_type=default_event_type, secret_ref=secret_ref),
        "retryDeduping": _retry_guidance(),
        "lifecycle": _lifecycle_guidance(handle),
        "safety": {
            "rawSecretReturned": False,
            "eventPayloadsAreUntrustedData": True,
            "doNotPutSecretsInEvents": True,
        },
    }

    if normalized_mode == "dynamic_workflows":
        payload["dynamicWorkflows"] = _dynamic_workflows_recipe(handle, event_url=event_url, secret_ref=secret_ref)
    elif normalized_mode == "github_actions":
        payload["githubActions"] = _github_actions_recipe(handle, event_url=event_url, event_type=default_event_type, secret_ref=secret_ref)
    elif normalized_mode == "local_script":
        payload["localScript"] = _local_script_recipe(secret_ref=secret_ref)
    elif normalized_mode == "debug_curl":
        payload["debugCurl"] = _debug_curl(secret_ref=secret_ref, event_url=event_url, handle=handle, event_type=default_event_type)
        payload["debugCurl"]["sensitive"] = False
        payload["debugCurl"]["requiresExplicitSensitiveOutput"] = True
        if include_sensitive_secret:
            payload["safety"]["rawSecretReturned"] = True
            payload["debugCurl"]["sensitive"] = True
            payload["debugCurl"]["warning"] = "contains literal signing secret; do not paste into chat, logs, prompts, or issue comments"
            payload["debugCurl"]["rawSecret"] = handle.secret
            payload["debugCurl"]["commandWithLiteralSecret"] = _debug_curl_command(
                event_url=event_url,
                handle=handle,
                event_type=default_event_type,
                secret_expr=repr(handle.secret),
            )
    else:
        payload["genericContract"] = _generic_contract_recipe()

    if normalized_mode in {"local_script", "github_actions"} and create_files:
        payload["files"] = write_handoff_files(
            handle,
            event_url=event_url,
            secret_ref=secret_ref,
            mode=normalized_mode,
            event_type=default_event_type,
            root=handoff_root,
        )

    return payload


def write_handoff_files(
    handle: AsyncThreadHandle,
    *,
    event_url: str,
    secret_ref: dict[str, Any],
    mode: str,
    event_type: str,
    root: str | Path | None = None,
) -> dict[str, Any]:
    base = handoff_root(root)
    directory = base / _safe_path_token(handle.thread_key)
    _mkdir_private(base)
    _mkdir_private(directory)
    config_file = directory / "producer_handoff.json"
    script_file = directory / "emit_async_thread_event.py"
    config = {
        "version": HANDOFF_VERSION,
        "mode": mode,
        "helperModule": "async_threads.emitter",
        "helperPythonPath": str(Path(__file__).resolve().parents[1]),
        "url": event_url,
        "threadKey": handle.thread_key,
        "producerId": handle.producer_id,
        "allowedEventTypes": list(handle.allowed_event_types),
        "defaultEventType": event_type,
        "secretFile": secret_ref.get("secretFile", ""),
        "contractFile": secret_ref.get("contractFile", ""),
        "retryDeduping": _retry_guidance(),
        "lifecycle": _lifecycle_guidance(handle),
    }
    _write_private_text(config_file, json.dumps(config, indent=2, sort_keys=True) + "\n")
    _write_private_text(script_file, _emitter_script())
    result: dict[str, Any] = {
        "directory": str(directory),
        "configFile": str(config_file),
        "emitterScript": str(script_file),
        "permissions": "0600",
        "containsRawSecret": False,
        "run": f"ATH_HANDOFF_CONFIG={_shell_quote(str(config_file))} python3 {_shell_quote(str(script_file))}",
    }
    if mode == "github_actions":
        workflow_file = directory / "github_actions_step.yml"
        _write_private_text(workflow_file, _github_actions_step(event_url=event_url, handle=handle, event_type=event_type))
        result["githubActionsStep"] = str(workflow_file)
    return result


def handoff_root(root: str | Path | None = None) -> Path:
    if root:
        return Path(root).expanduser().resolve()
    try:
        from hermes_constants import get_hermes_home

        home = get_hermes_home()
    except Exception:
        home = Path.home() / ".hermes"
    return (home / "data" / "async-threads" / "handoffs").expanduser().resolve()


def handoff_root_from_config(config: Any | None = None) -> Path:
    extra = getattr(config, "extra", {}) or {}
    configured = extra.get("handoff_root") or extra.get("handoffs_root") or extra.get("emitter_handoff_root")
    return handoff_root(configured)


def _normalize_mode(value: str) -> str:
    mode = str(value or "generic_contract").strip().lower().replace("-", "_")
    aliases = {
        "generic": "generic_contract",
        "contract": "generic_contract",
        "local": "local_script",
        "script": "local_script",
        "ci": "github_actions",
        "github": "github_actions",
        "github_action": "github_actions",
        "dynamic_workflow": "dynamic_workflows",
        "dynamic_workflows": "dynamic_workflows",
        "dw": "dynamic_workflows",
        "curl": "debug_curl",
        "debug": "debug_curl",
    }
    mode = aliases.get(mode, mode)
    return mode if mode in {"generic_contract", "local_script", "github_actions", "debug_curl", "dynamic_workflows"} else "generic_contract"


def _select_event_type(handle: AsyncThreadHandle, requested: str) -> str:
    requested = str(requested or "").strip()
    if requested and (not handle.allowed_event_types or requested in handle.allowed_event_types):
        return requested
    if handle.allowed_event_types:
        return handle.allowed_event_types[0]
    return f"{handle.producer_id}.finished"


def _minimal_event(handle: AsyncThreadHandle, *, event_type: str) -> dict[str, Any]:
    return {
        "version": "async-thread-event/v1",
        "eventId": f"{handle.producer_id}-example-001",
        "eventType": event_type,
        "producer": {"id": handle.producer_id},
        "occurredAt": _utc_now_iso(),
        "asyncThread": {"threadKey": handle.thread_key},
        "summary": f"{event_type} completed",
        "tailMode": "compact",
        "payload": {"status": "passed"},
    }


def _contract_summary(handle: AsyncThreadHandle, *, event_url: str, event_type: str, secret_ref: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": "async-thread-event/v1",
        "url": event_url,
        "threadKey": handle.thread_key,
        "producerId": handle.producer_id,
        "allowedEventTypes": list(handle.allowed_event_types),
        "defaultEventType": event_type,
        "secretFile": secret_ref.get("secretFile", ""),
        "contractFile": secret_ref.get("contractFile", ""),
        "signature": "HMAC-SHA256 over exact UTF-8 JSON bytes using exact ATH_SECRET_FILE contents",
        "helperModule": "async_threads.emitter",
        "headers": {"Content-Type": "application/json", "X-Hermes-Signature-256": "sha256=<hex>"},
    }


def _retry_guidance() -> dict[str, Any]:
    return {
        "stableEventIdRequired": True,
        "reuseEventIdOnRetry": True,
        "newEventIdOnlyForNewRealWorldEvent": True,
        "timestampMustBeFresh": True,
        "defaultReplayWindowSeconds": 300,
        "duplicateResponseMeansAlreadyAccepted": True,
        "retryTransportErrorsAnd502WithSameEventId": True,
    }


def _lifecycle_guidance(handle: AsyncThreadHandle) -> dict[str, Any]:
    policy = handle.lifecycle_policy
    terminal_events = list(policy.terminal_event_types)
    return {
        "terminalEventTypes": terminal_events,
        "terminalStages": list(policy.terminal_stages),
        "autoRetireOnTerminal": policy.auto_retire_on_terminal,
        "sharedListener": policy.shared_listener,
        "producerSelfExit": "after emitting a terminal event, stop the producer/supervisor loop instead of polling forever",
        "rotateSecret": "refresh producer config from the listener's secretFile after rotation",
        "retire": "retire or revoke temporary listeners when the workflow is merged, abandoned, or no longer needs wakeups; single-goal listeners can opt into auto-retire on terminal events",
        "trace": "use ath_trace_event or /ath trace <eventId> for delivery/de-dupe diagnostics",
    }


def _generic_contract_recipe() -> dict[str, Any]:
    return {
        "useWhen": "another system will implement its own emitter",
        "steps": [
            "Read endpoint URL, threadKey, producerId, allowedEventTypes, and ATH_SECRET_FILE from this handoff.",
            "Build the JSON body once and sign those exact UTF-8 bytes.",
            "Reuse the same eventId when retrying the same upstream event.",
            "Keep payload compact and treat all producer fields as untrusted data.",
        ],
    }


def _dynamic_workflows_recipe(handle: AsyncThreadHandle, *, event_url: str, secret_ref: dict[str, Any]) -> dict[str, Any]:
    run_id = "run-42"
    spec_id = "example-loop"
    head = "a1b2c3d4"
    occurred_at = _utc_now_iso()
    compatibility = _dynamic_workflows_listener_compatibility(handle)
    base = {
        "version": "async-thread-event/v1",
        "producer": {"id": handle.producer_id},
        "occurredAt": occurred_at,
        "asyncThread": {"threadKey": handle.thread_key},
        "tailMode": "none",
        "workflowId": f"loop:{spec_id}:{run_id}",
        "seriesKey": f"loop:{spec_id}:{run_id}",
        "loop": {"runId": run_id, "specId": spec_id, "specName": "Example loop"},
        "refs": {"repo": "example/repo", "pullRequest": 37, "headSha": head},
    }
    examples = [
        base
        | {
            "eventId": "loop-run-42-started",
            "eventType": "loop.started",
            "summary": "example loop started",
            "stage": "started",
            "loop": base["loop"] | {"state": "running"},
            "correlation": {
                "correlationKey": f"{spec_id}:{run_id}:head-{head}",
                "idempotencyKey": "loop-run-42-started",
                "signalKey": f"loop.started:{spec_id}:{run_id}",
            },
            "nextExpectedSignal": {"signalKey": f"github.check_suite.completed:example/repo:37:{head}", "deadlineAt": "2026-06-23T17:15:00Z"},
        },
        base
        | {
            "eventId": "loop-run-42-waiting-checks",
            "eventType": "loop.waiting_for_event",
            "summary": "waiting for GitHub checks for PR 37",
            "stage": "blocked",
            "loop": base["loop"] | {"state": "waiting"},
            "correlation": {
                "correlationKey": f"{spec_id}:{run_id}:checks:{head}",
                "idempotencyKey": "loop-run-42-waiting-checks",
                "signalKey": f"github.check_suite.completed:example/repo:37:{head}",
            },
            "nextExpectedSignal": {"signalKey": f"github.check_suite.completed:example/repo:37:{head}", "deadlineAt": "2026-06-23T17:30:00Z"},
        },
        base
        | {
            "eventId": "loop-run-42-step-review-completed",
            "eventType": "loop.step_completed",
            "summary": "review step passed for PR 37",
            "stage": "review_passed",
            "loop": base["loop"] | {"state": "running"},
            "step": {"stepId": "review", "attempt": 1, "backend": "relay"},
            "correlation": {
                "correlationKey": f"{spec_id}:{run_id}:review:{head}",
                "idempotencyKey": "loop-run-42-step-review-completed",
                "signalKey": "relay.session.completed:run-42:review",
            },
            "evidence": {"kind": "review", "status": "passed", "url": "https://example.invalid/reviews/1"},
            "nextExpectedSignal": {"signalKey": f"approval.merge.requested:example/repo:37:{head}"},
        },
        base
        | {
            "eventId": "loop-run-42-converged",
            "eventType": "loop.converged",
            "summary": "example loop converged",
            "stage": "released",
            "loop": base["loop"] | {"state": "converged"},
            "correlation": {
                "correlationKey": f"{spec_id}:{run_id}:converged:{head}",
                "idempotencyKey": "loop-run-42-converged",
                "signalKey": f"loop.converged:{spec_id}:{run_id}",
            },
            "evidence": {"kind": "release_gate", "status": "passed", "url": "https://example.invalid/repo/pull/37"},
            "nextExpectedSignal": {"signalKey": "none", "reason": "loop converged"},
        },
        base
        | {
            "eventId": "loop-run-42-halted",
            "eventType": "loop.halted",
            "summary": "example loop halted after stale approval",
            "stage": "cancelled",
            "loop": base["loop"] | {"state": "halted"},
            "correlation": {
                "correlationKey": f"{spec_id}:{run_id}:halted:{head}",
                "idempotencyKey": "loop-run-42-halted",
                "signalKey": f"loop.halted:{spec_id}:{run_id}",
            },
            "evidence": {"kind": "brake", "status": "failed", "url": "https://example.invalid/loops/run-42"},
            "nextExpectedSignal": {"signalKey": "human.next_step", "reason": "inspect stale approval"},
        },
    ]
    return {
        "useWhen": "Dynamic Workflows needs to emit signed ATH loop lifecycle events for a mapped conversation",
        "endpoint": event_url,
        "env": {
            "ATH_SECRET_FILE": secret_ref.get("secretFile", ""),
            "ATH_CONTRACT_FILE": secret_ref.get("contractFile", ""),
            "ATH_THREAD_KEY": handle.thread_key,
            "ATH_PRODUCER_ID": handle.producer_id,
        },
        "recommendedListener": {
            "producer_hint": handle.producer_id,
            "event_types": DYNAMIC_WORKFLOW_EVENT_TYPES + DYNAMIC_WORKFLOW_OPTIONAL_EVENT_TYPES,
            "terminal_event_types": ["loop.converged", "loop.halted"],
            "auto_retire_on_terminal": False,
            "shared_listener": True,
        },
        "listenerCompatibility": compatibility,
        "sequence": ["loop.started", "loop.waiting_for_event", "external signal wakes controller", "loop.step_completed", "loop.converged or loop.halted"],
        "controllerBoundary": "Dynamic Workflows decides loop state transitions; ATH only authenticates, de-dupes, wakes, replies, renders, and records events.",
        "timestampHandling": "Set occurredAt to the current UTC emission time immediately before signing; events outside the replay window are rejected.",
        "waitingWithoutPolling": "Emit loop.waiting_for_event with nextExpectedSignal/deadline; let external signed producers or bounded timeout signals wake the controller instead of cron spam.",
        "secretHandling": "Read ATH_SECRET_FILE locally or from a secret manager; ordinary handoff output never includes the raw HMAC secret.",
        "examples": examples,
    }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _dynamic_workflows_listener_compatibility(handle: AsyncThreadHandle) -> dict[str, Any]:
    allowed = list(handle.allowed_event_types)
    missing = [event_type for event_type in DYNAMIC_WORKFLOW_EVENT_TYPES if allowed and event_type not in allowed]
    result: dict[str, Any] = {
        "requiredEventTypes": list(DYNAMIC_WORKFLOW_EVENT_TYPES),
        "allowedEventTypes": allowed,
        "missingRequiredEventTypes": missing,
        "canEmitExamples": not missing,
    }
    if missing:
        result["warning"] = "listener allowlist is missing Dynamic Workflows loop event types; emitted examples with those eventType values will be rejected until the listener is recreated or reused with the recommended allowlist"
    return result


def _local_script_recipe(*, secret_ref: dict[str, Any]) -> dict[str, Any]:
    return {
        "useWhen": "a local shell/script should report started/finished/failed events",
        "env": {"ATH_SECRET_FILE": secret_ref.get("secretFile", ""), "ATH_HANDOFF_CONFIG": "<producer_handoff.json>"},
        "secretHandling": "read ATH_SECRET_FILE locally; never print it",
    }


def _github_actions_recipe(
    handle: AsyncThreadHandle,
    *,
    event_url: str,
    event_type: str,
    secret_ref: dict[str, Any],
) -> dict[str, Any]:
    return {
        "useWhen": "a GitHub Actions workflow should emit completion events",
        "requiredSecrets": ["ATH_SECRET"],
        "requiredEnv": {
            "ATH_URL": event_url,
            "ATH_THREAD_KEY": handle.thread_key,
            "ATH_PRODUCER_ID": handle.producer_id,
            "ATH_EVENT_TYPE": event_type,
        },
        "localSecretFile": secret_ref.get("secretFile", ""),
        "note": "Copy the secret file contents into the repository/environment secret manager; do not paste it into workflow logs or issue comments.",
    }


def _debug_curl(*, secret_ref: dict[str, Any], event_url: str, handle: AsyncThreadHandle, event_type: str) -> dict[str, Any]:
    return {
        "safeCommand": _debug_curl_command(
            event_url=event_url,
            handle=handle,
            event_type=event_type,
            secret_expr='open(os.environ["ATH_SECRET_FILE"], "r", encoding="utf-8").read()',
        ),
        "env": {"ATH_SECRET_FILE": secret_ref.get("secretFile", "")},
        "containsRawSecret": False,
    }


def _debug_curl_command(*, event_url: str, handle: AsyncThreadHandle, event_type: str, secret_expr: str) -> str:
    return (
        "python3 - <<'PY'\n"
        "import hashlib, hmac, json, os, time, urllib.request\n"
        f"url = {event_url!r}\n"
        f"secret = {secret_expr}\n"
        "body_obj = {\n"
        "    'version': 'async-thread-event/v1',\n"
        "    'eventId': os.environ.get('ATH_EVENT_ID', 'debug-' + str(int(time.time() * 1000))),\n"
        f"    'eventType': {event_type!r},\n"
        f"    'producer': {{'id': {handle.producer_id!r}}},\n"
        "    'occurredAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),\n"
        f"    'asyncThread': {{'threadKey': {handle.thread_key!r}}},\n"
        f"    'summary': {event_type + ' debug event'!r},\n"
        "    'tailMode': 'compact',\n"
        "    'payload': {'status': 'debug'},\n"
        "}\n"
        "body = json.dumps(body_obj, sort_keys=True, separators=(',', ':')).encode('utf-8')\n"
        "sig = hmac.new(secret.encode('utf-8'), body, hashlib.sha256).hexdigest()\n"
        "req = urllib.request.Request(url, data=body, method='POST', headers={'Content-Type':'application/json','X-Hermes-Signature-256':'sha256='+sig})\n"
        "with urllib.request.urlopen(req, timeout=20) as res:\n"
        "    print(res.status, res.read().decode('utf-8', 'replace'))\n"
        "PY"
    )


def _emitter_script() -> str:
    return """#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time

config_path = os.environ.get("ATH_HANDOFF_CONFIG")
if not config_path:
    raise SystemExit("ATH_HANDOFF_CONFIG is required")
with open(config_path, "r", encoding="utf-8") as fh:
    config = json.load(fh)
helper_python_path = config.get("helperPythonPath")
if helper_python_path and helper_python_path not in sys.path:
    sys.path.insert(0, helper_python_path)
from async_threads.emitter import build_event_envelope, emit_event, exit_code_for_result

secret_file = os.environ.get("ATH_SECRET_FILE") or config["secretFile"]

event_type = os.environ.get("ATH_EVENT_TYPE") or config["defaultEventType"]
event = build_event_envelope(
    thread_key=config["threadKey"],
    producer_id=config["producerId"],
    event_id=os.environ.get("ATH_EVENT_ID") or f"{config['producerId']}-{event_type}-{int(time.time())}",
    event_type=event_type,
    occurred_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    summary=os.environ.get("ATH_SUMMARY") or f"{event_type} finished",
    tail_mode=os.environ.get("ATH_TAIL_MODE", "compact"),
    payload={"status": os.environ.get("ATH_STATUS") or "passed"},
)
result = emit_event(
    os.environ.get("ATH_URL") or config["url"],
    event,
    secret_file=secret_file,
    timeout=float(os.environ.get("ATH_TIMEOUT", "20")),
    dry_run=os.environ.get("ATH_DRY_RUN", "").lower() in {"1", "true", "yes"} or os.environ.get("ATH_VALIDATE_ONLY", "").lower() in {"1", "true", "yes"},
)
public = result.to_public_dict() if hasattr(result, "to_public_dict") else result
print(json.dumps(public, sort_keys=True))
raise SystemExit(exit_code_for_result(result))
"""


def _github_actions_step(*, event_url: str, handle: AsyncThreadHandle, event_type: str) -> str:
    return f"""- name: Emit async-thread event
  env:
    ATH_URL: {event_url}
    ATH_THREAD_KEY: {handle.thread_key}
    ATH_PRODUCER_ID: {handle.producer_id}
    ATH_EVENT_TYPE: {event_type}
    ATH_SECRET: ${{{{ secrets.ATH_SECRET }}}}
  shell: python
  run: |
    import hashlib, hmac, json, os, time, urllib.request
    body = {{
      "version": "async-thread-event/v1",
      "eventId": os.environ.get("GITHUB_RUN_ID", "manual") + "-" + os.environ.get("ATH_EVENT_TYPE", "event"),
      "eventType": os.environ["ATH_EVENT_TYPE"],
      "producer": {{"id": os.environ["ATH_PRODUCER_ID"]}},
      "occurredAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
      "asyncThread": {{"threadKey": os.environ["ATH_THREAD_KEY"]}},
      "summary": "GitHub Actions event finished",
      "tailMode": "compact",
      "payload": {{"status": "passed", "run_id": os.environ.get("GITHUB_RUN_ID", "")}},
    }}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    sig = hmac.new(os.environ["ATH_SECRET"].encode("utf-8"), raw, hashlib.sha256).hexdigest()
    req = urllib.request.Request(os.environ["ATH_URL"], data=raw, method="POST", headers={{"Content-Type":"application/json","X-Hermes-Signature-256":"sha256="+sig}})
    with urllib.request.urlopen(req, timeout=20) as res:
      print(res.status, res.read().decode("utf-8", "replace"))
"""


def _mkdir_private(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    _chmod(path, 0o700)


def _write_private_text(path: Path, content: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
    finally:
        _chmod(path, 0o600)


def _chmod(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
    except (AttributeError, NotImplementedError, PermissionError, OSError):
        pass


def _safe_path_token(value: str) -> str:
    cleaned = "".join(ch for ch in str(value or "") if ch.isalnum() or ch in {"-", "_"})
    return cleaned or "unknown"


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"
