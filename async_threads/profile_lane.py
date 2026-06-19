"""Helpers for producer-side ATH profile-lane events."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Mapping

DEFAULT_PROFILE_LANE_EVENT_VERSION = "async-thread-event/v1"
DEFAULT_PROFILE_LANE_TAIL_MODE = "compact"
_TAIL_KEYS = {"tail", "stdout", "stderr", "output", "transcript"}


def build_profile_lane_event(
    *,
    thread_key: str,
    producer_id: str,
    event_type: str,
    lane: str,
    profile: str,
    summary: str,
    issue: str = "",
    pr: str = "",
    head: str = "",
    log_path: str = "",
    status: str = "",
    process_id: str = "",
    delegation_id: str = "",
    telemetry: Mapping[str, Any] | None = None,
    payload: Mapping[str, Any] | None = None,
    tail_mode: str = DEFAULT_PROFILE_LANE_TAIL_MODE,
    event_id: str = "",
    occurred_at: str | float | int | None = None,
) -> dict[str, Any]:
    """Build the compact signed-event body for a background profile lane.

    This helper is producer-side: it does not trust or execute payload text. It
    simply normalizes stable handles and defaults the event to compact tail mode
    so long-running lane transcripts stay in logs instead of prompts.
    """
    tail_mode = tail_mode if tail_mode in {"none", "compact", "debug"} else DEFAULT_PROFILE_LANE_TAIL_MODE
    phase = event_type.rsplit(".", 1)[-1]
    subject = _drop_empty(
        {
            "profile": profile,
            "lane": lane,
            "issue": issue,
            "pr": pr,
            "head": head,
            "log_path": log_path,
            "status": status,
            "process_id": process_id,
            "delegation_id": delegation_id,
        }
    )
    body_payload: dict[str, Any] = {"phase": phase}
    body_payload.update(_compact_payload(dict(payload or {}), tail_mode=tail_mode))
    if telemetry:
        body_payload["telemetry"] = _drop_empty(dict(telemetry))
    occurred = occurred_at if occurred_at is not None else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    stable_id = event_id or _stable_event_id(event_type, lane, profile, occurred, time.time_ns())
    return {
        "version": DEFAULT_PROFILE_LANE_EVENT_VERSION,
        "eventId": stable_id,
        "eventType": event_type,
        "producer": {"id": producer_id},
        "occurredAt": occurred,
        "asyncThread": {"threadKey": thread_key},
        "summary": str(summary or "")[:500],
        "tailMode": tail_mode,
        "subject": subject,
        "payload": body_payload,
    }


def canonical_event_bytes(event: Mapping[str, Any]) -> bytes:
    return json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_event(event: Mapping[str, Any], secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), canonical_event_bytes(event), hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def emit_signed_event(*, url: str, event: Mapping[str, Any], secret: str, timeout: float = 20) -> tuple[int, str]:
    raw = canonical_event_bytes(event)
    request = urllib.request.Request(
        url,
        data=raw,
        method="POST",
        headers={"Content-Type": "application/json", "X-Hermes-Signature-256": sign_event(event, secret)},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read().decode("utf-8", "replace")
    except urllib.error.URLError as exc:
        return 0, str(exc.reason)


def load_registry_handle(registry_path: str | Path, thread_key: str) -> tuple[str, str]:
    import sqlite3

    con = sqlite3.connect(Path(registry_path).expanduser())
    try:
        row = con.execute(
            "select producer_id, secret from async_thread_handles where thread_key = ? and enabled = 1",
            (thread_key,),
        ).fetchone()
    finally:
        con.close()
    if not row:
        raise LookupError(f"enabled ATH handle not found: {thread_key}")
    return str(row[0]), str(row[1])


def _stable_event_id(event_type: str, lane: str, profile: str, occurred_at: Any, nonce: int) -> str:
    digest = hashlib.sha256(f"{event_type}|{lane}|{profile}|{occurred_at}|{nonce}".encode("utf-8")).hexdigest()[:16]
    return f"profile_lane_{digest}"


def _compact_payload(payload: dict[str, Any], *, tail_mode: str) -> dict[str, Any]:
    compacted: dict[str, Any] = {}
    for key, value in _drop_empty(payload).items():
        if key not in _TAIL_KEYS:
            compacted[key] = value
            continue
        if tail_mode == "none":
            continue
        if tail_mode == "debug":
            compacted[key] = value
            continue
        text = str(value)
        compacted[f"{key}_summary"] = {"omitted": True, "chars": len(text), "lines": text.count("\n") + (1 if text else 0)}
    return compacted


def _drop_empty(value: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in value.items() if v is not None and v != "" and v != () and v != []}
