"""Signature and payload validation helpers for async-thread events."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import time
from datetime import datetime, timezone
from typing import Any, Mapping


MAX_BODY_BYTES = 64 * 1024
DEFAULT_REPLAY_WINDOW_SECONDS = 5 * 60


class EventValidationError(ValueError):
    """Raised when an incoming async-thread event fails validation."""


def _reject_json_constant(constant: str) -> None:
    raise ValueError(f"invalid json constant: {constant}")


def parse_json_body(raw_body: bytes, *, max_bytes: int = MAX_BODY_BYTES) -> dict[str, Any]:
    if len(raw_body) > max_bytes:
        raise EventValidationError("body too large")
    try:
        data = json.loads(
            raw_body.decode("utf-8"),
            parse_constant=_reject_json_constant,
        )
    except Exception as exc:  # noqa: BLE001 - convert to safe client error
        raise EventValidationError("invalid json") from exc
    if not isinstance(data, dict):
        raise EventValidationError("json body must be an object")
    return data


def signature_header(headers: Mapping[str, str]) -> str:
    for key in (
        "X-Hermes-Signature-256",
        "X-Hermes-Signature",
        "X-Hub-Signature-256",
    ):
        value = headers.get(key)
        if value:
            return str(value).strip()
    return ""


def verify_hmac_signature(raw_body: bytes, secret: str, supplied: str) -> bool:
    if not secret or not supplied:
        return False
    supplied = supplied.strip().lower()
    if supplied.startswith("sha256="):
        supplied = supplied.split("=", 1)[1]
    if not supplied:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, supplied)


def event_field(data: Mapping[str, Any], path: str, *, required: bool = True) -> Any:
    cur: Any = data
    for part in path.split("."):
        if not isinstance(cur, Mapping) or part not in cur:
            if required:
                raise EventValidationError(f"missing field: {path}")
            return None
        cur = cur[part]
    if required and (cur is None or cur == ""):
        raise EventValidationError(f"missing field: {path}")
    return cur


def validate_timestamp(
    occurred_at: Any,
    *,
    now: float | None = None,
    replay_window_seconds: int = DEFAULT_REPLAY_WINDOW_SECONDS,
) -> None:
    if occurred_at is None or occurred_at == "":
        raise EventValidationError("missing occurredAt")
    now = time.time() if now is None else now
    try:
        if isinstance(occurred_at, (int, float)):
            ts = float(occurred_at)
        else:
            try:
                ts = float(occurred_at)
            except (TypeError, ValueError):
                text = str(occurred_at).strip()
                if text.endswith("Z"):
                    text = text[:-1] + "+00:00"
                ts = datetime.fromisoformat(text).astimezone(timezone.utc).timestamp()
    except Exception as exc:  # noqa: BLE001
        raise EventValidationError("invalid occurredAt") from exc
    if not math.isfinite(ts):
        raise EventValidationError("invalid occurredAt")
    if abs(now - ts) > replay_window_seconds:
        raise EventValidationError("event timestamp outside replay window")


def extract_envelope_fields(data: Mapping[str, Any]) -> dict[str, str]:
    version = str(event_field(data, "version"))
    if version != "async-thread-event/v1":
        raise EventValidationError("unsupported version")
    return {
        "event_id": str(event_field(data, "eventId"))[:200],
        "event_type": str(event_field(data, "eventType"))[:200],
        "producer_id": str(event_field(data, "producer.id"))[:200],
        "thread_key": str(event_field(data, "asyncThread.threadKey"))[:200],
        "summary": str(event_field(data, "summary", required=False) or "")[:2000],
    }
