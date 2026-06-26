"""Producer-side helpers for signed async-thread event emission.

The receiver owns authentication and delivery. This module owns the producer
side: build the stable v1 envelope, encode canonical JSON bytes, sign those
exact bytes from ``ATH_SECRET_FILE``, POST them, and classify the receiver
response without printing secret material.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .privacy import redact_secret_text
from .security import EventValidationError, extract_envelope_fields, validate_timestamp

EVENT_VERSION = "async-thread-event/v1"
SIGNATURE_HEADER = "X-Hermes-Signature-256"
SUCCESS_RECEIVER_STATUSES = {"delivered", "accepted", "queued", "duplicate"}
RETRYABLE_HTTP_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class EmitResult:
    """Public, secret-redacted result of an emitter attempt."""

    success: bool
    retryable: bool
    duplicate: bool = False
    http_status: int | None = None
    receiver_status: str = ""
    body: str = ""
    error: str = ""

    def to_public_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "success": self.success,
            "retryable": self.retryable,
            "duplicate": self.duplicate,
            "status": self.receiver_status,
        }
        if self.http_status is not None:
            result["httpStatus"] = self.http_status
        if self.body:
            result["body"] = self.body
        if self.error:
            result["error"] = self.error
        return result


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_event_envelope(
    *,
    thread_key: str,
    producer_id: str,
    event_id: str,
    event_type: str,
    summary: str = "",
    payload: Mapping[str, Any] | None = None,
    subject: Mapping[str, Any] | None = None,
    occurred_at: Any | None = None,
    tail_mode: str = "compact",
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an ``async-thread-event/v1`` envelope with compact safe defaults."""

    event: dict[str, Any] = {
        "version": EVENT_VERSION,
        "eventId": str(event_id or ""),
        "eventType": str(event_type or ""),
        "producer": {"id": str(producer_id or "")},
        "occurredAt": utc_now_iso() if occurred_at is None else occurred_at,
        "asyncThread": {"threadKey": str(thread_key or "")},
        "summary": str(summary or ""),
        "tailMode": str(tail_mode or "compact"),
        "payload": dict(payload or {}),
    }
    if subject is not None:
        event["subject"] = dict(subject)
    if extra:
        for key, value in extra.items():
            if key not in {"version", "eventId", "eventType", "producer", "occurredAt", "asyncThread"}:
                event[str(key)] = value
    validate_event_envelope(event)
    return event


def validate_event_envelope(
    event: Mapping[str, Any],
    *,
    allowed_event_types: tuple[str, ...] | list[str] = (),
    expected_thread_key: str = "",
    expected_producer_id: str = "",
    check_replay_window: bool = False,
) -> dict[str, str]:
    """Validate the stable producer envelope and optional local scope metadata."""

    fields = extract_envelope_fields(event)
    if allowed_event_types and fields["event_type"] not in set(str(item) for item in allowed_event_types):
        raise EventValidationError("eventType is not allowed by this handoff")
    if expected_thread_key and fields["thread_key"] != str(expected_thread_key):
        raise EventValidationError("asyncThread.threadKey does not match this handoff")
    if expected_producer_id and fields["producer_id"] != str(expected_producer_id):
        raise EventValidationError("producer.id does not match this handoff")
    if check_replay_window:
        validate_timestamp(event.get("occurredAt"))
    else:
        # Still require that the field exists; producers decide freshness at emit time.
        if event.get("occurredAt") in (None, ""):
            raise EventValidationError("missing occurredAt")
    return fields


def canonical_json_bytes(event: Mapping[str, Any]) -> bytes:
    """Return the canonical UTF-8 bytes producers sign and send."""

    validate_event_envelope(event)
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def read_secret_file(secret_file: str | os.PathLike[str] | None = None) -> str:
    """Read the exact ATH secret text from a file path or ``ATH_SECRET_FILE``."""

    path_text = str(secret_file or os.environ.get("ATH_SECRET_FILE", ""))
    if not path_text:
        raise EventValidationError("ATH_SECRET_FILE is required")
    path = Path(path_text).expanduser()
    return path.read_text(encoding="utf-8")


def sign_bytes(raw_body: bytes, secret: str) -> str:
    if not secret:
        raise EventValidationError("ATH secret is required")
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def signed_headers(raw_body: bytes, secret: str) -> dict[str, str]:
    return {"Content-Type": "application/json", SIGNATURE_HEADER: sign_bytes(raw_body, secret)}


def dry_run_metadata(
    *,
    event: Mapping[str, Any],
    raw_body: bytes | None = None,
    url: str = "",
    secret_file: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """Return validate/dry-run metadata without raw secret bytes or HMAC values."""

    raw = canonical_json_bytes(event) if raw_body is None else raw_body
    fields = validate_event_envelope(event)
    secret_path = str(secret_file or os.environ.get("ATH_SECRET_FILE", ""))
    return {
        "ok": True,
        "dryRun": True,
        "url": str(url or ""),
        "eventId": fields["event_id"],
        "eventType": fields["event_type"],
        "producerId": fields["producer_id"],
        "threadKey": fields["thread_key"],
        "bodyBytes": len(raw),
        "bodySha256": hashlib.sha256(raw).hexdigest(),
        "signatureHeader": SIGNATURE_HEADER,
        "signatureValue": "sha256=<redacted>",
        "secretFilePresent": bool(secret_path),
    }


def classify_receiver_response(
    http_status: int | None,
    body: bytes | str | Mapping[str, Any] | None = None,
    *,
    error: str = "",
    redact_values: tuple[str, ...] | list[str] = (),
) -> EmitResult:
    """Classify ATH receiver responses for idempotent producer retry logic."""

    body_text, parsed_status = _body_text_and_status(body)
    body_text = _redact_response_text(body_text, redact_values=redact_values)
    safe_error = _redact_response_text(error, redact_values=redact_values)
    receiver_status = parsed_status or ("transport_error" if http_status is None else "")
    duplicate = receiver_status == "duplicate"
    success = bool(http_status is not None and 200 <= http_status < 300 and receiver_status in SUCCESS_RECEIVER_STATUSES)
    retryable = (http_status is None) or (http_status in RETRYABLE_HTTP_STATUSES)
    if success:
        retryable = False
    return EmitResult(
        success=success,
        retryable=retryable,
        duplicate=duplicate,
        http_status=http_status,
        receiver_status=receiver_status,
        body=body_text,
        error=safe_error,
    )


def emit_event(
    url: str,
    event: Mapping[str, Any],
    *,
    secret_file: str | os.PathLike[str] | None = None,
    secret: str | None = None,
    timeout: float = 20,
    dry_run: bool = False,
) -> EmitResult | dict[str, Any]:
    """Sign and POST an event, or return dry-run metadata without posting."""

    resolved_secret = secret or ""
    try:
        raw = canonical_json_bytes(event)
        if dry_run:
            return dry_run_metadata(event=event, raw_body=raw, url=url, secret_file=secret_file)
        resolved_secret = secret if secret is not None else read_secret_file(secret_file)
        request = urllib.request.Request(
            str(url),
            data=raw,
            method="POST",
            headers=signed_headers(raw, resolved_secret),
        )
    except (EventValidationError, OSError, ValueError) as exc:
        return _local_config_error_result(str(exc), redact_values=(resolved_secret,))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response_body = response.read().decode("utf-8", "replace")
            return classify_receiver_response(
                int(response.status),
                response_body,
                redact_values=(resolved_secret,),
            )
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", "replace")
        return classify_receiver_response(
            int(exc.code),
            response_body,
            redact_values=(resolved_secret,),
        )
    except urllib.error.URLError as exc:
        return classify_receiver_response(None, None, error=str(getattr(exc, "reason", exc)), redact_values=(resolved_secret,))
    except OSError as exc:
        return classify_receiver_response(None, None, error=str(exc), redact_values=(resolved_secret,))


def _local_config_error_result(error: str, *, redact_values: tuple[str, ...] | list[str]) -> EmitResult:
    return EmitResult(
        success=False,
        retryable=False,
        receiver_status="local_config_error",
        error=_redact_response_text(error, redact_values=redact_values),
    )


def exit_code_for_result(result: EmitResult | Mapping[str, Any]) -> int:
    if isinstance(result, EmitResult):
        if result.success:
            return 0
        return 75 if result.retryable else 1
    return 0 if bool(result.get("ok")) else 1


def _body_text_and_status(body: bytes | str | Mapping[str, Any] | None) -> tuple[str, str]:
    if body is None:
        return "", ""
    if isinstance(body, Mapping):
        text = json.dumps(dict(body), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        status = str(body.get("status") or "")
        return text, status
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body)
    status = ""
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None
    if isinstance(parsed, Mapping):
        status = str(parsed.get("status") or "")
    return text, status


def _redact_response_text(value: str, *, redact_values: tuple[str, ...] | list[str]) -> str:
    text = redact_secret_text(value)
    for secret in redact_values:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text
