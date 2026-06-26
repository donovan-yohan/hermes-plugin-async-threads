import hashlib
import hmac
import json
import urllib.error

import pytest

from async_threads.emitter import (
    canonical_json_bytes,
    classify_receiver_response,
    dry_run_metadata,
    emit_event,
    read_secret_file,
    sign_bytes,
    signed_headers,
    validate_event_envelope,
)
from async_threads.security import EventValidationError


def _event(**overrides):
    event = {
        "version": "async-thread-event/v1",
        "eventId": "evt-1",
        "eventType": "kanban.task.completed",
        "producer": {"id": "ath-kanban-bridge"},
        "occurredAt": "2026-06-26T12:00:00Z",
        "asyncThread": {"threadKey": "ath_mg3BQeDs15Gm4DnF"},
        "summary": "task completed",
        "payload": {"status": "done"},
    }
    event.update(overrides)
    return event


def test_canonical_json_bytes_and_signature_are_exact():
    event = _event(summary="café", payload={"z": 1, "a": 2})

    raw = canonical_json_bytes(event)

    expected_raw = (
        '{"asyncThread":{"threadKey":"ath_mg3BQeDs15Gm4DnF"},'
        '"eventId":"evt-1","eventType":"kanban.task.completed",'
        '"occurredAt":"2026-06-26T12:00:00Z",'
        '"payload":{"a":2,"z":1},"producer":{"id":"ath-kanban-bridge"},'
        '"summary":"café","version":"async-thread-event/v1"}'
    ).encode("utf-8")
    assert raw == expected_raw
    expected = hmac.new("secret-value".encode("utf-8"), raw, hashlib.sha256).hexdigest()
    assert sign_bytes(raw, "secret-value") == f"sha256={expected}"
    assert signed_headers(raw, "secret-value") == {
        "Content-Type": "application/json",
        "X-Hermes-Signature-256": f"sha256={expected}",
    }


def test_validate_event_envelope_checks_required_fields_and_local_scope():
    fields = validate_event_envelope(
        _event(),
        allowed_event_types=["kanban.task.completed"],
        expected_thread_key="ath_mg3BQeDs15Gm4DnF",
        expected_producer_id="ath-kanban-bridge",
    )

    assert fields["event_id"] == "evt-1"
    with pytest.raises(EventValidationError, match="eventType is not allowed"):
        validate_event_envelope(_event(eventType="kanban.task.blocked"), allowed_event_types=["kanban.task.completed"])
    broken = _event()
    del broken["asyncThread"]
    with pytest.raises(EventValidationError, match="missing field: asyncThread.threadKey"):
        validate_event_envelope(broken)


def test_classify_receiver_response_models_success_duplicate_and_retryable_failures():
    delivered = classify_receiver_response(200, {"status": "delivered", "threadKey": "ath_1"})
    duplicate = classify_receiver_response(200, {"status": "duplicate", "threadKey": "ath_1"})
    retryable = classify_receiver_response(502, {"error": "event dispatch failed"})
    transport = classify_receiver_response(None, None, error="connection refused")
    auth = classify_receiver_response(401, {"error": "invalid signature"})

    assert delivered.success is True and delivered.retryable is False
    assert duplicate.success is True and duplicate.duplicate is True and duplicate.retryable is False
    assert retryable.success is False and retryable.retryable is True
    assert transport.success is False and transport.retryable is True and transport.receiver_status == "transport_error"
    assert auth.success is False and auth.retryable is False


def test_dry_run_metadata_and_response_redaction_do_not_leak_secret(tmp_path, monkeypatch):
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("super-secret-material", encoding="utf-8")
    monkeypatch.setenv("ATH_SECRET_FILE", str(secret_file))
    event = _event()
    raw = canonical_json_bytes(event)

    metadata = dry_run_metadata(event=event, raw_body=raw, url="https://ath.example.invalid/events")
    rendered_metadata = json.dumps(metadata, sort_keys=True)
    redacted = classify_receiver_response(500, f"secret=super-secret-material body {sign_bytes(raw, read_secret_file())}", redact_values=(read_secret_file(),))

    assert metadata["dryRun"] is True
    assert metadata["signatureValue"] == "sha256=<redacted>"
    assert "super-secret-material" not in rendered_metadata
    assert "super-secret-material" not in redacted.to_public_dict()["body"]
    assert "sha256=<redacted>" in redacted.to_public_dict()["body"]


def test_emit_event_transport_failure_is_retryable_with_same_event_id(monkeypatch, tmp_path):
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("secret-value", encoding="utf-8")

    def fail_urlopen(*args, **kwargs):
        raise urllib.error.URLError("temporary network fail")

    monkeypatch.setattr("urllib.request.urlopen", fail_urlopen)

    result = emit_event("https://ath.example.invalid/events", _event(), secret_file=secret_file)

    assert result.success is False
    assert result.retryable is True
    assert result.receiver_status == "transport_error"
    assert result.to_public_dict()["status"] == "transport_error"
