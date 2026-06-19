import hashlib
import hmac
import io
import json
import sqlite3
import subprocess
import sys
import urllib.error
from email.message import Message

import async_threads.profile_lane as profile_lane
from async_threads.profile_lane import (
    build_profile_lane_event,
    canonical_event_bytes,
    emit_signed_event,
    load_registry_handle,
    sign_event,
)


def test_build_profile_lane_event_defaults_to_compact_handles_and_telemetry():
    raw_tail = "do not render by default\nsecond line"
    event = build_profile_lane_event(
        thread_key="ath_thread",
        producer_id="relay-ath-dev",
        event_type="relay.lane.finished",
        lane="issue19-docs",
        profile="ebi",
        summary="finished without transcript",
        issue="#19",
        pr="23",
        head="abc1234",
        log_path="/tmp/ath/issue19.log",
        status="passed",
        process_id="pid-1",
        delegation_id="delegate-1",
        telemetry={"runtime_seconds": 12.3, "tokens": 456},
        payload={"verification": "tests passed", "tail": raw_tail},
        occurred_at="2026-06-19T00:00:00Z",
    )

    assert event["version"] == "async-thread-event/v1"
    assert event["tailMode"] == "compact"
    assert event["eventType"] == "relay.lane.finished"
    assert event["producer"] == {"id": "relay-ath-dev"}
    assert event["asyncThread"] == {"threadKey": "ath_thread"}
    assert event["subject"] == {
        "profile": "ebi",
        "lane": "issue19-docs",
        "issue": "#19",
        "pr": "23",
        "head": "abc1234",
        "log_path": "/tmp/ath/issue19.log",
        "status": "passed",
        "process_id": "pid-1",
        "delegation_id": "delegate-1",
    }
    assert event["payload"]["telemetry"] == {"runtime_seconds": 12.3, "tokens": 456}
    assert event["payload"]["verification"] == "tests passed"
    assert "tail" not in event["payload"]
    assert event["payload"]["tail_summary"] == {"omitted": True, "chars": len(raw_tail), "lines": 2}
    assert "do not render" not in json.dumps(event)


def test_profile_lane_signature_uses_canonical_body_without_printing_secret():
    event = build_profile_lane_event(
        thread_key="ath_thread",
        producer_id="relay-ath-dev",
        event_type="relay.lane.started",
        lane="issue19-docs",
        profile="ebi",
        summary="started",
        event_id="evt1",
        occurred_at=1,
    )
    secret = "not-for-output"
    expected = hmac.new(secret.encode(), canonical_event_bytes(event), hashlib.sha256).hexdigest()

    assert sign_event(event, secret) == f"sha256={expected}"
    assert secret not in json.dumps(event)
    assert secret not in sign_event(event, secret)


def test_default_profile_lane_event_ids_are_unique(monkeypatch):
    values = iter([101, 102])
    monkeypatch.setattr(profile_lane.time, "time_ns", lambda: next(values))
    first = build_profile_lane_event(
        thread_key="ath_thread",
        producer_id="relay-ath-dev",
        event_type="relay.lane.progress",
        lane="issue19-docs",
        profile="ebi",
        summary="progress",
        occurred_at="2026-06-19T00:00:00Z",
    )
    second = build_profile_lane_event(
        thread_key="ath_thread",
        producer_id="relay-ath-dev",
        event_type="relay.lane.progress",
        lane="issue19-docs",
        profile="ebi",
        summary="progress",
        occurred_at="2026-06-19T00:00:00Z",
    )

    assert first["eventId"] != second["eventId"]


def test_emit_signed_event_returns_http_error_body_without_traceback(monkeypatch):
    event = build_profile_lane_event(
        thread_key="ath_thread",
        producer_id="relay-ath-dev",
        event_type="relay.lane.failed",
        lane="issue19-docs",
        profile="ebi",
        summary="failed",
        event_id="evt-error",
    )

    def fail_urlopen(request, timeout):
        raise urllib.error.HTTPError(
            url=request.full_url,
            code=401,
            msg="invalid signature",
            hdrs=Message(),
            fp=io.BytesIO(b'{"error":"invalid signature"}'),
        )

    monkeypatch.setattr(profile_lane.urllib.request, "urlopen", fail_urlopen)

    assert emit_signed_event(url="http://127.0.0.1/events", event=event, secret="secret") == (
        401,
        '{"error":"invalid signature"}',
    )


def test_cli_help_runs_from_repo_root_without_pythonpath():
    result = subprocess.run(
        [sys.executable, "scripts/ath-profile-lane.py", "--help"],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    assert result.returncode == 0
    assert "--thread-key" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


def test_emit_signed_event_returns_url_error_without_traceback(monkeypatch):
    event = build_profile_lane_event(
        thread_key="ath_thread",
        producer_id="relay-ath-dev",
        event_type="relay.lane.failed",
        lane="issue19-docs",
        profile="ebi",
        summary="failed",
        event_id="evt-url-error",
    )

    def fail_urlopen(request, timeout):
        raise urllib.error.URLError("receiver down")

    monkeypatch.setattr(profile_lane.urllib.request, "urlopen", fail_urlopen)

    assert emit_signed_event(url="http://127.0.0.1/events", event=event, secret="secret") == (0, "receiver down")


def test_profile_lane_emit_smoke_start_finish_without_secret_leak(monkeypatch):
    captured = []

    class FakeResponse:
        status = 202

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"status":"accepted","threadKey":"ath_thread"}'

    def fake_urlopen(request, timeout):
        captured.append(request)
        return FakeResponse()

    monkeypatch.setattr(profile_lane.urllib.request, "urlopen", fake_urlopen)
    secret = "super-secret-sentinel"
    start = build_profile_lane_event(
        thread_key="ath_thread",
        producer_id="relay-ath-dev",
        event_type="relay.lane.started",
        lane="issue19-docs",
        profile="ebi",
        summary="started",
        event_id="evt-start",
    )
    finish = build_profile_lane_event(
        thread_key="ath_thread",
        producer_id="relay-ath-dev",
        event_type="relay.lane.finished",
        lane="issue19-docs",
        profile="ebi",
        summary="finished",
        event_id="evt-finish",
        telemetry={"runtime_seconds": 1, "tokens": 0},
        payload={"verification": "passed"},
    )

    assert emit_signed_event(url="http://127.0.0.1/events", event=start, secret=secret)[0] == 202
    assert emit_signed_event(url="http://127.0.0.1/events", event=finish, secret=secret)[0] == 202
    assert len(captured) == 2
    for request in captured:
        body = request.data.decode("utf-8")
        assert secret not in body
        assert secret not in str(request.headers)
        assert request.headers["X-hermes-signature-256"].startswith("sha256=")


def test_load_registry_handle_reads_enabled_handle_without_returning_disabled(tmp_path):
    db = tmp_path / "registry.sqlite3"
    con = sqlite3.connect(db)
    con.execute(
        "create table async_thread_handles(thread_key text primary key, producer_id text not null, secret text not null, enabled integer not null)"
    )
    con.execute("insert into async_thread_handles values('ath_enabled', 'relay-ath-dev', 'secret1', 1)")
    con.execute("insert into async_thread_handles values('ath_disabled', 'relay-ath-dev', 'secret2', 0)")
    con.commit()
    con.close()

    assert load_registry_handle(db, "ath_enabled") == ("relay-ath-dev", "secret1")
    try:
        load_registry_handle(db, "ath_disabled")
    except LookupError as exc:
        assert "ath_disabled" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("disabled handle unexpectedly loaded")
