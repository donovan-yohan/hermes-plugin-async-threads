import json
import sqlite3
from pathlib import Path

import pytest

from async_threads import registry as registry_module
from async_threads.registry import AsyncThreadRegistry, SCHEMA_VERSION, sanitize_event_detail


def test_registry_creates_lists_revokes_and_dedupes(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="relay",
        allowed_event_types=["relay.session.pr_opened"],
        session_key="agent:main:discord:channel:c:t",
        owner_user_id="u1",
    )

    assert handle.thread_key.startswith("ath_")
    assert handle.secret
    assert handle.enabled is True
    assert handle.allowed_event_types == ("relay.session.pr_opened",)
    with reg._connect() as conn:
        indexes = {
            row[0]
            for row in conn.execute("select name from sqlite_master where type = 'index'").fetchall()
        }
        event_log_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(event_log)").fetchall()
        }
        schema_version = conn.execute("select value from meta where key = 'schema_version'").fetchone()[0]
    assert "idx_event_log_thread_key" in indexes
    assert "detail_json" in event_log_columns
    assert schema_version == str(SCHEMA_VERSION)

    listed = reg.list_handles(owner_user_id="u1")
    assert [h.thread_key for h in listed] == [handle.thread_key]

    assert reg.mark_seen(producer_id="relay", event_id="evt1", thread_key=handle.thread_key) is True
    assert reg.mark_seen(producer_id="relay", event_id="evt1", thread_key=handle.thread_key) is False

    reg.log_event(
        producer_id="relay",
        event_id="evt1",
        thread_key=handle.thread_key,
        event_type="relay.session.pr_opened",
        outcome="accepted",
        summary="PR opened with a long summary that remains diagnostic only",
        detail={
            "target_platform": "discord",
            "policy": "agent_queue",
            "session_key_present": True,
            "active_session": False,
            "queued": False,
            "exception_message": "token=abc Bearer def should redact",
            "secret": "do-not-store",
            "payload": {"raw": "nope"},
            "signature_valid": False,
        },
    )
    reg.log_event(
        producer_id="other",
        event_id="evt2",
        thread_key="ath_other",
        event_type="relay.session.pr_opened",
        outcome="accepted",
        summary="not this user",
    )
    assert reg.count_handles(owner_user_id="u1") == 1
    assert reg.count_recent_events(owner_user_id="u1") == 1
    assert reg.count_recent_events(thread_key=handle.thread_key, owner_user_id="u1") == 1
    recent = reg.list_recent_events(thread_key=handle.thread_key, owner_user_id="u1", limit=5)
    assert len(recent) == 1
    assert recent[0].event_id == "evt1"
    assert recent[0].summary.startswith("PR opened")
    assert recent[0].detail == {
        "active_session": False,
        "exception_message": "token=<redacted> Bearer <redacted> should redact",
        "policy": "agent_queue",
        "queued": False,
        "session_key_present": True,
        "target_platform": "discord",
    }
    with reg._connect() as conn:
        detail_json = conn.execute("select detail_json from event_log where event_id = 'evt1'").fetchone()[0]
    assert detail_json == json.dumps(recent[0].detail, sort_keys=True, separators=(",", ":"))
    assert "do-not-store" not in detail_json
    assert "signature_valid" not in detail_json

    assert reg.revoke(handle.thread_key) is True
    assert reg.get_handle(handle.thread_key).enabled is False


def test_registry_connect_closes_connections(tmp_path: Path, monkeypatch):
    closed = []
    original_connect = registry_module.sqlite3.connect

    class TrackingConnection(registry_module.sqlite3.Connection):
        def close(self):
            closed.append(self)
            super().close()

    def tracking_connect(*args, **kwargs):
        kwargs.setdefault("factory", TrackingConnection)
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(registry_module.sqlite3, "connect", tracking_connect)
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    assert len(closed) == 1

    reg.list_handles(owner_user_id="u1")
    assert len(closed) == 2

    with pytest.raises(RuntimeError, match="boom"):
        with reg._connect() as conn:
            conn.execute("select 1")
            raise RuntimeError("boom")
    assert len(closed) == 3


def test_v1_registry_migrates_detail_json_without_data_loss(tmp_path: Path):
    db_path = tmp_path / "v1.sqlite3"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            create table meta(key text primary key, value text not null);
            insert into meta(key, value) values('schema_version', '1');
            create table async_thread_handles(
                thread_key text primary key,
                created_at text not null,
                updated_at text not null,
                enabled integer not null default 1,
                label text not null default '',
                source_json text not null,
                session_key text not null default '',
                session_id text not null default '',
                owner_user_id text not null default '',
                producer_id text not null,
                secret text not null,
                allowed_event_types_json text not null default '[]',
                policy text not null default 'agent_queue'
            );
            create table seen_events(
                producer_id text not null,
                event_id text not null,
                thread_key text not null,
                first_seen_at text not null,
                primary key (producer_id, event_id)
            );
            create table event_log(
                id integer primary key autoincrement,
                producer_id text not null,
                event_id text not null,
                thread_key text,
                event_type text,
                outcome text not null,
                summary text,
                created_at text not null
            );
            """
        )
        conn.execute(
            """
            insert into async_thread_handles(
                thread_key, created_at, updated_at, enabled, label, source_json,
                session_key, session_id, owner_user_id, producer_id, secret,
                allowed_event_types_json, policy
            ) values (?, ?, ?, 1, '', ?, '', '', ?, ?, ?, '[]', 'agent_queue')
            """,
            (
                "ath_v1",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
                json.dumps({"platform": "discord", "chat_id": "c", "chat_type": "channel"}),
                "u1",
                "relay",
                "secret",
            ),
        )
        conn.execute(
            """
            insert into event_log(producer_id, event_id, thread_key, event_type, outcome, summary, created_at)
            values ('relay', 'evt_old', 'ath_v1', 'relay.old', 'accepted', 'old row', '2026-01-01T00:00:01Z')
            """
        )
        conn.commit()
    finally:
        conn.close()

    reg = AsyncThreadRegistry(db_path)

    with reg._connect() as migrated:
        columns = {row["name"] for row in migrated.execute("pragma table_info(event_log)").fetchall()}
        schema_version = migrated.execute("select value from meta where key = 'schema_version'").fetchone()[0]
        detail_json = migrated.execute("select detail_json from event_log where event_id = 'evt_old'").fetchone()[0]
    assert "detail_json" in columns
    assert schema_version == str(SCHEMA_VERSION)
    assert detail_json == "{}"
    old_events = reg.list_recent_events(thread_key="ath_v1", owner_user_id="u1")
    assert len(old_events) == 1
    assert old_events[0].summary == "old row"
    assert old_events[0].detail == {}


def test_sanitize_event_detail_allowlists_and_redacts_safe_metadata():
    detail = sanitize_event_detail(
        {
            "target_platform": "discord",
            "policy": "agent_queue",
            "session_key_present": True,
            "active_session": False,
            "queued": False,
            "target_adapter_exists": True,
            "direct_send_success": False,
            "exception_class": "RuntimeError",
            "exception_message": "authorization: Basic abc123; signature sha256=deadbeef; cookie: sessionid=abc123; other=x; sessionKey=agent:secret-session-key",
            "error": "secret=value api_key=abc x-api-key: def token=ghi Bearer bearer-token",
            "secret": "drop",
            "token": "drop",
            "payload": {"drop": True},
            "raw_body": "drop",
            "unknown_safe_sounding": "drop",
        }
    )

    assert detail == {
        "active_session": False,
        "direct_send_success": False,
        "error": "secret=<redacted> api_key=<redacted> x-api-key=<redacted> token=<redacted> Bearer <redacted>",
        "exception_class": "RuntimeError",
        "exception_message": "authorization=<redacted>; signature=<redacted>; cookie=<redacted>; other=x; sessionKey=<redacted>",
        "policy": "agent_queue",
        "queued": False,
        "session_key_present": True,
        "target_adapter_exists": True,
        "target_platform": "discord",
    }


def test_sanitize_event_detail_bounds_regex_input_before_output_truncation():
    detail = sanitize_event_detail(
        {"exception_message": ("prefix " * 20) + "authorization: Basic abc123 signature sha256=deadbeef KeyError('agent:main:discord:channel:c:t')"}
    )

    assert len(detail["exception_message"]) <= 200
    assert "abc123" not in detail["exception_message"]
    assert "deadbeef" not in detail["exception_message"]
    assert "agent:main:discord:channel:c:t" not in detail["exception_message"]
    assert "authorization=<redacted>" in detail["exception_message"]


def test_sanitize_event_detail_redacts_raw_session_key_shapes():
    detail = sanitize_event_detail({"exception_message": "KeyError('agent:main:discord:channel:c:t')"})

    assert detail["exception_message"] == "KeyError('agent:<redacted>')"
