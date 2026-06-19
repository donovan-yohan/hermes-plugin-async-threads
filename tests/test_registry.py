from pathlib import Path

import pytest

from async_threads import registry as registry_module
from async_threads.registry import AsyncThreadRegistry


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
    assert "idx_event_log_thread_key" in indexes

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
