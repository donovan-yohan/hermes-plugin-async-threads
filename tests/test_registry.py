from pathlib import Path

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

    listed = reg.list_handles(owner_user_id="u1")
    assert [h.thread_key for h in listed] == [handle.thread_key]

    assert reg.mark_seen(producer_id="relay", event_id="evt1", thread_key=handle.thread_key) is True
    assert reg.mark_seen(producer_id="relay", event_id="evt1", thread_key=handle.thread_key) is False

    assert reg.revoke(handle.thread_key) is True
    assert reg.get_handle(handle.thread_key).enabled is False
