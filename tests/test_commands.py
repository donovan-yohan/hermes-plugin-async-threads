from types import SimpleNamespace

from async_threads.commands import _cmd_inspect, _cmd_list, _cmd_set_enabled, _run_command, handle_pre_gateway_dispatch
from async_threads.registry import AsyncThreadRegistry
from gateway.config import Platform, PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry
from gateway.session import SessionSource
from async_threads.adapter import AsyncThreadsAdapter


class FakeAdapter:
    def __init__(self):
        self.config = PlatformConfig(enabled=True, extra={"registry_path": ""})


class FakeStore:
    def get_session_by_key(self, session_key):
        return SimpleNamespace(session_id="sid1")


def test_listen_captures_current_source_and_returns_secret(tmp_path):
    registry_path = tmp_path / "ath.sqlite3"
    if not platform_registry.is_registered("async_threads"):
        platform_registry.register(
            PlatformEntry(
                name="async_threads",
                label="Async Threads",
                adapter_factory=lambda cfg: AsyncThreadsAdapter(cfg),
                check_fn=lambda: True,
            )
        )
    async_adapter = SimpleNamespace(config=PlatformConfig(enabled=True, extra={"registry_path": str(registry_path), "port": 9999}))
    gateway = SimpleNamespace(
        adapters={Platform("async_threads"): async_adapter},
        config=SimpleNamespace(group_sessions_per_user=True, thread_sessions_per_user=False),
        session_store=FakeStore(),
    )
    source = SessionSource(platform=Platform.DISCORD, chat_id="c", chat_type="channel", thread_id="t", user_id="u")
    event = SimpleNamespace(source=source)

    response = _run_command(
        "listen relay --events relay.session.pr_opened --label chunk --ack brief",
        event=event,
        gateway=gateway,
    )

    assert "created async-thread listener" in response
    assert "threadKey:" in response
    assert "secret:" in response
    assert "relay.session.pr_opened" in response
    assert "ack: `brief`" in response
    [handle] = AsyncThreadRegistry(registry_path).list_handles(owner_user_id="u")
    assert handle.ack_mode == "brief"

    direct_response = _run_command(
        "listen relay --policy direct --ack debug",
        event=event,
        gateway=gateway,
    )
    assert "policy: `direct`" in direct_response
    assert "ack: `none`" in direct_response


def test_help_for_unknown_command():
    gateway = SimpleNamespace(adapters={}, config=SimpleNamespace(), session_store=None)
    event = SimpleNamespace(source=SimpleNamespace(user_id="u"))
    assert "commands:" in _run_command("wat", event=event, gateway=gateway)


def test_pre_gateway_hook_returns_skip_dict_for_ath_help():
    source = SessionSource(platform=Platform.DISCORD, chat_id="c", chat_type="channel", thread_id="t", user_id="u")
    event = SimpleNamespace(text="/ath help", source=source)
    gateway = SimpleNamespace(
        adapters={},
        config=SimpleNamespace(group_sessions_per_user=True, thread_sessions_per_user=False),
        session_store=None,
        _is_user_authorized=lambda s: True,
    )

    result = handle_pre_gateway_dispatch(event=event, gateway=gateway, session_store=None)

    assert result == {"action": "skip", "reason": "async_threads_command"}


def test_listener_management_commands_are_owner_scoped(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    mine = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id="relay",
        owner_user_id="u1",
        session_key="agent:secret-session-key",
    )
    other = registry.create_handle(
        source={"platform": "discord", "chat_id": "c2", "chat_type": "channel", "thread_id": "t2"},
        producer_id="relay",
        owner_user_id="u2",
    )

    listing = _cmd_list(registry, owner_user_id="u1")
    assert mine.thread_key in listing
    assert other.thread_key not in listing

    assert _cmd_list(registry, owner_user_id="") == "no async-thread listeners for this user. create one with `/ath listen <producer>`."
    inspected_mine = _cmd_inspect(registry, mine.thread_key, owner_user_id="u1")
    assert "producer: `relay`" in inspected_mine
    assert "sessionKey: present hash=`" in inspected_mine
    assert "agent:secret-session-key" not in inspected_mine
    assert _cmd_inspect(registry, other.thread_key, owner_user_id="u1") == "async-thread listener not found."
    assert _cmd_set_enabled(registry, other.thread_key, False, "paused", owner_user_id="u1") == "async-thread listener not found."
    other_after_denied = registry.get_handle(other.thread_key)
    assert other_after_denied is not None
    assert other_after_denied.enabled is True
    assert _cmd_set_enabled(registry, mine.thread_key, False, "paused", owner_user_id="u1") == f"paused async-thread listener `{mine.thread_key}`."
    mine_after_pause = registry.get_handle(mine.thread_key)
    assert mine_after_pause is not None
    assert mine_after_pause.enabled is False


def test_status_events_and_inspect_show_owner_scoped_diagnostics(tmp_path):
    registry_path = tmp_path / "ath.sqlite3"
    registry = AsyncThreadRegistry(registry_path)
    mine = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id="relay",
        owner_user_id="u1",
    )
    other = registry.create_handle(
        source={"platform": "discord", "chat_id": "c2", "chat_type": "channel", "thread_id": "t2"},
        producer_id="relay",
        owner_user_id="u2",
    )
    registry.log_event(
        producer_id="relay",
        event_id="evt_123456789",
        thread_key=mine.thread_key,
        event_type="relay.session.pr_opened",
        outcome="accepted",
        summary="PR opened token=supersecret Bearer abc123 for agent:main:discord:channel:c:t and ready for review",
        detail={
            "target_platform": "discord",
            "gateway_runner_exists": True,
            "target_adapter_exists": True,
            "policy": "agent_queue",
            "ack_mode": "debug",
            "ack_sent": True,
            "ack_success": True,
            "session_key_present": True,
            "session_key_hash": "abc123def456",
            "active_session": False,
            "queued": False,
            "handle_message_called": True,
            "handle_message_returned": True,
            "secret": "not-stored",
        },
    )
    registry.log_event(
        producer_id="relay",
        event_id="evt_rejected",
        thread_key=mine.thread_key,
        event_type="relay.session.pr_opened",
        outcome="rejected_signature",
        summary="secret=bad should not echo",
    )
    registry.log_event(
        producer_id="relay",
        event_id="evt_other",
        thread_key=other.thread_key,
        event_type="relay.session.pr_opened",
        outcome="accepted",
        summary="should not leak",
    )
    async_adapter = SimpleNamespace(
        config=PlatformConfig(
            enabled=True,
            extra={"registry_path": str(registry_path), "host": "0.0.0.0", "port": 9999},
        ),
        _running=True,
    )
    gateway = SimpleNamespace(
        adapters={Platform("async_threads"): async_adapter},
        config=SimpleNamespace(group_sessions_per_user=True, thread_sessions_per_user=False),
        session_store=None,
    )
    event = SimpleNamespace(source=SimpleNamespace(user_id="u1"))

    status = _run_command("status", event=event, gateway=gateway)
    assert "receiver: `http://localhost:9999/async-threads/v1/events`" in status
    assert f"registry: `{registry_path}`" in status
    assert "listeners: 1" in status
    assert "recent events: 2" in status

    events = _run_command("events --limit 5", event=event, gateway=gateway)
    assert mine.thread_key in events
    assert "…23456789" in events
    assert "outcome=`agent_started (legacy accepted)`" in events
    assert "token=<redacted>" in events
    assert "Bearer <redacted>" in events
    assert "target_platform=discord" in events
    assert "gateway_runner_exists=True" in events
    assert "target_adapter_exists=True" in events
    assert "ack_mode=debug" in events
    assert "ack_sent=True" in events
    assert "ack_success=True" in events
    assert "session_key_hash=abc123def456" in events
    assert "handle_message_called=True" in events
    assert "not-stored" not in events
    assert "agent:main:discord:channel:c:t" not in events
    assert "agent:<redacted>" in events
    assert "supersecret" not in events
    assert "secret=bad" not in events
    assert "should not echo" not in events
    assert "should not leak" not in events

    inspected = _cmd_inspect(registry, mine.thread_key, owner_user_id="u1")
    assert "recent events:" in inspected
    assert "relay.session.pr_opened" in inspected
    assert "secret: hidden" in inspected
