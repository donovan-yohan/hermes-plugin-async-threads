from types import SimpleNamespace

from async_threads.commands import _run_command, handle_pre_gateway_dispatch
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
        "listen relay --events relay.session.pr_opened --label chunk",
        event=event,
        gateway=gateway,
    )

    assert "created async-thread listener" in response
    assert "threadKey:" in response
    assert "secret:" in response
    assert "relay.session.pr_opened" in response


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
