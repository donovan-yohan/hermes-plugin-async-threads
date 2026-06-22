import hashlib
import hmac
import json
import time
from types import SimpleNamespace

import pytest

from async_threads import commands as commands_module
from async_threads.adapter import AsyncThreadsAdapter
from async_threads.listeners import ListenResult, create_listener
from async_threads.registry import AsyncThreadHandle, AsyncThreadRegistry
from async_threads.workflows import WorkflowPolicy
from gateway.config import Platform, PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry
from gateway.session import SessionSource


class PluginPlatform:
    value = "async_threads"

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return getattr(other, "value", None) == self.value


@pytest.fixture(autouse=True)
def register_async_threads_platform():
    if not platform_registry.is_registered("async_threads"):
        platform_registry.register(
            PlatformEntry(
                name="async_threads",
                label="Async Threads",
                adapter_factory=lambda cfg: AsyncThreadsAdapter(cfg),
                check_fn=lambda: True,
            )
        )
    yield


class FakeStore:
    def get_session_by_key(self, session_key):
        return SimpleNamespace(session_id="sid-service")


class FakeSendAdapter:
    def __init__(self):
        self.config = SimpleNamespace(extra={"group_sessions_per_user": True, "thread_sessions_per_user": False})
        self.sent = []

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append((chat_id, content, metadata))
        return SimpleNamespace(success=True)


class FakeRequest:
    def __init__(self, body: bytes, secret: str):
        digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        self._body = body
        self.headers = {"X-Hermes-Signature-256": f"sha256={digest}"}
        self.remote = "127.0.0.1"

    async def read(self):
        return self._body


def _gateway(registry_path):
    async_adapter = SimpleNamespace(
        config=PlatformConfig(
            enabled=True,
            extra={"registry_path": str(registry_path), "host": "127.0.0.1", "port": 9999, "secret_root": str(registry_path.parent / "secrets")},
        )
    )
    return SimpleNamespace(
        adapters={PluginPlatform(): async_adapter},
        config=SimpleNamespace(group_sessions_per_user=True, thread_sessions_per_user=False),
        session_store=FakeStore(),
    )


def _source():
    return SessionSource(platform=Platform.DISCORD, chat_id="c1", chat_type="channel", thread_id="t1", user_id="u1")


def _event_body(handle, event_id="evt-service", event_type="demo.job.finished"):
    return json.dumps(
        {
            "version": "async-thread-event/v1",
            "eventId": event_id,
            "eventType": event_type,
            "producer": {"id": handle.producer_id},
            "occurredAt": time.time(),
            "asyncThread": {"threadKey": handle.thread_key},
            "summary": "job finished",
            "payload": {"status": "passed"},
        }
    ).encode()


def test_create_listener_persists_current_source_session_key_and_owner(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    source = _source()

    result = create_listener(
        registry=registry,
        source=source,
        gateway=_gateway(tmp_path / "ath.sqlite3"),
        producer_id="demo",
        allowed_event_types=["demo.job.finished"],
        label="job watcher",
        event_url="http://localhost:9999/async-threads/v1/events",
    )

    handle = registry.get_handle(result.thread_key)
    assert handle is not None
    assert handle.source == source.to_dict()
    assert handle.owner_user_id == "u1"
    assert handle.session_key
    assert handle.session_id == "sid-service"
    assert handle.label == "job watcher"
    assert handle.allowed_event_types == ("demo.job.finished",)
    assert result.public_summary()["threadKey"] == handle.thread_key
    assert result.public_summary()["sessionKeyPresent"] is True
    assert handle.secret not in json.dumps(result.public_summary(), sort_keys=True)


def test_create_listener_normalizes_direct_policy_ack_and_debounce(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")

    result = create_listener(
        registry=registry,
        source=_source(),
        gateway=_gateway(tmp_path / "ath.sqlite3"),
        producer_id="demo",
        policy="direct",
        ack_mode="debug",
        debounce_seconds=120,
        gate_order=["review", "qa"],
        gate_mode="parallel",
        stale_on_artifact_change=["review"],
        candidate_required=["qa"],
    )

    handle = result.handle
    assert handle.policy == "direct"
    assert handle.ack_mode == "none"
    assert handle.debounce_seconds == 0
    assert handle.workflow_policy == WorkflowPolicy(
        gate_order=("review", "qa"),
        gate_mode="parallel",
        stale_on_artifact_change=("review",),
        candidate_required=("qa",),
    )


def test_create_listener_returns_structured_result_without_formatting(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")

    result = create_listener(
        registry=registry,
        source=_source(),
        gateway=_gateway(tmp_path / "ath.sqlite3"),
        producer_id="demo",
        allowed_event_types=["demo.job.finished"],
    )

    assert isinstance(result, ListenResult)
    assert not isinstance(result, str)
    assert "created async-thread listener" not in json.dumps(result.public_summary())
    assert result.handle.secret not in json.dumps(result.public_summary())


def test_slash_listen_uses_shared_listener_service(monkeypatch, tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    source = _source()
    gateway = _gateway(tmp_path / "ath.sqlite3")
    captured = {}
    fake_handle = AsyncThreadHandle(
        thread_key="ath_fake",
        source=source.to_dict(),
        producer_id="demo",
        secret="fake-secret",
        policy="agent_queue",
        label="demo watcher",
        allowed_event_types=("demo.job.finished",),
        session_key="session-key",
        session_id="sid-service",
        owner_user_id="u1",
        ack_mode="brief",
        debounce_seconds=30,
        workflow_policy=WorkflowPolicy(gate_order=("review",), gate_mode="serial"),
    )

    def fake_create_listener(**kwargs):
        captured.update(kwargs)
        return ListenResult(handle=fake_handle, event_url=kwargs.get("event_url", ""), source=source.to_dict())

    monkeypatch.setattr(commands_module, "create_listener", fake_create_listener)

    output = commands_module._cmd_listen(
        ["demo", "--events", "demo.job.finished", "--label", "demo watcher", "--ack", "brief", "--debounce", "30", "--gate-order", "review"],
        event=SimpleNamespace(source=source),
        gateway=gateway,
        registry=registry,
    )

    assert captured["registry"] is registry
    assert captured["source"] is source
    assert captured["gateway"] is gateway
    assert captured["producer_id"] == "demo"
    assert captured["allowed_event_types"] == ["demo.job.finished"]
    assert captured["label"] == "demo watcher"
    assert captured["ack_mode"] == "brief"
    assert captured["debounce_seconds"] == "30"
    assert captured["gate_order"] == ["review"]
    assert "created async-thread listener" in output
    assert "secretFile:" in output
    assert "contractFile:" in output
    assert "raw secret is not printed" in output
    assert "fake-secret" not in output


@pytest.mark.asyncio
async def test_service_created_direct_handle_accepts_signed_event_like_slash_handle(tmp_path):
    registry_path = tmp_path / "ath.sqlite3"
    registry = AsyncThreadRegistry(registry_path)
    source = _source()
    result = create_listener(
        registry=registry,
        source=source,
        gateway=_gateway(registry_path),
        producer_id="demo",
        allowed_event_types=["demo.job.finished"],
        policy="direct",
    )
    target = FakeSendAdapter()
    adapter = AsyncThreadsAdapter(PlatformConfig(enabled=True, extra={"registry_path": str(registry_path)}))
    adapter.gateway_runner = SimpleNamespace(adapters={Platform.DISCORD: target})

    response = await adapter._handle_event(FakeRequest(_event_body(result.handle), result.handle.secret))

    assert response.status == 200
    assert len(target.sent) == 1
    chat_id, content, metadata = target.sent[0]
    assert chat_id == "c1"
    assert "job finished" in content
    assert metadata == {"thread_id": "t1"}
    recent = registry.list_recent_events(thread_key=result.thread_key, owner_user_id="u1", limit=5)
    assert [event.outcome for event in recent] == ["direct_delivered"]
