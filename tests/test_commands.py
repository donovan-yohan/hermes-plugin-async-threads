from types import SimpleNamespace

import pytest

from async_threads.commands import (
    _cmd_emit_command,
    _cmd_events,
    _cmd_inspect,
    _cmd_lifecycle,
    _cmd_list,
    _cmd_prune,
    _cmd_set_enabled,
    _cmd_trace,
    _cmd_workflows,
    _run_command,
    _send_notice,
    _utc_days_ago,
    handle_pre_gateway_dispatch,
)
from async_threads.registry import AsyncThreadRegistry
from gateway.config import Platform, PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry
from gateway.session import SessionSource
from async_threads.adapter import AsyncThreadsAdapter


class FakeAdapter:
    def __init__(self):
        self.config = PlatformConfig(enabled=True, extra={"registry_path": ""})


class FakeSendAdapter:
    def __init__(self):
        self.sent = []

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append((chat_id, content, metadata))
        return SimpleNamespace(success=True)


class FakeStore:
    def get_session_by_key(self, session_key):
        return SimpleNamespace(session_id="sid1")


def test_listen_captures_current_source_and_returns_secret_reference(tmp_path):
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
    async_adapter = SimpleNamespace(
        config=PlatformConfig(
            enabled=True,
            extra={"registry_path": str(registry_path), "port": 9999, "secret_root": str(tmp_path / "secrets")},
        )
    )
    gateway = SimpleNamespace(
        adapters={Platform("async_threads"): async_adapter},
        config=SimpleNamespace(group_sessions_per_user=True, thread_sessions_per_user=False),
        session_store=FakeStore(),
    )
    source = SessionSource(platform=Platform.DISCORD, chat_id="c", chat_type="channel", thread_id="t", user_id="u")
    event = SimpleNamespace(source=source)

    response = _run_command(
        "listen relay --events relay.session.pr_opened --label chunk --ack brief --debounce 45 --gate-order review,qa --gate-mode serial --stale-on-artifact-change review,qa --candidate-required qa",
        event=event,
        gateway=gateway,
    )

    assert "created async-thread listener" in response
    assert "threadKey:" in response
    assert "secretFile:" in response
    assert "contractFile:" in response
    assert "raw secret is not printed" in response
    assert "relay.session.pr_opened" in response
    assert "ack: `brief`" in response
    assert "debounce: `45s`" in response
    assert "workflow gates: serial order=review,qa; stale_on_artifact_change=review,qa; candidate_required=qa" in response
    [handle] = AsyncThreadRegistry(registry_path).list_handles(owner_user_id="u")
    assert handle.ack_mode == "brief"
    assert handle.debounce_seconds == 45
    assert handle.workflow_policy.gate_order == ("review", "qa")
    assert handle.workflow_policy.candidate_required == ("qa",)
    assert handle.secret not in response
    secret_file = tmp_path / "secrets" / handle.thread_key / "secret.txt"
    assert secret_file.read_text(encoding="utf-8").strip() == handle.secret

    direct_response = _run_command(
        "listen relay --policy direct --ack debug",
        event=event,
        gateway=gateway,
    )
    assert "policy: `direct`" in direct_response
    assert "ack: `none`" in direct_response
    assert "debounce: `0s`" in direct_response

    rotate_response = _run_command(f"rotate-secret {handle.thread_key}", event=event, gateway=gateway)
    rotated = AsyncThreadRegistry(registry_path).get_handle(handle.thread_key)
    assert rotated is not None
    assert rotated.secret != handle.secret
    assert "rotated async-thread listener secret" in rotate_response
    assert "secretFile:" in rotate_response
    assert rotated.secret not in rotate_response
    assert secret_file.read_text(encoding="utf-8").strip() == rotated.secret

    bad_response = _run_command(
        "listen relay --debounce 999",
        event=event,
        gateway=gateway,
    )
    assert bad_response == "invalid debounce seconds. use 0-300."


def test_help_for_unknown_command():
    gateway = SimpleNamespace(adapters={}, config=SimpleNamespace(), session_store=None)
    event = SimpleNamespace(source=SimpleNamespace(user_id="u"))
    assert "commands:" in _run_command("wat", event=event, gateway=gateway)


def test_pre_gateway_hook_fails_closed_without_authorized_source_or_auth_hook(tmp_path):
    registry_path = tmp_path / "ath.sqlite3"
    async_adapter = SimpleNamespace(config=PlatformConfig(enabled=True, extra={"registry_path": str(registry_path)}))
    source = SessionSource(platform=Platform.DISCORD, chat_id="c", chat_type="channel", thread_id="t", user_id="u")
    event = SimpleNamespace(text="/ath listen relay", source=source)

    for gateway in [
        SimpleNamespace(adapters={Platform("async_threads"): async_adapter}, config=SimpleNamespace(), session_store=None),
        SimpleNamespace(
            adapters={Platform("async_threads"): async_adapter},
            config=SimpleNamespace(),
            session_store=None,
            _is_user_authorized=lambda _source: (_ for _ in ()).throw(RuntimeError("auth backend down")),
        ),
    ]:
        assert handle_pre_gateway_dispatch(event=event, gateway=gateway, session_store=None) == {"action": "allow"}

    no_source_event = SimpleNamespace(text="/ath listen relay", source=None)
    gateway = SimpleNamespace(
        adapters={Platform("async_threads"): async_adapter},
        config=SimpleNamespace(),
        session_store=None,
        _is_user_authorized=lambda _source: True,
    )
    assert handle_pre_gateway_dispatch(event=no_source_event, gateway=gateway, session_store=None) == {"action": "allow"}
    assert AsyncThreadRegistry(registry_path).list_handles() == []


def test_events_command_redacts_bare_secret_shapes(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id="example-ci",
        owner_user_id="u1",
    )
    secrets = [
        "AKIA" + "IOSFODNN7EXAMPLE",
        "ghp_" + "abcdefghijklmnopqrstuvwxyz123456",
        "github_pat_" + ("A" * 22) + "_" + ("B" * 59),
        "sk-proj-" + "abcdefghijklmnopqrstuvwxyzABCDE12345",
        "xoxb-" + "123456789012-123456789012-abcdefghijklmnopqrstuv",
        "eyJ" + ("a" * 12) + "." + ("b" * 12) + "." + ("c" * 12),
        "abc123secret",
    ]
    registry.log_event(
        producer_id="example-ci",
        event_id="evt-" + secrets[1],
        thread_key=handle.thread_key,
        event_type="ci.build.finished",
        outcome="delivered",
        summary="finished with " + secrets[2],
        detail={"error": " ".join(secrets[:-1]) + " " + "-----BEGIN RSA " + "PRIVATE KEY-----\nabc123secret\n-----END RSA " + "PRIVATE KEY-----"},
    )

    output = _cmd_events(registry, thread_key=handle.thread_key, limit=5, owner_user_id="u1")

    for secret in secrets:
        assert secret not in output
    assert "<redacted>" in output or "redacted:" in output


def test_command_display_redacts_secret_shapes_across_list_inspect_and_workflows(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    classic_token = "ghp_" + "abcdefghijklmnopqrstuvwxyz123456"
    fine_grained_token = "github_pat_" + ("A" * 22) + "_" + ("B" * 59)
    handle = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id=classic_token,
        owner_user_id="u1",
        label="listener " + fine_grained_token,
    )
    registry.update_workflow_state_from_event(
        handle=handle,
        fields={
            "event_id": "evt-" + fine_grained_token,
            "event_type": "ci.build.finished",
            "producer_id": classic_token,
            "thread_key": handle.thread_key,
            "summary": "done " + fine_grained_token,
        },
        data={
            "workflowId": "wf-" + fine_grained_token,
            "stage": classic_token,
            "candidate": {"id": fine_grained_token, "readiness": "ready"},
            "evidence": {"kind": classic_token, "status": "passed"},
        },
    )

    outputs = [
        _cmd_list(registry, owner_user_id="u1"),
        _cmd_inspect(registry, handle.thread_key, owner_user_id="u1"),
        _cmd_workflows(registry, thread_key=handle.thread_key, limit=5, owner_user_id="u1"),
    ]

    for output in outputs:
        assert classic_token not in output
        assert fine_grained_token not in output
    assert any("<redacted>" in output or "redacted:" in output for output in outputs)


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


@pytest.mark.asyncio
async def test_command_notice_uses_platform_aware_send_metadata():
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="12345",
        chat_type="dm",
        thread_id="42",
        user_id="67890",
        message_id="99",
    )
    target = FakeSendAdapter()
    gateway = SimpleNamespace(adapters={Platform.TELEGRAM: target})
    event = SimpleNamespace(source=source)

    await _send_notice(gateway, event, "async-thread status")

    assert target.sent == [
        (
            "12345",
            "async-thread status",
            {
                "thread_id": "42",
                "telegram_dm_topic_reply_fallback": True,
                "direct_messages_topic_id": "42",
                "telegram_reply_to_message_id": "99",
            },
        )
    ]


def test_listener_management_commands_are_owner_scoped(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    mine = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id="relay",
        owner_user_id="u1",
        session_key="agent:secret-session-key",
        debounce_seconds=30,
    )
    other = registry.create_handle(
        source={"platform": "discord", "chat_id": "c2", "chat_type": "channel", "thread_id": "t2"},
        producer_id="relay",
        owner_user_id="u2",
    )

    listing = _cmd_list(registry, owner_user_id="u1")
    assert mine.thread_key in listing
    assert "debounce=30s" in listing
    assert other.thread_key not in listing

    assert _cmd_list(registry, owner_user_id="") == "no async-thread listeners for this user. create one with `/ath listen <producer>`."
    inspected_mine = _cmd_inspect(registry, mine.thread_key, owner_user_id="u1")
    assert "producer: `relay`" in inspected_mine
    assert "sessionKey: present hash=`" in inspected_mine
    assert "debounce: `30s`" in inspected_mine
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
    registry.update_workflow_state_from_event(
        handle=mine,
        fields={
            "event_id": "evt_workflow",
            "event_type": "job.review_passed",
            "producer_id": "relay",
            "thread_key": mine.thread_key,
            "summary": "workflow updated token=bad",
        },
        data={
            "workflowId": "wf-commands",
            "stage": "review_passed",
            "artifact": {"kind": "git_commit", "id": "abc123"},
            "candidate": {"id": "rc1", "readiness": "forming"},
            "evidence": {"kind": "review", "status": "passed"},
        },
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
    assert "workflows: 1" in status

    workflows = _run_command("workflows --limit 5", event=event, gateway=gateway)
    assert "wf-commands" in workflows
    assert "stage=`review_passed`" in workflows
    assert "review:passed" in workflows
    assert "token=bad" not in workflows

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


def test_trace_command_reports_sanitized_delivery_diagnostics(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id="example-ci",
        owner_user_id="u1",
    )
    token = "github_pat_" + ("A" * 22) + "_" + ("B" * 59)
    registry.log_event(
        producer_id="example-ci",
        event_id="evt-trace",
        thread_key=handle.thread_key,
        event_type="ci.build.finished",
        outcome="queued_active_session",
        summary="queued " + token,
        detail={"queued": True, "policy": "agent_queue", "exception_message": token},
    )

    text = _cmd_trace(registry, "evt-trace", as_json=False, owner_user_id="u1")
    as_json = _cmd_trace(registry, "evt-trace", as_json=True, owner_user_id="u1")

    assert "async-thread event trace" in text
    assert "queued behind an active target session" in text
    assert token not in text
    assert token not in as_json
    assert "<redacted>" in text or "<redacted>" in as_json
    assert _cmd_trace(registry, "evt-trace", as_json=False, owner_user_id="other") == "async-thread event not found."


def test_emit_command_template_is_owner_scoped_and_does_not_print_secret(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id="demo",
        owner_user_id="u1",
    )
    gateway = SimpleNamespace(adapters={}, config=SimpleNamespace(), session_store=None)

    output = _cmd_emit_command(
        registry,
        handle.thread_key,
        ["--event", "demo.job.finished", "--summary", "job ready"],
        gateway=gateway,
        owner_user_id="u1",
    )

    assert "sandbox-safe ATH emit template" in output
    assert "demo.job.finished" in output
    assert handle.thread_key in output
    assert handle.secret not in output
    assert "ATH_SECRET_FILE" in output
    assert "urllib.error.HTTPError" in output
    assert "urllib.error.URLError" in output
    assert _cmd_emit_command(registry, handle.thread_key, ["--event", "demo.job.finished"], gateway=gateway, owner_user_id="other") == "async-thread listener not found."


def test_lifecycle_command_documents_retirement_and_trace(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id="demo",
        owner_user_id="u1",
    )

    output = _cmd_lifecycle(registry, handle.thread_key, owner_user_id="u1")

    assert "started" in output
    assert "seriesKey" in output
    assert "/ath retire" in output
    assert "/ath trace" in output
    assert _cmd_lifecycle(registry, handle.thread_key, owner_user_id="other") == "async-thread listener not found."


def test_prune_command_dry_run_and_force_are_owner_scoped(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    mine = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "chat_type": "channel", "thread_id": "t"},
        producer_id="demo",
        owner_user_id="u1",
    )
    other = registry.create_handle(
        source={"platform": "discord", "chat_id": "c2", "chat_type": "channel", "thread_id": "t2"},
        producer_id="demo",
        owner_user_id="u2",
    )
    registry.mark_seen(producer_id="demo", event_id="old-mine", thread_key=mine.thread_key)
    registry.mark_seen(producer_id="demo", event_id="old-other", thread_key=other.thread_key)
    registry.log_event(producer_id="demo", event_id="old-mine", thread_key=mine.thread_key, event_type="demo.old", outcome="accepted", summary="old")
    registry.log_event(producer_id="demo", event_id="old-other", thread_key=other.thread_key, event_type="demo.old", outcome="accepted", summary="old")
    with registry._connect() as conn:
        conn.execute("update seen_events set first_seen_at = '2020-01-01T00:00:00Z'")
        conn.execute("update event_log set created_at = '2020-01-01T00:00:00Z'")

    config = PlatformConfig(enabled=True, extra={"event_log_retention_days": 1, "seen_event_retention_days": 1})
    dry = _cmd_prune(registry, ["--dry-run"], config=config, owner_user_id="u1")
    assert "would prune event_log rows: 1" in dry
    assert "would prune seen_events rows: 1" in dry
    assert registry.count_recent_events(owner_user_id="u1") == 1
    assert _cmd_prune(registry, ["--event-log-days", "-1"], config=config, owner_user_id="u1") == "invalid event-log retention days. use a non-negative integer."
    assert _cmd_prune(registry, ["--seen-days", "nope"], config=config, owner_user_id="u1") == "invalid seen-event retention days. use a non-negative integer."
    assert _utc_days_ago(999999) == "1970-01-01T00:00:00Z"

    forced = _cmd_prune(registry, ["--force"], config=config, owner_user_id="u1")
    assert "pruned event_log rows: 1" in forced
    assert "pruned seen_events rows: 1" in forced
    assert registry.count_recent_events(owner_user_id="u1") == 0
    assert registry.count_recent_events(owner_user_id="u2") == 1
    with registry._connect() as conn:
        assert conn.execute("select count(*) from seen_events where thread_key = ?", (mine.thread_key,)).fetchone()[0] == 0
        assert conn.execute("select count(*) from seen_events where thread_key = ?", (other.thread_key,)).fetchone()[0] == 1
