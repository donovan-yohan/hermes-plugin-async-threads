import asyncio
import json
import sqlite3
import threading
from pathlib import Path

import pytest

from async_threads import registry as registry_module
from async_threads.ingress_digest import resolve_ingress_digest_policy
from async_threads.registry import AsyncThreadRegistry, AsyncThreadRegistryAsync, SCHEMA_VERSION, sanitize_event_detail
from async_threads.workflows import WorkflowPolicy, normalize_workflow_event


class _SpyRegistry:
    def __init__(self):
        self.calls = []
        self.attr = "plain-value"

    def returns_str(self):
        self.calls.append(("returns_str", threading.get_ident()))
        return "ok"

    def returns_none(self):
        self.calls.append(("returns_none", threading.get_ident()))
        return None

    def raises(self):
        self.calls.append(("raises", threading.get_ident()))
        raise ValueError("boom")


@pytest.mark.asyncio
async def test_async_registry_facade_offloads_calls(monkeypatch):
    spy = _SpyRegistry()
    facade = AsyncThreadRegistryAsync(spy)
    caller_ident = threading.get_ident()
    seen = []
    real_to_thread = asyncio.to_thread

    async def _spy_to_thread(func, *args, **kwargs):
        seen.append(getattr(func, "__name__", repr(func)))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(registry_module.asyncio, "to_thread", _spy_to_thread)

    assert await facade.returns_str() == "ok"
    await facade.returns_none()

    assert "returns_str" in seen
    assert facade.returns_str is facade.returns_str
    assert spy.calls
    assert all(thread_ident != caller_ident for _name, thread_ident in spy.calls)


def test_async_registry_facade_exposes_plain_attributes():
    assert AsyncThreadRegistryAsync(_SpyRegistry()).attr == "plain-value"


@pytest.mark.asyncio
async def test_async_registry_facade_propagates_exceptions():
    with pytest.raises(ValueError, match="boom"):
        await AsyncThreadRegistryAsync(_SpyRegistry()).raises()


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
    assert handle.ack_mode == "none"
    with reg._connect() as conn:
        indexes = {
            row[0]
            for row in conn.execute("select name from sqlite_master where type = 'index'").fetchall()
        }
        event_log_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(event_log)").fetchall()
        }
        handle_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(async_thread_handles)").fetchall()
        }
        schema_version = conn.execute("select value from meta where key = 'schema_version'").fetchone()[0]
    assert "idx_event_log_thread_key" in indexes
    assert "detail_json" in event_log_columns
    assert "ack_mode" in handle_columns
    assert "workflow_policy_json" in handle_columns
    assert "continuation_policy_json" in handle_columns
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
        summary="PR opened token=supersecret Bearer bearer-secret with a long summary that remains diagnostic only",
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
    assert "supersecret" not in recent[0].summary
    assert "bearer-secret" not in recent[0].summary
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
        stored_summary = conn.execute("select summary from event_log where event_id = 'evt1'").fetchone()[0]
    assert detail_json == json.dumps(recent[0].detail, sort_keys=True, separators=(",", ":"))
    assert "supersecret" not in stored_summary
    assert "bearer-secret" not in stored_summary
    assert "do-not-store" not in detail_json
    assert "signature_valid" not in detail_json

    assert reg.revoke(handle.thread_key) is True
    assert reg.get_handle(handle.thread_key).enabled is False


def test_source_binding_registry_owner_scope_lifecycle_and_compatibility(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    mine = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="ath-kanban-bridge",
        allowed_event_types=["kanban.task.blocked", "kanban.task.completed"],
        owner_user_id="u1",
    )
    other = reg.create_handle(
        source={"platform": "discord", "chat_id": "c2", "thread_id": "t2", "chat_type": "channel"},
        producer_id="ath-kanban-bridge",
        owner_user_id="u2",
    )

    binding = reg.create_source_binding(
        owner_user_id="u1",
        source="kanban",
        source_ref={"board": "default", "api_token": "secret=do-not-store"},
        listener_thread_key=mine.thread_key,
        event_filter={"eventTypes": ["kanban.task.blocked", "kanban.task.completed"]},
        cursor={"last_event_id": 42, "token": "abc123secret"},
        coalesce={"windowSeconds": 20},
    )

    assert binding.binding_id.startswith("athb_")
    assert binding.source == "kanban"
    assert binding.source_ref["api_token"] == "secret=<redacted>"
    assert "abc123secret" not in json.dumps(binding.cursor, sort_keys=True)
    assert reg.source_binding_compatibility(binding) == {
        "bindingStatus": "active",
        "failClosed": False,
        "listenerAllowedEventTypes": ["kanban.task.blocked", "kanban.task.completed"],
        "listenerEnabled": True,
        "listenerProducerId": "ath-kanban-bridge",
        "listenerThreadKey": mine.thread_key,
        "reason": "ok",
        "valid": True,
    }
    assert [item.binding_id for item in reg.list_source_bindings(owner_user_id="u1")] == [binding.binding_id]
    assert reg.get_source_binding(binding_id=binding.binding_id, owner_user_id="u2") is None
    with pytest.raises(ValueError, match="not found"):
        reg.create_source_binding(owner_user_id="u1", source="kanban", source_ref={"board": "default"}, listener_thread_key=other.thread_key)

    assert reg.set_source_binding_status(binding_id=binding.binding_id, owner_user_id="u2", status="paused") is False
    assert reg.set_source_binding_status(binding_id=binding.binding_id, owner_user_id="u1", status="paused") is True
    paused = reg.get_source_binding(binding_id=binding.binding_id, owner_user_id="u1")
    assert paused is not None
    assert paused.status == "paused"
    assert reg.source_binding_compatibility(paused)["reason"] == "binding_paused"
    assert reg.set_source_binding_status(binding_id=binding.binding_id, owner_user_id="u1", status="active") is True
    reg.set_enabled(mine.thread_key, False)
    disabled = reg.get_source_binding(binding_id=binding.binding_id, owner_user_id="u1")
    assert disabled is not None
    assert reg.source_binding_compatibility(disabled)["reason"] == "listener_disabled"
    assert reg.set_source_binding_status(binding_id=binding.binding_id, owner_user_id="u1", status="retired") is True
    assert reg.get_handle(mine.thread_key).enabled is False
    assert reg.list_source_bindings(owner_user_id="u1") == []
    assert reg.list_source_bindings(owner_user_id="u1", include_retired=True)[0].status == "retired"


def test_source_binding_compatibility_resolves_kanban_event_kinds_to_event_types(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    listener = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="ath-kanban-bridge",
        allowed_event_types=["kanban.task.completed"],
        owner_user_id="u1",
    )
    binding = reg.create_source_binding(
        owner_user_id="u1",
        source="kanban",
        source_ref={"board": "default"},
        listener_thread_key=listener.thread_key,
        event_filter={"eventKinds": ["completed", "review-required", "unblocked"]},
    )

    compatibility = reg.source_binding_compatibility(binding)

    assert compatibility["reason"] == "disallowed_event_types"
    assert compatibility["missingEventTypes"] == ["kanban.task.ready_for_review", "kanban.task.unblocked"]
    assert compatibility["valid"] is False
    assert compatibility["failClosed"] is True


def test_ingress_digest_policy_precedence_and_payload_pointer_storage(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    listener = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="ath-kanban-bridge",
        owner_user_id="u1",
        ingress_digest_policy={"enabled": True, "mode": "inline_summary", "store_event": "none"},
    )
    binding = reg.create_source_binding(
        owner_user_id="u1",
        source="kanban",
        source_ref={"board": "default"},
        listener_thread_key=listener.thread_key,
        ingress_digest_policy={"enabled": True, "mode": "pointer_summary", "store_event": "raw_local", "retention_seconds": 600},
    )
    reg.upsert_source_binding_outbox(
        binding_id=binding.binding_id,
        upstream_event_id=42,
        ath_event_id="evt-payload-pointer",
        event_type="kanban.task.completed",
        action="emit",
    )

    located = reg.find_source_binding_for_event(
        thread_key=listener.thread_key,
        producer_id="ath-kanban-bridge",
        event_id="evt-payload-pointer",
    )
    assert located is not None
    assert located.binding_id == binding.binding_id
    policy = resolve_ingress_digest_policy(
        global_policy={"enabled": True, "mode": "pointer"},
        listener_policy=listener.ingress_digest_policy,
        source_binding_policy=located.ingress_digest_policy,
    )
    assert policy.mode == "pointer_summary"
    assert policy.store_event == "raw_local"
    assert policy.source == "source_binding"

    fields = {
        "producer_id": "ath-kanban-bridge",
        "event_id": "evt-payload-pointer",
        "event_type": "kanban.task.completed",
        "thread_key": listener.thread_key,
        "summary": "done Bearer summary-token",
    }
    record = reg.store_event_payload(
        handle=listener,
        data={"payload": {"token": "payload-marker", "safe": "ok"}, "summary": "done Bearer summary-token"},
        fields=fields,
        policy=policy,
        source_binding_id=binding.binding_id,
    )

    assert record is not None
    assert record.pointer_id.startswith("athp_")
    assert record.storage_mode == "raw_local"
    assert record.source_binding_id == binding.binding_id
    assert "payload-marker" not in json.dumps(record.redacted_payload, sort_keys=True)
    assert record.raw_payload["payload"]["token"] == "payload-marker"
    fetched = reg.get_event_payload(owner_user_id="u1", pointer_id=record.pointer_id)
    assert fetched is not None
    assert fetched.event_id == "evt-payload-pointer"
    assert reg.get_event_payload(owner_user_id="u2", pointer_id=record.pointer_id) is None

    redaction_shaped_pointer = "athp_token_shaped_lookup_id"
    with reg._connect() as conn:
        conn.execute("update event_payloads set pointer_id = ? where pointer_id = ?", (redaction_shaped_pointer, record.pointer_id))
    fetched_raw = reg.get_event_payload(owner_user_id="u1", pointer_id=redaction_shaped_pointer)
    assert fetched_raw is not None
    assert fetched_raw.pointer_id == redaction_shaped_pointer


def test_ingress_digest_higher_precedence_active_overrides_lower_explicit_off():
    listener_policy = resolve_ingress_digest_policy(
        global_policy={"enabled": False},
        listener_policy={"enabled": True, "mode": "pointer_summary", "store_event": "redacted"},
    )
    assert listener_policy.active is True
    assert listener_policy.mode == "pointer_summary"
    assert listener_policy.store_event == "redacted"
    assert listener_policy.source == "listener"

    source_binding_policy = resolve_ingress_digest_policy(
        global_policy={"enabled": False, "mode": "off"},
        source_binding_policy={"enabled": True, "store_event": "raw_local"},
    )
    assert source_binding_policy.active is True
    assert source_binding_policy.mode == "pointer_summary"
    assert source_binding_policy.store_event == "raw_local"
    assert source_binding_policy.source == "source_binding"

    source_binding_disabled = resolve_ingress_digest_policy(
        global_policy={"enabled": True, "mode": "pointer"},
        listener_policy={"enabled": True, "mode": "pointer_summary", "store_event": "raw_local"},
        source_binding_policy={"enabled": False},
    )
    assert source_binding_disabled.active is False
    assert source_binding_disabled.source == "source_binding"

    source_binding_over_listener = resolve_ingress_digest_policy(
        global_policy={"enabled": True, "mode": "pointer"},
        listener_policy={"enabled": False},
        source_binding_policy={"enabled": True, "mode": "pointer_summary", "store_event": "raw_local"},
    )
    assert source_binding_over_listener.active is True
    assert source_binding_over_listener.mode == "pointer_summary"
    assert source_binding_over_listener.store_event == "raw_local"
    assert source_binding_over_listener.source == "source_binding"


def test_ingress_digest_defaults_off_and_explicit_listener_disable_wins(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="relay",
        owner_user_id="u1",
    )
    default_policy = resolve_ingress_digest_policy()
    assert default_policy.active is False
    assert reg.store_event_payload(
        handle=handle,
        data={"payload": {"status": "ok"}},
        fields={"producer_id": "relay", "event_id": "evt-default-off", "event_type": "relay.done", "summary": "done"},
        policy=default_policy,
    ) is None

    disabled = resolve_ingress_digest_policy(
        global_policy={"enabled": True, "mode": "pointer_summary", "store_event": "redacted"},
        listener_policy={"enabled": False},
    )
    assert disabled.active is False
    assert disabled.source == "listener"

    reenabled = resolve_ingress_digest_policy(
        global_policy={"enabled": False},
        listener_policy={"enabled": True, "mode": "pointer_summary", "store_event": "redacted"},
    )
    assert reenabled.active is True
    assert reenabled.mode == "pointer_summary"
    assert reenabled.source == "listener"

    binding_disabled = resolve_ingress_digest_policy(
        global_policy={"enabled": True, "mode": "pointer_summary", "store_event": "redacted"},
        listener_policy={"enabled": True, "mode": "inline_summary", "store_event": "none"},
        source_binding_policy={"enabled": False},
    )
    assert binding_disabled.active is False
    assert binding_disabled.source == "source_binding"


def test_normalize_workflow_event_ignores_non_mapping_payloads():
    assert normalize_workflow_event([], {"event_id": "evt", "event_type": "job.progress", "summary": "ignored"}) is None
    assert normalize_workflow_event("not-json-object", {"event_id": "evt", "event_type": "job.progress"}) is None


def test_normalize_workflow_event_derives_artifact_from_refs_head_sha():
    event = normalize_workflow_event(
        {
            "workflowId": "loop:release-readiness:run-42",
            "stage": "needs_attention",
            "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "a1b2c3d4"},
            "evidence": {"kind": "approval", "status": "passed"},
        },
        {"event_id": "evt", "event_type": "loop.approval_granted", "summary": "approved"},
    )

    assert event is not None
    assert event["artifact"] == {"kind": "git_commit", "id": "a1b2c3d4"}
    assert event["artifact_fingerprint"]


def test_workflow_state_tracks_candidate_gates_and_stale_evidence(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="relay",
        owner_user_id="u1",
        workflow_policy=WorkflowPolicy(
            gate_order=("review", "qa"),
            gate_mode="serial",
            stale_on_artifact_change=("review", "qa"),
            candidate_required=("qa",),
        ),
    )

    state = reg.update_workflow_state_from_event(
        handle=handle,
        fields={
            "event_id": "evt_review_passed",
            "event_type": "job.review",
            "producer_id": "relay",
            "thread_key": handle.thread_key,
            "summary": "review passed Bearer nope",
        },
        data={
            "workflowId": "wf-feature-1",
            "stage": "review_passed",
            "artifact": {"kind": "git_commit", "id": "abc123"},
            "candidate": {"id": "rc1", "kind": "feature", "readiness": "forming"},
            "evidence": {"kind": "review", "status": "passed", "summary": "looks ok"},
        },
    )

    assert state is not None
    assert state.stage == "review_passed"
    assert state.evidence["review"]["status"] == "passed"
    assert state.gates["active"] == []
    assert state.gates["deferred"] == ["qa"]
    assert state.gates["states"]["qa"]["state"] == "deferred_candidate_not_ready"
    assert "nope" not in state.last_summary

    updated = reg.update_workflow_state_from_event(
        handle=handle,
        fields={
            "event_id": "evt_candidate_ready",
            "event_type": "job.candidate_ready",
            "producer_id": "relay",
            "thread_key": handle.thread_key,
            "summary": "candidate ready",
        },
        data={
            "workflowId": "wf-feature-1",
            "stage": "candidate_ready",
            "artifact": {"kind": "git_commit", "id": "def456"},
            "candidate": {"id": "rc1", "kind": "feature", "readiness": "ready"},
        },
    )

    assert updated is not None
    assert updated.evidence["review"]["status"] == "stale"
    assert updated.evidence["review"]["stale_reason"] == "artifact_changed"
    assert updated.gates["active"] == ["review"]
    assert updated.gates["states"]["qa"]["state"] == "deferred_serial_gate"
    assert reg.count_workflow_states(owner_user_id="u1") == 1
    [listed] = reg.list_workflow_states(owner_user_id="u1")
    assert listed.workflow_id == "wf-feature-1"


def test_workflow_state_marks_approval_evidence_stale_after_artifact_change(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="approval-bridge",
        owner_user_id="u1",
        workflow_policy=WorkflowPolicy(gate_order=("approval",), stale_on_artifact_change=("approval",)),
    )

    approved = reg.update_workflow_state_from_event(
        handle=handle,
        fields={
            "event_id": "approval-merge-run-42-head-a1b2c3d4-approved",
            "event_type": "loop.approval_granted",
            "producer_id": "approval-bridge",
            "thread_key": handle.thread_key,
            "summary": "approval granted",
        },
        data={
            "workflowId": "loop:release-readiness:run-42",
            "stage": "needs_attention",
            "artifact": {"kind": "git_commit", "id": "a1b2c3d4"},
            "evidence": {"kind": "approval", "status": "passed", "summary": "trusted maintainer approved current head"},
        },
    )
    moved = reg.update_workflow_state_from_event(
        handle=handle,
        fields={
            "event_id": "github-pr-86-head-bbbb2222",
            "event_type": "github.pr.head_changed",
            "producer_id": "approval-bridge",
            "thread_key": handle.thread_key,
            "summary": "head changed",
        },
        data={
            "workflowId": "loop:release-readiness:run-42",
            "stage": "progress",
            "artifact": {"kind": "git_commit", "id": "bbbb2222"},
        },
    )

    assert approved is not None
    assert approved.evidence["approval"]["status"] == "passed"
    assert moved is not None
    assert moved.artifact["id"] == "bbbb2222"
    assert moved.evidence["approval"]["status"] == "stale"
    assert moved.evidence["approval"]["stale_reason"] == "artifact_changed"


def test_late_old_head_approval_is_recorded_stale_not_passed(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="approval-bridge",
        owner_user_id="u1",
        workflow_policy=WorkflowPolicy(gate_order=("approval",), stale_on_artifact_change=("approval",)),
    )

    current = reg.update_workflow_state_from_event(
        handle=handle,
        fields={
            "event_id": "github-pr-86-head-bbbb2222",
            "event_type": "github.pr.head_changed",
            "producer_id": "approval-bridge",
            "thread_key": handle.thread_key,
            "summary": "head changed",
        },
        data={
            "workflowId": "loop:release-readiness:run-42",
            "stage": "progress",
            "artifact": {"kind": "git_commit", "id": "bbbb2222"},
        },
    )
    old_approval = reg.update_workflow_state_from_event(
        handle=handle,
        fields={
            "event_id": "approval-merge-run-42-head-a1b2c3d4-approved",
            "event_type": "loop.approval_granted",
            "producer_id": "approval-bridge",
            "thread_key": handle.thread_key,
            "summary": "old approval granted",
        },
        data={
            "workflowId": "loop:release-readiness:run-42",
            "stage": "needs_attention",
            "refs": {"repo": "example/repo", "pullRequest": 86, "headSha": "a1b2c3d4"},
            "evidence": {"kind": "approval", "status": "passed", "summary": "trusted maintainer approved old head"},
        },
    )

    assert current is not None
    assert current.artifact == {"kind": "git_commit", "id": "bbbb2222"}
    assert old_approval is not None
    assert old_approval.stage == "progress"
    assert old_approval.artifact == {"kind": "git_commit", "id": "bbbb2222"}
    assert old_approval.evidence["approval"]["status"] == "stale"
    assert old_approval.evidence["approval"]["previous_status"] == "passed"
    assert old_approval.evidence["approval"]["stale_reason"] == "artifact_changed"
    assert old_approval.gates["states"]["approval"]["state"] == "stale"


def test_parallel_workflow_gates_activate_independently(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="relay",
        owner_user_id="u1",
        workflow_policy=WorkflowPolicy(gate_order=("review", "qa", "deploy"), gate_mode="parallel"),
    )

    state = reg.update_workflow_state_from_event(
        handle=handle,
        fields={"event_id": "evt_started", "event_type": "job.started", "producer_id": "relay", "thread_key": handle.thread_key, "summary": "started"},
        data={"workflowId": "wf-parallel", "stage": "started", "candidate": {"id": "rc1", "readiness": "ready"}},
    )

    assert state is not None
    assert state.gates["active"] == ["review", "qa", "deploy"]
    assert state.gates["deferred"] == []
    assert {gate: item["state"] for gate, item in state.gates["states"].items()} == {
        "review": "pending",
        "qa": "pending",
        "deploy": "pending",
    }


def test_serial_candidate_required_gate_blocks_later_gates_and_terminal_stage_persists(tmp_path: Path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    handle = reg.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="relay",
        owner_user_id="u1",
        workflow_policy=WorkflowPolicy(
            gate_order=("qa", "release"),
            gate_mode="serial",
            candidate_required=("qa",),
        ),
    )

    blocked = reg.update_workflow_state_from_event(
        handle=handle,
        fields={"event_id": "evt_forming", "event_type": "job.progress", "producer_id": "relay", "thread_key": handle.thread_key, "summary": "forming"},
        data={"workflowId": "wf-terminal", "stage": "progress", "candidate": {"id": "rc1", "readiness": "forming"}},
    )

    assert blocked is not None
    assert blocked.gates["active"] == []
    assert blocked.gates["deferred"] == ["qa", "release"]
    assert blocked.gates["states"]["qa"]["state"] == "deferred_candidate_not_ready"
    assert blocked.gates["states"]["release"]["state"] == "deferred_serial_gate"

    released = reg.update_workflow_state_from_event(
        handle=handle,
        fields={"event_id": "evt_released", "event_type": "job.released", "producer_id": "relay", "thread_key": handle.thread_key, "summary": "released"},
        data={"workflowId": "wf-terminal", "stage": "released", "candidate": {"id": "rc1", "readiness": "released"}},
    )
    late_progress = reg.update_workflow_state_from_event(
        handle=handle,
        fields={"event_id": "evt_late_progress", "event_type": "job.progress", "producer_id": "relay", "thread_key": handle.thread_key, "summary": "late progress"},
        data={"workflowId": "wf-terminal", "stage": "progress"},
    )

    assert released is not None and released.stage == "released"
    assert late_progress is not None and late_progress.stage == "released"


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
        handle_columns = {row["name"] for row in migrated.execute("pragma table_info(async_thread_handles)").fetchall()}
        source_binding_columns = {row["name"] for row in migrated.execute("pragma table_info(source_bindings)").fetchall()}
        schema_version = migrated.execute("select value from meta where key = 'schema_version'").fetchone()[0]
        detail_json = migrated.execute("select detail_json from event_log where event_id = 'evt_old'").fetchone()[0]
        ack_mode = migrated.execute("select ack_mode from async_thread_handles where thread_key = 'ath_v1'").fetchone()[0]
    assert "detail_json" in columns
    assert "ack_mode" in handle_columns
    assert "workflow_policy_json" in handle_columns
    assert "continuation_policy_json" in handle_columns
    assert {"binding_id", "listener_thread_key", "cursor_json", "status"}.issubset(source_binding_columns)
    assert schema_version == str(SCHEMA_VERSION)
    assert detail_json == "{}"
    assert ack_mode == "none"
    old_events = reg.list_recent_events(thread_key="ath_v1", owner_user_id="u1")
    assert len(old_events) == 1
    assert old_events[0].summary == "old row"
    assert old_events[0].detail == {}


def test_sanitize_event_detail_allowlists_and_redacts_safe_metadata():
    detail = sanitize_event_detail(
        {
            "target_platform": "discord",
            "policy": "agent_queue",
            "ack_mode": "brief",
            "ack_sent": True,
            "ack_success": False,
            "ack_error": "signature sha256=deadbeef",
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
        "ack_error": "signature=<redacted>",
        "ack_mode": "brief",
        "ack_sent": True,
        "ack_success": False,
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


def test_sanitize_event_detail_redacts_bare_secret_shapes():
    detail = sanitize_event_detail(
        {
            "error": " ".join([
                "AKIA" + "IOSFODNN7EXAMPLE",
                "ghp_" + "abcdefghijklmnopqrstuvwxyz123456",
                "github_pat_" + ("A" * 22) + "_" + ("B" * 59),
                "sk-proj-" + "abcdefghijklmnopqrstuvwxyzABCDE12345",
                "xoxb-" + "123456789012-123456789012-abcdefghijklmnopqrstuv",
                "eyJ" + ("a" * 12) + "." + ("b" * 12) + "." + ("c" * 12),
            ]),
            "exception_message": "-----BEGIN RSA " + "PRIVATE KEY-----\nabc123secret\n-----END RSA " + "PRIVATE KEY-----",
        }
    )

    combined = " ".join(str(value) for value in detail.values())
    for sentinel in ["AKIAIO", "ghp_", "github_pat_", "sk-proj", "xoxb-", "eyJ", "abc123secret"]:
        assert sentinel not in combined
    assert "<redacted>" in combined


def test_event_log_redacts_bare_secret_shapes_before_storage(tmp_path):
    reg = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    token = "github_pat_" + ("A" * 22) + "_" + ("B" * 59)
    reg.log_event(
        producer_id=token,
        event_id="evt-" + token,
        thread_key="ath_123",
        event_type="ci.build.finished",
        outcome="delivered",
        summary="done " + token,
        detail={"error": "AKIA" + "IOSFODNN7EXAMPLE " + "-----BEGIN RSA " + "PRIVATE KEY-----\nabc123secret\n-----END RSA " + "PRIVATE KEY-----"},
    )

    [event] = reg.list_recent_events(limit=5)
    serialized = f"{event.producer_id} {event.event_id} {event.summary} {event.detail}"
    for sentinel in ["github_pat_", "AKIAIO", "abc123secret"]:
        assert sentinel not in serialized
    assert "<redacted>" in serialized or "redacted:" in serialized


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
