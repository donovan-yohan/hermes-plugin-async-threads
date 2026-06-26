import json
import sqlite3

from async_threads.emitter import EmitResult
from async_threads.registry import AsyncThreadRegistry
from async_threads.source_runner import SourceBindingRunConfig, run_source_binding_once, source_binding_runner_status


def _make_board_db(path, rows):
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            create table tasks(
                id text primary key,
                title text not null,
                body text,
                assignee text,
                status text not null,
                priority integer default 0,
                result text
            );
            create table task_events(
                id integer primary key autoincrement,
                task_id text not null,
                run_id integer,
                kind text not null,
                payload text,
                created_at integer not null
            );
            """
        )
        conn.execute(
            "insert into tasks(id, title, body, assignee, status, priority, result) values (?, ?, ?, ?, ?, ?, ?)",
            ("t_runner", "Runner token=supersecret", "raw transcript should stay out", "kani-backend", "running", 70, "secret result"),
        )
        for kind, payload, created_at in rows:
            conn.execute(
                "insert into task_events(task_id, run_id, kind, payload, created_at) values ('t_runner', 1, ?, ?, ?)",
                (kind, json.dumps(payload) if payload is not None else None, created_at),
            )
        conn.commit()
    finally:
        conn.close()


def _binding(tmp_path, *, db_path, cursor=None, allowed=None, coalesce=None):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    listener = registry.create_handle(
        source={"platform": "discord", "chat_id": "c", "thread_id": "t", "chat_type": "channel"},
        producer_id="ath-kanban-bridge",
        allowed_event_types=allowed or ["kanban.task.completed", "kanban.task.blocked", "kanban.task.ready_for_review"],
        owner_user_id="u1",
    )
    binding = registry.create_source_binding(
        owner_user_id="u1",
        source="kanban",
        source_ref={"board": "ath", "path": str(db_path)},
        listener_thread_key=listener.thread_key,
        event_filter={"eventTypes": allowed or ["kanban.task.completed", "kanban.task.blocked", "kanban.task.ready_for_review"]},
        cursor=cursor or {},
        coalesce=coalesce or {},
    )
    return registry, listener, binding


def test_runner_persists_outbox_emits_terminal_rows_and_advances_cursor(tmp_path):
    board_db = tmp_path / "kanban.db"
    _make_board_db(
        board_db,
        [
            ("completed", {"summary": "done with ghp_abcdefghijklmnopqrstuvwxyz123456"}, 1700000001),
            ("heartbeat", None, 1700000002),
            ("commented", {"author": "ebi", "len": 99}, 1700000003),
        ],
    )
    registry, _listener, binding = _binding(tmp_path, db_path=board_db, coalesce={"mode": "digest"})
    calls = []

    def fake_emit(url, event, **kwargs):
        calls.append((url, event, kwargs))
        return EmitResult(success=True, retryable=False, http_status=202, receiver_status="accepted")

    report = run_source_binding_once(
        registry=registry,
        binding=binding,
        config=SourceBindingRunConfig(event_url="http://127.0.0.1:8765/async-threads/v1/events"),
        emit=fake_emit,
    )

    assert report["ok"] is True
    assert report["counts"]["emitted"] == 1
    assert report["counts"]["coalesced"] == 2
    assert [call[1]["eventId"] for call in calls] == ["ath:t_runner:1"]
    refreshed = registry.get_source_binding(binding_id=binding.binding_id, owner_user_id="u1")
    assert refreshed.cursor["last_event_id"] == 3
    outbox = registry.source_binding_outbox_status(binding_id=binding.binding_id)
    assert outbox["counts"] == {"coalesced": 2, "succeeded": 1}
    with registry._connect() as conn:
        stored = conn.execute("select envelope_json from source_binding_outbox where upstream_event_id = 1").fetchone()[0]
    assert "abcdefghijklmnopqrstuvwxyz123456" not in stored
    assert "supersecret" not in stored
    assert "raw transcript" not in stored


def test_runner_retries_transport_failure_with_same_event_id_then_reconciles_duplicate(tmp_path):
    board_db = tmp_path / "kanban.db"
    _make_board_db(board_db, [("completed", {"summary": "done"}, 1700000001)])
    registry, _listener, binding = _binding(tmp_path, db_path=board_db)
    event_ids = []

    def retryable_emit(url, event, **kwargs):
        event_ids.append(event["eventId"])
        return EmitResult(success=False, retryable=True, http_status=502, receiver_status="", body='{"error":"event dispatch failed"}')

    first = run_source_binding_once(
        registry=registry,
        binding=binding,
        config=SourceBindingRunConfig(event_url="http://127.0.0.1:8765/async-threads/v1/events"),
        emit=retryable_emit,
    )
    assert first["ok"] is False
    assert registry.get_source_binding(binding_id=binding.binding_id, owner_user_id="u1").cursor == {}
    assert registry.source_binding_outbox_status(binding_id=binding.binding_id)["last"]["status"] == "pending"

    binding_after_retry = registry.get_source_binding(binding_id=binding.binding_id, owner_user_id="u1")

    def duplicate_emit(url, event, **kwargs):
        event_ids.append(event["eventId"])
        return EmitResult(success=True, retryable=False, duplicate=True, http_status=200, receiver_status="duplicate")

    second = run_source_binding_once(
        registry=registry,
        binding=binding_after_retry,
        config=SourceBindingRunConfig(event_url="http://127.0.0.1:8765/async-threads/v1/events"),
        emit=duplicate_emit,
    )

    assert second["ok"] is True
    assert event_ids == ["ath:t_runner:1", "ath:t_runner:1"]
    refreshed = registry.get_source_binding(binding_id=binding.binding_id, owner_user_id="u1")
    assert refreshed.cursor["last_event_id"] == 1
    status = registry.source_binding_outbox_status(binding_id=binding.binding_id)["last"]
    assert status["status"] == "duplicate"
    assert status["attempts"] == 1


def test_runner_disabled_listener_fails_closed_and_is_diagnosable(tmp_path):
    board_db = tmp_path / "kanban.db"
    _make_board_db(board_db, [("completed", {"summary": "done"}, 1700000001)])
    registry, listener, binding = _binding(tmp_path, db_path=board_db)
    registry.set_enabled(listener.thread_key, False)
    binding = registry.get_source_binding(binding_id=binding.binding_id, owner_user_id="u1")
    calls = []

    report = run_source_binding_once(
        registry=registry,
        binding=binding,
        config=SourceBindingRunConfig(event_url="http://127.0.0.1:8765/async-threads/v1/events"),
        emit=lambda *args, **kwargs: calls.append(args),
    )
    diagnostics = source_binding_runner_status(registry=registry, binding=binding)

    assert calls == []
    assert report["ok"] is False
    assert report["health"] == "fail_closed"
    assert report["compatibility"]["reason"] == "listener_disabled"
    assert diagnostics["health"] == "fail_closed"
    assert diagnostics["compatibility"]["reason"] == "listener_disabled"
