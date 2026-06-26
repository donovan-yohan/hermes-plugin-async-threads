"""Kanban source-binding reader and safe ATH event transform.

This module is intentionally runner-free. It can read durable Hermes Kanban
``task_events`` rows from a board DB, map material transitions to
``async-thread-event/v1`` envelopes, and produce dry-run reports. A future
native source-binding runner can call these functions, sign with the existing
emitter helper, and persist cursors after successful handling.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .emitter import build_event_envelope
from .privacy import redact_metadata_text, redact_secret_text, sanitize_untrusted_value
from .registry import AsyncThreadRegistry, AsyncThreadSourceBinding
from .source_filters import KANBAN_DEFAULT_EVENT_TYPES, KANBAN_DEFAULT_MATERIAL_KINDS, KANBAN_EVENT_PREFIX, source_filter_allows_event

KANBAN_PRODUCER_ID = "ath-kanban-bridge"
DEFAULT_MATERIAL_KINDS = KANBAN_DEFAULT_MATERIAL_KINDS
NOISY_KINDS = {"heartbeat", "claimed", "spawned", "promoted", "commented"}
DIGEST_EVENT_TYPE = "kanban.task.comment_digest"


@dataclass(frozen=True)
class KanbanTaskEvent:
    """A compact, schema-tolerant view of one durable Kanban task_events row."""

    id: int
    task_id: str
    run_id: int | None
    kind: str
    payload: dict[str, Any]
    payload_malformed: bool
    created_at: int
    task: dict[str, Any]


def read_kanban_task_events(
    board_db_path: str | Path,
    *,
    since_event_id: int = 0,
    limit: int = 100,
    task_id: str | None = None,
) -> list[KanbanTaskEvent]:
    """Read Kanban task_events rows after a cursor without mutating state."""

    db_path = Path(str(board_db_path)).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(f"kanban board DB not found: {db_path}")
    since = _nonnegative_int(since_event_id, default=0)
    bounded_limit = max(1, min(_nonnegative_int(limit, default=100), 500))
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        event_columns = _table_columns(conn, "task_events")
        required = {"id", "task_id", "kind", "created_at"}
        if not required.issubset(event_columns):
            missing = ", ".join(sorted(required.difference(event_columns)))
            raise ValueError(f"task_events missing required columns: {missing}")
        task_columns = _table_columns(conn, "tasks")
        has_tasks = "id" in task_columns
        task_selects = []
        for column in ("title", "assignee", "status", "priority"):
            if column in task_columns:
                task_selects.append(f"t.{column} as task_{column}")
        select = ["e.id", "e.task_id", "e.kind", "e.created_at"]
        select.append("e.run_id" if "run_id" in event_columns else "null as run_id")
        select.append("e.payload" if "payload" in event_columns else "null as payload")
        select.extend(task_selects)
        sql = f"select {', '.join(select)} from task_events e"
        params: list[Any] = [since]
        if has_tasks:
            sql += " left join tasks t on t.id = e.task_id"
        sql += " where e.id > ?"
        if task_id:
            sql += " and e.task_id = ?"
            params.append(str(task_id))
        sql += " order by e.id asc limit ?"
        params.append(bounded_limit)
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return [_row_to_kanban_event(row) for row in rows]


def transform_kanban_task_event(
    event: KanbanTaskEvent,
    *,
    board: str,
    thread_key: str,
    producer_id: str = KANBAN_PRODUCER_ID,
    event_filter: Mapping[str, Any] | None = None,
    coalesce: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Transform one Kanban event into a dry-run action and optional envelope."""

    board_name = _clean_board(board)
    producer = _clean_producer(producer_id)
    event_kind = _clean_kind(event.kind)
    mapped_kind = _mapped_material_kind(event_kind, event.payload)
    event_type = f"{KANBAN_EVENT_PREFIX}{mapped_kind}" if mapped_kind else ""
    upstream_id = f"{board_name}:{event.task_id}:{event.id}"
    base = {
        "upstreamEventId": event.id,
        "taskId": event.task_id,
        "eventKind": event_kind,
        "eventId": upstream_id,
    }

    if event_kind in NOISY_KINDS:
        if _coalesce_enabled_for(event_kind, coalesce):
            return {
                **base,
                "action": "would_coalesce",
                "reason": "routine_event_digest_enabled",
                "digestEventType": DIGEST_EVENT_TYPE,
                "seriesKey": _series_key(board_name, event.task_id),
            }
        return {**base, "action": "suppressed", "reason": "routine_event"}

    if not mapped_kind:
        return {**base, "action": "suppressed", "reason": "not_material"}

    if not _event_allowed(event_type, event_filter):
        return {**base, "action": "suppressed", "reason": "event_type_not_allowed", "eventType": event_type}

    envelope = _build_kanban_envelope(
        event,
        board=board_name,
        thread_key=thread_key,
        producer_id=producer,
        event_id=upstream_id,
        event_type=event_type,
        stage=mapped_kind,
    )
    return {
        **base,
        "action": "would_emit",
        "eventType": event_type,
        "seriesKey": envelope["seriesKey"],
        "workflowId": envelope["workflowId"],
        "envelope": envelope,
    }


def dry_run_kanban_source_binding(
    *,
    registry: AsyncThreadRegistry | None,
    binding: AsyncThreadSourceBinding,
    board_db_path: str | Path | None = None,
    since_event_id: int | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Preview a Kanban source binding without sending or advancing its cursor."""

    if binding.source != "kanban":
        return _invalid_binding_report(binding, "unsupported_source")
    compatibility = registry.source_binding_compatibility(binding) if registry is not None else {"valid": True, "reason": "not_checked", "failClosed": False}
    if not bool(compatibility.get("valid")):
        return _invalid_binding_report(binding, str(compatibility.get("reason") or "invalid_binding"), compatibility=compatibility)

    source_ref = binding.source_ref if isinstance(binding.source_ref, Mapping) else {}
    board = _clean_board(str(source_ref.get("board") or source_ref.get("boardRef") or "default"))
    db_path = board_db_path or source_ref.get("dbPath") or source_ref.get("boardDbPath") or source_ref.get("path")
    if not db_path:
        return _invalid_binding_report(binding, "kanban_db_path_required", compatibility=compatibility)
    cursor = _cursor_event_id(binding.cursor, default=0) if since_event_id is None else _nonnegative_int(since_event_id, default=0)
    task_id = str(source_ref.get("task") or source_ref.get("taskId") or "").strip() or None

    try:
        events = read_kanban_task_events(db_path, since_event_id=cursor, limit=limit, task_id=task_id)
    except (OSError, ValueError) as exc:
        return kanban_read_failed_report(binding, exc, compatibility=compatibility)
    results = [
        transform_kanban_task_event(
            event,
            board=board,
            thread_key=binding.listener_thread_key,
            producer_id=binding.producer_id or KANBAN_PRODUCER_ID,
            event_filter=binding.event_filter,
            coalesce=binding.coalesce,
        )
        for event in events
    ]
    counts = _count_actions(results)
    max_seen = max([event.id for event in events], default=cursor)
    return {
        "ok": True,
        "dryRun": True,
        "source": "kanban",
        "bindingId": binding.binding_id,
        "board": board,
        "cursor": {"fromEventId": cursor, "wouldAdvanceToEventId": max_seen, "advanced": False},
        "counts": counts,
        "events": results,
        "compatibility": compatibility,
    }


def kanban_read_failed_report(
    binding: AsyncThreadSourceBinding,
    error: Exception | str,
    *,
    compatibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the canonical dry-run report for Kanban DB read failures."""

    return _invalid_binding_report(
        binding,
        "kanban_read_failed",
        compatibility=compatibility,
        error="kanban_read_failed",
        message=redact_metadata_text(str(error), max_chars=300),
    )


def _build_kanban_envelope(
    event: KanbanTaskEvent,
    *,
    board: str,
    thread_key: str,
    producer_id: str,
    event_id: str,
    event_type: str,
    stage: str,
) -> dict[str, Any]:
    subject = _safe_mapping(
        {
            "board": board,
            "task": event.task_id,
            "title": event.task.get("title", ""),
            "assignee": event.task.get("assignee", ""),
            "status": event.task.get("status", ""),
            "priority": event.task.get("priority", None),
        }
    )
    payload = _safe_mapping(
        {
            "status": stage,
            "kanbanEventKind": event.kind,
            "taskEventId": event.id,
            "taskId": event.task_id,
            "runId": event.run_id,
            "payloadParseError": event.payload_malformed,
        }
    )
    detail = _event_detail(event)
    if detail:
        payload["eventDetail"] = detail
    summary = f"Kanban task {event.task_id} {stage.replace('_', ' ')}"
    return build_event_envelope(
        thread_key=thread_key,
        producer_id=producer_id,
        event_id=event_id,
        event_type=event_type,
        summary=summary,
        subject=subject,
        payload=payload,
        occurred_at=_epoch_to_iso(event.created_at),
        tail_mode="compact",
        extra={
            "seriesKey": _series_key(board, event.task_id),
            "workflowId": _series_key(board, event.task_id),
            "stage": stage,
        },
    )


def _row_to_kanban_event(row: sqlite3.Row) -> KanbanTaskEvent:
    payload, malformed = _parse_payload(row["payload"] if "payload" in row.keys() else None)
    task: dict[str, Any] = {}
    for key in ("title", "assignee", "status", "priority"):
        row_key = f"task_{key}"
        if row_key in row.keys():
            task[key] = row[row_key]
    return KanbanTaskEvent(
        id=int(row["id"]),
        task_id=str(row["task_id"] or ""),
        run_id=int(row["run_id"]) if "run_id" in row.keys() and row["run_id"] is not None else None,
        kind=str(row["kind"] or ""),
        payload=payload,
        payload_malformed=malformed,
        created_at=int(row["created_at"] or 0),
        task=task,
    )


def _parse_payload(raw: Any) -> tuple[dict[str, Any], bool]:
    if raw in (None, ""):
        return {}, False
    try:
        parsed = json.loads(str(raw))
    except Exception:
        return {}, True
    if not isinstance(parsed, dict):
        return {}, True
    return parsed, False


def _mapped_material_kind(kind: str, payload: Mapping[str, Any]) -> str:
    if kind == "blocked" and _is_review_required(payload):
        return "ready_for_review"
    if kind in DEFAULT_MATERIAL_KINDS:
        return kind
    return ""


def _is_review_required(payload: Mapping[str, Any]) -> bool:
    reason = str(payload.get("reason") or payload.get("summary") or "").strip().lower().replace("-", "_")
    return reason.startswith("review_required:") or reason.startswith("review_required") or reason.startswith("review required")


def _event_allowed(event_type: str, event_filter: Mapping[str, Any] | None) -> bool:
    return source_filter_allows_event("kanban", event_type, event_filter, default_event_types=KANBAN_DEFAULT_EVENT_TYPES)


def _coalesce_enabled_for(kind: str, coalesce: Mapping[str, Any] | None) -> bool:
    if not isinstance(coalesce, Mapping) or not coalesce:
        return False
    mode = str(coalesce.get("mode") or coalesce.get("strategy") or "").lower()
    enabled = bool(coalesce.get("enabled") or coalesce.get("digest") or mode in {"digest", "debounce", "coalesce"})
    kinds = _string_list_from_keys(coalesce, ("eventKinds", "event_kinds", "kinds"))
    if kinds:
        return kind in set(kinds)
    return enabled


def _string_list_from_keys(value: Mapping[str, Any] | None, keys: Iterable[str]) -> tuple[str, ...]:
    if not isinstance(value, Mapping):
        return ()
    for key in keys:
        raw = value.get(key)
        if isinstance(raw, str):
            return tuple(item.strip() for item in raw.split(",") if item.strip())
        if isinstance(raw, Iterable) and not isinstance(raw, (bytes, bytearray, str)):
            return tuple(str(item).strip() for item in raw if str(item).strip())
    return ()


def _event_detail(event: KanbanTaskEvent) -> str:
    if not event.payload:
        return "payload parse error" if event.payload_malformed else ""
    for key in ("reason", "summary", "error", "outcome"):
        if key in event.payload and event.payload[key] not in (None, ""):
            return _clip(redact_secret_text(event.payload[key], max_input_chars=1000, max_output_chars=500), 500)
    return ""


def _safe_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    cleaned = sanitize_untrusted_value({key: item for key, item in value.items() if item is not None})
    return cleaned if isinstance(cleaned, dict) else {}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"pragma table_info({table})").fetchall()}


def _cursor_event_id(cursor: Mapping[str, Any], *, default: int) -> int:
    if not isinstance(cursor, Mapping):
        return default
    for key in ("last_event_id", "lastEventId", "taskEventId", "task_event_id"):
        if key in cursor:
            return _nonnegative_int(cursor.get(key), default=default)
    return default


def _invalid_binding_report(
    binding: AsyncThreadSourceBinding,
    reason: str,
    *,
    compatibility: Mapping[str, Any] | None = None,
    error: str | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    report = {
        "ok": False,
        "dryRun": True,
        "source": redact_metadata_text(binding.source),
        "bindingId": binding.binding_id,
        "counts": {"invalid_binding": 1, "would_emit": 0, "suppressed": 0, "would_coalesce": 0},
        "events": [{"action": "invalid_binding", "reason": reason}],
        "compatibility": dict(compatibility or {}),
    }
    if error:
        report["error"] = error
    if message:
        report["message"] = message
    return report


def _count_actions(results: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"would_emit": 0, "suppressed": 0, "would_coalesce": 0, "invalid_binding": 0}
    for result in results:
        action = str(result.get("action") or "")
        counts[action] = counts.get(action, 0) + 1
    return counts


def _epoch_to_iso(value: int) -> str:
    try:
        seconds = max(0, int(value))
    except (TypeError, ValueError):
        seconds = 0
    return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _series_key(board: str, task_id: str) -> str:
    return f"kanban:{board}:{task_id}"


def _clean_board(value: str) -> str:
    return _clean_token(value, default="default", max_len=100)


def _clean_producer(value: str) -> str:
    return _clean_token(value, default=KANBAN_PRODUCER_ID, max_len=100)


def _clean_kind(value: str) -> str:
    return _clean_token(value, default="", max_len=80)


def _clean_token(value: str, *, default: str, max_len: int) -> str:
    cleaned = "".join(ch for ch in str(value or "").strip() if ch.isalnum() or ch in {"-", "_", ".", ":"})
    return cleaned[:max_len] or default


def _nonnegative_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, parsed)


def _clip(value: str, max_len: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"
