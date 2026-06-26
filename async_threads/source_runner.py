"""Native source-binding runner for durable ATH producer bridges.

The runner is intentionally plugin-owned rather than cron-owned: it reads a
configured upstream source cursor, persists an outbox row before attempting an
ATH emit, and only advances the binding cursor after the outbox row reaches a
terminal-safe state. Retrying a pending row reuses the same ATH event id, so a
send-before-mark crash reconciles through the receiver's duplicate response.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from .emitter import EmitResult, emit_event, utc_now_iso
from .kanban import (
    KANBAN_READ_FAILURE_EXCEPTIONS,
    KANBAN_PRODUCER_ID,
    dry_run_kanban_source_binding,
    read_kanban_task_events,
    transform_kanban_task_event,
)
from .privacy import redact_metadata_text, redact_secret_text
from .registry import AsyncThreadRegistry, AsyncThreadSourceBinding

EmitCallable = Callable[..., EmitResult | Mapping[str, Any]]

# "error" is terminal for non-retryable failures; retryable transport rows stay pending.
TERMINAL_SAFE_OUTBOX_STATUSES = {"succeeded", "duplicate", "suppressed", "coalesced", "error"}


@dataclass(frozen=True)
class SourceBindingRunConfig:
    """One-shot runner configuration."""

    event_url: str
    board_db_path: str | Path | None = None
    limit: int = 100
    timeout: float = 20


def run_source_binding_once(
    *,
    registry: AsyncThreadRegistry,
    binding: AsyncThreadSourceBinding,
    config: SourceBindingRunConfig,
    emit: EmitCallable = emit_event,
) -> dict[str, Any]:
    """Run one bounded source-binding batch and return redacted diagnostics."""

    if binding.source != "kanban":
        return _runner_report(binding, ok=False, health="invalid_binding", error="unsupported_source")

    compatibility = registry.source_binding_compatibility(binding)
    if not bool(compatibility.get("valid")):
        return _runner_report(
            binding,
            ok=False,
            health="fail_closed",
            compatibility=compatibility,
            error=str(compatibility.get("reason") or "invalid_binding"),
        )

    source_ref = binding.source_ref if isinstance(binding.source_ref, Mapping) else {}
    board = str(source_ref.get("board") or source_ref.get("boardRef") or "default")
    db_path = config.board_db_path or source_ref.get("dbPath") or source_ref.get("boardDbPath") or source_ref.get("path")
    if not db_path:
        return _runner_report(binding, ok=False, health="fail_closed", compatibility=compatibility, error="kanban_db_path_required")

    cursor = _cursor_event_id(binding.cursor)
    task_id = str(source_ref.get("task") or source_ref.get("taskId") or "").strip() or None
    try:
        upstream_events = read_kanban_task_events(db_path, since_event_id=cursor, limit=config.limit, task_id=task_id)
    except KANBAN_READ_FAILURE_EXCEPTIONS as exc:
        return _runner_report(
            binding,
            ok=False,
            health="read_failed",
            compatibility=compatibility,
            error="kanban_read_failed",
            message=str(exc),
        )

    counts = {"emitted": 0, "duplicate": 0, "suppressed": 0, "coalesced": 0, "retryable_error": 0, "error": 0}
    processed: list[dict[str, Any]] = []
    stopped = False
    for event in upstream_events:
        action = transform_kanban_task_event(
            event,
            board=board,
            thread_key=binding.listener_thread_key,
            producer_id=binding.producer_id or KANBAN_PRODUCER_ID,
            event_filter=binding.event_filter,
            coalesce=binding.coalesce,
        )
        envelope = _runner_envelope(action)
        outbox = registry.upsert_source_binding_outbox(
            binding_id=binding.binding_id,
            upstream_event_id=event.id,
            ath_event_id=str(action.get("eventId") or f"{board}:{event.task_id}:{event.id}"),
            event_type=str(action.get("eventType") or action.get("digestEventType") or ""),
            action=str(action.get("action") or ""),
            envelope=envelope,
        )
        prior_status = str(outbox.get("status") or "")
        if prior_status in TERMINAL_SAFE_OUTBOX_STATUSES:
            registry.advance_source_binding_cursor(binding_id=binding.binding_id, upstream_event_id=event.id)
            processed.append(_processed_item(event.id, action, prior_status))
            continue

        action_name = str(action.get("action") or "")
        if action_name == "suppressed":
            registry.mark_source_binding_outbox(binding_id=binding.binding_id, upstream_event_id=event.id, status="suppressed")
            registry.advance_source_binding_cursor(binding_id=binding.binding_id, upstream_event_id=event.id)
            counts["suppressed"] += 1
            processed.append(_processed_item(event.id, action, "suppressed"))
            continue
        if action_name == "would_coalesce":
            registry.mark_source_binding_outbox(binding_id=binding.binding_id, upstream_event_id=event.id, status="coalesced")
            registry.advance_source_binding_cursor(binding_id=binding.binding_id, upstream_event_id=event.id)
            counts["coalesced"] += 1
            processed.append(_processed_item(event.id, action, "coalesced"))
            continue
        if action_name != "would_emit":
            registry.mark_source_binding_outbox(
                binding_id=binding.binding_id,
                upstream_event_id=event.id,
                status="error",
                error=f"unsupported_action:{action_name}",
            )
            counts["error"] += 1
            processed.append(_processed_item(event.id, action, "error"))
            stopped = True
            break

        handle = registry.get_handle(binding.listener_thread_key)
        if handle is None or not handle.enabled or handle.producer_id != (binding.producer_id or handle.producer_id):
            registry.mark_source_binding_outbox(
                binding_id=binding.binding_id,
                upstream_event_id=event.id,
                status="error",
                error="listener_unavailable_or_incompatible",
            )
            counts["error"] += 1
            processed.append(_processed_item(event.id, action, "error"))
            stopped = True
            break

        result = emit(
            config.event_url,
            envelope,
            secret=handle.secret,
            timeout=config.timeout,
        )
        result_dict = _emit_result_dict(result)
        if bool(result_dict.get("success")):
            duplicate = bool(result_dict.get("duplicate"))
            status = "duplicate" if duplicate else "succeeded"
            registry.mark_source_binding_outbox(
                binding_id=binding.binding_id,
                upstream_event_id=event.id,
                status=status,
                http_status=result_dict.get("httpStatus"),
                receiver_status=str(result_dict.get("status") or ""),
            )
            registry.advance_source_binding_cursor(binding_id=binding.binding_id, upstream_event_id=event.id)
            counts["duplicate" if duplicate else "emitted"] += 1
            processed.append(_processed_item(event.id, action, status))
            continue

        retryable = bool(result_dict.get("retryable"))
        registry.mark_source_binding_outbox(
            binding_id=binding.binding_id,
            upstream_event_id=event.id,
            status="pending" if retryable else "error",
            error=_emit_failure_diagnostic(result_dict),
            http_status=result_dict.get("httpStatus"),
            receiver_status=str(result_dict.get("status") or ""),
            increment_attempts=True,
        )
        if not retryable:
            registry.advance_source_binding_cursor(binding_id=binding.binding_id, upstream_event_id=event.id)
        counts["retryable_error" if retryable else "error"] += 1
        processed.append(_processed_item(event.id, action, "retryable_error" if retryable else "error"))
        stopped = True
        break

    diagnostics = registry.source_binding_outbox_status(binding_id=binding.binding_id)
    fresh_binding = registry.get_source_binding(binding_id=binding.binding_id, owner_user_id=binding.owner_user_id) or binding
    return _runner_report(
        fresh_binding,
        ok=not stopped and counts["error"] == 0 and counts["retryable_error"] == 0,
        health="ok" if not stopped else "blocked",
        compatibility=compatibility,
        counts=counts,
        processed=processed,
        outbox=diagnostics,
        upstreamRows=len(upstream_events),
    )


def source_binding_runner_status(
    *,
    registry: AsyncThreadRegistry,
    binding: AsyncThreadSourceBinding,
    board_db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Return redacted runner diagnostics without mutating source or cursor."""

    compatibility = registry.source_binding_compatibility(binding)
    outbox = registry.source_binding_outbox_status(binding_id=binding.binding_id)
    cursor = _cursor_event_id(binding.cursor)
    lag: int | None = None
    source_ref = binding.source_ref if isinstance(binding.source_ref, Mapping) else {}
    db_path = board_db_path or source_ref.get("dbPath") or source_ref.get("boardDbPath") or source_ref.get("path")
    if binding.source == "kanban" and db_path:
        try:
            report = dry_run_kanban_source_binding(registry=registry, binding=binding, board_db_path=db_path, since_event_id=cursor, limit=1)
            report_cursor = report.get("cursor", {}) if isinstance(report, Mapping) else {}
            would_advance = int(report_cursor.get("wouldAdvanceToEventId") or cursor)
            lag = max(0, would_advance - cursor)
        except Exception:  # diagnostics must not poison command/tool surfaces
            lag = None
    return {
        "health": "ok" if compatibility.get("valid") else "fail_closed",
        "bindingId": binding.binding_id,
        "source": redact_metadata_text(binding.source),
        "cursor": {"lastEventId": cursor},
        "lag": lag,
        "compatibility": compatibility,
        "outbox": outbox,
    }


def _runner_report(binding: AsyncThreadSourceBinding, *, ok: bool, health: str, **extra: Any) -> dict[str, Any]:
    report = {
        "ok": ok,
        "health": health,
        "bindingId": binding.binding_id,
        "source": redact_metadata_text(binding.source),
        "cursor": {"lastEventId": _cursor_event_id(binding.cursor)},
    }
    for key, value in extra.items():
        if key == "message" and value:
            report[key] = redact_secret_text(str(value), max_input_chars=1000, max_output_chars=300)
        elif key == "error" and value:
            report[key] = redact_secret_text(str(value), max_input_chars=1000, max_output_chars=120)
        else:
            report[key] = value
    return report


def _processed_item(upstream_event_id: int, action: Mapping[str, Any], status: str) -> dict[str, Any]:
    return {
        "upstreamEventId": upstream_event_id,
        "eventId": redact_metadata_text(str(action.get("eventId") or "")),
        "eventType": redact_metadata_text(str(action.get("eventType") or action.get("digestEventType") or "")),
        "action": str(action.get("action") or ""),
        "status": status,
    }


def _emit_result_dict(result: EmitResult | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(result, EmitResult):
        return result.to_public_dict()
    return dict(result)


def _emit_failure_diagnostic(result: Mapping[str, Any]) -> str:
    """Return a compact failure classification without raw receiver body text."""

    try:
        http_status = int(result.get("httpStatus"))
    except (TypeError, ValueError):
        http_status = None
    parts = ["emit_failed", f"http_{http_status}" if http_status is not None else "transport_error"]
    receiver_status = _safe_diagnostic_fragment(result.get("status"))
    if receiver_status and receiver_status != "transport_error":
        parts.append(receiver_status)
    return ":".join(parts)


def _safe_diagnostic_fragment(value: Any) -> str:
    """Keep only short enum-like status values in durable diagnostics."""

    redacted = redact_metadata_text(value, max_chars=64).strip().lower()
    if not redacted or redacted.startswith("redacted:") or "<redacted>" in redacted or len(redacted) > 32:
        return ""
    normalized = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in redacted).strip("_-")
    return normalized if normalized and len(normalized) <= 32 else ""


def _runner_envelope(action: Mapping[str, Any]) -> dict[str, Any]:
    envelope = dict(action.get("envelope") if isinstance(action.get("envelope"), Mapping) else {})
    if envelope:
        # task_events.created_at is upstream state, not producer emission time.
        # The ATH receiver enforces a replay window on occurredAt, so a durable
        # runner must sign a fresh body on each retry while preserving eventId.
        envelope["occurredAt"] = utc_now_iso()
    return envelope


def _cursor_event_id(cursor: Mapping[str, Any]) -> int:
    if not isinstance(cursor, Mapping):
        return 0
    for key in ("last_event_id", "lastEventId", "taskEventId", "task_event_id"):
        if key not in cursor:
            continue
        try:
            return max(0, int(cursor.get(key, 0)))
        except (TypeError, ValueError):
            continue
    return 0
