"""Shared source-binding event filter normalization helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

KANBAN_EVENT_PREFIX = "kanban.task."
KANBAN_DEFAULT_MATERIAL_KINDS = ("completed", "blocked", "unblocked", "gave_up", "crashed", "timed_out")
KANBAN_READY_FOR_REVIEW_KIND = "ready_for_review"
KANBAN_DEFAULT_EVENT_TYPES = tuple(
    f"{KANBAN_EVENT_PREFIX}{kind}" for kind in (*KANBAN_DEFAULT_MATERIAL_KINDS, KANBAN_READY_FOR_REVIEW_KIND)
)


def source_binding_event_types(
    source: str,
    event_filter: Mapping[str, Any] | None,
    *,
    default_event_types: Iterable[str] = (),
) -> tuple[str, ...]:
    """Resolve a source-binding filter to exact ATH event types.

    Compatibility checks and source transforms both need this exact view. If one
    accepts high-level aliases like Kanban eventKinds while the other validates
    only exact eventTypes, a binding can dry-run as emit-ready but fail at the
    listener boundary.
    """

    exact = _string_list_from_keys(event_filter, ("eventTypes", "event_types", "allowedEventTypes", "allowed_event_types"))
    if exact:
        return _dedupe(exact)
    if _source_name(source) == "kanban":
        kinds = _string_list_from_keys(event_filter, ("eventKinds", "event_kinds", "kinds"))
        if kinds:
            return _dedupe(_kanban_event_type_for_kind(kind) for kind in kinds)
        return _dedupe(default_event_types)
    return _dedupe(default_event_types)


def source_filter_allows_event(
    source: str,
    event_type: str,
    event_filter: Mapping[str, Any] | None,
    *,
    default_event_types: Iterable[str] = (),
) -> bool:
    """Return whether a resolved source-binding filter permits an exact event type."""

    allowed = source_binding_event_types(source, event_filter, default_event_types=default_event_types)
    return event_type in set(allowed) if allowed else True


def _kanban_event_type_for_kind(kind: str) -> str:
    normalized = _normalize_kanban_kind(kind)
    return f"{KANBAN_EVENT_PREFIX}{normalized}" if normalized else ""


def _normalize_kanban_kind(kind: str) -> str:
    value = str(kind or "").strip().lower().replace("-", "_").replace(" ", "_")
    if value == "review_required":
        return KANBAN_READY_FOR_REVIEW_KIND
    return "".join(ch for ch in value if ch.isalnum() or ch in {"_", ".", ":"})


def _source_name(source: str) -> str:
    return str(source or "").strip().lower().replace("_", "-").replace(" ", "-")


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


def _dedupe(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))
