"""Listener lifecycle policy and terminal-event detection."""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

DEFAULT_TERMINAL_EVENT_PATTERNS = (
    "*.goal.finished",
    "*.phase.finished",
    "*.session.finished",
    "*.run.finished",
)
DEFAULT_TERMINAL_STAGES = ("released", "cancelled", "canceled")


@dataclass(frozen=True)
class LifecyclePolicy:
    """Lifecycle behavior for temporary async-thread listeners.

    Terminal detection is conservative by default: only explicit workflow-ish
    terminal event names/stages count. Plain ``producer.finished`` remains a
    normal completion event unless the listener declares it terminal.
    """

    terminal_event_types: tuple[str, ...] = DEFAULT_TERMINAL_EVENT_PATTERNS
    terminal_stages: tuple[str, ...] = DEFAULT_TERMINAL_STAGES
    auto_retire_on_terminal: bool = False
    shared_listener: bool = False

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "LifecyclePolicy":
        if not isinstance(value, Mapping):
            return cls()
        raw_types = _first_non_none(value, "terminal_event_types", "terminalEventTypes")
        raw_stages = _first_non_none(value, "terminal_stages", "terminalStages")
        return cls(
            terminal_event_types=_as_tuple(raw_types) if raw_types is not None else DEFAULT_TERMINAL_EVENT_PATTERNS,
            terminal_stages=_as_tuple(raw_stages) if raw_stages is not None else DEFAULT_TERMINAL_STAGES,
            auto_retire_on_terminal=bool(value.get("auto_retire_on_terminal", value.get("autoRetireOnTerminal", False))),
            shared_listener=bool(value.get("shared_listener", value.get("sharedListener", False))),
        )

    @classmethod
    def from_json(cls, value: str | None) -> "LifecyclePolicy":
        if not value:
            return cls()
        try:
            parsed = json.loads(value)
        except Exception:
            return cls()
        return cls.from_mapping(parsed if isinstance(parsed, Mapping) else None)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "terminal_event_types": list(self.terminal_event_types),
            "terminal_stages": list(self.terminal_stages),
            "auto_retire_on_terminal": self.auto_retire_on_terminal,
            "shared_listener": self.shared_listener,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_mapping(), sort_keys=True, separators=(",", ":"))

    def public_summary(self) -> dict[str, Any]:
        return {
            "terminalEventTypes": list(self.terminal_event_types),
            "terminalStages": list(self.terminal_stages),
            "autoRetireOnTerminal": self.auto_retire_on_terminal,
            "sharedListener": self.shared_listener,
        }


def is_terminal_event(data: Mapping[str, Any], fields: Mapping[str, str], policy: LifecyclePolicy) -> bool:
    event_type = str(fields.get("event_type") or "").strip().lower()
    if event_type and any(_matches_event_pattern(event_type, pattern) for pattern in policy.terminal_event_types):
        return True
    stage = _terminal_stage_value(data)
    return bool(stage and stage in {item.lower() for item in policy.terminal_stages})


def terminal_action(policy: LifecyclePolicy) -> str:
    if policy.shared_listener:
        return "shared_listener_kept_enabled"
    if policy.auto_retire_on_terminal:
        return "auto_retired"
    return "warn_only"


def _matches_event_pattern(event_type: str, pattern: str) -> bool:
    normalized = str(pattern or "").strip().lower()
    if not normalized:
        return False
    return fnmatch.fnmatchcase(event_type, normalized)


def _terminal_stage_value(data: Mapping[str, Any]) -> str:
    for key in ("stage", "workflowStage"):
        value = str(data.get(key) or "").strip().lower()
        if value:
            return value
    payload = data.get("payload")
    if isinstance(payload, Mapping):
        for key in ("stage", "workflowStage"):
            value = str(payload.get(key) or "").strip().lower()
            if value:
                return value
    return ""


def _first_non_none(value: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        item = value.get(key)
        if item is not None:
            return item
    return None


def _as_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    else:
        try:
            items = [str(item).strip() for item in value]
        except TypeError:
            items = [str(value).strip()]
    return tuple(item for item in items if item)
