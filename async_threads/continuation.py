"""Continuation policy metadata for agent-queue async-thread events."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

DEFAULT_MAX_TURNS = 1
DEFAULT_MAX_TOOL_CALLS = 0
DEFAULT_TIMEOUT_SECONDS = 120
MAX_ALLOWED_TURNS = 5
MAX_ALLOWED_TOOL_CALLS = 20
MAX_ALLOWED_TIMEOUT_SECONDS = 600


@dataclass(frozen=True)
class ContinuationPolicy:
    """Bounded-continuation intent stored with a listener.

    Hermes core does not currently expose a plugin-local per-event runtime cap
    that this plugin can enforce when it calls a platform adapter's
    ``handle_message``. The policy is therefore explicit metadata by default;
    callers that require a hard cap can set ``fail_closed_without_core_bounds``
    so dispatch returns a retryable failure instead of starting an unbounded
    continuation.
    """

    max_turns: int = DEFAULT_MAX_TURNS
    max_tool_calls: int = DEFAULT_MAX_TOOL_CALLS
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    toolsets: tuple[str, ...] = field(default_factory=tuple)
    fail_closed_without_core_bounds: bool = False

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "ContinuationPolicy":
        value = value or {}
        return cls(
            max_turns=_bounded_int(value.get("max_turns", value.get("maxTurns")), DEFAULT_MAX_TURNS, 1, MAX_ALLOWED_TURNS),
            max_tool_calls=_bounded_int(
                value.get("max_tool_calls", value.get("maxToolCalls")),
                DEFAULT_MAX_TOOL_CALLS,
                0,
                MAX_ALLOWED_TOOL_CALLS,
            ),
            timeout_seconds=_bounded_int(
                value.get("timeout_seconds", value.get("timeoutSeconds")),
                DEFAULT_TIMEOUT_SECONDS,
                10,
                MAX_ALLOWED_TIMEOUT_SECONDS,
            ),
            toolsets=_normalize_toolsets(value.get("toolsets", ())),
            fail_closed_without_core_bounds=bool(
                value.get("fail_closed_without_core_bounds", value.get("failClosedWithoutCoreBounds", False))
            ),
        )

    @classmethod
    def from_json(cls, value: str | None) -> "ContinuationPolicy":
        if not value:
            return cls()
        import json

        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            parsed = {}
        return cls.from_mapping(parsed if isinstance(parsed, Mapping) else {})

    def to_mapping(self) -> dict[str, Any]:
        return {
            "max_turns": self.max_turns,
            "max_tool_calls": self.max_tool_calls,
            "timeout_seconds": self.timeout_seconds,
            "toolsets": list(self.toolsets),
            "fail_closed_without_core_bounds": self.fail_closed_without_core_bounds,
        }

    def public_summary(self, *, core_enforced: bool = False) -> dict[str, Any]:
        return {
            "maxTurns": self.max_turns,
            "maxToolCalls": self.max_tool_calls,
            "timeoutSeconds": self.timeout_seconds,
            "toolsets": list(self.toolsets),
            "failClosedWithoutCoreBounds": self.fail_closed_without_core_bounds,
            "coreEnforced": core_enforced,
        }

    def to_json(self) -> str:
        import json

        return json.dumps(self.to_mapping(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _normalize_toolsets(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items: Iterable[Any] = (part.strip() for part in value.split(","))
    elif isinstance(value, Iterable):
        items = value
    else:
        items = (value,)
    normalized: list[str] = []
    for item in items:
        token = str(item or "").strip()
        if not token:
            continue
        token = "".join(ch for ch in token.lower() if ch.isalnum() or ch in {"_", "-"})[:40]
        if token and token not in normalized:
            normalized.append(token)
    return tuple(normalized[:8])


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))
