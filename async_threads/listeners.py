"""Shared listener lifecycle service for slash commands and model tools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from .continuation import ContinuationPolicy
from .registry import AsyncThreadHandle, AsyncThreadRegistry
from .workflows import WorkflowPolicy


VALID_ACK_MODES = {"none", "brief", "debug"}
VALID_POLICIES = {"agent_queue", "direct"}
VALID_GATE_MODES = {"serial", "parallel"}


class ListenValidationError(ValueError):
    """Raised when a listener request cannot be normalized safely."""


@dataclass(frozen=True)
class ListenRequest:
    """Normalized listener creation request.

    This is intentionally independent from `/ath` command parsing so model-facing
    tools can reuse the same lifecycle path without scraping command strings.
    """

    producer_id: str
    allowed_event_types: tuple[str, ...] = ()
    label: str = ""
    policy: str = "agent_queue"
    ack_mode: str = "none"
    debounce_seconds: int = 0
    owner_user_id: str = ""
    gate_order: tuple[str, ...] = ()
    gate_mode: str = "serial"
    stale_on_artifact_change: tuple[str, ...] = ()
    candidate_required: tuple[str, ...] = ()
    workflow_policy: WorkflowPolicy = field(default_factory=WorkflowPolicy)
    continuation_policy: ContinuationPolicy = field(default_factory=ContinuationPolicy)


@dataclass(frozen=True)
class ListenResult:
    """Structured result from listener creation.

    The registry handle still contains the receiver-side secret so inbound events
    can be validated. Callers that render user-facing output should prefer
    `public_summary()` or secret-file references instead of literal secret values.
    """

    handle: AsyncThreadHandle
    event_url: str = ""
    source: dict[str, Any] = field(default_factory=dict)
    session_key: str = ""
    session_id: str = ""

    @property
    def thread_key(self) -> str:
        return self.handle.thread_key

    @property
    def producer_id(self) -> str:
        return self.handle.producer_id

    @property
    def allowed_event_types(self) -> tuple[str, ...]:
        return self.handle.allowed_event_types

    @property
    def policy(self) -> str:
        return self.handle.policy

    @property
    def ack_mode(self) -> str:
        return self.handle.ack_mode

    @property
    def debounce_seconds(self) -> int:
        return self.handle.debounce_seconds

    def public_summary(self) -> dict[str, Any]:
        """Return non-secret structured data suitable for tool results."""

        return {
            "threadKey": self.handle.thread_key,
            "producerId": self.handle.producer_id,
            "allowedEventTypes": list(self.handle.allowed_event_types),
            "policy": self.handle.policy,
            "ackMode": self.handle.ack_mode,
            "debounceSeconds": self.handle.debounce_seconds,
            "workflowPolicy": {
                "gate_order": list(self.handle.workflow_policy.gate_order),
                "gate_mode": self.handle.workflow_policy.gate_mode,
                "stale_on_artifact_change": list(self.handle.workflow_policy.stale_on_artifact_change),
                "candidate_required": list(self.handle.workflow_policy.candidate_required),
            },
            "continuationPolicy": self.handle.continuation_policy.public_summary(core_enforced=False),
            "eventUrl": self.event_url,
            "source": self.source,
            "sessionKeyPresent": bool(self.session_key),
            "sessionId": self.session_id,
        }


def create_listener(
    *,
    registry: AsyncThreadRegistry,
    source: Any,
    gateway: Any | None = None,
    producer_id: str,
    allowed_event_types: Iterable[str] = (),
    label: str = "",
    policy: str = "agent_queue",
    ack_mode: str = "none",
    debounce_seconds: Any = 0,
    workflow_policy: WorkflowPolicy | Mapping[str, Any] | None = None,
    continuation_policy: ContinuationPolicy | Mapping[str, Any] | None = None,
    gate_order: Iterable[str] = (),
    gate_mode: str = "serial",
    stale_on_artifact_change: Iterable[str] = (),
    candidate_required: Iterable[str] = (),
    session_key: str | None = None,
    session_id: str | None = None,
    owner_user_id: str = "",
    event_url: str = "",
) -> ListenResult:
    """Create a durable async-thread listener from a normalized request.

    This function owns lifecycle normalization shared by the slash command and
    future model-facing tools. It deliberately does not return preformatted chat
    text.
    """

    request = normalize_listen_request(
        producer_id=producer_id,
        allowed_event_types=allowed_event_types,
        label=label,
        policy=policy,
        ack_mode=ack_mode,
        debounce_seconds=debounce_seconds,
        workflow_policy=workflow_policy,
        continuation_policy=continuation_policy,
        gate_order=gate_order,
        gate_mode=gate_mode,
        stale_on_artifact_change=stale_on_artifact_change,
        candidate_required=candidate_required,
        owner_user_id=owner_user_id,
    )
    source_dict = source_to_dict(source)
    resolved_session_key = session_key if session_key is not None else _session_key_for_source(gateway, source)
    resolved_session_id = session_id if session_id is not None else _session_id_for_key(gateway, resolved_session_key)
    resolved_owner = request.owner_user_id or str(getattr(source, "user_id", "") or source_dict.get("user_id") or "")
    handle = registry.create_handle(
        source=source_dict,
        producer_id=request.producer_id,
        label=request.label,
        allowed_event_types=request.allowed_event_types,
        policy=request.policy,
        session_key=resolved_session_key,
        session_id=resolved_session_id,
        owner_user_id=resolved_owner,
        ack_mode=request.ack_mode,
        debounce_seconds=request.debounce_seconds,
        workflow_policy=request.workflow_policy,
        continuation_policy=request.continuation_policy,
    )
    return ListenResult(
        handle=handle,
        event_url=event_url,
        source=source_dict,
        session_key=resolved_session_key,
        session_id=resolved_session_id,
    )


def normalize_listen_request(
    *,
    producer_id: str,
    allowed_event_types: Iterable[str] = (),
    label: str = "",
    policy: str = "agent_queue",
    ack_mode: str = "none",
    debounce_seconds: Any = 0,
    workflow_policy: WorkflowPolicy | Mapping[str, Any] | None = None,
    continuation_policy: ContinuationPolicy | Mapping[str, Any] | None = None,
    gate_order: Iterable[str] = (),
    gate_mode: str = "serial",
    stale_on_artifact_change: Iterable[str] = (),
    candidate_required: Iterable[str] = (),
    owner_user_id: str = "",
) -> ListenRequest:
    normalized_policy = str(policy or "agent_queue")
    if normalized_policy not in VALID_POLICIES:
        normalized_policy = "agent_queue"

    normalized_ack = str(ack_mode or "none")
    if normalized_ack not in VALID_ACK_MODES:
        raise ListenValidationError("invalid ack mode. use one of: none, brief, debug.")

    try:
        normalized_debounce = int(debounce_seconds or 0)
    except (TypeError, ValueError) as exc:
        raise ListenValidationError("invalid debounce seconds. use 0-300.") from exc
    if normalized_debounce < 0 or normalized_debounce > 300:
        raise ListenValidationError("invalid debounce seconds. use 0-300.")

    normalized_gate_mode = str(gate_mode or "serial").lower()
    if normalized_gate_mode not in VALID_GATE_MODES:
        raise ListenValidationError("invalid gate mode. use one of: serial, parallel.")

    if normalized_policy == "direct":
        normalized_ack = "none"
        normalized_debounce = 0

    workflow_policy_obj = _workflow_policy_from_request(
        workflow_policy=workflow_policy,
        gate_order=gate_order,
        gate_mode=normalized_gate_mode,
        stale_on_artifact_change=stale_on_artifact_change,
        candidate_required=candidate_required,
    )
    continuation_policy_obj = (
        continuation_policy if isinstance(continuation_policy, ContinuationPolicy) else ContinuationPolicy.from_mapping(continuation_policy)
    )
    return ListenRequest(
        producer_id=str(producer_id or ""),
        allowed_event_types=tuple(str(event_type) for event_type in allowed_event_types),
        label=str(label or ""),
        policy=normalized_policy,
        ack_mode=normalized_ack,
        debounce_seconds=normalized_debounce,
        owner_user_id=str(owner_user_id or ""),
        gate_order=tuple(str(item) for item in gate_order),
        gate_mode=normalized_gate_mode,
        stale_on_artifact_change=tuple(str(item) for item in stale_on_artifact_change),
        candidate_required=tuple(str(item) for item in candidate_required),
        workflow_policy=workflow_policy_obj,
        continuation_policy=continuation_policy_obj,
    )


def source_to_dict(source: Any) -> dict[str, Any]:
    if source is None:
        raise ListenValidationError("current gateway source is unavailable.")
    if hasattr(source, "to_dict"):
        value = source.to_dict()
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(source, Mapping):
        return dict(source)
    if hasattr(source, "__dict__"):
        return {key: value for key, value in vars(source).items() if not key.startswith("_")}
    return dict(source)


def _workflow_policy_from_request(
    *,
    workflow_policy: WorkflowPolicy | Mapping[str, Any] | None,
    gate_order: Iterable[str],
    gate_mode: str,
    stale_on_artifact_change: Iterable[str],
    candidate_required: Iterable[str],
) -> WorkflowPolicy:
    if isinstance(workflow_policy, WorkflowPolicy):
        return workflow_policy
    if workflow_policy is not None:
        return WorkflowPolicy.from_mapping(workflow_policy)
    return WorkflowPolicy.from_mapping(
        {
            "gate_order": tuple(gate_order),
            "gate_mode": gate_mode,
            "stale_on_artifact_change": tuple(stale_on_artifact_change),
            "candidate_required": tuple(candidate_required),
        }
    )


def _session_key_for_source(gateway: Any | None, source: Any) -> str:
    if gateway is None:
        return ""
    from gateway.session import build_session_key

    return build_session_key(
        source,
        group_sessions_per_user=getattr(getattr(gateway, "config", None), "group_sessions_per_user", True),
        thread_sessions_per_user=getattr(getattr(gateway, "config", None), "thread_sessions_per_user", False),
    )


def _session_id_for_key(gateway: Any | None, session_key: str) -> str:
    if gateway is None or not session_key:
        return ""
    store = getattr(gateway, "session_store", None)
    if store is None:
        return ""
    try:
        entry = store.get_session_by_key(session_key)
    except Exception:
        entry = None
    if entry is None:
        return ""
    return str(getattr(entry, "session_id", "") or "")
