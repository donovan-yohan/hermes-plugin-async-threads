"""Generic workflow-stage primitives for async-thread events."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .privacy import redact_metadata_text, redact_secret_text, sanitize_untrusted_value

WORKFLOW_STAGES = {
    "started",
    "progress",
    "ready_for_review",
    "review_requested",
    "review_passed",
    "review_failed",
    "candidate_ready",
    "qa_requested",
    "qa_passed",
    "qa_failed",
    "blocked",
    "needs_attention",
    "released",
    "cancelled",
}
TERMINAL_STAGE_VALUES = {"released", "cancelled"}
READY_CANDIDATE_VALUES = {"ready", "released"}
PASSED_EVIDENCE_STATUS = "passed"


@dataclass(frozen=True)
class WorkflowPolicy:
    gate_order: tuple[str, ...] = ()
    gate_mode: str = "serial"
    stale_on_artifact_change: tuple[str, ...] = ()
    candidate_required: tuple[str, ...] = ()

    @classmethod
    def from_json(cls, value: str | None) -> "WorkflowPolicy":
        if not value:
            return cls()
        try:
            data = json.loads(value)
        except Exception:
            return cls()
        return cls.from_mapping(data if isinstance(data, Mapping) else {})

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "WorkflowPolicy":
        value = value or {}
        mode = str(value.get("gate_mode") or value.get("mode") or "serial").lower()
        if mode not in {"serial", "parallel"}:
            mode = "serial"
        return cls(
            gate_order=_clean_gate_list(value.get("gate_order") or value.get("gates")),
            gate_mode=mode,
            stale_on_artifact_change=_clean_gate_list(value.get("stale_on_artifact_change")),
            candidate_required=_clean_gate_list(value.get("candidate_required")),
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "gate_order": list(self.gate_order),
                "gate_mode": self.gate_mode,
                "stale_on_artifact_change": list(self.stale_on_artifact_change),
                "candidate_required": list(self.candidate_required),
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def normalize_workflow_event(data: Any, fields: Mapping[str, str]) -> dict[str, Any] | None:
    """Extract producer-agnostic workflow fields from an authenticated event.

    Missing workflow data is fine: ordinary ATH events stay as wakeups. When a
    workflow id is present, all untrusted producer values are sanitized before
    persistence or diagnostics.
    """
    if not isinstance(data, Mapping):
        return None
    workflow_id = _first_string(
        data.get("workflowId"),
        _get(data, "workflow.id"),
        _get(data, "workflow.workflow_id"),
        _get(data, "subject.workflow_id"),
        _get(data, "subject.workflowId"),
    )
    if not workflow_id:
        return None
    stage = _normalize_stage(
        _first_string(
            data.get("stage"),
            _get(data, "workflow.stage"),
            _get(data, "payload.stage"),
            _get(data, "payload.phase"),
        )
    )
    artifact = _clean_object(data.get("artifact") or _get(data, "payload.artifact") or _get(data, "subject.artifact"))
    if not artifact:
        artifact = _artifact_from_refs(data.get("refs"))
    candidate = _clean_object(data.get("candidate") or _get(data, "payload.candidate") or _get(data, "subject.candidate"))
    evidence = _clean_evidence(data.get("evidence") or _get(data, "payload.evidence"))
    return {
        "workflow_id": redact_metadata_text(workflow_id, max_chars=200),
        "stage": stage,
        "artifact": artifact,
        "artifact_fingerprint": fingerprint_json(artifact) if artifact else "",
        "candidate": candidate,
        "evidence": evidence,
        "last_event_id": redact_metadata_text(fields.get("event_id", ""), max_chars=200),
        "last_event_type": redact_metadata_text(fields.get("event_type", ""), max_chars=200),
        "last_summary": redact_secret_text(fields.get("summary", ""), max_input_chars=1000, max_output_chars=500),
    }


def apply_workflow_transition(
    *,
    previous: Mapping[str, Any] | None,
    event: Mapping[str, Any],
    policy: WorkflowPolicy,
    now: str,
) -> dict[str, Any]:
    evidence = _evidence_map(previous.get("evidence") if previous else None)
    artifact = event.get("artifact") or (previous.get("artifact") if previous else {}) or {}
    artifact_fingerprint = event.get("artifact_fingerprint") or (previous.get("artifact_fingerprint") if previous else "") or ""
    previous_artifact_fingerprint = str(previous.get("artifact_fingerprint") or "") if previous else ""
    artifact_changed = bool(artifact_fingerprint and previous_artifact_fingerprint and artifact_fingerprint != previous_artifact_fingerprint)
    if artifact_changed:
        evidence = _mark_stale_evidence(
            evidence,
            stale_gates=policy.stale_on_artifact_change,
            now=now,
        )
    incoming_evidence = event.get("evidence")
    preserve_previous_state_for_stale_incoming = False
    if isinstance(incoming_evidence, Mapping):
        kind = _clean_gate_name(incoming_evidence.get("kind"))
        if kind:
            incoming = dict(incoming_evidence)
            if artifact_changed and _evidence_stales_on_artifact_change(kind, policy):
                if str(incoming.get("status") or "") == PASSED_EVIDENCE_STATUS:
                    incoming["previous_status"] = incoming.get("status")
                    incoming["status"] = "stale"
                    incoming["stale_reason"] = "artifact_changed"
                    incoming["stale_at"] = now
                    preserve_previous_state_for_stale_incoming = previous is not None
            evidence[kind] = {
                **incoming,
                "kind": kind,
                "artifact_fingerprint": artifact_fingerprint,
                "updated_at": now,
            }
    candidate = event.get("candidate") or (previous.get("candidate") if previous else {}) or {}
    stage = str(event.get("stage") or (previous.get("stage") if previous else "") or "")
    previous_stage = str(previous.get("stage") or "") if previous else ""
    if previous_stage in TERMINAL_STAGE_VALUES and stage not in TERMINAL_STAGE_VALUES:
        stage = previous_stage
    elif preserve_previous_state_for_stale_incoming:
        stage = previous_stage
        artifact = (previous or {}).get("artifact") or {}
        artifact_fingerprint = previous_artifact_fingerprint
    gates = compute_gates(policy=policy, evidence=evidence, candidate=candidate)
    return {
        "workflow_id": str(event["workflow_id"]),
        "stage": stage,
        "artifact": artifact,
        "artifact_fingerprint": artifact_fingerprint,
        "candidate": candidate,
        "evidence": evidence,
        "gates": gates,
        "last_event_id": str(event.get("last_event_id") or ""),
        "last_event_type": str(event.get("last_event_type") or ""),
        "last_summary": str(event.get("last_summary") or ""),
        "updated_at": now,
    }


def compute_gates(*, policy: WorkflowPolicy, evidence: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    gate_order = list(policy.gate_order)
    gate_set = list(dict.fromkeys(gate_order + sorted(evidence.keys())))
    candidate_ready = _candidate_ready(candidate)
    states: dict[str, dict[str, Any]] = {}
    active: list[str] = []
    deferred: list[str] = []
    for index, gate in enumerate(gate_set):
        item = evidence.get(gate, {}) if isinstance(evidence.get(gate), Mapping) else {}
        status = str(item.get("status") or "unknown")
        requires_candidate = gate in policy.candidate_required
        if requires_candidate and not candidate_ready:
            gate_state = "deferred_candidate_not_ready"
            deferred.append(gate)
        elif status == PASSED_EVIDENCE_STATUS:
            gate_state = "passed"
        elif status == "failed":
            gate_state = "failed"
            active.append(gate)
        elif status == "stale":
            gate_state = "stale"
            active.append(gate)
        else:
            gate_state = "pending"
            active.append(gate)
        states[gate] = {
            "status": status,
            "state": gate_state,
            "candidate_required": requires_candidate,
        }
        blocking_serial_gate = gate_state in {"pending", "failed", "stale", "deferred_candidate_not_ready"}
        if policy.gate_mode == "serial" and blocking_serial_gate:
            # Later serial gates should not activate while an earlier gate is
            # still pending/failed/stale/deferred. Mark them as deferred unless
            # evidence already proves they passed.
            for later in gate_set[index + 1 :]:
                later_item = evidence.get(later, {}) if isinstance(evidence.get(later), Mapping) else {}
                later_status = str(later_item.get("status") or "unknown")
                states[later] = {
                    "status": later_status,
                    "state": "passed" if later_status == PASSED_EVIDENCE_STATUS else "deferred_serial_gate",
                    "candidate_required": later in policy.candidate_required,
                }
                if later_status != PASSED_EVIDENCE_STATUS:
                    deferred.append(later)
            break
    if policy.gate_mode == "serial":
        active = active[:1]
    return {
        "mode": policy.gate_mode,
        "order": gate_order,
        "active": active,
        "deferred": list(dict.fromkeys(deferred)),
        "states": states,
    }


def fingerprint_json(value: Any) -> str:
    if not value:
        return ""
    rendered = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()[:16]


def _get(data: Mapping[str, Any], path: str) -> Any:
    cur: Any = data
    for part in path.split("."):
        if not isinstance(cur, Mapping) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _first_string(*values: Any) -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _clean_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    cleaned = sanitize_untrusted_value(value)
    return cleaned if isinstance(cleaned, dict) else {}


def _clean_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    cleaned = _clean_object(value)
    kind = _clean_gate_name(cleaned.get("kind"))
    if not kind:
        return {}
    status = str(cleaned.get("status") or "unknown").lower().replace("-", "_")
    if status not in {"passed", "failed", "stale", "unknown"}:
        status = "unknown"
    cleaned["kind"] = kind
    cleaned["status"] = status
    return cleaned


def _artifact_from_refs(value: Any) -> dict[str, Any]:
    refs = _clean_object(value)
    if not refs:
        return {}
    head_sha = _first_string(refs.get("headSha"), refs.get("head_sha"), refs.get("commitSha"), refs.get("sha"))
    if head_sha:
        return {"kind": "git_commit", "id": head_sha}
    revision = _first_string(refs.get("artifactRevision"), refs.get("artifact_revision"), refs.get("revision"))
    if revision:
        return {"kind": "artifact_revision", "id": revision}
    return {}


def _evidence_stales_on_artifact_change(kind: str, policy: WorkflowPolicy) -> bool:
    return "all" in policy.stale_on_artifact_change or kind in policy.stale_on_artifact_change


def _normalize_stage(value: str) -> str:
    stage = str(value or "").strip().lower().replace("-", "_")
    return stage if stage in WORKFLOW_STAGES else stage[:80]


def _clean_gate_list(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        items = list(value)
    else:
        items = []
    cleaned = [_clean_gate_name(item) for item in items]
    return tuple(dict.fromkeys(item for item in cleaned if item))


def _clean_gate_name(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    return "".join(ch for ch in text if ch.isalnum() or ch in {"_", ".", ":"})[:80]


def _evidence_map(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for key, item in value.items():
        gate = _clean_gate_name(key)
        if gate and isinstance(item, Mapping):
            result[gate] = dict(item)
    return result


def _mark_stale_evidence(
    evidence: dict[str, dict[str, Any]],
    *,
    stale_gates: tuple[str, ...],
    now: str,
) -> dict[str, dict[str, Any]]:
    if not stale_gates:
        return evidence
    mark_all = "all" in stale_gates
    updated = {gate: dict(item) for gate, item in evidence.items()}
    for gate, item in updated.items():
        if mark_all or gate in stale_gates:
            if str(item.get("status") or "") == PASSED_EVIDENCE_STATUS:
                item["previous_status"] = item.get("status")
                item["status"] = "stale"
                item["stale_reason"] = "artifact_changed"
                item["stale_at"] = now
    return updated


def _candidate_ready(candidate: Mapping[str, Any]) -> bool:
    readiness = str(candidate.get("readiness") or candidate.get("status") or "").lower().replace("-", "_")
    stage = str(candidate.get("stage") or "").lower().replace("-", "_")
    return readiness in READY_CANDIDATE_VALUES or stage == "candidate_ready"
