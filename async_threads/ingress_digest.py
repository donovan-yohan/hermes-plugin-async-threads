"""Ingress digest policy and pointer payload helpers."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Mapping

from .privacy import redact_metadata_text, redact_secret_text, sanitize_untrusted_value

MODES = {"off", "pointer", "pointer_summary", "inline_summary"}
STORE_EVENT_MODES = {"none", "redacted", "raw_local"}
FETCH_DEFAULTS = {"redacted", "raw_local"}

_DEFAULT_MAX_INPUT_CHARS = 12_000
_DEFAULT_MAX_OUTPUT_TOKENS = 256
_DEFAULT_REDACTED_RETENTION_SECONDS = 7 * 24 * 60 * 60
_DEFAULT_RAW_RETENTION_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class IngressDigestPolicy:
    enabled: bool = False
    mode: str = "off"
    provider: str = "auto"
    model: str = "auto"
    max_input_chars: int = _DEFAULT_MAX_INPUT_CHARS
    max_output_tokens: int = _DEFAULT_MAX_OUTPUT_TOKENS
    store_event: str = "none"
    fetch_default: str = "redacted"
    retention_seconds: int = _DEFAULT_REDACTED_RETENTION_SECONDS
    source: str = "off"

    @property
    def active(self) -> bool:
        return bool(self.enabled and self.mode != "off")

    @property
    def stores_payload(self) -> bool:
        return self.active and self.store_event in {"redacted", "raw_local"}

    def to_mapping(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "provider": self.provider,
            "model": self.model,
            "max_input_chars": self.max_input_chars,
            "max_output_tokens": self.max_output_tokens,
            "store_event": self.store_event,
            "fetch_default": self.fetch_default,
            "retention_seconds": self.retention_seconds,
        }

    def public_summary(self) -> dict[str, Any]:
        return {
            "effectiveMode": self.mode if self.active else "off",
            "storeEvent": self.store_event if self.active else "none",
            "provider": redact_metadata_text(self.provider),
            "model": redact_metadata_text(self.model),
            "source": self.source if self.active or self.source != "off" else "off",
        }


def disabled_policy(*, source: str = "off") -> IngressDigestPolicy:
    return IngressDigestPolicy(source=source)


def normalize_ingress_digest_policy(value: Mapping[str, Any] | None, *, source: str = "override") -> IngressDigestPolicy:
    raw = _canonical_mapping(value)
    if _explicit_disable(raw):
        return disabled_policy(source=source)
    if not raw:
        return disabled_policy(source="off")
    enabled = bool(raw.get("enabled", False))
    mode = _mode(raw.get("mode"), default="pointer_summary" if enabled else "off")
    if not enabled or mode == "off":
        return disabled_policy(source=source)
    store_default = "redacted" if mode in {"pointer", "pointer_summary"} else "none"
    store_event = _store_event(raw.get("store_event"), default=store_default)
    if mode in {"pointer", "pointer_summary"} and store_event == "none":
        store_event = "redacted"
    fetch_default = _fetch_default(raw.get("fetch_default"), default="redacted")
    if fetch_default == "raw_local" and store_event != "raw_local":
        fetch_default = "redacted"
    retention_default = _DEFAULT_RAW_RETENTION_SECONDS if store_event == "raw_local" else _DEFAULT_REDACTED_RETENTION_SECONDS
    return IngressDigestPolicy(
        enabled=True,
        mode=mode,
        provider=_clean_meta(raw.get("provider"), default="auto"),
        model=_clean_meta(raw.get("model"), default="auto"),
        max_input_chars=_bounded_int(raw.get("max_input_chars"), default=_DEFAULT_MAX_INPUT_CHARS, minimum=1000, maximum=100_000),
        max_output_tokens=_bounded_int(raw.get("max_output_tokens"), default=_DEFAULT_MAX_OUTPUT_TOKENS, minimum=32, maximum=2048),
        store_event=store_event,
        fetch_default=fetch_default,
        retention_seconds=_bounded_int(raw.get("retention_seconds"), default=retention_default, minimum=60, maximum=30 * 24 * 60 * 60),
        source=source,
    )


def resolve_ingress_digest_policy(
    *,
    global_policy: Mapping[str, Any] | None = None,
    listener_policy: Mapping[str, Any] | None = None,
    source_binding_policy: Mapping[str, Any] | None = None,
) -> IngressDigestPolicy:
    merged: dict[str, Any] = {}
    source = "off"
    for name, layer in (
        ("global", global_policy),
        ("listener", listener_policy),
        ("source_binding", source_binding_policy),
    ):
        raw = _canonical_mapping(layer)
        if not raw:
            continue
        if _explicit_disable(raw):
            merged = {"enabled": False}
        else:
            merged.update(raw)
        source = name
    if not merged:
        return disabled_policy(source="off")
    return normalize_ingress_digest_policy(merged, source=source)


def policy_override_mapping(value: Any) -> dict[str, Any]:
    """Return a safe normalized mapping for storing listener/binding overrides."""

    raw = _canonical_mapping(value if isinstance(value, Mapping) else None)
    if not raw:
        return {}
    if _explicit_disable(raw):
        return {"enabled": False, "mode": "off"}
    policy = normalize_ingress_digest_policy(raw)
    return policy.to_mapping() if policy.active else {"enabled": False, "mode": "off"}


def build_pointer_id(*, owner_user_id: str, thread_key: str, producer_id: str, event_id: str) -> str:
    material = "\0".join([owner_user_id, thread_key, producer_id, event_id]).encode("utf-8", "replace")
    return "athp_" + hashlib.sha256(material).hexdigest()[:24]


def payload_expires_at(policy: IngressDigestPolicy, *, now: float | None = None) -> str:
    ts = time.gmtime((time.time() if now is None else now) + max(60, int(policy.retention_seconds)))
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", ts)


def bounded_event_payload(data: Mapping[str, Any], *, max_chars: int, redacted: bool) -> dict[str, Any]:
    event: dict[str, Any] = {}
    for key in (
        "summary",
        "subject",
        "payload",
        "workflowId",
        "workflow",
        "stage",
        "artifact",
        "candidate",
        "evidence",
        "refs",
        "loop",
        "step",
        "correlation",
        "nextExpectedSignal",
        "seriesKey",
    ):
        if key in data:
            event[key] = data[key]
    cleaned = sanitize_untrusted_value(event) if redacted else _bounded_raw_value(event)
    return _bound_json_mapping(cleaned, max_chars=max_chars)


def local_digest(data: Mapping[str, Any], *, event_type: str, producer_id: str, summary: str, max_chars: int) -> dict[str, Any]:
    subject = data.get("subject", {}) if isinstance(data.get("subject"), Mapping) else {}
    workflow: dict[str, Any] = {}
    for key in ("workflowId", "workflow", "stage", "artifact", "candidate", "evidence", "seriesKey"):
        if key in data:
            workflow[key] = data[key]
    return {
        "status": "model_unavailable_pointer_only",
        "summary": redact_secret_text(summary or f"{producer_id} emitted {event_type}", max_input_chars=max_chars, max_output_chars=min(max_chars, 1000)),
        "subjects": _bound_json_mapping(sanitize_untrusted_value(subject), max_chars=2000),
        "routingFacts": _bound_json_mapping(sanitize_untrusted_value(workflow), max_chars=2000),
        "safetyNotes": [
            "digest is context hygiene only",
            "payload remains untrusted data",
            "no model route was invoked",
        ],
    }


def _canonical_mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    aliases = {
        "maxInputChars": "max_input_chars",
        "maxOutputTokens": "max_output_tokens",
        "storeEvent": "store_event",
        "fetchDefault": "fetch_default",
        "retentionSeconds": "retention_seconds",
    }
    out: dict[str, Any] = {}
    for key, item in value.items():
        text = str(key)
        out[aliases.get(text, text)] = item
    return out


def _explicit_disable(raw: Mapping[str, Any]) -> bool:
    if "enabled" in raw and not bool(raw.get("enabled")):
        return True
    return str(raw.get("mode") or "").strip().lower().replace("-", "_") == "off"


def _mode(value: Any, *, default: str) -> str:
    mode = str(value or default).strip().lower().replace("-", "_")
    return mode if mode in MODES else default


def _store_event(value: Any, *, default: str) -> str:
    mode = str(value or default).strip().lower().replace("-", "_")
    return mode if mode in STORE_EVENT_MODES else default


def _fetch_default(value: Any, *, default: str) -> str:
    mode = str(value or default).strip().lower().replace("-", "_")
    return mode if mode in FETCH_DEFAULTS else default


def _clean_meta(value: Any, *, default: str) -> str:
    text = redact_metadata_text(str(value or default), max_chars=100)
    return text or default


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _bounded_raw_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 8:
        return "<max-depth>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:MAX_RAW_STRING_CHARS]
    if isinstance(value, Mapping):
        return {str(k)[:200]: _bounded_raw_value(v, depth=depth + 1) for k, v in list(value.items())[:100]}
    if isinstance(value, (list, tuple)):
        return [_bounded_raw_value(item, depth=depth + 1) for item in list(value)[:100]]
    return str(value)[:MAX_RAW_STRING_CHARS]


MAX_RAW_STRING_CHARS = 4000


def _bound_json_mapping(value: Any, *, max_chars: int) -> dict[str, Any]:
    payload = value if isinstance(value, dict) else {"value": value}
    try:
        rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    except Exception:
        rendered = json.dumps({"value": redact_secret_text(str(payload))}, ensure_ascii=False)
    if len(rendered) <= max_chars:
        return payload
    digest = hashlib.sha256(rendered.encode("utf-8", "replace")).hexdigest()[:16]
    preview = redact_secret_text(rendered[: max(0, max_chars - 200)], max_input_chars=max_chars, max_output_chars=max(0, max_chars - 200))
    return {"truncated": True, "chars": len(rendered), "sha256Prefix": digest, "preview": preview}
