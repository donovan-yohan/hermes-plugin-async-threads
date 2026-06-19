"""Privacy helpers for hostile async-thread event text."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any

MAX_REDACTION_INPUT_CHARS = 4000
MAX_REDACTION_OUTPUT_CHARS = 4000

_SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"(?i)\b(authorization)\b\s*[:=]\s*(?:Bearer|Basic)?\s*[^,;\r\n]+"),
        r"\1=<redacted>",
    ),
    (
        re.compile(r"(?i)\b(cookie)\b\s*[:= ]\s*[^,;\r\n]+"),
        r"\1=<redacted>",
    ),
    (
        re.compile(r"(?i)\b(x-hermes-signature-256|signature)\b\s*[:= ]\s*(?:sha256=)?[^,;\r\n\s]+"),
        r"\1=<redacted>",
    ),
    (
        re.compile(r"(?i)\b(session[-_]?key|sessionKey)\b\s*[:= ]\s*[^,;\r\n]+"),
        r"\1=<redacted>",
    ),
    (
        re.compile(r"(?i)\b(x[-_]?api[-_]?key|api[-_]?key|secret|token|password|credential)\b\s*[:=]\s*\S+"),
        r"\1=<redacted>",
    ),
    (
        re.compile(r"(?i)\b(access[-_]?token|refresh[-_]?token|id[-_]?token)\b\s*[:=]\s*[^&\s,;\r\n]+"),
        r"\1=<redacted>",
    ),
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+"), "Bearer <redacted>"),
    (re.compile(r"(?i)\bbasic\s+[A-Za-z0-9._~+/=-]+"), "Basic <redacted>"),
    (re.compile(r"\bsha256=[A-Fa-f0-9]{8,}\b"), "sha256=<redacted>"),
    (re.compile(r"\bagent:[A-Za-z0-9._:-]+"), "agent:<redacted>"),
)

_UNSAFE_KEY_RE = re.compile(
    r"^(?:secret|token|authorization|cookie|signature|password|credential|api[-_]?key|x[-_]?api[-_]?key|session[-_]?key|headers|access[-_]?token|refresh[-_]?token|id[-_]?token)$"
    r"|(?:^|[_-])(?:secret|password|credential|api[-_]?key|x[-_]?api[-_]?key|session[-_]?key)(?:$|[_-])",
    re.IGNORECASE,
)
_SENSITIVE_WORD_RE = re.compile(
    r"(?i)bearer|basic|api[-_]?key|cookie|signature|session[-_]?key|secret|token|password|credential"
)


def redact_secret_text(
    value: Any,
    *,
    max_input_chars: int = MAX_REDACTION_INPUT_CHARS,
    max_output_chars: int | None = MAX_REDACTION_OUTPUT_CHARS,
) -> str:
    """Redact credential-shaped substrings from hostile text.

    Input is bounded before regex passes so long hostile strings cannot turn
    redaction itself into the bottleneck.
    """
    text = str(value or "")[: max(0, max_input_chars)]
    for pattern, replacement in _SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    if max_output_chars is not None and len(text) > max_output_chars:
        return text[:max_output_chars] + "\n...<truncated>"
    return text


def redact_metadata_text(value: Any, *, max_chars: int = 200) -> str:
    raw = str(value or "")
    redacted = redact_secret_text(raw, max_input_chars=max_chars, max_output_chars=max_chars)
    if "<redacted>" not in redacted and _SENSITIVE_WORD_RE.search(raw):
        return f"redacted:{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:12]}"
    return redacted


def safe_event_id(value: Any) -> str:
    raw = str(value or "")
    if not raw:
        return ""
    redacted = redact_metadata_text(raw, max_chars=200)
    if "<redacted>" in redacted or _SENSITIVE_WORD_RE.search(raw):
        return f"redacted:{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:12]}"
    return redacted[:200]


def sanitize_untrusted_value(value: Any, *, depth: int = 0) -> Any:
    """Return a JSON-safe value with secret-shaped content redacted.

    This is for authenticated-but-hostile producer-controlled event fields before
    they enter prompts, visible notices, registry display surfaces, or logs.
    """
    if depth > 8:
        return "<max-depth>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return redact_secret_text(value)
    if isinstance(value, Mapping):
        cleaned: dict[str, Any] = {}
        for key, item in list(value.items())[:100]:
            key_text = redact_secret_text(str(key), max_output_chars=200)
            if _UNSAFE_KEY_RE.search(str(key)):
                cleaned[key_text] = "<redacted>"
            else:
                cleaned[key_text] = sanitize_untrusted_value(item, depth=depth + 1)
        return cleaned
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [sanitize_untrusted_value(item, depth=depth + 1) for item in list(value)[:100]]
    return redact_secret_text(value)
