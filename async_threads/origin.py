"""Resolve the current gateway origin for model-facing ATH tools."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .listeners import source_to_dict

UNAVAILABLE_ERROR = "source_unavailable"
LOCAL_ONLY_PLATFORMS = {"", "local", "cli", "api_server"}


@dataclass(frozen=True)
class OriginResolution:
    """Result of resolving the active conversation origin."""

    ok: bool
    source: Any | None = None
    source_dict: dict[str, Any] = field(default_factory=dict)
    session_key: str = ""
    session_id: str = ""
    owner_user_id: str = ""
    source_kind: str = ""
    error: str = ""
    message: str = ""
    remediation: str = ""

    @classmethod
    def unavailable(cls, message: str = "current gateway origin unavailable") -> "OriginResolution":
        return cls(
            ok=False,
            error=UNAVAILABLE_ERROR,
            message=message,
            remediation="Use this from a live gateway conversation, or fall back to /ath listen from the target thread.",
        )

    def public_error(self) -> dict[str, Any]:
        """Return a structured error safe for model-tool output."""

        if self.ok:
            return {"ok": True}
        return {
            "ok": False,
            "error": self.error or UNAVAILABLE_ERROR,
            "message": self.message or "current gateway origin unavailable",
            "remediation": self.remediation,
        }


class OriginIndex:
    """Small process-local fallback index for trusted gateway-origin captures.

    The persisted Hermes SessionStore / sessions.json remains the preferred
    source of truth. This index exists only as a last-resort bridge for tests or
    future host paths that can safely pass source metadata but not a store.
    """

    def __init__(self) -> None:
        self._by_session_id: dict[str, OriginResolution] = {}
        self._by_session_key: dict[str, OriginResolution] = {}

    def remember(
        self,
        *,
        source: Any,
        session_key: str = "",
        session_id: str = "",
        source_kind: str = "index",
    ) -> OriginResolution:
        resolution = _resolution_from_source(
            source,
            session_key=session_key,
            session_id=session_id,
            source_kind=source_kind,
        )
        if not resolution.ok:
            return resolution
        if resolution.session_id:
            self._by_session_id[resolution.session_id] = resolution
        if resolution.session_key:
            self._by_session_key[resolution.session_key] = resolution
        return resolution

    def lookup(self, *, session_id: str = "", session_key: str = "") -> OriginResolution | None:
        if session_id and session_id in self._by_session_id:
            return self._by_session_id[session_id]
        if session_key and session_key in self._by_session_key:
            return self._by_session_key[session_key]
        return None


_ORIGIN_INDEX = OriginIndex()


def get_origin_index() -> OriginIndex:
    return _ORIGIN_INDEX


def resolve_current_origin(
    *,
    source: Any | None = None,
    trusted_context: Mapping[str, Any] | None = None,
    session_id: str = "",
    session_key: str = "",
    gateway: Any | None = None,
    session_store: Any | None = None,
    sessions_file: str | Path | None = None,
    origin_index: OriginIndex | None = None,
) -> OriginResolution:
    """Resolve the gateway origin for a model-facing ATH operation.

    Resolution order is deliberately conservative:

    1. explicit trusted source object (`source`, `gateway_source`, or
       `session_source`);
    2. active SessionStore lookup by session_id;
    3. active SessionStore lookup by session_key;
    4. persisted profile sessions.json lookup;
    5. plugin-local trusted origin index;
    6. task-local Hermes session context variables.

    This function does **not** parse arbitrary model/tool arguments as source
    metadata. A user-provided JSON blob saying `{platform: discord, ...}` is not
    proof of the current conversation.
    """

    context = dict(trusted_context or {})
    trusted_source = source or context.get("gateway_source") or context.get("session_source")
    explicit_sid = _first_text(session_id, context.get("session_id"))
    explicit_skey = _first_text(session_key, context.get("session_key"))
    explicit_lookup = bool(explicit_sid or explicit_skey)
    sid = explicit_sid or _session_env("HERMES_SESSION_ID")
    skey = explicit_skey or ("" if explicit_sid else _session_env("HERMES_SESSION_KEY"))

    if trusted_source is not None:
        return _resolution_from_source(
            trusted_source,
            session_key=skey,
            session_id=sid,
            source_kind="explicit",
        )

    store = session_store or getattr(gateway, "session_store", None)
    from_store = _lookup_store(store, session_id=sid, session_key=skey)
    if from_store is not None:
        return from_store

    from_file = _lookup_sessions_file(
        Path(sessions_file) if sessions_file is not None else _default_sessions_file(),
        session_id=sid,
        session_key=skey,
    )
    if from_file is not None:
        return from_file

    index = origin_index or get_origin_index()
    indexed = index.lookup(session_id=sid, session_key=skey)
    if indexed is not None:
        return indexed

    if explicit_lookup:
        return OriginResolution.unavailable()

    from_env = _resolution_from_session_context(session_id=sid, session_key=skey)
    if from_env is not None:
        return from_env

    return OriginResolution.unavailable()


def remember_gateway_origin(
    *,
    event: Any | None = None,
    source: Any | None = None,
    gateway: Any | None = None,
    session_store: Any | None = None,
    origin_index: OriginIndex | None = None,
) -> OriginResolution:
    """Remember a trusted gateway source in the process-local fallback index."""

    actual_source = source if source is not None else getattr(event, "source", None)
    if actual_source is None:
        return OriginResolution.unavailable()
    store = session_store or getattr(gateway, "session_store", None)
    session_key = _session_key_for_source(gateway, actual_source)
    session_id = ""
    entry = _get_store_entry_by_key(store, session_key) if session_key else None
    if entry is not None:
        session_id = str(getattr(entry, "session_id", "") or "")
    return (origin_index or get_origin_index()).remember(
        source=actual_source,
        session_key=session_key,
        session_id=session_id,
        source_kind="gateway_event",
    )


def _resolution_from_source(
    source: Any,
    *,
    session_key: str = "",
    session_id: str = "",
    source_kind: str,
) -> OriginResolution:
    try:
        source_dict = source_to_dict(source)
    except Exception:
        return OriginResolution.unavailable()
    if not _source_is_gateway_routable(source_dict):
        return OriginResolution.unavailable()
    return OriginResolution(
        ok=True,
        source=source,
        source_dict=source_dict,
        session_key=str(session_key or ""),
        session_id=str(session_id or ""),
        owner_user_id=str(source_dict.get("user_id") or ""),
        source_kind=source_kind,
    )


def _lookup_store(store: Any, *, session_id: str, session_key: str) -> OriginResolution | None:
    if store is None:
        return None
    entry = None
    if session_id:
        lookup = getattr(store, "lookup_by_session_id", None)
        if callable(lookup):
            try:
                entry = lookup(session_id)
            except Exception:
                entry = None
    if entry is None and session_key:
        entry = _get_store_entry_by_key(store, session_key)
    if entry is None:
        return None
    source = getattr(entry, "origin", None)
    if source is None:
        return None
    return _resolution_from_source(
        source,
        session_key=str(getattr(entry, "session_key", "") or session_key),
        session_id=str(getattr(entry, "session_id", "") or session_id),
        source_kind="session_store",
    )


def _get_store_entry_by_key(store: Any, session_key: str) -> Any | None:
    if store is None or not session_key:
        return None
    get_by_key = getattr(store, "get_session_by_key", None)
    if callable(get_by_key):
        try:
            return get_by_key(session_key)
        except Exception:
            return None
    try:
        ensure = getattr(store, "_ensure_loaded", None)
        if callable(ensure):
            ensure()
        entries = getattr(store, "_entries", {}) or {}
        return entries.get(session_key)
    except Exception:
        return None


def _lookup_sessions_file(path: Path | None, *, session_id: str, session_key: str) -> OriginResolution | None:
    if path is None or (not session_id and not session_key) or not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, Mapping):
        return None
    for key, raw_entry in data.items():
        if not isinstance(raw_entry, Mapping):
            continue
        entry_session_key = str(raw_entry.get("session_key") or key or "")
        entry_session_id = str(raw_entry.get("session_id") or "")
        if session_id and entry_session_id != session_id:
            continue
        if session_key and entry_session_key != session_key:
            continue
        origin = raw_entry.get("origin")
        if not isinstance(origin, Mapping):
            continue
        return _resolution_from_source(
            dict(origin),
            session_key=entry_session_key,
            session_id=entry_session_id,
            source_kind="sessions_file",
        )
    return None


def _resolution_from_session_context(*, session_id: str, session_key: str) -> OriginResolution | None:
    platform = _session_env("HERMES_SESSION_PLATFORM")
    chat_id = _session_env("HERMES_SESSION_CHAT_ID")
    if not platform or not chat_id or platform in LOCAL_ONLY_PLATFORMS:
        return None
    source = {
        "platform": platform,
        "chat_id": chat_id,
        "chat_name": _session_env("HERMES_SESSION_CHAT_NAME") or None,
        "chat_type": "channel",
        "user_id": _session_env("HERMES_SESSION_USER_ID") or None,
        "user_name": _session_env("HERMES_SESSION_USER_NAME") or None,
        "thread_id": _session_env("HERMES_SESSION_THREAD_ID") or None,
        "message_id": _session_env("HERMES_SESSION_MESSAGE_ID") or None,
    }
    return _resolution_from_source(source, session_key=session_key, session_id=session_id, source_kind="session_context")


def _source_is_gateway_routable(source: Mapping[str, Any]) -> bool:
    platform = str(source.get("platform") or "")
    chat_id = str(source.get("chat_id") or "")
    return bool(platform and chat_id and platform not in LOCAL_ONLY_PLATFORMS)


def _session_key_for_source(gateway: Any | None, source: Any) -> str:
    if gateway is None or source is None:
        return ""
    try:
        from gateway.session import build_session_key

        return build_session_key(
            source,
            group_sessions_per_user=getattr(getattr(gateway, "config", None), "group_sessions_per_user", True),
            thread_sessions_per_user=getattr(getattr(gateway, "config", None), "thread_sessions_per_user", False),
        )
    except Exception:
        return ""


def _default_sessions_file() -> Path | None:
    try:
        from hermes_constants import get_hermes_home

        return get_hermes_home() / "sessions" / "sessions.json"
    except Exception:
        return None


def _session_env(name: str) -> str:
    try:
        from gateway.session_context import get_session_env

        return str(get_session_env(name, "") or "")
    except Exception:
        return ""


def _first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""
