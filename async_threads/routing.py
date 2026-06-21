"""Routing helpers for sending notices back to stored Hermes session sources."""

from __future__ import annotations

from typing import Any, Callable, cast

try:  # pragma: no cover - fallback covers older Hermes checkouts.
    from gateway.platforms.base import _thread_metadata_for_source as _raw_thread_metadata_for_source  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    _raw_thread_metadata_for_source = None

_core_thread_metadata_for_source = cast(
    Callable[..., dict[str, Any] | None] | None,
    _raw_thread_metadata_for_source,
)


def _thread_id_fallback(source: Any) -> dict[str, Any] | None:
    thread_id = getattr(source, "thread_id", None)
    if thread_id is None:
        return None
    return {"thread_id": thread_id}


def send_metadata_for_source(source: Any, *, reply_to_message_id: str | None = None) -> dict[str, Any] | None:
    """Return platform-aware metadata for sends targeting a captured source.

    Hermes currently exposes this as a private gateway helper, so the plugin
    centralizes usage here instead of hand-rolling ``{"thread_id": ...}`` at
    each send site. Issue #32 tracks replacing this shim with a stable public
    continuation/routing API once Hermes core provides one.
    """
    if callable(_core_thread_metadata_for_source):
        try:
            return _core_thread_metadata_for_source(source, reply_to_message_id=reply_to_message_id)
        except TypeError:
            # Older Hermes checkouts may expose the helper without the newer
            # reply anchor kwarg. Preserve generic thread routing rather than
            # failing notice/direct-delivery sends at runtime.
            return _thread_id_fallback(source)
    return _thread_id_fallback(source)
