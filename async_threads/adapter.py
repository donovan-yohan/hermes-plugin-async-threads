"""Gateway platform adapter for async-thread event ingress."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping

from .registry import AsyncThreadHandle, AsyncThreadRegistry, safe_session_key_hash
from .rendering import render_event_message
from .security import (
    DEFAULT_REPLAY_WINDOW_SECONDS,
    EventValidationError,
    event_field,
    extract_envelope_fields,
    parse_json_body,
    signature_header,
    validate_timestamp,
    verify_hmac_signature,
)

logger = logging.getLogger(__name__)


class DispatchEventError(RuntimeError):
    def __init__(self, message: str, *, detail: dict[str, Any] | None = None):
        super().__init__(message)
        self.detail = detail or {}


def check_requirements() -> bool:
    try:
        import aiohttp  # noqa: F401
    except Exception:
        return False
    return True


def validate_config(config: Any) -> bool:
    extra = getattr(config, "extra", {}) or {}
    try:
        int(extra.get("port", 8765))
    except (TypeError, ValueError):
        return False
    return True


def registry_path_from_config(config: Any) -> Path:
    extra = getattr(config, "extra", {}) or {}
    configured = extra.get("registry_path")
    if configured:
        return Path(str(configured)).expanduser()
    try:
        from hermes_constants import get_hermes_home
    except Exception:  # pragma: no cover - old Hermes fallback
        from hermes_cli.config import get_hermes_home  # type: ignore
    return get_hermes_home() / "async_threads" / "registry.sqlite3"


def registry_from_config(config: Any) -> AsyncThreadRegistry:
    return AsyncThreadRegistry(registry_path_from_config(config))


class AsyncThreadsAdapter:  # subclassed dynamically to keep imports test-friendly
    pass


def _build_adapter_base():
    from gateway.config import Platform
    from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult, merge_pending_message_event
    from gateway.session import SessionSource, build_session_key

    class _AsyncThreadsAdapter(BasePlatformAdapter):
        """HTTP receiver that injects authenticated events into existing sessions."""

        def __init__(self, config: Any):
            super().__init__(config, Platform("async_threads"))
            self.gateway_runner = None
            self._registry = registry_from_config(config)
            self._runner = None
            self._site = None
            self._host = str((config.extra or {}).get("host", "127.0.0.1"))
            self._port = int((config.extra or {}).get("port", 8765))
            self._max_body_bytes = int((config.extra or {}).get("max_body_bytes", 64 * 1024))
            self._replay_window_seconds = int(
                (config.extra or {}).get("replay_window_seconds", DEFAULT_REPLAY_WINDOW_SECONDS)
            )

        async def connect(self) -> bool:
            from aiohttp import web

            app = web.Application(client_max_size=self._max_body_bytes)
            app.router.add_get("/async-threads/v1/health", self._handle_health)
            app.router.add_post("/async-threads/v1/events", self._handle_event)
            self._runner = web.AppRunner(app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()
            self._running = True
            logger.info("Async Threads receiver listening on %s:%s", self._host, self._port)
            return True

        async def disconnect(self) -> None:
            self._running = False
            if self._site is not None:
                await self._site.stop()
                self._site = None
            if self._runner is not None:
                await self._runner.cleanup()
                self._runner = None

        async def send(self, chat_id: str, content: str, reply_to=None, metadata=None):
            return SendResult(success=False, error="async_threads is an ingress-only platform")

        async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
            return {"id": chat_id, "type": "async_threads", "name": "Async Threads"}

        async def _handle_health(self, request):
            from aiohttp import web

            return web.json_response({"ok": True, "platform": "async_threads"})

        async def _handle_event(self, request):
            from aiohttp import web

            raw = await request.read()
            try:
                data = parse_json_body(raw, max_bytes=self._max_body_bytes)
                fields = extract_envelope_fields(data)
                validate_timestamp(
                    data.get("occurredAt"),
                    replay_window_seconds=self._replay_window_seconds,
                )
                handle = self._registry.get_handle(fields["thread_key"])
                if handle is None or not handle.enabled:
                    self._registry.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="rejected_missing_or_disabled_handle",
                        summary=fields["summary"],
                        detail={"handle_enabled": False},
                    )
                    return web.json_response({"error": "invalid signature"}, status=401)
                if handle.producer_id != fields["producer_id"]:
                    self._registry.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="rejected_producer_scope",
                        summary=fields["summary"],
                        detail={"handle_enabled": handle.enabled, "policy": handle.policy, "target_platform": handle.platform},
                    )
                    return web.json_response({"error": "invalid signature"}, status=401)
                if handle.allowed_event_types and fields["event_type"] not in handle.allowed_event_types:
                    self._registry.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="rejected_event_type",
                        summary=fields["summary"],
                        detail={"handle_enabled": handle.enabled, "policy": handle.policy, "target_platform": handle.platform},
                    )
                    return web.json_response({"error": "invalid signature"}, status=401)
                if not verify_hmac_signature(raw, handle.secret, signature_header(request.headers)):
                    self._registry.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="rejected_signature",
                        summary=fields["summary"],
                        detail={"handle_enabled": handle.enabled, "policy": handle.policy, "target_platform": handle.platform},
                    )
                    return web.json_response({"error": "invalid signature"}, status=401)
                if not self._registry.mark_seen(
                    producer_id=fields["producer_id"],
                    event_id=fields["event_id"],
                    thread_key=fields["thread_key"],
                ):
                    self._registry.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="duplicate",
                        summary=fields["summary"],
                        detail={"policy": handle.policy, "target_platform": handle.platform},
                    )
                    return web.json_response({"status": "duplicate", "threadKey": handle.thread_key})

                try:
                    outcome, detail = await self.dispatch_event(handle, data, fields)
                except Exception as exc:
                    self._registry.forget_seen(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                    )
                    detail = dict(getattr(exc, "detail", {}) or {})
                    detail.update(
                        {
                            "policy": handle.policy,
                            "target_platform": handle.platform,
                            "exception_class": type(exc).__name__,
                            "exception_message": str(exc),
                        }
                    )
                    self._registry.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="dispatch_failed",
                        summary=fields["summary"],
                        detail=detail,
                    )
                    raise
                self._registry.log_event(
                    producer_id=fields["producer_id"],
                    event_id=fields["event_id"],
                    thread_key=fields["thread_key"],
                    event_type=fields["event_type"],
                    outcome=outcome,
                    summary=fields["summary"],
                    detail=detail,
                )
                status = 200 if outcome == "delivered" else 202
                return web.json_response({"status": outcome, "threadKey": handle.thread_key}, status=status)
            except EventValidationError as exc:
                return web.json_response({"error": str(exc)}, status=400)
            except Exception as exc:  # noqa: BLE001
                logger.error("async-thread event dispatch failed: %s", type(exc).__name__)
                return web.json_response({"error": "event dispatch failed"}, status=502)

        async def dispatch_event(
            self,
            handle: AsyncThreadHandle,
            data: Mapping[str, Any],
            fields: Mapping[str, str],
        ) -> tuple[str, dict[str, Any]]:
            source = SessionSource.from_dict(handle.source)
            detail: dict[str, Any] = {
                "policy": handle.policy,
                "target_platform": source.platform.value,
                "gateway_runner_exists": self.gateway_runner is not None,
                "session_key_present": bool(handle.session_key),
            }
            if handle.session_key:
                detail["session_key_hash"] = safe_session_key_hash(handle.session_key)
            runner = self.gateway_runner
            if runner is None:
                detail["target_adapter_exists"] = False
                raise DispatchEventError("gateway runner unavailable", detail=detail)
            target_adapter = runner.adapters.get(source.platform)
            detail["target_adapter_exists"] = target_adapter is not None
            if target_adapter is None:
                raise DispatchEventError(f"target platform not connected: {source.platform.value}", detail=detail)

            text = render_event_message(
                data,
                event_type=fields["event_type"],
                producer_id=fields["producer_id"],
                summary=fields.get("summary", ""),
            )
            if handle.policy == "direct":
                metadata = {"thread_id": source.thread_id} if source.thread_id else None
                try:
                    result = await target_adapter.send(source.chat_id, text, metadata=metadata)
                except Exception as exc:
                    detail["direct_send_success"] = False
                    raise DispatchEventError(str(exc), detail=detail) from exc
                detail["direct_send_success"] = bool(getattr(result, "success", False))
                if not detail["direct_send_success"]:
                    error = getattr(result, "error", None) or "direct delivery failed"
                    raise DispatchEventError(str(error), detail=detail)
                return "delivered", detail

            event = MessageEvent(
                text=text,
                message_type=MessageType.TEXT,
                source=source,
                raw_message={
                    "async_thread_event": True,
                    "eventId": fields["event_id"],
                    "eventType": fields["event_type"],
                    "producerId": fields["producer_id"],
                    "threadKey": handle.thread_key,
                },
                message_id=fields["event_id"],
                internal=True,
            )
            session_key = handle.session_key or build_session_key(
                source,
                group_sessions_per_user=target_adapter.config.extra.get("group_sessions_per_user", True),
                thread_sessions_per_user=target_adapter.config.extra.get("thread_sessions_per_user", False),
            )
            detail["session_key_present"] = bool(session_key)
            if session_key:
                detail["session_key_hash"] = safe_session_key_hash(session_key)
            active_sessions = getattr(target_adapter, "_active_sessions", {})
            active_session = session_key in active_sessions
            detail["active_session"] = active_session
            detail["queued"] = False
            detail["handle_message_called"] = False
            detail["handle_message_returned"] = False
            if active_session:
                merge_pending_message_event(
                    target_adapter._pending_messages,
                    session_key,
                    event,
                    merge_text=True,
                )
                detail["queued"] = True
                return "queued", detail
            detail["handle_message_called"] = True
            try:
                await target_adapter.handle_message(event)
            except Exception as exc:
                raise DispatchEventError(str(exc), detail=detail) from exc
            detail["handle_message_returned"] = True
            return "accepted", detail

    return _AsyncThreadsAdapter


AsyncThreadsAdapter = _build_adapter_base()
