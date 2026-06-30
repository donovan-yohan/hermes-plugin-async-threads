"""Gateway platform adapter for async-thread event ingress."""

from __future__ import annotations

import logging
import asyncio
import hashlib
from pathlib import Path
from typing import Any, Mapping

from .ingress_digest import resolve_ingress_digest_policy
from .lifecycle import is_terminal_event, terminal_action
from .privacy import redact_metadata_text, safe_event_id
from .registry import AsyncThreadHandle, AsyncThreadRegistry, AsyncThreadRegistryAsync, safe_session_key_hash
from .rendering import render_event_message, tail_mode_from_event
from .routing import send_metadata_for_source
from .secrets import remove_secret_artifact, secret_root_from_config
from .source_runner import SourceBindingRunConfig, run_source_binding_once
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
        int(extra.get("source_binding_runner_interval_seconds", 30))
        int(extra.get("source_binding_runner_limit", 100))
    except (TypeError, ValueError):
        return False
    return True


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _event_url_from_config(config: Any, *, host: str, port: int) -> str:
    extra = getattr(config, "extra", {}) or {}
    configured = extra.get("source_binding_event_url") or extra.get("event_url")
    if configured:
        return str(configured)
    public_base = str(extra.get("public_base_url") or "").rstrip("/")
    if public_base:
        return f"{public_base}/async-threads/v1/events"
    return f"http://{host}:{port}/async-threads/v1/events"


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


def _producer_status(outcome: str) -> str:
    return {
        "agent_started": "accepted",
        "queued_active_session": "queued",
        "direct_delivered": "delivered",
    }.get(outcome, outcome)


def _ack_notice_text(
    *,
    ack_mode: str,
    producer_id: str,
    event_type: str,
    event_id: str,
    thread_key: str,
    outcome: str,
) -> str:
    producer = _safe_ack_token(redact_metadata_text(producer_id))
    event = _safe_ack_token(redact_metadata_text(event_type))
    if ack_mode == "debug":
        return (
            "async-thread event received\n"
            f"producer: `{producer}`\n"
            f"eventType: `{event}`\n"
            f"eventId: `{_short_ack_id(event_id)}`\n"
            f"threadKey: `{_safe_ack_token(thread_key)}`\n"
            f"initialOutcome: `{_safe_ack_token(outcome)}`"
        )
    return f"received {event} from {producer}; starting continuation…"


def _safe_ack_token(value: str) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isalnum() or ch in {"-", "_", ".", ":"})
    return text[:100] or "-"


def _short_ack_id(event_id: str) -> str:
    text = safe_event_id(event_id)
    return f"…{text[-8:]}" if len(text) > 8 else text


def _compact_event_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    keys = [
        "profile",
        "lane",
        "issue",
        "pr",
        "status",
        "state",
        "verdict",
        "head_sha",
        "pr_url",
        "comment_url",
        "verification",
        "log_path",
        "pid",
        "delegation_id",
    ]
    compact: dict[str, Any] = {}
    for key in keys:
        if key in payload:
            compact[key] = payload[key]
    return compact


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
            self._registry_async = AsyncThreadRegistryAsync(self._registry)
            self._runner = None
            self._site = None
            self._host = str((config.extra or {}).get("host", "127.0.0.1"))
            self._port = int((config.extra or {}).get("port", 8765))
            self._max_body_bytes = int((config.extra or {}).get("max_body_bytes", 64 * 1024))
            configured_ingress_digest = (config.extra or {}).get("ingress_digest")
            self._ingress_digest_config = configured_ingress_digest if isinstance(configured_ingress_digest, Mapping) else {}
            self._replay_window_seconds = int(
                (config.extra or {}).get("replay_window_seconds", DEFAULT_REPLAY_WINDOW_SECONDS)
            )
            self._coalesced_events: dict[str, list[dict[str, Any]]] = {}
            self._coalesced_inflight: dict[str, list[dict[str, Any]]] = {}
            self._coalesce_tasks: dict[str, asyncio.Task] = {}
            self._source_binding_runner_enabled = _truthy((config.extra or {}).get("source_binding_runner_enabled", False))
            self._source_binding_runner_interval_seconds = max(
                1,
                int((config.extra or {}).get("source_binding_runner_interval_seconds", 30)),
            )
            self._source_binding_runner_limit = max(1, min(int((config.extra or {}).get("source_binding_runner_limit", 100)), 500))
            self._source_binding_runner_task: asyncio.Task | None = None

        async def connect(self, *args: Any, **kwargs: Any) -> bool:
            from aiohttp import web

            app = web.Application(client_max_size=self._max_body_bytes)
            app.router.add_get("/async-threads/v1/health", self._handle_health)
            app.router.add_post("/async-threads/v1/events", self._handle_event)
            self._runner = web.AppRunner(app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()
            self._running = True
            if self._source_binding_runner_enabled:
                self._source_binding_runner_task = asyncio.create_task(self._source_binding_runner_loop())
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
            if self._source_binding_runner_task is not None:
                self._source_binding_runner_task.cancel()
                try:
                    await self._source_binding_runner_task
                except asyncio.CancelledError:
                    pass
                self._source_binding_runner_task = None
            for task in list(self._coalesce_tasks.values()):
                task.cancel()
            for pending in list(self._coalesced_events.values()) + list(self._coalesced_inflight.values()):
                for item in pending:
                    await self._registry_async.forget_seen(
                        producer_id=item["fields"]["producer_id"],
                        event_id=item["fields"]["event_id"],
                    )
            self._coalesce_tasks.clear()
            self._coalesced_events.clear()
            self._coalesced_inflight.clear()

        async def send(self, chat_id: str, content: str, reply_to=None, metadata=None):
            return SendResult(success=False, error="async_threads is an ingress-only platform")

        async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
            return {"id": chat_id, "type": "async_threads", "name": "Async Threads"}

        async def _source_binding_runner_loop(self) -> None:
            while self._running:
                await self._run_source_bindings_once()
                await asyncio.sleep(self._source_binding_runner_interval_seconds)

        async def _run_source_bindings_once(self) -> list[dict[str, Any]]:
            bindings = [
                binding
                for binding in await self._registry_async.list_source_bindings(source="kanban", include_retired=False, limit=100)
                if binding.status == "active"
            ]
            event_url = _event_url_from_config(self.config, host=self._host, port=self._port)
            results: list[dict[str, Any]] = []
            for binding in bindings:
                cfg = SourceBindingRunConfig(
                    event_url=event_url,
                    limit=self._source_binding_runner_limit,
                )
                try:
                    result = await asyncio.to_thread(
                        run_source_binding_once,
                        registry=self._registry,
                        binding=binding,
                        config=cfg,
                    )
                except Exception as exc:  # noqa: BLE001 - keep runner alive and diagnosable
                    logger.error("async-thread source binding runner failed: %s", type(exc).__name__)
                    result = {"ok": False, "health": "runner_error", "bindingId": binding.binding_id, "error": type(exc).__name__}
                results.append(result)
            return results

        async def _handle_health(self, request):
            from aiohttp import web

            return web.json_response(
                {
                    "ok": True,
                    "platform": "async_threads",
                    "sourceBindingRunner": {
                        "enabled": self._source_binding_runner_enabled,
                        "intervalSeconds": self._source_binding_runner_interval_seconds,
                    },
                }
            )

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
                handle = await self._registry_async.get_handle(fields["thread_key"])
                if handle is None:
                    return web.json_response({"error": "invalid signature"}, status=401)
                if not verify_hmac_signature(raw, handle.secret, signature_header(request.headers)):
                    return web.json_response({"error": "invalid signature"}, status=401)
                if not handle.enabled:
                    if await self._registry_async.has_seen(producer_id=fields["producer_id"], event_id=fields["event_id"]):
                        await self._registry_async.log_event(
                            producer_id=fields["producer_id"],
                            event_id=fields["event_id"],
                            thread_key=fields["thread_key"],
                            event_type=fields["event_type"],
                            outcome="duplicate",
                            summary=fields["summary"],
                            detail={"handle_enabled": False, "policy": handle.policy, "target_platform": handle.platform},
                        )
                        return web.json_response({"status": "duplicate", "threadKey": handle.thread_key})
                    await self._registry_async.log_event(
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
                    await self._registry_async.log_event(
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
                    await self._registry_async.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="rejected_event_type",
                        summary=fields["summary"],
                        detail={"handle_enabled": handle.enabled, "policy": handle.policy, "target_platform": handle.platform},
                    )
                    return web.json_response({"error": "invalid signature"}, status=401)
                if not await self._registry_async.mark_seen(
                    producer_id=fields["producer_id"],
                    event_id=fields["event_id"],
                    thread_key=fields["thread_key"],
                ):
                    if self._pending_coalesced_contains(
                        thread_key=fields["thread_key"],
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                    ):
                        return web.json_response({"status": "queued", "threadKey": handle.thread_key}, status=202)
                    await self._registry_async.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="duplicate",
                        summary=fields["summary"],
                        detail={"policy": handle.policy, "target_platform": handle.platform},
                    )
                    return web.json_response({"status": "duplicate", "threadKey": handle.thread_key})

                source_binding, ingress_policy, payload_record = await self._resolve_ingress_payload(handle, data, fields)

                if self._has_pending_coalesced(handle) and self._is_priority_event(data, fields):
                    await self._flush_coalesced(handle.thread_key, reason="priority_flush")
                elif self._should_coalesce(handle, data, fields):
                    detail = await self._queue_coalesced_event(handle, data, fields, source_binding=source_binding)
                    await self._registry_async.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="coalesced_pending",
                        summary=fields["summary"],
                        detail=detail,
                    )
                    return web.json_response({"status": "queued", "threadKey": handle.thread_key}, status=202)

                try:
                    outcome, detail = await self.dispatch_event(handle, data, fields, ingress_policy=ingress_policy, payload_record=payload_record)
                except Exception as exc:
                    await self._registry_async.forget_seen(
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
                    await self._registry_async.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="dispatch_failed",
                        summary=fields["summary"],
                        detail=detail,
                    )
                    raise
                await self._record_workflow_state(handle, data, fields, detail)
                await self._apply_lifecycle_after_dispatch(handle, data, fields, detail)
                await self._registry_async.log_event(
                    producer_id=fields["producer_id"],
                    event_id=fields["event_id"],
                    thread_key=fields["thread_key"],
                    event_type=fields["event_type"],
                    outcome=outcome,
                    summary=fields["summary"],
                    detail=detail,
                )
                status = 200 if outcome == "direct_delivered" else 202
                return web.json_response({"status": _producer_status(outcome), "threadKey": handle.thread_key}, status=status)
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
            *,
            ingress_policy: Any | None = None,
            payload_record: Any | None = None,
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
                ingress_policy=ingress_policy,
                payload_record=payload_record,
            )
            if ingress_policy is not None:
                detail["ingress_digest_mode"] = getattr(ingress_policy, "mode", "off")
                detail["ingress_digest_source"] = getattr(ingress_policy, "source", "off")
            if payload_record is not None:
                detail["ingress_digest_status"] = getattr(payload_record, "status", "stored")
                detail["ingress_digest_storage_mode"] = getattr(payload_record, "storage_mode", "")
                detail["ingress_digest_pointer_id"] = getattr(payload_record, "pointer_id", "")
            if handle.policy == "direct":
                metadata = send_metadata_for_source(source)
                try:
                    result = await target_adapter.send(source.chat_id, text, metadata=metadata)
                except Exception as exc:
                    detail["direct_send_success"] = False
                    raise DispatchEventError(str(exc), detail=detail) from exc
                detail["direct_send_success"] = bool(getattr(result, "success", False))
                if not detail["direct_send_success"]:
                    error = getattr(result, "error", None) or "direct delivery failed"
                    raise DispatchEventError(str(error), detail=detail)
                return "direct_delivered", detail

            safe_message_id = safe_event_id(fields["event_id"])
            continuation_summary = handle.continuation_policy.public_summary(core_enforced=False)
            detail["continuation_policy"] = continuation_summary
            detail["continuation_core_enforced"] = False
            if handle.continuation_policy.fail_closed_without_core_bounds:
                detail["continuation_fail_closed"] = True
                detail["continuation_limit_reason"] = "core_bounds_unavailable"
                raise DispatchEventError("bounded continuation unavailable", detail=detail)
            event = MessageEvent(
                text=text,
                message_type=MessageType.TEXT,
                source=source,
                raw_message={
                    "async_thread_event": True,
                    "eventId": safe_message_id,
                    "eventType": redact_metadata_text(fields["event_type"]),
                    "producerId": redact_metadata_text(fields["producer_id"]),
                    "threadKey": handle.thread_key,
                    "continuationPolicy": continuation_summary,
                    "continuationPolicyCoreEnforced": False,
                },
                message_id=safe_message_id,
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
            initial_outcome = "queued_active_session" if active_session else "agent_started"
            await self._send_ack_notice(
                target_adapter=target_adapter,
                source=source,
                handle=handle,
                fields=fields,
                outcome=initial_outcome,
                detail=detail,
            )
            if active_session:
                merge_pending_message_event(
                    target_adapter._pending_messages,
                    session_key,
                    event,
                    merge_text=True,
                )
                detail["queued"] = True
                return "queued_active_session", detail
            detail["handle_message_called"] = True
            try:
                await target_adapter.handle_message(event)
            except Exception as exc:
                raise DispatchEventError(str(exc), detail=detail) from exc
            detail["handle_message_returned"] = True
            return "agent_started", detail

        async def _resolve_ingress_payload(
            self,
            handle: AsyncThreadHandle,
            data: Mapping[str, Any],
            fields: Mapping[str, str],
            *,
            source_binding: Any | None = None,
        ) -> tuple[Any | None, Any, Any | None]:
            if source_binding is None:
                source_binding = await self._registry_async.find_source_binding_for_event(
                    thread_key=handle.thread_key,
                    producer_id=fields["producer_id"],
                    event_id=fields["event_id"],
                )
            ingress_policy = resolve_ingress_digest_policy(
                global_policy=self._ingress_digest_config,
                listener_policy=handle.ingress_digest_policy,
                source_binding_policy=source_binding.ingress_digest_policy if source_binding is not None else None,
            )
            payload_record = await self._registry_async.store_event_payload(
                handle=handle,
                data=data,
                fields=fields,
                policy=ingress_policy,
                source_binding_id=source_binding.binding_id if source_binding is not None else "",
            )
            return source_binding, ingress_policy, payload_record

        async def _record_workflow_state(
            self,
            handle: AsyncThreadHandle,
            data: Mapping[str, Any],
            fields: Mapping[str, str],
            detail: dict[str, Any],
        ) -> None:
            try:
                state = await self._registry_async.update_workflow_state_from_event(handle=handle, data=data, fields=fields)
            except Exception as exc:  # noqa: BLE001 - event was already dispatched; diagnostics must not poison retry semantics
                logger.error("async-thread workflow state update failed: %s", type(exc).__name__)
                detail["error"] = "workflow_state_update_failed"
                return
            if state is None:
                return
            detail["workflow_id"] = state.workflow_id
            detail["workflow_stage"] = state.stage

        async def _apply_lifecycle_after_dispatch(
            self,
            handle: AsyncThreadHandle,
            data: Mapping[str, Any],
            fields: Mapping[str, str],
            detail: dict[str, Any],
        ) -> None:
            policy = handle.lifecycle_policy
            if not is_terminal_event(data, fields, policy):
                return
            action = terminal_action(policy)
            detail["terminal_event"] = True
            detail["terminal_action"] = action
            detail["lifecycle_policy"] = policy.public_summary()
            if action != "auto_retired":
                detail["terminal_retired"] = False
                return
            retired = await self._registry_async.set_enabled(handle.thread_key, False)
            remove_secret_artifact(handle.thread_key, root=secret_root_from_config(self.config))
            detail["terminal_retired"] = bool(retired)

        def _should_coalesce(
            self,
            handle: AsyncThreadHandle,
            data: Mapping[str, Any],
            fields: Mapping[str, str],
        ) -> bool:
            return (
                handle.policy == "agent_queue"
                and handle.debounce_seconds > 0
                and self._is_routine_event(fields)
                and not self._is_priority_event(data, fields)
            )

        def _has_pending_coalesced(self, handle: AsyncThreadHandle) -> bool:
            return bool(self._coalesced_events.get(handle.thread_key))

        def _pending_coalesced_contains(self, *, thread_key: str, producer_id: str, event_id: str) -> bool:
            for coalesced in (self._coalesced_events, self._coalesced_inflight):
                for item in coalesced.get(thread_key, []):
                    fields = item.get("fields") or {}
                    if fields.get("producer_id") == producer_id and fields.get("event_id") == event_id:
                        return True
            return False

        async def _queue_coalesced_event(
            self,
            handle: AsyncThreadHandle,
            data: Mapping[str, Any],
            fields: Mapping[str, str],
            *,
            source_binding: Any | None = None,
        ) -> dict[str, Any]:
            bucket = self._coalesced_events.setdefault(handle.thread_key, [])
            bucket.append({"handle": handle, "data": dict(data), "fields": dict(fields), "source_binding": source_binding})
            task = self._coalesce_tasks.get(handle.thread_key)
            if task is None or task.done():
                self._coalesce_tasks[handle.thread_key] = asyncio.create_task(
                    self._flush_coalesced_after(handle.thread_key, handle.debounce_seconds)
                )
            detail = {
                "coalesced_count": len(bucket),
                "coalesced_reason": "debounce_window",
                "debounce_seconds": handle.debounce_seconds,
            }
            await self._record_workflow_state(handle, data, fields, detail)
            return detail

        async def _flush_coalesced_after(self, thread_key: str, delay_seconds: int) -> None:
            try:
                await asyncio.sleep(max(0, delay_seconds))
                await self._flush_coalesced(thread_key, reason="debounce_elapsed")
            except asyncio.CancelledError:
                return
            except Exception as exc:  # noqa: BLE001
                logger.error("async-thread coalesced flush failed: %s", type(exc).__name__)

        async def _flush_coalesced(self, thread_key: str, *, reason: str) -> None:
            if thread_key in self._coalesced_inflight:
                self._reschedule_coalesced_if_needed(thread_key)
                return
            pending = self._pop_coalesced(thread_key)
            if not pending:
                return
            self._coalesced_inflight[thread_key] = pending
            handle = pending[-1]["handle"]
            data, fields = self._coalesced_digest(pending, reason=reason)
            _source_binding, ingress_policy, payload_record = await self._resolve_ingress_payload(
                handle,
                data,
                fields,
                source_binding=pending[-1].get("source_binding"),
            )
            try:
                try:
                    outcome, detail = await self.dispatch_event(handle, data, fields, ingress_policy=ingress_policy, payload_record=payload_record)
                except Exception as exc:  # noqa: BLE001
                    detail = dict(getattr(exc, "detail", {}) or {})
                    detail.update(
                        {
                            "coalesced_count": len(pending),
                            "coalesced_reason": reason,
                            "exception_class": type(exc).__name__,
                            "exception_message": str(exc),
                        }
                    )
                    await self._registry_async.log_event(
                        producer_id=fields["producer_id"],
                        event_id=fields["event_id"],
                        thread_key=fields["thread_key"],
                        event_type=fields["event_type"],
                        outcome="dispatch_failed",
                        summary=fields["summary"],
                        detail=detail,
                    )
                    if self._requeue_failed_coalesced(thread_key, pending):
                        return
                    for item in pending:
                        await self._registry_async.forget_seen(
                            producer_id=item["fields"]["producer_id"],
                            event_id=item["fields"]["event_id"],
                        )
                    return
                detail.update({"coalesced_count": len(pending), "coalesced_reason": reason})
                await self._registry_async.log_event(
                    producer_id=fields["producer_id"],
                    event_id=fields["event_id"],
                    thread_key=fields["thread_key"],
                    event_type=fields["event_type"],
                    outcome=outcome,
                    summary=fields["summary"],
                    detail=detail,
                )
            finally:
                self._coalesced_inflight.pop(thread_key, None)

        def _pop_coalesced(self, thread_key: str) -> list[dict[str, Any]]:
            task = self._coalesce_tasks.pop(thread_key, None)
            current = asyncio.current_task()
            if task is not None and task is not current:
                task.cancel()
            return self._coalesced_events.pop(thread_key, [])

        def _requeue_failed_coalesced(self, thread_key: str, pending: list[dict[str, Any]]) -> bool:
            attempts = max(int(item.get("attempts", 0)) for item in pending) + 1
            if attempts > 3 or not self._running:
                return False
            for item in pending:
                item["attempts"] = attempts
            queued_during_flush = self._coalesced_events.pop(thread_key, [])
            self._coalesced_events[thread_key] = pending + queued_during_flush
            self._reschedule_coalesced_if_needed(thread_key)
            return True

        def _reschedule_coalesced_if_needed(self, thread_key: str) -> None:
            pending = self._coalesced_events.get(thread_key)
            current = asyncio.current_task()
            task = self._coalesce_tasks.get(thread_key)
            if not pending:
                if task is current:
                    self._coalesce_tasks.pop(thread_key, None)
                return
            if task is not None and task is not current and not task.done():
                return
            handle = pending[-1]["handle"]
            delay = max(1, min(handle.debounce_seconds or 1, 30))
            self._coalesce_tasks[thread_key] = asyncio.create_task(self._flush_coalesced_after(thread_key, delay))

        def _coalesced_digest(self, pending: list[dict[str, Any]], *, reason: str) -> tuple[dict[str, Any], dict[str, str]]:
            last_fields = pending[-1]["fields"]
            digest_id = hashlib.sha256(
                "|".join(item["fields"]["event_id"] for item in pending).encode("utf-8")
            ).hexdigest()[:16]
            events = []
            for item in pending:
                fields = item["fields"]
                payload = item["data"].get("payload", {}) if isinstance(item["data"], Mapping) else {}
                events.append(
                    {
                        "event_id": safe_event_id(fields["event_id"]),
                        "event_type": redact_metadata_text(fields["event_type"]),
                        "summary": fields.get("summary", ""),
                        "payload": _compact_event_payload(payload),
                    }
                )
            data = {
                "tailMode": "compact",
                "subject": {"thread_key": last_fields["thread_key"], "coalesced_count": len(pending)},
                "payload": {"reason": reason, "events": events},
            }
            fields = {
                "event_id": f"coalesced_{digest_id}",
                "event_type": "async_threads.coalesced",
                "producer_id": last_fields["producer_id"],
                "thread_key": last_fields["thread_key"],
                "summary": f"{len(pending)} async-thread routine events coalesced",
            }
            return data, fields

        def _is_routine_event(self, fields: Mapping[str, str]) -> bool:
            event_type = str(fields.get("event_type", "")).lower()
            return any(marker in event_type for marker in ("started", "progress"))

        def _is_priority_event(self, data: Mapping[str, Any], fields: Mapping[str, str]) -> bool:
            event_type = str(fields.get("event_type", "")).lower().replace("-", "_")
            if any(
                marker in event_type
                for marker in ("finished", "completed", "succeeded", "done", "failed", "failure", "error", "blocked", "needs_attention", "approval", "timeout", "watchdog")
            ):
                return True
            if tail_mode_from_event(data) == "debug":
                return True
            payload = data.get("payload", {}) if isinstance(data, Mapping) else {}
            if isinstance(payload, Mapping):
                for key in ("state", "status", "verdict"):
                    value = str(payload.get(key, "")).lower().replace("-", "_")
                    if value in {"finished", "completed", "succeeded", "done", "failed", "failure", "errored", "error", "blocked", "needs_attention"}:
                        return True
            return False

        async def _send_ack_notice(
            self,
            *,
            target_adapter: Any,
            source: Any,
            handle: AsyncThreadHandle,
            fields: Mapping[str, str],
            outcome: str,
            detail: dict[str, Any],
        ) -> None:
            ack_mode = handle.ack_mode if handle.ack_mode in {"brief", "debug"} else "none"
            detail["ack_mode"] = ack_mode
            if ack_mode == "none":
                detail["ack_sent"] = False
                return
            content = _ack_notice_text(
                ack_mode=ack_mode,
                producer_id=fields["producer_id"],
                event_type=fields["event_type"],
                event_id=fields["event_id"],
                thread_key=handle.thread_key,
                outcome=outcome,
            )
            metadata = send_metadata_for_source(source)
            detail["ack_sent"] = True
            try:
                result = await target_adapter.send(source.chat_id, content, metadata=metadata)
            except Exception as exc:  # noqa: BLE001 - ack must not block continuation
                detail["ack_success"] = False
                detail["ack_error"] = str(exc)
                return
            detail["ack_success"] = bool(getattr(result, "success", False))
            if not detail["ack_success"]:
                detail["ack_error"] = str(getattr(result, "error", None) or "ack send failed")

    return _AsyncThreadsAdapter


AsyncThreadsAdapter = _build_adapter_base()
