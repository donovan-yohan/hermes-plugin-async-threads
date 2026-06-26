"""Model-facing tools for async-thread listener lifecycle management."""

from __future__ import annotations

import json
import re
from types import SimpleNamespace
from typing import Any, Iterable, Mapping

from .continuation import ContinuationPolicy
from .adapter import registry_from_config
from .handoffs import build_producer_handoff, handoff_root_from_config
from .listeners import ListenValidationError, create_listener
from .origin import OriginResolution, resolve_current_origin
from .privacy import redact_metadata_text, safe_event_id
from .registry import AsyncThreadHandle, AsyncThreadRegistry, sanitize_event_detail
from .secrets import describe_secret_artifact, remove_secret_artifact, secret_root_from_config

TOOLSET = "plugin_async_threads"

_CREATE_SCHEMA = {
    "name": "ath_create_listener",
    "description": "Create or reuse an async-thread listener bound to the current gateway conversation. Use for event-driven wakeups back to this thread.",
    "parameters": {
        "type": "object",
        "properties": {
            "purpose": {"type": "string", "description": "Human-readable reason for the listener."},
            "producer_hint": {"type": "string", "description": "Short producer/source hint, e.g. github-pr-review or demo-ci."},
            "producer_id": {"type": "string", "description": "Optional exact producer id. Prefer producer_hint unless the producer id is already known."},
            "event_kinds": {"type": "array", "items": {"type": "string"}, "description": "High-level events like started, progress, finished, failed."},
            "event_types": {"type": "array", "items": {"type": "string"}, "description": "Exact allowed event types. Overrides event_kinds when provided."},
            "delivery": {"type": "string", "enum": ["agent_queue", "direct"], "description": "Delivery policy. Default agent_queue."},
            "ack": {"type": "string", "enum": ["none", "brief", "debug"], "description": "Producer acknowledgement mode. Default brief."},
            "target": {"type": "string", "enum": ["current_conversation"], "description": "Only current_conversation is supported."},
            "reuse": {"type": "boolean", "description": "Reuse an equivalent active listener when one exists. Default true."},
            "label": {"type": "string", "description": "Optional display label. Defaults to purpose."},
            "max_turns": {"type": "integer", "description": "Agent-queue continuation intent. Default 1; metadata only until Hermes exposes plugin-enforced per-event caps."},
            "max_tool_calls": {"type": "integer", "description": "Agent-queue tool-call cap intent. Default 0."},
            "timeout_seconds": {"type": "integer", "description": "Agent-queue timeout intent. Default 120 seconds."},
            "continuation_toolsets": {"type": "array", "items": {"type": "string"}, "description": "Optional toolsets intended for continuation policy metadata."},
            "fail_closed_without_core_bounds": {"type": "boolean", "description": "If true, reject agent_queue dispatch while Hermes core lacks hard per-event bounds."},
            "terminal_event_types": {"type": "array", "items": {"type": "string"}, "description": "Event type patterns that mean this listener's workflow is terminal, e.g. *.goal.finished."},
            "auto_retire_on_terminal": {"type": "boolean", "description": "If true, retire this listener after a configured terminal event. Use only for single-goal listeners."},
            "shared_listener": {"type": "boolean", "description": "If true, never auto-retire on terminal events because multiple workflows may share the listener."},
        },
        "required": ["purpose"],
    },
}

_LIST_SCHEMA = {
    "name": "ath_list_listeners",
    "description": "List async-thread listeners scoped to the current user/conversation owner.",
    "parameters": {
        "type": "object",
        "properties": {
            "include_disabled": {"type": "boolean", "description": "Include retired/revoked listeners. Default false."},
            "current_conversation_only": {"type": "boolean", "description": "Only listeners for the current conversation. Default false."},
        },
    },
}

_INSPECT_SCHEMA = {
    "name": "ath_get_listener",
    "description": "Inspect one async-thread listener scoped to the current owner. Secrets are never returned.",
    "parameters": {
        "type": "object",
        "properties": {"thread_key": {"type": "string", "description": "ATH listener thread key."}},
        "required": ["thread_key"],
    },
}

_RETIRE_SCHEMA = {
    "name": "ath_retire_listener",
    "description": "Retire/revoke an async-thread listener scoped to the current owner.",
    "parameters": {
        "type": "object",
        "properties": {"thread_key": {"type": "string", "description": "ATH listener thread key."}},
        "required": ["thread_key"],
    },
}

_ROTATE_SCHEMA = {
    "name": "ath_rotate_listener_secret",
    "description": "Rotate a listener signing secret and refresh its local secret-file reference. The raw secret is never returned.",
    "parameters": {
        "type": "object",
        "properties": {"thread_key": {"type": "string", "description": "ATH listener thread key."}},
        "required": ["thread_key"],
    },
}

_HANDOFF_SCHEMA = {
    "name": "ath_generate_producer_handoff",
    "description": "Generate a safe producer handoff for an existing listener: contract, local emitter files, GitHub Actions recipe, Dynamic Workflows loop recipe, or debug curl-like emitter. Raw secrets are not returned unless explicitly requested for debug output.",
    "parameters": {
        "type": "object",
        "properties": {
            "thread_key": {"type": "string", "description": "ATH listener thread key."},
            "mode": {
                "type": "string",
                "enum": ["generic_contract", "local_script", "github_actions", "debug_curl", "dynamic_workflows"],
                "description": "Handoff shape. Default generic_contract.",
            },
            "event_type": {"type": "string", "description": "Optional allowed event type to use in examples. Defaults to the first listener event type."},
            "create_files": {"type": "boolean", "description": "For local_script/github_actions modes, write helper files. Default true."},
            "include_sensitive_secret": {
                "type": "boolean",
                "description": "Debug-only escape hatch that returns a literal secret. Default false; do not use in normal chat.",
            },
        },
        "required": ["thread_key"],
    },
}

_TRACE_SCHEMA = {
    "name": "ath_trace_event",
    "description": "Inspect recent async-thread delivery/de-dupe diagnostics scoped to the current owner.",
    "parameters": {
        "type": "object",
        "properties": {
            "event_id": {"type": "string", "description": "Optional exact event id to inspect."},
            "thread_key": {"type": "string", "description": "Optional listener thread key to list recent events for."},
            "limit": {"type": "integer", "description": "Recent event limit, 1-20. Default 10."},
        },
    },
}

_CREATE_SOURCE_BINDING_SCHEMA = {
    "name": "ath_create_source_binding",
    "description": "Create a producer-agnostic source binding from an upstream source (for example kanban) to an existing ATH listener. Does not create or retarget listeners.",
    "parameters": {
        "type": "object",
        "properties": {
            "source": {"type": "string", "description": "Producer source type, e.g. kanban."},
            "source_ref": {"type": "object", "description": "Source-specific reference such as {board: default}."},
            "board_ref": {"type": "string", "description": "Convenience board ref for source=kanban."},
            "listener_thread_key": {"type": "string", "description": "Existing ATH listener thread key to target."},
            "producer_id": {"type": "string", "description": "Optional producer id. Defaults to the listener producer id."},
            "event_filter": {"type": "object", "description": "Source event filter, e.g. {eventTypes: [...]}."},
            "transform": {"type": "object", "description": "Trusted transform config name/options, not code."},
            "cursor": {"type": "object", "description": "Producer cursor/checkpoint shape."},
            "coalesce": {"type": "object", "description": "Coalescing/debounce policy."},
            "delivery_policy": {"type": "string", "enum": ["agent_queue", "direct"], "description": "Delivery policy metadata for future runners."},
        },
        "required": ["source", "listener_thread_key"],
    },
}

_LIST_SOURCE_BINDINGS_SCHEMA = {
    "name": "ath_list_source_bindings",
    "description": "List source bindings scoped to the current owner. Secret-shaped material is redacted.",
    "parameters": {
        "type": "object",
        "properties": {
            "source": {"type": "string", "description": "Optional source filter, e.g. kanban."},
            "include_retired": {"type": "boolean", "description": "Include retired bindings. Default false."},
            "limit": {"type": "integer", "description": "Limit 1-100. Default 50."},
        },
    },
}

_GET_SOURCE_BINDING_SCHEMA = {
    "name": "ath_get_source_binding",
    "description": "Inspect one owner-scoped source binding and listener compatibility. Secrets are never returned.",
    "parameters": {
        "type": "object",
        "properties": {"binding_id": {"type": "string", "description": "Source binding id."}},
        "required": ["binding_id"],
    },
}

_SOURCE_BINDING_STATUS_SCHEMA = {
    "name": "ath_set_source_binding_status",
    "description": "Pause, resume, or retire an owner-scoped source binding without changing the listener lifecycle.",
    "parameters": {
        "type": "object",
        "properties": {
            "binding_id": {"type": "string", "description": "Source binding id."},
            "status": {"type": "string", "enum": ["active", "paused", "retired"], "description": "New binding status."},
        },
        "required": ["binding_id", "status"],
    },
}


def register_tools(ctx: Any) -> None:
    """Register model-facing ATH tools through PluginContext."""

    ctx.register_tool(
        name="ath_create_listener",
        toolset=TOOLSET,
        schema=_CREATE_SCHEMA,
        handler=ath_create_listener_tool,
        description=_CREATE_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_list_listeners",
        toolset=TOOLSET,
        schema=_LIST_SCHEMA,
        handler=ath_list_listeners_tool,
        description=_LIST_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_get_listener",
        toolset=TOOLSET,
        schema=_INSPECT_SCHEMA,
        handler=ath_get_listener_tool,
        description=_INSPECT_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_retire_listener",
        toolset=TOOLSET,
        schema=_RETIRE_SCHEMA,
        handler=ath_retire_listener_tool,
        description=_RETIRE_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_rotate_listener_secret",
        toolset=TOOLSET,
        schema=_ROTATE_SCHEMA,
        handler=ath_rotate_listener_secret_tool,
        description=_ROTATE_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_generate_producer_handoff",
        toolset=TOOLSET,
        schema=_HANDOFF_SCHEMA,
        handler=ath_generate_producer_handoff_tool,
        description=_HANDOFF_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_trace_event",
        toolset=TOOLSET,
        schema=_TRACE_SCHEMA,
        handler=ath_trace_event_tool,
        description=_TRACE_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_create_source_binding",
        toolset=TOOLSET,
        schema=_CREATE_SOURCE_BINDING_SCHEMA,
        handler=ath_create_source_binding_tool,
        description=_CREATE_SOURCE_BINDING_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_list_source_bindings",
        toolset=TOOLSET,
        schema=_LIST_SOURCE_BINDINGS_SCHEMA,
        handler=ath_list_source_bindings_tool,
        description=_LIST_SOURCE_BINDINGS_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_get_source_binding",
        toolset=TOOLSET,
        schema=_GET_SOURCE_BINDING_SCHEMA,
        handler=ath_get_source_binding_tool,
        description=_GET_SOURCE_BINDING_SCHEMA["description"],
        emoji="🧵",
    )
    ctx.register_tool(
        name="ath_set_source_binding_status",
        toolset=TOOLSET,
        schema=_SOURCE_BINDING_STATUS_SCHEMA,
        handler=ath_set_source_binding_status_tool,
        description=_SOURCE_BINDING_STATUS_SCHEMA["description"],
        emoji="🧵",
    )


def ath_create_listener_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    if not origin.owner_user_id:
        return _json(_error("owner_unavailable", "current gateway user is unavailable"))

    try:
        spec = _listener_spec(args)
    except ValueError as exc:
        return _json(_error("invalid_request", str(exc)))

    if bool(args.get("reuse", True)):
        existing = _find_equivalent_listener(
            registry,
            owner_user_id=origin.owner_user_id,
            origin=origin,
            producer_id=spec["producer_id"],
            event_types=tuple(spec["event_types"]),
            policy=spec["policy"],
            ack_mode=spec["ack_mode"],
            continuation_policy=spec["continuation_policy"],
            lifecycle_policy=spec["lifecycle_policy"],
        )
        if existing is not None:
            return _json(
                {
                    "ok": True,
                    "action": "reused",
                    "listener": _handle_summary(existing, event_url=_event_url(config), secret_root=secret_root_from_config(config), ensure_secret_ref=True),
                    "secret": _secret_reference(existing, event_url=_event_url(config), config=config),
                }
            )

    try:
        result = create_listener(
            registry=registry,
            source=origin.source,
            producer_id=spec["producer_id"],
            allowed_event_types=spec["event_types"],
            label=spec["label"],
            policy=spec["policy"],
            ack_mode=spec["ack_mode"],
            session_key=origin.session_key,
            session_id=origin.session_id,
            owner_user_id=origin.owner_user_id,
            event_url=_event_url(config),
            continuation_policy=spec["continuation_policy"],
            lifecycle_policy=spec["lifecycle_policy"],
        )
    except ListenValidationError as exc:
        return _json(_error("invalid_request", str(exc)))

    return _json(
        {
            "ok": True,
            "action": "created",
            "listener": _handle_summary(result.handle, event_url=result.event_url, secret_root=secret_root_from_config(config), ensure_secret_ref=True),
            "secret": _secret_reference(result.handle, event_url=result.event_url, config=config),
        }
    )


def ath_list_listeners_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    if not origin.owner_user_id:
        return _json(_error("owner_unavailable", "current gateway user is unavailable"))
    include_disabled = bool(args.get("include_disabled", False))
    current_only = bool(args.get("current_conversation_only", False))
    listeners = registry.list_handles(owner_user_id=origin.owner_user_id, include_disabled=include_disabled)
    if current_only:
        listeners = [handle for handle in listeners if _same_origin(handle, origin)]
    terminal_by_thread = registry.latest_terminal_events(thread_keys=[handle.thread_key for handle in listeners])
    return _json(
        {
            "ok": True,
            "listeners": [
                _handle_summary_with_terminal(
                    registry,
                    handle,
                    event_url=_event_url(config),
                    secret_root=secret_root_from_config(config),
                    ensure_secret_ref=False,
                    terminal_event=terminal_by_thread.get(handle.thread_key),
                )
                for handle in listeners
            ],
            "count": len(listeners),
        }
    )


def ath_get_listener_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    handle = _owned_handle(registry, str(args.get("thread_key") or ""), origin)
    if handle is None:
        return _json(_error("not_found", "async-thread listener not found"))
    ensure_secret_ref = bool(handle.enabled)
    return _json({"ok": True, "listener": _handle_summary_with_terminal(registry, handle, event_url=_event_url(config), secret_root=secret_root_from_config(config), ensure_secret_ref=ensure_secret_ref)})


def ath_retire_listener_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    thread_key = str(args.get("thread_key") or "")
    handle = _owned_handle(registry, thread_key, origin)
    if handle is None:
        return _json(_error("not_found", "async-thread listener not found"))
    changed = registry.set_enabled(handle.thread_key, False)
    removed_secret_material = remove_secret_artifact(handle.thread_key, root=secret_root_from_config(config))
    return _json({"ok": changed, "threadKey": handle.thread_key, "enabled": False, "action": "retired", "secretMaterialRemoved": removed_secret_material})


def ath_rotate_listener_secret_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    thread_key = str(args.get("thread_key") or "")
    handle = _owned_handle(registry, thread_key, origin)
    if handle is None:
        return _json(_error("not_found", "async-thread listener not found"))
    if not handle.enabled:
        return _json(_error("listener_disabled", "async-thread listener is disabled; resume before rotating secret"))
    rotated = registry.rotate_secret(handle.thread_key)
    if rotated is None:
        return _json(_error("not_found", "async-thread listener not found"))
    event_url = _event_url(config)
    return _json(
        {
            "ok": True,
            "action": "rotated",
            "listener": _handle_summary(rotated, event_url=event_url, secret_root=secret_root_from_config(config), ensure_secret_ref=True),
            "secret": _secret_reference(rotated, event_url=event_url, config=config),
        }
    )


def ath_generate_producer_handoff_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    thread_key = str(args.get("thread_key") or "")
    handle = _owned_handle(registry, thread_key, origin)
    if handle is None:
        return _json(_error("not_found", "async-thread listener not found"))
    if not handle.enabled:
        return _json(_error("listener_disabled", "async-thread listener is disabled; resume before generating producer handoff"))
    mode = str(args.get("mode") or "generic_contract")
    create_files_value = args.get("create_files", True)
    include_sensitive_secret_value = args.get("include_sensitive_secret", False)
    if not isinstance(create_files_value, bool):
        return _json(_error("invalid_request", "create_files must be a boolean"))
    if not isinstance(include_sensitive_secret_value, bool):
        return _json(_error("invalid_request", "include_sensitive_secret must be a boolean"))
    create_files = create_files_value
    include_sensitive_secret = include_sensitive_secret_value
    sensitive_allowed = mode.strip().lower().replace("-", "_") in {"debug_curl", "debug", "curl"}
    if include_sensitive_secret and not sensitive_allowed:
        return _json(_error("invalid_request", "include_sensitive_secret is only allowed with debug_curl mode"))
    payload = build_producer_handoff(
        handle,
        event_url=_event_url(config),
        secret_root=secret_root_from_config(config),
        handoff_root=handoff_root_from_config(config),
        mode=mode,
        event_type=str(args.get("event_type") or ""),
        create_files=create_files,
        include_sensitive_secret=include_sensitive_secret,
    )
    return _json(payload)


def ath_trace_event_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, _config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    if not origin.owner_user_id:
        return _json(_error("owner_unavailable", "current gateway user is unavailable"))

    event_id = str(args.get("event_id") or "").strip()
    if event_id:
        event = registry.get_event_by_id(event_id=event_id, owner_user_id=origin.owner_user_id)
        if event is None:
            return _json(_error("not_found", "async-thread event not found"))
        return _json({"ok": True, "event": _event_summary(event)})

    thread_key = str(args.get("thread_key") or "").strip() or None
    if thread_key and _owned_handle(registry, thread_key, origin) is None:
        return _json(_error("not_found", "async-thread listener not found"))
    limit = _bounded_int(args.get("limit"), default=10, minimum=1, maximum=20)
    events = registry.list_recent_events(thread_key=thread_key, owner_user_id=origin.owner_user_id, limit=limit)
    return _json({"ok": True, "events": [_event_summary(event) for event in events], "count": len(events)})


def ath_create_source_binding_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, _config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    if not origin.owner_user_id:
        return _json(_error("owner_unavailable", "current gateway user is unavailable"))
    try:
        spec = _source_binding_spec(args)
        binding = registry.create_source_binding(owner_user_id=origin.owner_user_id, **spec)
    except ValueError as exc:
        return _json(_error("invalid_request", str(exc)))
    return _json({"ok": True, "action": "created", "binding": _source_binding_summary(registry, binding)})


def ath_list_source_bindings_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, _config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    if not origin.owner_user_id:
        return _json(_error("owner_unavailable", "current gateway user is unavailable"))
    bindings = registry.list_source_bindings(
        owner_user_id=origin.owner_user_id,
        source=str(args.get("source") or "") or None,
        include_retired=bool(args.get("include_retired", False)),
        limit=_bounded_int(args.get("limit"), default=50, minimum=1, maximum=100),
    )
    return _json({"ok": True, "bindings": [_source_binding_summary(registry, binding) for binding in bindings], "count": len(bindings)})


def ath_get_source_binding_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, _config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    if not origin.owner_user_id:
        return _json(_error("owner_unavailable", "current gateway user is unavailable"))
    binding = registry.get_source_binding(binding_id=str(args.get("binding_id") or ""), owner_user_id=origin.owner_user_id)
    if binding is None:
        return _json(_error("not_found", "source binding not found"))
    return _json({"ok": True, "binding": _source_binding_summary(registry, binding, include_config=True)})


def ath_set_source_binding_status_tool(args: dict[str, Any], **kwargs: Any) -> str:
    registry, _config = _registry_and_config(kwargs)
    origin = _resolve_origin(kwargs)
    if not origin.ok:
        return _json(origin.public_error())
    if not origin.owner_user_id:
        return _json(_error("owner_unavailable", "current gateway user is unavailable"))
    status = str(args.get("status") or "")
    if status not in {"active", "paused", "retired"}:
        return _json(_error("invalid_request", "status must be active, paused, or retired"))
    binding_id = str(args.get("binding_id") or "")
    changed = registry.set_source_binding_status(binding_id=binding_id, owner_user_id=origin.owner_user_id, status=status)
    if not changed:
        return _json(_error("not_found", "source binding not found"))
    binding = registry.get_source_binding(binding_id=binding_id, owner_user_id=origin.owner_user_id)
    return _json({"ok": True, "action": status, "binding": _source_binding_summary(registry, binding) if binding else {"bindingId": binding_id, "status": status}})


def _registry_and_config(kwargs: Mapping[str, Any]) -> tuple[AsyncThreadRegistry, Any]:
    registry = kwargs.get("registry")
    config = kwargs.get("config") or _platform_config_from_loaded_config()
    if isinstance(registry, AsyncThreadRegistry):
        return registry, config
    return registry_from_config(config), config


def _platform_config_from_loaded_config() -> Any:
    extra: dict[str, Any] = {}
    try:
        from hermes_cli.config import load_config

        cfg = load_config()
    except Exception:
        cfg = {}
    for path in (
        ("gateway", "platforms", "async_threads"),
        ("platforms", "async_threads"),
        ("async_threads",),
    ):
        node: Any = cfg
        for key in path:
            node = node.get(key) if isinstance(node, Mapping) else None
        if isinstance(node, Mapping):
            node_extra = node.get("extra")
            if isinstance(node_extra, Mapping):
                extra.update({str(key): value for key, value in node_extra.items()})
            else:
                extra.update({str(key): value for key, value in node.items()})
            break
    return SimpleNamespace(enabled=True, extra=extra)


def _resolve_origin(kwargs: Mapping[str, Any]) -> OriginResolution:
    return resolve_current_origin(
        session_id=str(kwargs.get("session_id") or ""),
        session_key=str(kwargs.get("session_key") or ""),
        session_store=kwargs.get("session_store"),
        sessions_file=kwargs.get("sessions_file"),
        origin_index=kwargs.get("origin_index"),
        trusted_context=kwargs.get("trusted_context") if isinstance(kwargs.get("trusted_context"), Mapping) else None,
    )


def _listener_spec(args: Mapping[str, Any]) -> dict[str, Any]:
    target = str(args.get("target") or "current_conversation")
    if target != "current_conversation":
        raise ValueError("target must be current_conversation")
    purpose = str(args.get("purpose") or "").strip()
    if not purpose:
        raise ValueError("purpose is required")
    producer_id = _clean_token(str(args.get("producer_id") or args.get("producer_hint") or purpose), default="ath")
    exact_event_types = [str(item) for item in _as_list(args.get("event_types")) if str(item).strip()]
    event_kinds = [str(item) for item in _as_list(args.get("event_kinds")) if str(item).strip()]
    if exact_event_types:
        event_types = [_clean_event_type(item, producer_id=producer_id) for item in exact_event_types]
    elif event_kinds:
        event_types = [_clean_event_type(item, producer_id=producer_id) for item in event_kinds]
    else:
        event_types = [f"{producer_id}.finished", f"{producer_id}.failed"]
    event_types = list(dict.fromkeys(item for item in event_types if item))
    policy = str(args.get("delivery") or "agent_queue")
    if policy not in {"agent_queue", "direct"}:
        policy = "agent_queue"
    ack_mode = str(args.get("ack") or "brief")
    if ack_mode not in {"none", "brief", "debug"}:
        ack_mode = "brief"
    if policy == "direct":
        ack_mode = "none"
    continuation_policy = {
        "max_turns": _bounded_int(args.get("max_turns"), default=1, minimum=1, maximum=5),
        "max_tool_calls": _bounded_int(args.get("max_tool_calls"), default=0, minimum=0, maximum=20),
        "timeout_seconds": _bounded_int(args.get("timeout_seconds"), default=120, minimum=10, maximum=600),
        "toolsets": [str(item) for item in _as_list(args.get("continuation_toolsets")) if str(item).strip()],
        "fail_closed_without_core_bounds": bool(args.get("fail_closed_without_core_bounds", False)),
    }
    lifecycle_policy = {
        "terminal_event_types": [str(item) for item in _as_list(args.get("terminal_event_types")) if str(item).strip()],
        "auto_retire_on_terminal": bool(args.get("auto_retire_on_terminal", False)),
        "shared_listener": bool(args.get("shared_listener", False)),
    }
    return {
        "producer_id": producer_id,
        "event_types": tuple(event_types),
        "label": str(args.get("label") or purpose)[:120],
        "policy": policy,
        "ack_mode": ack_mode,
        "continuation_policy": continuation_policy,
        "lifecycle_policy": lifecycle_policy,
    }


def _source_binding_spec(args: Mapping[str, Any]) -> dict[str, Any]:
    source = _clean_token(str(args.get("source") or ""), default="")
    if not source:
        raise ValueError("source is required")
    listener_thread_key = str(args.get("listener_thread_key") or args.get("thread_key") or "").strip()
    if not listener_thread_key:
        raise ValueError("listener_thread_key is required")
    source_ref = _mapping_arg(args.get("source_ref"))
    board_ref = str(args.get("board_ref") or args.get("board") or "").strip()
    if board_ref and "board" not in source_ref:
        source_ref["board"] = board_ref
    if source == "kanban" and not source_ref.get("board"):
        raise ValueError("source=kanban requires source_ref.board or board_ref")
    delivery_policy = str(args.get("delivery_policy") or args.get("delivery") or "agent_queue")
    if delivery_policy not in {"agent_queue", "direct"}:
        delivery_policy = "agent_queue"
    return {
        "source": source,
        "source_ref": source_ref,
        "listener_thread_key": listener_thread_key,
        "producer_id": str(args.get("producer_id") or ""),
        "event_filter": _mapping_arg(args.get("event_filter")),
        "transform": _mapping_arg(args.get("transform")),
        "cursor": _mapping_arg(args.get("cursor")),
        "coalesce": _mapping_arg(args.get("coalesce")),
        "delivery_policy": delivery_policy,
    }


def _source_binding_summary(registry: AsyncThreadRegistry, binding: Any, *, include_config: bool = False) -> dict[str, Any]:
    payload = {
        "bindingId": binding.binding_id,
        "source": redact_metadata_text(binding.source),
        "sourceRef": _public_value(binding.source_ref),
        "listenerThreadKey": binding.listener_thread_key,
        "producerId": redact_metadata_text(binding.producer_id),
        "eventFilter": _public_value(binding.event_filter),
        "deliveryPolicy": binding.delivery_policy,
        "status": binding.status,
        "createdAt": binding.created_at,
        "updatedAt": binding.updated_at,
        "compatibility": registry.source_binding_compatibility(binding),
    }
    if include_config:
        payload.update(
            {
                "transform": _public_value(binding.transform),
                "cursor": _public_value(binding.cursor),
                "coalesce": _public_value(binding.coalesce),
            }
        )
    return payload


def _mapping_arg(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _public_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {redact_metadata_text(str(key)): _public_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_public_value(item) for item in value[:100]]
    if isinstance(value, tuple):
        return [_public_value(item) for item in value[:100]]
    if isinstance(value, str):
        return redact_metadata_text(value, max_chars=1000)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return redact_metadata_text(str(value), max_chars=1000)


def _find_equivalent_listener(
    registry: AsyncThreadRegistry,
    *,
    owner_user_id: str,
    origin: OriginResolution,
    producer_id: str,
    event_types: tuple[str, ...],
    policy: str,
    ack_mode: str,
    continuation_policy: Mapping[str, Any],
    lifecycle_policy: Mapping[str, Any],
) -> AsyncThreadHandle | None:
    expected_continuation = ContinuationPolicy.from_mapping(continuation_policy).to_mapping()
    from .lifecycle import LifecyclePolicy

    expected_lifecycle = LifecyclePolicy.from_mapping(lifecycle_policy).to_mapping()
    for handle in registry.list_handles(owner_user_id=owner_user_id, include_disabled=False):
        if handle.producer_id != producer_id:
            continue
        if tuple(handle.allowed_event_types) != tuple(event_types):
            continue
        if handle.policy != policy:
            continue
        if handle.ack_mode != ack_mode:
            continue
        if handle.continuation_policy.to_mapping() != expected_continuation:
            continue
        if handle.lifecycle_policy.to_mapping() != expected_lifecycle:
            continue
        if _same_origin(handle, origin):
            return handle
    return None


def _owned_handle(registry: AsyncThreadRegistry, thread_key: str, origin: OriginResolution) -> AsyncThreadHandle | None:
    if not thread_key or not origin.owner_user_id:
        return None
    handle = registry.get_handle(thread_key)
    if handle is None or handle.owner_user_id != origin.owner_user_id:
        return None
    return handle


def _same_origin(handle: AsyncThreadHandle, origin: OriginResolution) -> bool:
    if origin.session_key and handle.session_key:
        return handle.session_key == origin.session_key
    source = handle.source or {}
    current = origin.source_dict or {}
    return all(
        str(source.get(key) or "") == str(current.get(key) or "")
        for key in ("platform", "chat_id", "thread_id", "parent_chat_id")
    )


def _handle_summary_with_terminal(
    registry: AsyncThreadRegistry,
    handle: AsyncThreadHandle,
    *,
    event_url: str = "",
    secret_root: Any | None = None,
    ensure_secret_ref: bool = False,
    terminal_event: Any | None = None,
) -> dict[str, Any]:
    summary = _handle_summary(handle, event_url=event_url, secret_root=secret_root, ensure_secret_ref=ensure_secret_ref)
    terminal = terminal_event if terminal_event is not None else registry.latest_terminal_event(thread_key=handle.thread_key)
    if terminal is not None:
        summary["terminalState"] = {
            "eventId": terminal.event_id,
            "eventType": terminal.event_type,
            "action": terminal.detail.get("terminal_action", ""),
            "createdAt": terminal.created_at,
            "staleEnabled": bool(handle.enabled and terminal.detail.get("terminal_action") in {"warn_only", "shared_listener_kept_enabled"}),
        }
    return summary


def _handle_summary(
    handle: AsyncThreadHandle,
    *,
    event_url: str = "",
    secret_root: Any | None = None,
    ensure_secret_ref: bool = False,
) -> dict[str, Any]:
    summary = {
        "threadKey": handle.thread_key,
        "enabled": handle.enabled,
        "producerId": redact_metadata_text(handle.producer_id),
        "label": redact_metadata_text(handle.label),
        "allowedEventTypes": [redact_metadata_text(item) for item in handle.allowed_event_types],
        "policy": handle.policy,
        "ackMode": handle.ack_mode,
        "debounceSeconds": handle.debounce_seconds,
        "eventUrl": event_url,
        "target": {
            "platform": handle.source.get("platform"),
            "chat_id": handle.source.get("chat_id"),
            "thread_id": handle.source.get("thread_id"),
            "parent_chat_id": handle.source.get("parent_chat_id"),
        },
        "sessionKeyPresent": bool(handle.session_key),
        "sessionId": handle.session_id,
        "secretAvailable": bool(handle.secret),
        "continuationPolicy": handle.continuation_policy.public_summary(core_enforced=False),
        "lifecyclePolicy": handle.lifecycle_policy.public_summary(),
    }
    summary["secretRef"] = describe_secret_artifact(
        handle,
        event_url=event_url,
        root=secret_root,
        ensure=ensure_secret_ref and bool(handle.enabled),
    )
    return summary


def _event_summary(event: Any) -> dict[str, Any]:
    return {
        "eventId": safe_event_id(getattr(event, "event_id", "")),
        "threadKey": getattr(event, "thread_key", ""),
        "producerId": redact_metadata_text(getattr(event, "producer_id", "")),
        "eventType": redact_metadata_text(getattr(event, "event_type", "")),
        "outcome": getattr(event, "outcome", ""),
        "summary": redact_metadata_text(getattr(event, "summary", "")),
        "detail": sanitize_event_detail(getattr(event, "detail", {}) or {}),
        "createdAt": getattr(event, "created_at", ""),
    }


def _secret_reference(handle: AsyncThreadHandle, *, event_url: str = "", config: Any | None = None) -> dict[str, Any]:
    ref = describe_secret_artifact(
        handle,
        event_url=event_url,
        root=secret_root_from_config(config),
        ensure=True,
    )
    ref["reason"] = "raw signing secrets are intentionally not returned; pass ATH_SECRET_FILE to producer code"
    return ref


def _event_url(config: Any) -> str:
    extra = getattr(config, "extra", {}) or {}
    public_url = str(extra.get("public_url") or "").rstrip("/")
    if public_url:
        return f"{public_url}/async-threads/v1/events"
    scheme = "https" if extra.get("public_https") else "http"
    host = str(extra.get("public_host") or extra.get("host") or "127.0.0.1")
    port = int(extra.get("public_port") or extra.get("port") or 8765)
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        return f"{scheme}://{host}/async-threads/v1/events"
    return f"{scheme}://{host}:{port}/async-threads/v1/events"


def _clean_token(value: str, *, default: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value.strip().lower()).strip("-._")
    return (token or default)[:80]


def _clean_event_type(value: str, *, producer_id: str) -> str:
    raw = value.strip().lower()
    if "." not in raw:
        raw = f"{producer_id}.{raw}"
    return _clean_token(raw, default=f"{producer_id}.event")


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable):
        return list(value)
    return [value]


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _error(error: str, message: str) -> dict[str, Any]:
    return {"ok": False, "error": error, "message": message}


def _json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)
