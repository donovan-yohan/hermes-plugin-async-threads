"""Gateway /ath command interception for listener management."""

from __future__ import annotations

import re
import shlex
from typing import Any

from .adapter import registry_from_config, registry_path_from_config


USAGE = """async threads (/ath)
commands:
  /ath listen <producer> [--events a,b] [--label text] [--policy agent_queue|direct]
  /ath status
  /ath list
  /ath events [thread_key] [--limit N]
  /ath inspect <thread_key>
  /ath pause <thread_key>
  /ath resume <thread_key>
  /ath revoke <thread_key>
""".strip()


def ath_help(raw_args: str = "") -> str:
    return USAGE


def handle_pre_gateway_dispatch(**kwargs):
    """Intercept /ath commands before normal dispatch so we can capture source."""
    event = kwargs.get("event")
    gateway = kwargs.get("gateway")
    if event is None or gateway is None:
        return None
    text = str(getattr(event, "text", "") or "").strip()
    if not (text == "/ath" or text.startswith("/ath ") or text.startswith("!ath ")):
        return None

    source = getattr(event, "source", None)
    auth_fn = getattr(gateway, "_is_user_authorized", None)
    if callable(auth_fn) and source is not None:
        try:
            if not auth_fn(source):
                # Let the normal gateway auth path handle/drop the message.
                return {"action": "allow"}
        except Exception:
            return {"action": "allow"}

    args = text.split(maxsplit=1)[1] if " " in text else ""
    try:
        response = _run_command(args, event=event, gateway=gateway)
    except Exception as exc:  # noqa: BLE001 - never leak stack traces into chat
        response = f"async-thread command failed: {type(exc).__name__}: {str(exc)[:200]}"
    _schedule_notice(gateway, event, response)
    return {"action": "skip", "reason": "async_threads_command"}


def _run_command(raw_args: str, *, event: Any, gateway: Any) -> str:
    argv = shlex.split(raw_args or "")
    if not argv or argv[0] in {"help", "-h", "--help"}:
        return USAGE
    command = argv[0].lower()
    config = _platform_config(gateway)
    registry = registry_from_config(config)

    if command == "listen":
        return _cmd_listen(argv[1:], event=event, gateway=gateway, registry=registry)
    owner_user_id = str(getattr(event.source, "user_id", "") or "")
    if command == "status":
        return _cmd_status(registry, config=config, gateway=gateway, owner_user_id=owner_user_id)
    if command in {"list", "ls"}:
        return _cmd_list(registry, owner_user_id=owner_user_id)
    if command == "events":
        thread_key, limit = _parse_events_args(argv[1:])
        return _cmd_events(registry, thread_key=thread_key, limit=limit, owner_user_id=owner_user_id)
    if command == "inspect" and len(argv) >= 2:
        return _cmd_inspect(registry, argv[1], owner_user_id=owner_user_id)
    if command in {"pause", "disable"} and len(argv) >= 2:
        return _cmd_set_enabled(registry, argv[1], False, "paused", owner_user_id=owner_user_id)
    if command in {"resume", "enable"} and len(argv) >= 2:
        return _cmd_set_enabled(registry, argv[1], True, "resumed", owner_user_id=owner_user_id)
    if command in {"revoke", "remove", "rm"} and len(argv) >= 2:
        return _cmd_set_enabled(registry, argv[1], False, "revoked", owner_user_id=owner_user_id)
    return USAGE


def _cmd_listen(args: list[str], *, event: Any, gateway: Any, registry: Any) -> str:
    if not args:
        return "usage: /ath listen <producer> [--events a,b] [--label text] [--policy agent_queue|direct]"
    producer_id = args[0]
    events: list[str] = []
    label = ""
    policy = "agent_queue"
    i = 1
    while i < len(args):
        arg = args[i]
        if arg == "--events" and i + 1 < len(args):
            events = [e.strip() for e in args[i + 1].split(",") if e.strip()]
            i += 2
            continue
        if arg == "--label" and i + 1 < len(args):
            label = args[i + 1]
            i += 2
            continue
        if arg == "--policy" and i + 1 < len(args):
            policy = args[i + 1]
            i += 2
            continue
        return f"unknown option for /ath listen: {arg}"

    source = event.source
    source_dict = source.to_dict() if hasattr(source, "to_dict") else dict(source)
    session_key = _session_key_for_source(gateway, source)
    session_id = _session_id_for_key(gateway, session_key)
    handle = registry.create_handle(
        source=source_dict,
        producer_id=producer_id,
        label=label,
        allowed_event_types=events,
        policy=policy,
        session_key=session_key,
        session_id=session_id,
        owner_user_id=str(getattr(source, "user_id", "") or ""),
    )
    url = _event_url(gateway)
    events_text = ", ".join(handle.allowed_event_types) if handle.allowed_event_types else "all events"
    return (
        "created async-thread listener\n"
        f"threadKey: `{handle.thread_key}`\n"
        f"producer: `{handle.producer_id}`\n"
        f"policy: `{handle.policy}`\n"
        f"events: {events_text}\n"
        f"url: `{url}`\n"
        f"secret: `{handle.secret}`\n"
        "sign request body with HMAC-SHA256 as `X-Hermes-Signature-256: sha256=<hex>`."
    )


def _cmd_status(registry: Any, *, config: Any, gateway: Any, owner_user_id: str) -> str:
    adapter = _async_threads_adapter(gateway)
    extra = getattr(config, "extra", {}) or {}
    host = str(extra.get("host", "127.0.0.1"))
    port = int(extra.get("port", 8765))
    running = "yes" if getattr(adapter, "_running", False) else "unknown"
    listener_count = registry.count_handles(owner_user_id=owner_user_id or "") if owner_user_id else 0
    event_count = registry.count_recent_events(owner_user_id=owner_user_id or "") if owner_user_id else 0
    return (
        "async-thread status\n"
        f"receiver: `{_event_url(gateway)}` ({host}:{port})\n"
        f"running: {running}\n"
        f"registry: `{registry_path_from_config(config)}`\n"
        f"listeners: {listener_count}\n"
        f"recent events: {event_count}"
    )


def _cmd_events(registry: Any, *, thread_key: str | None, limit: int, owner_user_id: str) -> str:
    if not owner_user_id:
        return "no async-thread events for this user."
    events = registry.list_recent_events(thread_key=thread_key, owner_user_id=owner_user_id, limit=limit)
    if not events:
        suffix = f" for `{thread_key}`" if thread_key else ""
        return f"no async-thread events{suffix}."
    lines = ["recent async-thread events:"]
    for event in events:
        lines.append(_format_event_row(event))
    return "\n".join(lines)


def _parse_events_args(args: list[str]) -> tuple[str | None, int]:
    thread_key: str | None = None
    limit = 20
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--limit" and i + 1 < len(args):
            try:
                limit = int(args[i + 1])
            except ValueError:
                limit = 20
            i += 2
            continue
        if not arg.startswith("--") and thread_key is None:
            thread_key = arg
            i += 1
            continue
        i += 1
    return thread_key, max(1, min(limit, 50))


def _format_event_row(event: Any) -> str:
    summary = _diagnostic_summary(event)
    summary_text = f" — {summary}" if summary else ""
    return (
        f"- {event.created_at} `{event.thread_key or '-'}` "
        f"{event.producer_id}/{event.event_type} "
        f"id={_short_event_id(event.event_id)} outcome=`{event.outcome}`{summary_text}"
    )


def _clip(value: str, max_len: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _diagnostic_summary(event: Any) -> str:
    # Rejected events can be attacker-controlled probes. Keep diagnostics useful
    # without echoing unauthenticated text back into chat.
    if str(getattr(event, "outcome", "")).startswith("rejected_"):
        return ""
    return _clip(_redact_diagnostic_text(getattr(event, "summary", "")), 80)


def _redact_diagnostic_text(value: str) -> str:
    text = str(value or "")
    text = re.sub(
        r"(?i)\b(secret|token|authorization|cookie|signature|password|credential)\b\s*[:=]\s*\S+",
        lambda match: f"{match.group(1)}=<redacted>",
        text,
    )
    text = re.sub(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+", "Bearer <redacted>", text)
    return text


def _short_event_id(event_id: str) -> str:
    text = str(event_id or "")
    return f"…{text[-8:]}" if len(text) > 8 else (text or "-")


def _cmd_list(registry: Any, *, owner_user_id: str) -> str:
    if not owner_user_id:
        return "no async-thread listeners for this user. create one with `/ath listen <producer>`."
    handles = registry.list_handles(owner_user_id=owner_user_id)
    if not handles:
        return "no async-thread listeners for this user. create one with `/ath listen <producer>`."
    lines = ["async-thread listeners:"]
    for h in handles[:20]:
        state = "enabled" if h.enabled else "disabled"
        label = f" — {h.label}" if h.label else ""
        thread = f" thread={h.thread_id}" if h.thread_id else ""
        lines.append(f"- `{h.thread_key}` {state} producer=`{h.producer_id}` policy=`{h.policy}`{thread}{label}")
    return "\n".join(lines)


def _cmd_inspect(registry: Any, thread_key: str, *, owner_user_id: str) -> str:
    h = registry.get_handle(thread_key)
    if h is None or not owner_user_id or h.owner_user_id != owner_user_id:
        return "async-thread listener not found."
    state = "enabled" if h.enabled else "disabled"
    events = ", ".join(h.allowed_event_types) if h.allowed_event_types else "all"
    recent_events = registry.list_recent_events(thread_key=thread_key, owner_user_id=owner_user_id, limit=3)
    recent_text = "\n".join(_format_event_row(event) for event in recent_events) if recent_events else "none"
    return (
        f"`{h.thread_key}` {state}\n"
        f"producer: `{h.producer_id}`\n"
        f"policy: `{h.policy}`\n"
        f"events: {events}\n"
        f"platform/chat/thread: `{h.platform}` / `{h.chat_id}` / `{h.thread_id or '-'}`\n"
        f"sessionKey: `{h.session_key or '-'}`\n"
        f"created: {h.created_at}\n"
        "secret: hidden\n"
        f"recent events:\n{recent_text}"
    )


def _cmd_set_enabled(registry: Any, thread_key: str, enabled: bool, verb: str, *, owner_user_id: str) -> str:
    h = registry.get_handle(thread_key)
    if h is None or not owner_user_id or h.owner_user_id != owner_user_id:
        return "async-thread listener not found."
    if not registry.set_enabled(thread_key, enabled):
        return "async-thread listener not found."
    return f"{verb} async-thread listener `{thread_key}`."


def _schedule_notice(gateway: Any, event: Any, content: str) -> None:
    import asyncio

    coro = _send_notice(gateway, event, content)
    try:
        asyncio.create_task(coro)
    except RuntimeError:
        # No running loop (mostly tests / unusual CLI import); avoid an
        # un-awaited coroutine warning and let normal dispatch continue.
        coro.close()


async def _send_notice(gateway: Any, event: Any, content: str) -> None:
    source = event.source
    adapter = gateway.adapters.get(source.platform)
    if adapter is None:
        return
    metadata = {"thread_id": source.thread_id} if getattr(source, "thread_id", None) else None
    await adapter.send(source.chat_id, content, metadata=metadata)


def _async_threads_adapter(gateway: Any) -> Any:
    for platform, adapter in getattr(gateway, "adapters", {}).items():
        if getattr(platform, "value", None) == "async_threads":
            return adapter
    return None


def _platform_config(gateway: Any) -> Any:
    try:
        from gateway.config import PlatformConfig
    except Exception:  # pragma: no cover
        PlatformConfig = None  # type: ignore
    for platform, adapter in getattr(gateway, "adapters", {}).items():
        if getattr(platform, "value", None) == "async_threads":
            return adapter.config
    if PlatformConfig is None:
        raise RuntimeError("gateway PlatformConfig unavailable")
    return PlatformConfig(enabled=True, extra={})


def _session_key_for_source(gateway: Any, source: Any) -> str:
    from gateway.session import build_session_key

    return build_session_key(
        source,
        group_sessions_per_user=getattr(gateway.config, "group_sessions_per_user", True),
        thread_sessions_per_user=getattr(gateway.config, "thread_sessions_per_user", False),
    )


def _session_id_for_key(gateway: Any, session_key: str) -> str:
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


def _event_url(gateway: Any) -> str:
    config = _platform_config(gateway)
    extra = getattr(config, "extra", {}) or {}
    public_url = str(extra.get("public_url") or "").rstrip("/")
    if public_url:
        return f"{public_url}/async-threads/v1/events"
    host = str(extra.get("host", "127.0.0.1"))
    port = int(extra.get("port", 8765))
    display_host = "localhost" if host == "0.0.0.0" else host
    return f"http://{display_host}:{port}/async-threads/v1/events"
