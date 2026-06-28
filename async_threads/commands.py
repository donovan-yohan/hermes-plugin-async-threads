"""Gateway /ath command interception for listener management."""

from __future__ import annotations

import json
import shlex
import time
from typing import Any

from .adapter import registry_from_config, registry_path_from_config
from .kanban import KANBAN_READ_FAILURE_EXCEPTIONS, dry_run_kanban_source_binding, kanban_read_failed_report
from .listeners import ListenValidationError, create_listener
from .origin import remember_gateway_origin
from .privacy import redact_metadata_text, redact_secret_text, safe_event_id
from .registry import safe_session_key_hash
from .routing import send_metadata_for_source
from .secrets import describe_secret_artifact, remove_secret_artifact, secret_root_from_config
from .source_runner import source_binding_runner_status
from .workflows import WorkflowPolicy


USAGE = """async threads (/ath)
commands:
  /ath listen <producer> [--events a,b] [--label text] [--policy agent_queue|direct] [--ack none|brief|debug] [--debounce seconds] [--terminal-events a,b] [--auto-retire-terminal] [--shared-listener] [--gate-order review,qa] [--gate-mode serial|parallel] [--stale-on-artifact-change review,qa|all] [--candidate-required qa]
  /ath status
  /ath list
  /ath events [thread_key] [--limit N]
  /ath trace <event_id> [--json]
  /ath payload <pointer-or-event-id> [--thread <thread_key>] [--raw-local] [--json]
  /ath workflows [thread_key] [--limit N]
  /ath inspect <thread_key>
  /ath emit-command <thread_key> --event event.type [--summary text]
  /ath rotate-secret <thread_key>
  /ath lifecycle [thread_key]
  /ath prune [--dry-run|--force] [--event-log-days N] [--seen-days N]
  /ath bind-source <source> <thread_key> [--board board] [--source-ref k=v,...] [--producer id] [--events a,b] [--policy agent_queue|direct]
  /ath dry-run-binding <binding_id> [--db path] [--since N] [--limit N] [--json]
  /ath bindings [--source source] [--include-retired]
  /ath inspect-binding <binding_id>
  /ath pause-binding <binding_id>
  /ath resume-binding <binding_id>
  /ath retire-binding <binding_id>
  /ath pause <thread_key>
  /ath resume <thread_key>
  /ath retire <thread_key>
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
    if source is None or not callable(auth_fn):
        # Fail closed for plugin side effects. Let the normal gateway path handle
        # or ignore the message instead of executing privileged /ath commands
        # when the host cannot prove the caller is authorized.
        return {"action": "allow"}
    try:
        if not auth_fn(source):
            # Let the normal gateway auth path handle/drop the message.
            return {"action": "allow"}
        remember_gateway_origin(event=event, gateway=gateway, session_store=kwargs.get("session_store"))
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
    if command == "trace" and len(argv) >= 2:
        return _cmd_trace(registry, argv[1], as_json="--json" in argv[2:], owner_user_id=owner_user_id)
    if command == "payload" and len(argv) >= 2:
        return _cmd_payload(registry, argv[1], argv[2:], owner_user_id=owner_user_id)
    if command in {"workflows", "runs"}:
        thread_key, limit = _parse_events_args(argv[1:])
        return _cmd_workflows(registry, thread_key=thread_key, limit=limit, owner_user_id=owner_user_id)
    if command == "inspect" and len(argv) >= 2:
        return _cmd_inspect(registry, argv[1], owner_user_id=owner_user_id)
    if command == "emit-command" and len(argv) >= 2:
        return _cmd_emit_command(registry, argv[1], argv[2:], gateway=gateway, owner_user_id=owner_user_id)
    if command in {"rotate-secret", "rotate_secret"} and len(argv) >= 2:
        return _cmd_rotate_secret(registry, argv[1], config=config, gateway=gateway, owner_user_id=owner_user_id)
    if command == "lifecycle":
        return _cmd_lifecycle(registry, argv[1] if len(argv) >= 2 else "", owner_user_id=owner_user_id)
    if command == "prune":
        return _cmd_prune(registry, argv[1:], config=config, owner_user_id=owner_user_id)
    if command in {"bind-source", "bind_source"}:
        return _cmd_bind_source(registry, argv[1:], owner_user_id=owner_user_id)
    if command in {"dry-run-binding", "dry_run_binding", "preview-binding", "preview_binding"} and len(argv) >= 2:
        return _cmd_dry_run_binding(registry, argv[1], argv[2:], owner_user_id=owner_user_id)
    if command in {"bindings", "source-bindings", "source_bindings"}:
        return _cmd_bindings(registry, argv[1:], owner_user_id=owner_user_id)
    if command in {"inspect-binding", "inspect_binding"} and len(argv) >= 2:
        return _cmd_inspect_binding(registry, argv[1], owner_user_id=owner_user_id)
    if command in {"pause-binding", "disable-binding"} and len(argv) >= 2:
        return _cmd_set_binding_status(registry, argv[1], "paused", owner_user_id=owner_user_id)
    if command in {"resume-binding", "enable-binding"} and len(argv) >= 2:
        return _cmd_set_binding_status(registry, argv[1], "active", owner_user_id=owner_user_id)
    if command in {"retire-binding", "revoke-binding", "remove-binding", "rm-binding"} and len(argv) >= 2:
        return _cmd_set_binding_status(registry, argv[1], "retired", owner_user_id=owner_user_id)
    if command in {"pause", "disable"} and len(argv) >= 2:
        return _cmd_set_enabled(registry, argv[1], False, "paused", owner_user_id=owner_user_id, config=config)
    if command in {"resume", "enable"} and len(argv) >= 2:
        return _cmd_set_enabled(registry, argv[1], True, "resumed", owner_user_id=owner_user_id, config=config)
    if command in {"retire", "revoke", "remove", "rm"} and len(argv) >= 2:
        return _cmd_set_enabled(registry, argv[1], False, "retired" if command == "retire" else "revoked", owner_user_id=owner_user_id, config=config)
    return USAGE


def _cmd_listen(args: list[str], *, event: Any, gateway: Any, registry: Any) -> str:
    if not args:
        return "usage: /ath listen <producer> [--events a,b] [--label text] [--policy agent_queue|direct] [--ack none|brief|debug] [--debounce seconds] [--terminal-events a,b] [--auto-retire-terminal] [--shared-listener] [--gate-order review,qa] [--gate-mode serial|parallel] [--stale-on-artifact-change review,qa|all] [--candidate-required qa]"
    producer_id = args[0]
    events: list[str] = []
    terminal_events: list[str] = []
    auto_retire_terminal = False
    shared_listener = False
    label = ""
    policy = "agent_queue"
    ack_mode = "none"
    debounce_seconds: int | str = 0
    gate_order: list[str] = []
    gate_mode = "serial"
    stale_on_artifact_change: list[str] = []
    candidate_required: list[str] = []
    i = 1
    while i < len(args):
        arg = args[i]
        if arg == "--events" and i + 1 < len(args):
            events = [e.strip() for e in args[i + 1].split(",") if e.strip()]
            i += 2
            continue
        if arg == "--terminal-events" and i + 1 < len(args):
            terminal_events = [e.strip() for e in args[i + 1].split(",") if e.strip()]
            i += 2
            continue
        if arg == "--auto-retire-terminal":
            auto_retire_terminal = True
            i += 1
            continue
        if arg == "--shared-listener":
            shared_listener = True
            i += 1
            continue
        if arg == "--label" and i + 1 < len(args):
            label = args[i + 1]
            i += 2
            continue
        if arg == "--policy" and i + 1 < len(args):
            policy = args[i + 1]
            i += 2
            continue
        if arg == "--ack" and i + 1 < len(args):
            ack_mode = args[i + 1]
            i += 2
            continue
        if arg == "--debounce" and i + 1 < len(args):
            debounce_seconds = args[i + 1]
            i += 2
            continue
        if arg == "--gate-order" and i + 1 < len(args):
            gate_order = _split_csv(args[i + 1])
            i += 2
            continue
        if arg == "--gate-mode" and i + 1 < len(args):
            gate_mode = args[i + 1].lower()
            i += 2
            continue
        if arg == "--stale-on-artifact-change" and i + 1 < len(args):
            stale_on_artifact_change = _split_csv(args[i + 1])
            i += 2
            continue
        if arg == "--candidate-required" and i + 1 < len(args):
            candidate_required = _split_csv(args[i + 1])
            i += 2
            continue
        return f"unknown option for /ath listen: {arg}"

    url = _event_url(gateway)
    try:
        result = create_listener(
            registry=registry,
            source=event.source,
            gateway=gateway,
            producer_id=producer_id,
            label=label,
            allowed_event_types=events,
            policy=policy,
            ack_mode=ack_mode,
            debounce_seconds=debounce_seconds,
            gate_order=gate_order,
            gate_mode=gate_mode,
            stale_on_artifact_change=stale_on_artifact_change,
            candidate_required=candidate_required,
            lifecycle_policy={
                "terminal_event_types": terminal_events,
                "auto_retire_on_terminal": auto_retire_terminal,
                "shared_listener": shared_listener,
            },
            event_url=url,
        )
    except ListenValidationError as exc:
        return str(exc)
    handle = result.handle
    secret_ref = describe_secret_artifact(
        handle,
        event_url=url,
        root=secret_root_from_config(_platform_config(gateway)),
    )
    events_text = ", ".join(handle.allowed_event_types) if handle.allowed_event_types else "all events"
    return (
        "created async-thread listener\n"
        f"threadKey: `{handle.thread_key}`\n"
        f"producer: `{handle.producer_id}`\n"
        f"policy: `{handle.policy}`\n"
        f"ack: `{handle.ack_mode}`\n"
        f"debounce: `{handle.debounce_seconds}s`\n"
        f"workflow gates: {_format_workflow_policy(handle.workflow_policy)}\n"
        f"lifecycle: {_format_lifecycle_policy(handle.lifecycle_policy)}\n"
        f"events: {events_text}\n"
        f"url: `{url}`\n"
        f"secretFile: `{secret_ref['secretFile']}`\n"
        f"contractFile: `{secret_ref['contractFile']}`\n"
        "raw secret is not printed; pass `ATH_SECRET_FILE` to producer code.\n"
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
    workflow_count = registry.count_workflow_states(owner_user_id=owner_user_id or "") if owner_user_id else 0
    stale_terminal_count = len(registry.list_stale_terminal_handles(owner_user_id=owner_user_id or "")) if owner_user_id else 0
    return (
        "async-thread status\n"
        f"receiver: `{_event_url(gateway)}` ({host}:{port})\n"
        f"running: {running}\n"
        f"registry: `{registry_path_from_config(config)}`\n"
        f"listeners: {listener_count}\n"
        f"recent events: {event_count}\n"
        f"workflows: {workflow_count}\n"
        f"stale terminal listeners: {stale_terminal_count}"
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


def _cmd_trace(registry: Any, event_id: str, *, as_json: bool, owner_user_id: str) -> str:
    if not owner_user_id:
        return "async-thread event not found."
    event = registry.get_event_by_id(event_id=event_id, owner_user_id=owner_user_id)
    if event is None:
        return "async-thread event not found."
    trace = {
        "eventId": event.event_id,
        "threadKey": event.thread_key,
        "producerId": event.producer_id,
        "eventType": event.event_type,
        "outcome": event.outcome,
        "createdAt": event.created_at,
        "summary": _redact_diagnostic_text(event.summary),
        "detail": event.detail,
        "interpretation": _trace_interpretation(event.outcome, event.detail),
    }
    if as_json:
        return json.dumps(trace, sort_keys=True, indent=2)
    lines = [
        "async-thread event trace:",
        f"eventId: `{_short_event_id(event.event_id)}`",
        f"threadKey: `{_display_metadata(event.thread_key, 80)}`",
        f"producer/event: `{_display_metadata(event.producer_id, 80)}` / `{_display_metadata(event.event_type, 80)}`",
        f"outcome: `{_display_outcome(event.outcome)}`",
        f"created: {event.created_at}",
        f"interpretation: {_trace_interpretation(event.outcome, event.detail)}",
    ]
    summary = _clip(_redact_diagnostic_text(event.summary), 160)
    if summary:
        lines.append(f"summary: {summary}")
    detail = _format_event_detail(event.detail)
    if detail:
        lines.append(f"detail: {detail}")
    return "\n".join(lines)


def _trace_interpretation(outcome: str, detail: dict[str, Any]) -> str:
    if outcome == "duplicate":
        return "duplicate retry after final handling"
    if outcome == "coalesced_pending":
        return "accepted into debounce/coalescing bucket"
    if outcome == "queued_active_session":
        return "queued behind an active target session"
    if outcome == "agent_started":
        return "queued into idle target session for agent continuation"
    if outcome == "direct_delivered":
        return "delivered directly to the mapped gateway thread"
    if outcome == "dispatch_failed":
        return "delivery failed after authentication; producer should retry the same event id"
    if str(outcome).startswith("rejected_"):
        return "authenticated request rejected by listener scope or disabled handle"
    if detail.get("queued") is True:
        return "queued behind an active target session"
    return "recorded diagnostic event"


def _cmd_payload(registry: Any, identifier: str, args: list[str], *, owner_user_id: str) -> str:
    if not owner_user_id:
        return "async-thread event payload not found."
    thread_key = ""
    raw_local = False
    as_json = False
    i = 0
    while i < len(args):
        if args[i] == "--thread" and i + 1 < len(args):
            thread_key = args[i + 1]
            i += 2
            continue
        if args[i] == "--raw-local":
            raw_local = True
            i += 1
            continue
        if args[i] == "--json":
            as_json = True
            i += 1
            continue
        return "usage: /ath payload <pointer-or-event-id> [--thread <thread_key>] [--raw-local] [--json]"
    is_pointer = str(identifier).startswith("athp_")
    record = registry.get_event_payload(
        owner_user_id=owner_user_id,
        pointer_id=identifier if is_pointer else "",
        event_id="" if is_pointer else identifier,
        thread_key=thread_key or None,
    )
    if record is None:
        return "async-thread event payload not found."
    if raw_local:
        if getattr(record, "storage_mode", "") != "raw_local" or not getattr(record, "raw_payload", {}):
            return "raw_local payload is unavailable for this event."
        payload = record.raw_payload
        redaction = "raw_local"
    else:
        payload = record.redacted_payload
        redaction = "redacted"
    result = {
        "pointerId": record.pointer_id,
        "eventId": record.event_id,
        "threadKey": record.thread_key,
        "eventType": record.event_type,
        "redaction": redaction,
        "payload": payload,
        "digest": record.digest,
        "untrustedData": True,
        "createdAt": record.created_at,
        "expiresAt": record.expires_at,
    }
    if as_json:
        return json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False)
    return (
        "async-thread event payload (untrusted data)\n"
        f"pointerId: `{_display_metadata(record.pointer_id, 80)}`\n"
        f"eventId: `{_short_event_id(record.event_id)}`\n"
        f"threadKey: `{_display_metadata(record.thread_key, 80)}`\n"
        f"eventType: `{_display_metadata(record.event_type, 80)}`\n"
        f"redaction: `{redaction}`\n"
        f"payload: `{_display_text(json.dumps(payload, sort_keys=True, ensure_ascii=False), 500)}`"
    )


def _cmd_workflows(registry: Any, *, thread_key: str | None, limit: int, owner_user_id: str) -> str:
    if not owner_user_id:
        return "no async-thread workflows for this user."
    workflows = registry.list_workflow_states(thread_key=thread_key, owner_user_id=owner_user_id, limit=limit)
    if not workflows:
        suffix = f" for `{thread_key}`" if thread_key else ""
        return f"no async-thread workflows{suffix}."
    lines = ["async-thread workflows:"]
    for workflow in workflows:
        lines.append(_format_workflow_row(workflow))
    return "\n".join(lines)


def _cmd_emit_command(registry: Any, thread_key: str, args: list[str], *, gateway: Any, owner_user_id: str) -> str:
    handle = registry.get_handle(thread_key)
    if handle is None or not owner_user_id or handle.owner_user_id != owner_user_id:
        return "async-thread listener not found."
    event_type = ""
    summary = "event ready"
    i = 0
    while i < len(args):
        if args[i] == "--event" and i + 1 < len(args):
            event_type = args[i + 1]
            i += 2
            continue
        if args[i] == "--summary" and i + 1 < len(args):
            summary = args[i + 1]
            i += 2
            continue
        i += 1
    if not event_type:
        return "usage: /ath emit-command <thread_key> --event event.type [--summary text]"
    url = _event_url(gateway)
    producer = _display_metadata(handle.producer_id, 100)
    event = _display_metadata(event_type, 100)
    safe_summary = _display_text(summary, 160)
    return (
        "sandbox-safe ATH emit template\n"
        "- set ATH_SECRET_FILE outside prompts/logs; this template does not print it.\n"
        "- standalone: only Python standard library is required in the producer sandbox.\n"
        "```bash\n"
        f"export ATH_URL={shlex.quote(url)}\n"
        f"export ATH_THREAD_KEY={shlex.quote(handle.thread_key)}\n"
        f"export ATH_PRODUCER_ID={shlex.quote(producer)}\n"
        "export ATH_SECRET_FILE=/path/to/ath-secret-file\n"
        "python3 - <<'PY'\n"
        "import hashlib, hmac, json, os, time, urllib.error, urllib.request\n"
        "SUCCESS_STATUSES = {'delivered', 'accepted', 'queued', 'duplicate'}\n"
        "RETRYABLE_HTTP = {408, 409, 425, 429, 500, 502, 503, 504}\n"
        "def emit_public(result):\n"
        "    print(json.dumps(result, sort_keys=True))\n"
        "    raise SystemExit(0 if result.get('success') else (75 if result.get('retryable') else 1))\n"
        "def local_config_error(message):\n"
        "    emit_public({'success': False, 'retryable': False, 'duplicate': False, 'status': 'local_config_error', 'error': str(message)})\n"
        "secret_file = os.environ.get('ATH_SECRET_FILE', '')\n"
        "if not secret_file:\n"
        "    local_config_error('ATH_SECRET_FILE is required')\n"
        "try:\n"
        "    with open(secret_file, 'r', encoding='utf-8') as fh:\n"
        "        secret = fh.read()\n"
        "except OSError as exc:\n"
        "    local_config_error(exc)\n"
        "if not secret:\n"
        "    local_config_error('ATH secret is required')\n"
        "body_obj = {\n"
        "    'version': 'async-thread-event/v1',\n"
        "    'eventId': os.environ.get('ATH_EVENT_ID', 'manual-' + str(int(time.time()))),\n"
        f"    'eventType': {event!r},\n"
        "    'producer': {'id': os.environ['ATH_PRODUCER_ID']},\n"
        "    'occurredAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),\n"
        "    'asyncThread': {'threadKey': os.environ['ATH_THREAD_KEY']},\n"
        f"    'summary': {safe_summary!r},\n"
        "    'tailMode': 'compact',\n"
        "    'payload': {'status': os.environ.get('ATH_STATUS', 'ready')},\n"
        "}\n"
        "body = json.dumps(body_obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')\n"
        "sig = hmac.new(secret.encode('utf-8'), body, hashlib.sha256).hexdigest()\n"
        "headers = {'Content-Type': 'application/json', 'X-Hermes-Signature-256': 'sha256=' + sig}\n"
        "req = urllib.request.Request(os.environ['ATH_URL'], data=body, method='POST', headers=headers)\n"
        "def classify(http_status, response_body='', error=''):\n"
        "    text = response_body.decode('utf-8', 'replace') if isinstance(response_body, bytes) else str(response_body or '')\n"
        "    text = text.replace(secret, '<redacted>')\n"
        "    safe_error = str(error or '').replace(secret, '<redacted>')\n"
        "    parsed_status = ''\n"
        "    try:\n"
        "        parsed = json.loads(text) if text else {}\n"
        "        if isinstance(parsed, dict):\n"
        "            parsed_status = str(parsed.get('status') or '')\n"
        "    except Exception:\n"
        "        pass\n"
        "    receiver_status = parsed_status or ('transport_error' if http_status is None else '')\n"
        "    success = bool(http_status is not None and 200 <= http_status < 300 and receiver_status in SUCCESS_STATUSES)\n"
        "    retryable = (http_status is None) or (http_status in RETRYABLE_HTTP)\n"
        "    if success:\n"
        "        retryable = False\n"
        "    result = {'success': success, 'retryable': retryable, 'duplicate': receiver_status == 'duplicate', 'status': receiver_status}\n"
        "    if http_status is not None:\n"
        "        result['httpStatus'] = http_status\n"
        "    if text:\n"
        "        result['body'] = text\n"
        "    if safe_error:\n"
        "        result['error'] = safe_error\n"
        "    emit_public(result)\n"
        "try:\n"
        "    with urllib.request.urlopen(req, timeout=float(os.environ.get('ATH_TIMEOUT', '20'))) as res:\n"
        "        classify(int(res.status), res.read())\n"
        "except urllib.error.HTTPError as exc:\n"
        "    classify(int(exc.code), exc.read())\n"
        "except urllib.error.URLError as exc:\n"
        "    classify(None, error=getattr(exc, 'reason', exc))\n"
        "except OSError as exc:\n"
        "    classify(None, error=exc)\n"
        "PY\n"
        "```"
    )


def _cmd_rotate_secret(registry: Any, thread_key: str, *, config: Any, gateway: Any, owner_user_id: str) -> str:
    handle = registry.get_handle(thread_key)
    if handle is None or not owner_user_id or handle.owner_user_id != owner_user_id:
        return "async-thread listener not found."
    if not handle.enabled:
        return "async-thread listener is disabled; resume before rotating secret."
    rotated = registry.rotate_secret(thread_key)
    if rotated is None:
        return "async-thread listener not found."
    url = _event_url(gateway)
    secret_ref = describe_secret_artifact(rotated, event_url=url, root=secret_root_from_config(config))
    return (
        "rotated async-thread listener secret\n"
        f"threadKey: `{rotated.thread_key}`\n"
        f"secretFile: `{secret_ref['secretFile']}`\n"
        f"contractFile: `{secret_ref['contractFile']}`\n"
        "raw secret is not printed; update producer code to use the refreshed `ATH_SECRET_FILE`."
    )


def _cmd_lifecycle(registry: Any, thread_key: str, *, owner_user_id: str) -> str:
    handle = registry.get_handle(thread_key) if thread_key else None
    if thread_key and (handle is None or not owner_user_id or handle.owner_user_id != owner_user_id):
        return "async-thread listener not found."
    scope = f" for `{_display_metadata(thread_key, 80)}`" if thread_key else ""
    stale = registry.list_stale_terminal_handles(owner_user_id=owner_user_id) if owner_user_id else []
    stale_lines = []
    for stale_handle, terminal_event in stale[:10]:
        stale_lines.append(
            f"- `{_display_metadata(stale_handle.thread_key, 80)}` terminal={_display_metadata(terminal_event.event_type, 80)} id={_short_event_id(terminal_event.event_id)} action={_display_text(terminal_event.detail.get('terminal_action', '-'), 40)}"
        )
    stale_text = "\nstale terminal listeners:\n" + "\n".join(stale_lines) if stale_lines else "\nstale terminal listeners: none"
    return (
        f"async-thread scoped lifecycle{scope}\n"
        "recommended event stages: `started`, `progress`, `blocked`, `ready_for_review`, `review_passed`, `qa_passed`, `released`, `cancelled`\n"
        "terminal cleanup convention: use terminal event types like `*.goal.finished`, `*.phase.finished`, `*.session.finished`, `*.run.finished`, or listener-specific `--terminal-events`.\n"
        "for single-goal listeners, create with `--auto-retire-terminal`; for shared listeners, use `--shared-listener` and retire manually after all consumers are done.\n"
        "recommended producer fields: `workflowId`, `stage`, `artifact`, `candidate`, `evidence`, plus `seriesKey`/`subject.artifact.revision` for repeated artifact events.\n"
        "retired listeners reject new events with the generic auth error; duplicate retries of an already-accepted terminal event stay idempotent.\n"
        "producer handoffs should make terminal producers self-exit after emitting the final event.\n"
        "use `/ath workflows <thread_key>` for current state, `/ath trace <event-id>` for per-event delivery diagnostics, and `/ath retire <thread_key>` for manual cleanup."
        f"{stale_text}"
    )


def _cmd_prune(registry: Any, args: list[str], *, config: Any, owner_user_id: str) -> str:
    if not owner_user_id:
        return "no async-thread registry rows for this user."
    extra = getattr(config, "extra", {}) or {}
    event_days = _configured_nonnegative_days(extra.get("event_log_retention_days", extra.get("retention_event_log_days", 30)), 30)
    seen_days = _configured_nonnegative_days(extra.get("seen_event_retention_days", extra.get("retention_seen_days", 7)), 7)
    dry_run = True
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--force":
            dry_run = False
            i += 1
            continue
        if arg == "--dry-run":
            dry_run = True
            i += 1
            continue
        if arg == "--event-log-days" and i + 1 < len(args):
            parsed = _parse_nonnegative_days(args[i + 1])
            if parsed is None:
                return "invalid event-log retention days. use a non-negative integer."
            event_days = parsed
            i += 2
            continue
        if arg == "--seen-days" and i + 1 < len(args):
            parsed = _parse_nonnegative_days(args[i + 1])
            if parsed is None:
                return "invalid seen-event retention days. use a non-negative integer."
            seen_days = parsed
            i += 2
            continue
        return "usage: /ath prune [--dry-run|--force] [--event-log-days N] [--seen-days N]"
    event_cutoff = _utc_days_ago(event_days)
    seen_cutoff = _utc_days_ago(seen_days)
    result = registry.prune_old_rows(
        owner_user_id=owner_user_id,
        event_log_before=event_cutoff,
        seen_before=seen_cutoff,
        dry_run=dry_run,
    )
    action = "would prune" if dry_run else "pruned"
    suffix = "use `--force` to delete rows." if dry_run else "replay protection inside the configured retention window was preserved."
    return (
        f"async-thread prune {'dry-run' if dry_run else 'complete'}\n"
        "scope: owner-scoped\n"
        f"{action} event_log rows: {result['event_log']} before {event_cutoff}\n"
        f"{action} seen_events rows: {result['seen_events']} before {seen_cutoff}\n"
        f"{action} event_payloads rows: {result['event_payloads']} before {result['payload_before']}\n"
        f"{suffix}"
    )


def _cmd_bind_source(registry: Any, args: list[str], *, owner_user_id: str) -> str:
    if not owner_user_id:
        return "no async-thread source bindings for this user."
    if len(args) < 2:
        return "usage: /ath bind-source <source> <thread_key> [--board board] [--source-ref k=v,...] [--producer id] [--events a,b] [--policy agent_queue|direct]"
    source = args[0]
    thread_key = args[1]
    source_ref: dict[str, Any] = {}
    event_filter: dict[str, Any] = {}
    producer_id = ""
    delivery_policy = "agent_queue"
    i = 2
    while i < len(args):
        arg = args[i]
        if arg == "--board" and i + 1 < len(args):
            source_ref["board"] = args[i + 1]
            i += 2
            continue
        if arg == "--source-ref" and i + 1 < len(args):
            source_ref.update(_parse_kv_csv(args[i + 1]))
            i += 2
            continue
        if arg == "--producer" and i + 1 < len(args):
            producer_id = args[i + 1]
            i += 2
            continue
        if arg == "--events" and i + 1 < len(args):
            event_filter["eventTypes"] = _split_csv(args[i + 1])
            i += 2
            continue
        if arg == "--policy" and i + 1 < len(args):
            delivery_policy = args[i + 1]
            i += 2
            continue
        return f"unknown option for /ath bind-source: {arg}"
    if source == "kanban" and not source_ref.get("board"):
        return "source=kanban requires --board or --source-ref board=<board>."
    try:
        binding = registry.create_source_binding(
            owner_user_id=owner_user_id,
            source=source,
            source_ref=source_ref,
            listener_thread_key=thread_key,
            producer_id=producer_id,
            event_filter=event_filter,
            delivery_policy=delivery_policy,
        )
    except ValueError as exc:
        return str(exc)
    compatibility = registry.source_binding_compatibility(binding)
    return (
        "created async-thread source binding\n"
        f"bindingId: `{binding.binding_id}`\n"
        f"source: `{_display_metadata(binding.source, 80)}` ref={_format_binding_map(binding.source_ref)}\n"
        f"listener: `{_display_metadata(binding.listener_thread_key, 80)}`\n"
        f"producer: `{_display_metadata(binding.producer_id, 80)}`\n"
        f"filter: {_format_binding_map(binding.event_filter)}\n"
        f"status: `{binding.status}` compatibility=`{_display_text(compatibility.get('reason'), 80)}` failClosed=`{compatibility.get('failClosed')}`"
    )


def _cmd_dry_run_binding(registry: Any, binding_id: str, args: list[str], *, owner_user_id: str) -> str:
    if not owner_user_id:
        return "async-thread source binding not found."
    binding = registry.get_source_binding(binding_id=binding_id, owner_user_id=owner_user_id)
    if binding is None:
        return "async-thread source binding not found."
    board_db_path = ""
    since_event_id: int | None = None
    limit = 100
    as_json = False
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--db" and i + 1 < len(args):
            board_db_path = args[i + 1]
            i += 2
            continue
        if arg == "--since" and i + 1 < len(args):
            try:
                since_event_id = max(0, int(args[i + 1]))
            except ValueError:
                return "invalid --since event id. use a non-negative integer."
            i += 2
            continue
        if arg == "--limit" and i + 1 < len(args):
            try:
                limit = max(1, min(int(args[i + 1]), 500))
            except ValueError:
                return "invalid --limit. use an integer 1-500."
            i += 2
            continue
        if arg == "--json":
            as_json = True
            i += 1
            continue
        return "usage: /ath dry-run-binding <binding_id> [--db path] [--since N] [--limit N] [--json]"
    try:
        report = dry_run_kanban_source_binding(
            registry=registry,
            binding=binding,
            board_db_path=board_db_path or None,
            since_event_id=since_event_id,
            limit=limit,
        )
    except KANBAN_READ_FAILURE_EXCEPTIONS as exc:
        report = kanban_read_failed_report(binding, exc)
    if as_json:
        return json.dumps(report, sort_keys=True, indent=2)
    counts = report.get("counts", {}) if isinstance(report, dict) else {}
    cursor = report.get("cursor", {}) if isinstance(report, dict) else {}
    lines = [
        "async-thread source binding dry-run",
        f"bindingId: `{_display_metadata(binding.binding_id, 80)}`",
        f"source: `{_display_metadata(binding.source, 40)}` board=`{_display_metadata(report.get('board', ''), 80)}`",
        f"would_emit: {counts.get('would_emit', 0)} suppressed: {counts.get('suppressed', 0)} would_coalesce: {counts.get('would_coalesce', 0)} invalid_binding: {counts.get('invalid_binding', 0)}",
    ]
    if cursor:
        lines.append(f"cursor: from={cursor.get('fromEventId')} wouldAdvanceTo={cursor.get('wouldAdvanceToEventId')} advanced=false")
    for item in list(report.get("events", []))[:10]:
        action = _display_text(item.get("action", ""), 40)
        event_id = _display_metadata(item.get("eventId", item.get("upstreamEventId", "")), 80)
        event_type = _display_metadata(item.get("eventType", item.get("digestEventType", "")), 80)
        reason = _display_text(item.get("reason", ""), 80)
        reason_text = f" reason={reason}" if reason else ""
        type_text = f" type={event_type}" if event_type else ""
        lines.append(f"- {action} id={event_id}{type_text}{reason_text}")
    if len(report.get("events", [])) > 10:
        lines.append(f"... {len(report.get('events', [])) - 10} more events omitted; rerun with --json for full dry-run data.")
    lines.append("dry-run only: no events sent and cursor was not advanced.")
    return "\n".join(lines)


def _cmd_bindings(registry: Any, args: list[str], *, owner_user_id: str) -> str:
    if not owner_user_id:
        return "no async-thread source bindings for this user."
    source = ""
    include_retired = False
    i = 0
    while i < len(args):
        if args[i] == "--source" and i + 1 < len(args):
            source = args[i + 1]
            i += 2
            continue
        if args[i] == "--include-retired":
            include_retired = True
            i += 1
            continue
        return "usage: /ath bindings [--source source] [--include-retired]"
    bindings = registry.list_source_bindings(owner_user_id=owner_user_id, source=source or None, include_retired=include_retired, limit=50)
    if not bindings:
        return "no async-thread source bindings for this user."
    lines = ["async-thread source bindings:"]
    for binding in bindings:
        compatibility = registry.source_binding_compatibility(binding)
        lines.append(
            f"- `{_display_metadata(binding.binding_id, 80)}` {binding.status} source=`{_display_metadata(binding.source, 40)}` "
            f"ref={_format_binding_map(binding.source_ref)} listener=`{_display_metadata(binding.listener_thread_key, 80)}` "
            f"producer=`{_display_metadata(binding.producer_id, 80)}` compatibility=`{_display_text(compatibility.get('reason'), 80)}` failClosed=`{compatibility.get('failClosed')}`"
        )
    return "\n".join(lines)


def _cmd_inspect_binding(registry: Any, binding_id: str, *, owner_user_id: str) -> str:
    if not owner_user_id:
        return "async-thread source binding not found."
    binding = registry.get_source_binding(binding_id=binding_id, owner_user_id=owner_user_id)
    if binding is None:
        return "async-thread source binding not found."
    compatibility = registry.source_binding_compatibility(binding)
    runner_status = source_binding_runner_status(registry=registry, binding=binding)
    return (
        f"`{_display_metadata(binding.binding_id, 80)}` {binding.status}\n"
        f"source: `{_display_metadata(binding.source, 80)}`\n"
        f"sourceRef: {_format_binding_map(binding.source_ref)}\n"
        f"listener: `{_display_metadata(binding.listener_thread_key, 80)}`\n"
        f"producer: `{_display_metadata(binding.producer_id, 80)}`\n"
        f"filter: {_format_binding_map(binding.event_filter)}\n"
        f"transform: {_format_binding_map(binding.transform)}\n"
        f"cursor: {_format_binding_map(binding.cursor)}\n"
        f"coalesce: {_format_binding_map(binding.coalesce)}\n"
        f"deliveryPolicy: `{_display_text(binding.delivery_policy, 40)}`\n"
        f"compatibility: valid=`{compatibility.get('valid')}` failClosed=`{compatibility.get('failClosed')}` reason=`{_display_text(compatibility.get('reason'), 80)}`\n"
        f"runner: health=`{_display_text(runner_status.get('health'), 40)}` cursor=`{_display_text(runner_status.get('cursor', {}).get('lastEventId'), 20)}` lag=`{_display_text(runner_status.get('lag'), 20)}` outbox={_format_binding_map(runner_status.get('outbox', {}).get('counts', {}))}\n"
        f"created: {binding.created_at}"
    )


def _cmd_set_binding_status(registry: Any, binding_id: str, status: str, *, owner_user_id: str) -> str:
    if not registry.set_source_binding_status(binding_id=binding_id, owner_user_id=owner_user_id, status=status):
        return "async-thread source binding not found."
    verb = {"active": "resumed", "paused": "paused", "retired": "retired"}[status]
    return f"{verb} async-thread source binding `{binding_id}`. listener lifecycle was not changed."


def _configured_nonnegative_days(value: Any, default: int) -> int:
    parsed = _parse_nonnegative_days(value)
    return parsed if parsed is not None else default


def _parse_nonnegative_days(value: Any) -> int | None:
    try:
        days = int(value)
    except (TypeError, ValueError):
        return None
    return days if days >= 0 else None


def _utc_days_ago(days: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(max(0.0, time.time() - max(0, days) * 86400)))


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _parse_kv_csv(value: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in _split_csv(value):
        key, sep, raw_value = item.partition("=")
        if sep and key.strip():
            parsed[key.strip()] = raw_value.strip()
    return parsed


def _display_text(value: Any, max_len: int) -> str:
    return _clip(redact_secret_text(str(value or ""), max_input_chars=1000, max_output_chars=1000), max_len)


def _display_metadata(value: Any, max_len: int) -> str:
    return _clip(redact_metadata_text(str(value or ""), max_chars=1000), max_len)


def _format_binding_map(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "{}"
    parts = []
    for key, item in list(value.items())[:8]:
        if isinstance(item, (dict, list, tuple)):
            rendered = json.dumps(item, sort_keys=True, ensure_ascii=False)
        else:
            rendered = str(item)
        parts.append(f"{_display_metadata(key, 40)}={_display_metadata(rendered, 80)}")
    suffix = ",…" if len(value) > 8 else ""
    return "{" + ",".join(parts) + suffix + "}"


def _format_lifecycle_policy(policy: Any) -> str:
    terminal_types = list(getattr(policy, "terminal_event_types", ()) or ())
    auto_retire = bool(getattr(policy, "auto_retire_on_terminal", False))
    shared = bool(getattr(policy, "shared_listener", False))
    parts = ["terminal=" + (",".join(_display_text(item, 40) for item in terminal_types) or "default")]
    if auto_retire:
        parts.append("auto_retire=true")
    if shared:
        parts.append("shared=true")
    if not auto_retire and not shared:
        parts.append("auto_retire=false")
    return "; ".join(parts)


def _format_workflow_policy(policy: WorkflowPolicy) -> str:
    if not policy.gate_order:
        return "none"
    parts = [f"{policy.gate_mode} order={','.join(_display_text(item, 40) for item in policy.gate_order)}"]
    if policy.stale_on_artifact_change:
        parts.append(f"stale_on_artifact_change={','.join(_display_text(item, 40) for item in policy.stale_on_artifact_change)}")
    if policy.candidate_required:
        parts.append(f"candidate_required={','.join(_display_text(item, 40) for item in policy.candidate_required)}")
    return "; ".join(parts)


def _format_workflow_row(workflow: Any) -> str:
    gates = getattr(workflow, "gates", {}) or {}
    active = gates.get("active", []) if isinstance(gates, dict) else []
    deferred = gates.get("deferred", []) if isinstance(gates, dict) else []
    evidence = getattr(workflow, "evidence", {}) or {}
    evidence_bits: list[str] = []
    if isinstance(evidence, dict):
        for gate, item in sorted(evidence.items()):
            if isinstance(item, dict):
                evidence_bits.append(f"{_display_text(gate, 40)}:{_display_text(item.get('status', 'unknown'), 24)}")
    candidate = getattr(workflow, "candidate", {}) or {}
    candidate_id = candidate.get("id") if isinstance(candidate, dict) else ""
    candidate_readiness = candidate.get("readiness") if isinstance(candidate, dict) else ""
    candidate_text = ""
    if candidate_id or candidate_readiness:
        candidate_text = f" candidate={_display_text(candidate_id or '-', 40)}:{_display_text(candidate_readiness or '-', 24)}"
    active_text = ",".join(_display_text(item, 40) for item in active) or "-"
    deferred_text = ",".join(_display_text(item, 40) for item in deferred) or "-"
    evidence_text = ",".join(evidence_bits) or "-"
    summary = _clip(_redact_diagnostic_text(getattr(workflow, "last_summary", "")), 80)
    summary_text = f" — {summary}" if summary else ""
    return (
        f"- {workflow.updated_at} `{_display_metadata(workflow.thread_key, 80)}` workflow=`{_display_text(workflow.workflow_id, 80)}` "
        f"stage=`{_display_text(workflow.stage or '-', 40)}`{candidate_text} "
        f"active={active_text} deferred={deferred_text} evidence={evidence_text} "
        f"last={_display_text(workflow.last_event_type or '-', 80)} id={_short_event_id(workflow.last_event_id)}{summary_text}"
    )


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
    detail = _format_event_detail(getattr(event, "detail", {}) or {})
    detail_text = f" [{detail}]" if detail else ""
    producer = _clip(redact_metadata_text(getattr(event, "producer_id", "")), 80)
    event_type = _clip(redact_metadata_text(getattr(event, "event_type", "")), 80)
    return (
        f"- {event.created_at} `{event.thread_key or '-'}` "
        f"{producer}/{event_type} "
        f"id={_short_event_id(event.event_id)} outcome=`{_display_outcome(event.outcome)}`{summary_text}{detail_text}"
    )


def _display_outcome(outcome: str) -> str:
    text = str(outcome or "")
    return {
        "accepted": "agent_started (legacy accepted)",
        "queued": "queued_active_session (legacy queued)",
        "delivered": "direct_delivered (legacy delivered)",
    }.get(text, text or "-")


def _format_event_detail(detail: dict[str, Any]) -> str:
    keys = [
        "target_platform",
        "gateway_runner_exists",
        "target_adapter_exists",
        "policy",
        "ack_mode",
        "ack_sent",
        "ack_success",
        "ack_error",
        "coalesced_count",
        "coalesced_reason",
        "debounce_seconds",
        "session_key_present",
        "session_key_hash",
        "workflow_id",
        "workflow_stage",
        "active_session",
        "queued",
        "handle_message_called",
        "handle_message_returned",
        "direct_send_success",
        "exception_class",
        "exception_message",
    ]
    parts: list[str] = []
    for key in keys:
        if key in detail:
            value = _clip(redact_secret_text(str(detail[key]), max_input_chars=1000, max_output_chars=1000), 48)
            parts.append(f"{key}={value}")
    return ", ".join(parts)


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
    return redact_secret_text(value, max_input_chars=1000, max_output_chars=1000)


def _short_event_id(event_id: str) -> str:
    text = safe_event_id(event_id)
    if not text:
        return "-"
    return f"…{text[-8:]}" if len(text) > 8 else text


def _cmd_list(registry: Any, *, owner_user_id: str) -> str:
    if not owner_user_id:
        return "no async-thread listeners for this user. create one with `/ath listen <producer>`."
    handles = registry.list_handles(owner_user_id=owner_user_id)
    if not handles:
        return "no async-thread listeners for this user. create one with `/ath listen <producer>`."
    displayed = handles[:20]
    terminal_by_thread = registry.latest_terminal_events(thread_keys=[h.thread_key for h in displayed])
    lines = ["async-thread listeners:"]
    for h in displayed:
        state = "enabled" if h.enabled else "disabled"
        label = f" — {_display_text(h.label, 80)}" if h.label else ""
        thread = f" thread={_display_metadata(h.thread_id, 80)}" if h.thread_id else ""
        debounce = f" debounce={h.debounce_seconds}s" if h.debounce_seconds else ""
        workflow = f" workflow={_format_workflow_policy(h.workflow_policy)}" if h.workflow_policy.gate_order else ""
        lifecycle = f" lifecycle={_format_lifecycle_policy(h.lifecycle_policy)}"
        terminal_event = terminal_by_thread.get(h.thread_key)
        terminal = ""
        if terminal_event is not None:
            stale = h.enabled and terminal_event.detail.get("terminal_action") in {"warn_only", "shared_listener_kept_enabled"}
            terminal = f" terminal={'stale' if stale else terminal_event.detail.get('terminal_action', 'seen')}"
        lines.append(
            f"- `{_display_metadata(h.thread_key, 80)}` {state} "
            f"producer=`{_display_metadata(h.producer_id, 80)}` policy=`{_display_text(h.policy, 40)}`"
            f"{debounce}{workflow}{lifecycle}{terminal}{thread}{label}"
        )
    return "\n".join(lines)


def _cmd_inspect(registry: Any, thread_key: str, *, owner_user_id: str) -> str:
    h = registry.get_handle(thread_key)
    if h is None or not owner_user_id or h.owner_user_id != owner_user_id:
        return "async-thread listener not found."
    state = "enabled" if h.enabled else "disabled"
    events = ", ".join(_display_text(event_type, 80) for event_type in h.allowed_event_types) if h.allowed_event_types else "all"
    recent_events = registry.list_recent_events(thread_key=thread_key, owner_user_id=owner_user_id, limit=3)
    recent_text = "\n".join(_format_event_row(event) for event in recent_events) if recent_events else "none"
    workflows = registry.list_workflow_states(thread_key=thread_key, owner_user_id=owner_user_id, limit=3)
    workflow_text = "\n".join(_format_workflow_row(workflow) for workflow in workflows) if workflows else "none"
    session_key_state = "present" if h.session_key else "-"
    session_key_hash = safe_session_key_hash(h.session_key) or "-"
    return (
        f"`{_display_metadata(h.thread_key, 80)}` {state}\n"
        f"producer: `{_display_metadata(h.producer_id, 80)}`\n"
        f"policy: `{_display_text(h.policy, 40)}`\n"
        f"ack: `{_display_text(h.ack_mode, 40)}`\n"
        f"debounce: `{h.debounce_seconds}s`\n"
        f"workflow gates: {_format_workflow_policy(h.workflow_policy)}\n"
        f"lifecycle: {_format_lifecycle_policy(h.lifecycle_policy)}\n"
        f"events: {events}\n"
        f"platform/chat/thread: `{_display_text(h.platform, 40)}` / `{_display_metadata(h.chat_id, 80)}` / `{_display_metadata(h.thread_id or '-', 80)}`\n"
        f"sessionKey: {session_key_state} hash=`{session_key_hash}`\n"
        f"created: {h.created_at}\n"
        "secret: hidden\n"
        f"workflows:\n{workflow_text}\n"
        f"recent events:\n{recent_text}"
    )


def _cmd_set_enabled(registry: Any, thread_key: str, enabled: bool, verb: str, *, owner_user_id: str, config: Any | None = None) -> str:
    h = registry.get_handle(thread_key)
    if h is None or not owner_user_id or h.owner_user_id != owner_user_id:
        return "async-thread listener not found."
    if not registry.set_enabled(thread_key, enabled):
        return "async-thread listener not found."
    if not enabled and verb in {"retired", "revoked"}:
        remove_secret_artifact(thread_key, root=secret_root_from_config(config))
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
    metadata = send_metadata_for_source(source)
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
