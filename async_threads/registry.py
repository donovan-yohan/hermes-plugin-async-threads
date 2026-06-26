"""SQLite registry for async-thread handles and event de-dupe."""

from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from .continuation import ContinuationPolicy
from .lifecycle import LifecyclePolicy
from .privacy import redact_metadata_text, redact_secret_text, safe_event_id, sanitize_untrusted_value
from .workflows import WorkflowPolicy, apply_workflow_transition, normalize_workflow_event


SCHEMA_VERSION = 8

SOURCE_BINDING_STATUSES = {"active", "paused", "retired"}
SOURCE_BINDING_DELIVERY_POLICIES = {"agent_queue", "direct"}
SOURCE_BINDINGS_DDL = """
create table if not exists source_bindings(
    binding_id text primary key,
    created_at text not null,
    updated_at text not null,
    owner_user_id text not null,
    source text not null,
    source_ref_json text not null default '{}',
    listener_thread_key text not null,
    producer_id text not null,
    filter_json text not null default '{}',
    transform_json text not null default '{}',
    cursor_json text not null default '{}',
    coalesce_json text not null default '{}',
    delivery_policy text not null default 'agent_queue',
    status text not null default 'active'
);

create index if not exists idx_source_bindings_owner
    on source_bindings(owner_user_id, status, created_at desc);
create index if not exists idx_source_bindings_listener
    on source_bindings(listener_thread_key);
create index if not exists idx_source_bindings_source
    on source_bindings(source, owner_user_id);
"""

SAFE_DETAIL_KEYS = {
    "ack_mode",
    "ack_sent",
    "ack_success",
    "continuation_core_enforced",
    "continuation_fail_closed",
    "continuation_limit_reason",
    "continuation_policy",
    "coalesced_count",
    "coalesced_reason",
    "debounce_seconds",
    "ack_error",
    "active_session",
    "direct_send_success",
    "error",
    "exception_class",
    "exception_message",
    "gateway_runner_exists",
    "handle_enabled",
    "handle_message_called",
    "handle_message_returned",
    "lifecycle_policy",
    "policy",
    "queued",
    "session_key_hash",
    "session_key_present",
    "terminal_action",
    "terminal_event",
    "terminal_retired",
    "workflow_id",
    "workflow_stage",
    "target_adapter_exists",
    "target_platform",
}
UNSAFE_DETAIL_KEY_RE = re.compile(
    r"secret|token|authorization|cookie|signature|password|credential|payload|body|headers|raw|env",
    re.IGNORECASE,
)


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass(frozen=True)
class AsyncThreadHandle:
    thread_key: str
    source: dict[str, Any]
    producer_id: str
    secret: str
    policy: str = "agent_queue"
    enabled: bool = True
    label: str = ""
    allowed_event_types: tuple[str, ...] = ()
    session_key: str = ""
    session_id: str = ""
    owner_user_id: str = ""
    ack_mode: str = "none"
    debounce_seconds: int = 0
    workflow_policy: WorkflowPolicy = field(default_factory=WorkflowPolicy)
    continuation_policy: ContinuationPolicy = field(default_factory=ContinuationPolicy)
    lifecycle_policy: LifecyclePolicy = field(default_factory=LifecyclePolicy)
    created_at: str = ""
    updated_at: str = ""

    @property
    def platform(self) -> str:
        return str(self.source.get("platform") or "")

    @property
    def chat_id(self) -> str:
        return str(self.source.get("chat_id") or "")

    @property
    def thread_id(self) -> str:
        return str(self.source.get("thread_id") or "")


@dataclass(frozen=True)
class AsyncThreadEventLog:
    id: int
    producer_id: str
    event_id: str
    thread_key: str
    event_type: str
    outcome: str
    summary: str
    created_at: str
    detail: dict[str, Any]


@dataclass(frozen=True)
class AsyncThreadWorkflowState:
    thread_key: str
    workflow_id: str
    stage: str
    artifact: dict[str, Any]
    artifact_fingerprint: str
    candidate: dict[str, Any]
    evidence: dict[str, Any]
    gates: dict[str, Any]
    last_event_id: str
    last_event_type: str
    last_summary: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class AsyncThreadSourceBinding:
    binding_id: str
    owner_user_id: str
    source: str
    source_ref: dict[str, Any]
    listener_thread_key: str
    producer_id: str
    event_filter: dict[str, Any]
    transform: dict[str, Any]
    cursor: dict[str, Any]
    coalesce: dict[str, Any]
    delivery_policy: str
    status: str
    created_at: str
    updated_at: str


class AsyncThreadRegistry:
    """Durable listener registry.

    The DB intentionally stores producer secrets for the MVP because the event
    receiver needs to validate per-handle HMAC signatures. Command and model-tool
    surfaces expose secret-file references only, not literal secret values.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                create table if not exists meta(
                    key text primary key,
                    value text not null
                );

                create table if not exists async_thread_handles(
                    thread_key text primary key,
                    created_at text not null,
                    updated_at text not null,
                    enabled integer not null default 1,
                    label text not null default '',
                    source_json text not null,
                    session_key text not null default '',
                    session_id text not null default '',
                    owner_user_id text not null default '',
                    producer_id text not null,
                    secret text not null,
                    allowed_event_types_json text not null default '[]',
                    policy text not null default 'agent_queue',
                    ack_mode text not null default 'none',
                    debounce_seconds integer not null default 0,
                    workflow_policy_json text not null default '{}',
                    continuation_policy_json text not null default '{}',
                    lifecycle_policy_json text not null default '{}'
                );

                create index if not exists idx_async_thread_handles_owner
                    on async_thread_handles(owner_user_id);
                """
            )
            # Keep the query planner useful for the list command without
            # requiring generated columns (older SQLite compatibility).
            conn.execute(
                "create index if not exists idx_async_thread_handles_producer "
                "on async_thread_handles(producer_id)"
            )
            conn.executescript(
                """
                create table if not exists seen_events(
                    producer_id text not null,
                    event_id text not null,
                    thread_key text not null,
                    first_seen_at text not null,
                    primary key (producer_id, event_id)
                );

                create table if not exists event_log(
                    id integer primary key autoincrement,
                    producer_id text not null,
                    event_id text not null,
                    thread_key text,
                    event_type text,
                    outcome text not null,
                    summary text,
                    detail_json text not null default '{}',
                    created_at text not null
                );

                create index if not exists idx_event_log_thread_key
                    on event_log(thread_key);

                create table if not exists workflow_state(
                    thread_key text not null,
                    workflow_id text not null,
                    created_at text not null,
                    updated_at text not null,
                    current_stage text not null default '',
                    artifact_json text not null default '{}',
                    artifact_fingerprint text not null default '',
                    candidate_json text not null default '{}',
                    evidence_json text not null default '{}',
                    gates_json text not null default '{}',
                    last_event_id text not null default '',
                    last_event_type text not null default '',
                    last_summary text not null default '',
                    primary key (thread_key, workflow_id),
                    foreign key (thread_key) references async_thread_handles(thread_key) on delete cascade
                );

                create index if not exists idx_workflow_state_thread_updated
                    on workflow_state(thread_key, updated_at desc);

                """
                + SOURCE_BINDINGS_DDL
            )
            self._migrate_schema(conn)
            conn.execute(
                "insert or replace into meta(key, value) values('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        event_log_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(event_log)").fetchall()
        }
        if "detail_json" not in event_log_columns:
            conn.execute("alter table event_log add column detail_json text not null default '{}'")
        handle_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(async_thread_handles)").fetchall()
        }
        if "ack_mode" not in handle_columns:
            conn.execute("alter table async_thread_handles add column ack_mode text not null default 'none'")
        if "debounce_seconds" not in handle_columns:
            conn.execute("alter table async_thread_handles add column debounce_seconds integer not null default 0")
        if "workflow_policy_json" not in handle_columns:
            conn.execute("alter table async_thread_handles add column workflow_policy_json text not null default '{}'")
        if "continuation_policy_json" not in handle_columns:
            conn.execute("alter table async_thread_handles add column continuation_policy_json text not null default '{}'")
        if "lifecycle_policy_json" not in handle_columns:
            conn.execute("alter table async_thread_handles add column lifecycle_policy_json text not null default '{}'")
        conn.executescript(SOURCE_BINDINGS_DDL)

    def create_handle(
        self,
        *,
        source: dict[str, Any],
        producer_id: str,
        label: str = "",
        allowed_event_types: Iterable[str] = (),
        policy: str = "agent_queue",
        session_key: str = "",
        session_id: str = "",
        owner_user_id: str = "",
        ack_mode: str = "none",
        debounce_seconds: int = 0,
        workflow_policy: WorkflowPolicy | dict[str, Any] | None = None,
        continuation_policy: ContinuationPolicy | dict[str, Any] | None = None,
        lifecycle_policy: LifecyclePolicy | dict[str, Any] | None = None,
    ) -> AsyncThreadHandle:
        producer_id = _clean_token(producer_id, default="default")
        policy = policy if policy in {"agent_queue", "direct"} else "agent_queue"
        ack_mode = ack_mode if ack_mode in {"none", "brief", "debug"} else "none"
        debounce_seconds = max(0, min(int(debounce_seconds or 0), 300))
        workflow_policy_obj = workflow_policy if isinstance(workflow_policy, WorkflowPolicy) else WorkflowPolicy.from_mapping(workflow_policy)
        continuation_policy_obj = (
            continuation_policy if isinstance(continuation_policy, ContinuationPolicy) else ContinuationPolicy.from_mapping(continuation_policy)
        )
        lifecycle_policy_obj = lifecycle_policy if isinstance(lifecycle_policy, LifecyclePolicy) else LifecyclePolicy.from_mapping(lifecycle_policy)
        thread_key = f"ath_{secrets.token_urlsafe(12).replace('-', '').replace('_', '')[:16]}"
        secret = secrets.token_urlsafe(32)
        now = utc_now()
        event_types = tuple(_clean_event_type(e) for e in allowed_event_types if _clean_event_type(e))
        with self._connect() as conn:
            conn.execute(
                """
                insert into async_thread_handles(
                    thread_key, created_at, updated_at, enabled, label, source_json,
                    session_key, session_id, owner_user_id, producer_id, secret,
                    allowed_event_types_json, policy, ack_mode, debounce_seconds, workflow_policy_json,
                    continuation_policy_json, lifecycle_policy_json
                ) values (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    thread_key,
                    now,
                    now,
                    label.strip(),
                    json.dumps(source, sort_keys=True),
                    session_key or "",
                    session_id or "",
                    owner_user_id or "",
                    producer_id,
                    secret,
                    json.dumps(list(event_types)),
                    policy,
                    ack_mode,
                    debounce_seconds,
                    workflow_policy_obj.to_json(),
                    continuation_policy_obj.to_json(),
                    lifecycle_policy_obj.to_json(),
                ),
            )
        return self.get_handle(thread_key)  # type: ignore[return-value]

    def get_handle(self, thread_key: str) -> AsyncThreadHandle | None:
        with self._connect() as conn:
            row = conn.execute(
                "select * from async_thread_handles where thread_key = ?",
                (thread_key,),
            ).fetchone()
        return _row_to_handle(row) if row else None

    def list_handles(
        self,
        *,
        owner_user_id: str | None = None,
        include_disabled: bool = True,
    ) -> list[AsyncThreadHandle]:
        sql = "select * from async_thread_handles"
        clauses: list[str] = []
        params: list[Any] = []
        if owner_user_id:
            clauses.append("owner_user_id = ?")
            params.append(owner_user_id)
        if not include_disabled:
            clauses.append("enabled = 1")
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc, thread_key desc"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_handle(row) for row in rows]

    def count_handles(self, *, owner_user_id: str | None = None) -> int:
        sql = "select count(*) from async_thread_handles"
        params: list[Any] = []
        if owner_user_id:
            sql += " where owner_user_id = ?"
            params.append(owner_user_id)
        with self._connect() as conn:
            return int(conn.execute(sql, params).fetchone()[0])

    def count_recent_events(
        self,
        *,
        thread_key: str | None = None,
        owner_user_id: str | None = None,
    ) -> int:
        sql, params = _event_query(
            "select count(*)",
            thread_key=thread_key,
            owner_user_id=owner_user_id,
        )
        with self._connect() as conn:
            return int(conn.execute(sql, params).fetchone()[0])

    def list_recent_events(
        self,
        *,
        thread_key: str | None = None,
        owner_user_id: str | None = None,
        limit: int = 20,
    ) -> list[AsyncThreadEventLog]:
        limit = max(1, min(int(limit or 20), 50))
        sql, params = _event_query(
            """
            select e.id, e.producer_id, e.event_id, e.thread_key, e.event_type,
                   e.outcome, e.summary, e.detail_json, e.created_at
            """,
            thread_key=thread_key,
            owner_user_id=owner_user_id,
        )
        sql += " order by e.id desc limit ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_event(row) for row in rows]

    def get_event_by_id(
        self,
        *,
        event_id: str,
        owner_user_id: str | None = None,
    ) -> AsyncThreadEventLog | None:
        safe_id = safe_event_id(event_id)
        if not safe_id:
            return None
        sql, params = _event_query(
            """
            select e.id, e.producer_id, e.event_id, e.thread_key, e.event_type,
                   e.outcome, e.summary, e.detail_json, e.created_at
            """,
            owner_user_id=owner_user_id,
        )
        sql += " and e.event_id = ?" if " where " in sql else " where e.event_id = ?"
        params.append(safe_id)
        sql += " order by e.id desc limit 1"
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return _row_to_event(row) if row else None

    def prune_old_rows(
        self,
        *,
        owner_user_id: str,
        event_log_before: str,
        seen_before: str,
        dry_run: bool = True,
    ) -> dict[str, int | bool | str]:
        """Prune old diagnostic/de-dupe rows for one owner.

        Cutoffs are UTC ISO strings from utc_now-style formatting. Lexicographic
        comparison is intentional for this fixed-width timestamp format.
        """
        if not owner_user_id:
            return {"dry_run": dry_run, "event_log": 0, "seen_events": 0, "owner_scoped": False}
        with self._connect() as conn:
            if not dry_run:
                conn.execute("BEGIN IMMEDIATE")
            event_count = int(
                conn.execute(
                    """
                    select count(*)
                    from event_log e
                    join async_thread_handles h on h.thread_key = e.thread_key
                    where h.owner_user_id = ? and e.created_at < ?
                    """,
                    (owner_user_id, event_log_before),
                ).fetchone()[0]
            )
            seen_count = int(
                conn.execute(
                    """
                    select count(*)
                    from seen_events s
                    join async_thread_handles h on h.thread_key = s.thread_key
                    where h.owner_user_id = ? and s.first_seen_at < ?
                    """,
                    (owner_user_id, seen_before),
                ).fetchone()[0]
            )
            if not dry_run:
                conn.execute(
                    """
                    delete from event_log
                    where id in (
                        select e.id
                        from event_log e
                        join async_thread_handles h on h.thread_key = e.thread_key
                        where h.owner_user_id = ? and e.created_at < ?
                    )
                    """,
                    (owner_user_id, event_log_before),
                )
                conn.execute(
                    """
                    delete from seen_events
                    where (producer_id, event_id) in (
                        select s.producer_id, s.event_id
                        from seen_events s
                        join async_thread_handles h on h.thread_key = s.thread_key
                        where h.owner_user_id = ? and s.first_seen_at < ?
                    )
                    """,
                    (owner_user_id, seen_before),
                )
        return {
            "dry_run": dry_run,
            "event_log": event_count,
            "seen_events": seen_count,
            "event_log_before": event_log_before,
            "seen_before": seen_before,
            "owner_scoped": True,
        }

    def set_enabled(self, thread_key: str, enabled: bool) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "update async_thread_handles set enabled = ?, updated_at = ? where thread_key = ?",
                (1 if enabled else 0, utc_now(), thread_key),
            )
            return cur.rowcount > 0

    def rotate_secret(self, thread_key: str) -> AsyncThreadHandle | None:
        new_secret = secrets.token_urlsafe(32)
        with self._connect() as conn:
            cur = conn.execute(
                "update async_thread_handles set secret = ?, updated_at = ? where thread_key = ?",
                (new_secret, utc_now(), thread_key),
            )
            if cur.rowcount == 0:
                return None
        return self.get_handle(thread_key)

    def revoke(self, thread_key: str) -> bool:
        return self.set_enabled(thread_key, False)

    def mark_seen(self, *, producer_id: str, event_id: str, thread_key: str) -> bool:
        """Return True only for the first sighting of an event id."""
        try:
            with self._connect() as conn:
                conn.execute(
                    "insert into seen_events(producer_id, event_id, thread_key, first_seen_at) values (?, ?, ?, ?)",
                    (producer_id, event_id, thread_key, utc_now()),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def has_seen(self, *, producer_id: str, event_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "select 1 from seen_events where producer_id = ? and event_id = ? limit 1",
                (producer_id, event_id),
            ).fetchone()
        return row is not None

    def forget_seen(self, *, producer_id: str, event_id: str) -> None:
        """Remove a seen marker after dispatch failure so producers can retry."""
        with self._connect() as conn:
            conn.execute(
                "delete from seen_events where producer_id = ? and event_id = ?",
                (producer_id, event_id),
            )

    def log_event(
        self,
        *,
        producer_id: str,
        event_id: str,
        outcome: str,
        thread_key: str | None = None,
        event_type: str | None = None,
        summary: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        detail_json = json.dumps(sanitize_event_detail(detail), sort_keys=True, separators=(",", ":"))
        with self._connect() as conn:
            conn.execute(
                """
                insert into event_log(producer_id, event_id, thread_key, event_type, outcome, summary, detail_json, created_at)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    redact_metadata_text(producer_id),
                    safe_event_id(event_id),
                    redact_metadata_text(thread_key) if thread_key else thread_key,
                    redact_metadata_text(event_type) if event_type else event_type,
                    outcome,
                    redact_secret_text(summary or "", max_input_chars=1000, max_output_chars=500),
                    detail_json,
                    utc_now(),
                ),
            )

    def update_workflow_state_from_event(
        self,
        *,
        handle: AsyncThreadHandle,
        data: dict[str, Any] | Mapping[str, Any],
        fields: Mapping[str, str],
    ) -> AsyncThreadWorkflowState | None:
        normalized = normalize_workflow_event(data, fields)
        if normalized is None:
            return None
        now = utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            previous_row = conn.execute(
                "select * from workflow_state where thread_key = ? and workflow_id = ?",
                (handle.thread_key, normalized["workflow_id"]),
            ).fetchone()
            previous = _row_to_workflow_dict(previous_row) if previous_row else None
            state = apply_workflow_transition(
                previous=previous,
                event=normalized,
                policy=handle.workflow_policy,
                now=now,
            )
            created_at = previous["created_at"] if previous else now
            conn.execute(
                """
                insert into workflow_state(
                    thread_key, workflow_id, created_at, updated_at, current_stage,
                    artifact_json, artifact_fingerprint, candidate_json, evidence_json,
                    gates_json, last_event_id, last_event_type, last_summary
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(thread_key, workflow_id) do update set
                    updated_at = excluded.updated_at,
                    current_stage = excluded.current_stage,
                    artifact_json = excluded.artifact_json,
                    artifact_fingerprint = excluded.artifact_fingerprint,
                    candidate_json = excluded.candidate_json,
                    evidence_json = excluded.evidence_json,
                    gates_json = excluded.gates_json,
                    last_event_id = excluded.last_event_id,
                    last_event_type = excluded.last_event_type,
                    last_summary = excluded.last_summary
                """,
                (
                    handle.thread_key,
                    state["workflow_id"],
                    created_at,
                    now,
                    state["stage"],
                    _json_dump(state["artifact"]),
                    state["artifact_fingerprint"],
                    _json_dump(state["candidate"]),
                    _json_dump(state["evidence"]),
                    _json_dump(state["gates"]),
                    state["last_event_id"],
                    state["last_event_type"],
                    state["last_summary"],
                ),
            )
            row = conn.execute(
                "select * from workflow_state where thread_key = ? and workflow_id = ?",
                (handle.thread_key, state["workflow_id"]),
            ).fetchone()
        return _row_to_workflow_state(row) if row else None

    def get_workflow_state(self, *, thread_key: str, workflow_id: str) -> AsyncThreadWorkflowState | None:
        with self._connect() as conn:
            row = conn.execute(
                "select * from workflow_state where thread_key = ? and workflow_id = ?",
                (thread_key, workflow_id),
            ).fetchone()
        return _row_to_workflow_state(row) if row else None

    def list_workflow_states(
        self,
        *,
        thread_key: str | None = None,
        owner_user_id: str | None = None,
        limit: int = 20,
    ) -> list[AsyncThreadWorkflowState]:
        limit = max(1, min(int(limit or 20), 50))
        sql = "select w.* from workflow_state w"
        params: list[Any] = []
        clauses: list[str] = []
        if owner_user_id:
            sql += " join async_thread_handles h on h.thread_key = w.thread_key"
            clauses.append("h.owner_user_id = ?")
            params.append(owner_user_id)
        if thread_key:
            clauses.append("w.thread_key = ?")
            params.append(thread_key)
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by w.updated_at desc, w.workflow_id desc limit ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_workflow_state(row) for row in rows]

    def count_workflow_states(self, *, owner_user_id: str | None = None) -> int:
        sql = "select count(*) from workflow_state w"
        params: list[Any] = []
        if owner_user_id:
            sql += " join async_thread_handles h on h.thread_key = w.thread_key where h.owner_user_id = ?"
            params.append(owner_user_id)
        with self._connect() as conn:
            return int(conn.execute(sql, params).fetchone()[0])

    def create_source_binding(
        self,
        *,
        owner_user_id: str,
        source: str,
        source_ref: Mapping[str, Any],
        listener_thread_key: str,
        producer_id: str = "",
        event_filter: Mapping[str, Any] | None = None,
        transform: Mapping[str, Any] | None = None,
        cursor: Mapping[str, Any] | None = None,
        coalesce: Mapping[str, Any] | None = None,
        delivery_policy: str = "agent_queue",
        status: str = "active",
    ) -> AsyncThreadSourceBinding:
        owner_user_id = str(owner_user_id or "")
        if not owner_user_id:
            raise ValueError("owner_user_id is required")
        handle = self.get_handle(listener_thread_key)
        if handle is None or handle.owner_user_id != owner_user_id:
            raise ValueError("async-thread listener not found")
        source_token = _clean_token(source, default="source")
        producer_token = _clean_token(producer_id or handle.producer_id or source_token, default=source_token)
        delivery = delivery_policy if delivery_policy in SOURCE_BINDING_DELIVERY_POLICIES else "agent_queue"
        normalized_status = status if status in SOURCE_BINDING_STATUSES else "active"
        binding_id = f"athb_{secrets.token_urlsafe(12).replace('-', '').replace('_', '')[:16]}"
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                insert into source_bindings(
                    binding_id, created_at, updated_at, owner_user_id, source, source_ref_json,
                    listener_thread_key, producer_id, filter_json, transform_json, cursor_json,
                    coalesce_json, delivery_policy, status
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    binding_id,
                    now,
                    now,
                    owner_user_id,
                    source_token,
                    _json_dump(_redacted_mapping(source_ref)),
                    handle.thread_key,
                    producer_token,
                    _json_dump(_redacted_mapping(event_filter)),
                    _json_dump(_redacted_mapping(transform)),
                    _json_dump(_redacted_mapping(cursor)),
                    _json_dump(_redacted_mapping(coalesce)),
                    delivery,
                    normalized_status,
                ),
            )
        binding = self.get_source_binding(binding_id=binding_id, owner_user_id=owner_user_id)
        if binding is None:  # pragma: no cover - inserted row should be visible
            raise RuntimeError("source binding insert failed")
        return binding

    def get_source_binding(self, *, binding_id: str, owner_user_id: str | None = None) -> AsyncThreadSourceBinding | None:
        sql = "select * from source_bindings where binding_id = ?"
        params: list[Any] = [str(binding_id or "")]
        if owner_user_id:
            sql += " and owner_user_id = ?"
            params.append(owner_user_id)
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return _row_to_source_binding(row) if row else None

    def list_source_bindings(
        self,
        *,
        owner_user_id: str | None = None,
        source: str | None = None,
        include_retired: bool = False,
        limit: int = 50,
    ) -> list[AsyncThreadSourceBinding]:
        limit = max(1, min(int(limit or 50), 100))
        sql = "select * from source_bindings"
        params: list[Any] = []
        clauses: list[str] = []
        if owner_user_id:
            clauses.append("owner_user_id = ?")
            params.append(owner_user_id)
        if source:
            clauses.append("source = ?")
            params.append(_clean_token(source, default="source"))
        if not include_retired:
            clauses.append("status != 'retired'")
        if clauses:
            sql += " where " + " and ".join(clauses)
        sql += " order by created_at desc, binding_id desc limit ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_source_binding(row) for row in rows]

    def set_source_binding_status(self, *, binding_id: str, owner_user_id: str, status: str) -> bool:
        if status not in SOURCE_BINDING_STATUSES or not owner_user_id:
            return False
        with self._connect() as conn:
            cur = conn.execute(
                """
                update source_bindings
                set status = ?, updated_at = ?
                where binding_id = ? and owner_user_id = ?
                """,
                (status, utc_now(), binding_id, owner_user_id),
            )
            return cur.rowcount > 0

    def source_binding_compatibility(self, binding: AsyncThreadSourceBinding) -> dict[str, Any]:
        handle = self.get_handle(binding.listener_thread_key)
        base = {
            "listenerThreadKey": binding.listener_thread_key,
            "bindingStatus": binding.status,
            "valid": False,
            "failClosed": True,
            "reason": "unknown",
        }
        if binding.status != "active":
            return {**base, "reason": f"binding_{binding.status}"}
        if handle is None:
            return {**base, "reason": "listener_missing"}
        base.update(
            {
                "listenerEnabled": bool(handle.enabled),
                "listenerProducerId": redact_metadata_text(handle.producer_id),
                "listenerAllowedEventTypes": [redact_metadata_text(item) for item in handle.allowed_event_types],
            }
        )
        if handle.owner_user_id != binding.owner_user_id:
            return {**base, "reason": "listener_not_owned"}
        if not handle.enabled:
            return {**base, "reason": "listener_disabled"}
        if binding.producer_id != handle.producer_id:
            return {**base, "reason": "producer_mismatch"}
        required_events = _source_binding_event_types(binding.event_filter)
        missing = sorted(set(required_events).difference(handle.allowed_event_types)) if handle.allowed_event_types else []
        if missing:
            return {**base, "reason": "disallowed_event_types", "missingEventTypes": [redact_metadata_text(item) for item in missing]}
        return {**base, "valid": True, "failClosed": False, "reason": "ok"}

    def latest_terminal_event(self, *, thread_key: str) -> AsyncThreadEventLog | None:
        return self.latest_terminal_events(thread_keys=[thread_key]).get(thread_key)

    def latest_terminal_events(self, *, thread_keys: Iterable[str]) -> dict[str, AsyncThreadEventLog]:
        keys = [str(key) for key in dict.fromkeys(thread_keys) if str(key)]
        if not keys:
            return {}
        placeholders = ",".join("?" for _ in keys)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                select e.*
                from event_log e
                join (
                    select thread_key, max(id) as latest_id
                    from event_log
                    where thread_key in ({placeholders})
                      and detail_json like '%"terminal_event":true%'
                    group by thread_key
                ) latest on latest.latest_id = e.id
                order by e.id desc
                """,
                keys,
            ).fetchall()
        return {str(row["thread_key"]): _row_to_event(row) for row in rows}

    def list_stale_terminal_handles(self, *, owner_user_id: str | None = None) -> list[tuple[AsyncThreadHandle, AsyncThreadEventLog]]:
        handles = self.list_handles(owner_user_id=owner_user_id, include_disabled=False)
        terminal_by_thread = self.latest_terminal_events(thread_keys=[handle.thread_key for handle in handles])
        stale: list[tuple[AsyncThreadHandle, AsyncThreadEventLog]] = []
        for handle in handles:
            event = terminal_by_thread.get(handle.thread_key)
            if event is None:
                continue
            if event.detail.get("terminal_action") in {"warn_only", "shared_listener_kept_enabled"}:
                stale.append((handle, event))
        return stale


def _row_to_handle(row: sqlite3.Row) -> AsyncThreadHandle:
    return AsyncThreadHandle(
        thread_key=row["thread_key"],
        source=json.loads(row["source_json"]),
        producer_id=row["producer_id"],
        secret=row["secret"],
        policy=row["policy"],
        enabled=bool(row["enabled"]),
        label=row["label"],
        allowed_event_types=tuple(json.loads(row["allowed_event_types_json"] or "[]")),
        session_key=row["session_key"],
        session_id=row["session_id"],
        owner_user_id=row["owner_user_id"],
        ack_mode=row["ack_mode"] or "none",
        debounce_seconds=int(row["debounce_seconds"] or 0),
        workflow_policy=WorkflowPolicy.from_json(row["workflow_policy_json"] if "workflow_policy_json" in row.keys() else "{}"),
        continuation_policy=ContinuationPolicy.from_json(row["continuation_policy_json"] if "continuation_policy_json" in row.keys() else "{}"),
        lifecycle_policy=LifecyclePolicy.from_json(row["lifecycle_policy_json"] if "lifecycle_policy_json" in row.keys() else "{}"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_source_binding(row: sqlite3.Row) -> AsyncThreadSourceBinding:
    return AsyncThreadSourceBinding(
        binding_id=row["binding_id"],
        owner_user_id=row["owner_user_id"],
        source=row["source"],
        source_ref=_parse_json_object(row["source_ref_json"]),
        listener_thread_key=row["listener_thread_key"],
        producer_id=row["producer_id"],
        event_filter=_parse_json_object(row["filter_json"]),
        transform=_parse_json_object(row["transform_json"]),
        cursor=_parse_json_object(row["cursor_json"]),
        coalesce=_parse_json_object(row["coalesce_json"]),
        delivery_policy=row["delivery_policy"] or "agent_queue",
        status=row["status"] or "active",
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_event(row: sqlite3.Row) -> AsyncThreadEventLog:
    return AsyncThreadEventLog(
        id=int(row["id"]),
        producer_id=row["producer_id"],
        event_id=row["event_id"],
        thread_key=row["thread_key"] or "",
        event_type=row["event_type"] or "",
        outcome=row["outcome"],
        summary=row["summary"] or "",
        created_at=row["created_at"],
        detail=_parse_detail_json(row["detail_json"]),
    )


def _row_to_workflow_state(row: sqlite3.Row) -> AsyncThreadWorkflowState:
    return AsyncThreadWorkflowState(
        thread_key=row["thread_key"],
        workflow_id=row["workflow_id"],
        stage=row["current_stage"] or "",
        artifact=_parse_json_object(row["artifact_json"]),
        artifact_fingerprint=row["artifact_fingerprint"] or "",
        candidate=_parse_json_object(row["candidate_json"]),
        evidence=_parse_json_object(row["evidence_json"]),
        gates=_parse_json_object(row["gates_json"]),
        last_event_id=row["last_event_id"] or "",
        last_event_type=row["last_event_type"] or "",
        last_summary=row["last_summary"] or "",
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_workflow_dict(row: sqlite3.Row) -> dict[str, Any]:
    state = _row_to_workflow_state(row)
    return {
        "workflow_id": state.workflow_id,
        "stage": state.stage,
        "artifact": state.artifact,
        "artifact_fingerprint": state.artifact_fingerprint,
        "candidate": state.candidate,
        "evidence": state.evidence,
        "gates": state.gates,
        "last_event_id": state.last_event_id,
        "last_event_type": state.last_event_type,
        "last_summary": state.last_summary,
        "created_at": state.created_at,
        "updated_at": state.updated_at,
    }


def _json_dump(value: Any) -> str:
    return json.dumps(value if isinstance(value, (dict, list)) else {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _redacted_mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    cleaned = sanitize_untrusted_value(dict(value))
    return cleaned if isinstance(cleaned, dict) else {}


def _source_binding_event_types(event_filter: Mapping[str, Any]) -> tuple[str, ...]:
    for key in ("eventTypes", "event_types", "allowedEventTypes", "allowed_event_types"):
        raw = event_filter.get(key) if isinstance(event_filter, Mapping) else None
        if isinstance(raw, str):
            return tuple(item.strip() for item in raw.split(",") if item.strip())
        if isinstance(raw, Iterable) and not isinstance(raw, (bytes, bytearray, str)):
            return tuple(str(item).strip() for item in raw if str(item).strip())
    return ()


def _parse_json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _event_query(
    select_clause: str,
    *,
    thread_key: str | None = None,
    owner_user_id: str | None = None,
) -> tuple[str, list[Any]]:
    sql = f"{select_clause} from event_log e"
    params: list[Any] = []
    clauses: list[str] = []
    if owner_user_id:
        sql += " join async_thread_handles h on h.thread_key = e.thread_key"
        clauses.append("h.owner_user_id = ?")
        params.append(owner_user_id)
    if thread_key:
        clauses.append("e.thread_key = ?")
        params.append(thread_key)
    if clauses:
        sql += " where " + " and ".join(clauses)
    return sql, params


def sanitize_event_detail(detail: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(detail, dict):
        return {}
    sanitized: dict[str, Any] = {}
    for key, value in detail.items():
        key_text = str(key)
        if key_text not in SAFE_DETAIL_KEYS or UNSAFE_DETAIL_KEY_RE.search(key_text):
            continue
        if key_text == "continuation_policy" and isinstance(value, dict):
            sanitized[key_text] = _sanitize_continuation_policy(value)
            continue
        if key_text == "lifecycle_policy" and isinstance(value, dict):
            sanitized[key_text] = _sanitize_lifecycle_policy(value)
            continue
        cleaned = _sanitize_detail_value(value)
        if cleaned is not None:
            sanitized[key_text] = cleaned
    return sanitized


def _sanitize_continuation_policy(value: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key in ("maxTurns", "maxToolCalls", "timeoutSeconds"):
        try:
            safe[key] = int(value.get(key, 0))
        except (TypeError, ValueError):
            safe[key] = 0
    toolsets = value.get("toolsets", [])
    if isinstance(toolsets, list):
        safe["toolsets"] = [redact_metadata_text(str(item))[:40] for item in toolsets[:8] if str(item).strip()]
    else:
        safe["toolsets"] = []
    safe["failClosedWithoutCoreBounds"] = bool(value.get("failClosedWithoutCoreBounds", False))
    safe["coreEnforced"] = bool(value.get("coreEnforced", False))
    return safe


def _sanitize_lifecycle_policy(value: dict[str, Any]) -> dict[str, Any]:
    def _safe_list(key: str) -> list[str]:
        items = value.get(key, [])
        if not isinstance(items, list):
            return []
        return [redact_metadata_text(str(item))[:80] for item in items[:16] if str(item).strip()]

    return {
        "terminalEventTypes": _safe_list("terminalEventTypes"),
        "terminalStages": _safe_list("terminalStages"),
        "autoRetireOnTerminal": bool(value.get("autoRetireOnTerminal", False)),
        "sharedListener": bool(value.get("sharedListener", False)),
    }


def safe_session_key_hash(session_key: str | None) -> str:
    if not session_key:
        return ""
    return hashlib.sha256(session_key.encode("utf-8")).hexdigest()[:12]


def _sanitize_detail_value(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return round(value, 6) if math.isfinite(value) else None
    text = _redact_detail_text(str(value)[:1000])[:200]
    return text


def _redact_detail_text(value: str) -> str:
    return redact_secret_text(value, max_input_chars=1000, max_output_chars=200)


def _parse_detail_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _clean_token(value: str, *, default: str) -> str:
    cleaned = "".join(ch for ch in str(value or "").strip() if ch.isalnum() or ch in {"-", "_", ".", ":"})
    return cleaned[:100] or default


def _clean_event_type(value: str) -> str:
    return _clean_token(value, default="")
