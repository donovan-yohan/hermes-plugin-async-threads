"""SQLite registry for async-thread handles and event de-dupe."""

from __future__ import annotations

import json
import secrets
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1


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


class AsyncThreadRegistry:
    """Durable listener registry.

    The DB intentionally stores producer secrets for the MVP because the event
    receiver needs to validate per-handle HMAC signatures. Command surfaces only
    reveal the generated secret at creation time.
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
                    policy text not null default 'agent_queue'
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
                    created_at text not null
                );
                """
            )
            conn.execute(
                "insert or replace into meta(key, value) values('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

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
    ) -> AsyncThreadHandle:
        producer_id = _clean_token(producer_id, default="default")
        policy = policy if policy in {"agent_queue", "direct"} else "agent_queue"
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
                    allowed_event_types_json, policy
                ) values (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    def set_enabled(self, thread_key: str, enabled: bool) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "update async_thread_handles set enabled = ?, updated_at = ? where thread_key = ?",
                (1 if enabled else 0, utc_now(), thread_key),
            )
            return cur.rowcount > 0

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
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                insert into event_log(producer_id, event_id, thread_key, event_type, outcome, summary, created_at)
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    producer_id,
                    event_id,
                    thread_key,
                    event_type,
                    outcome,
                    (summary or "")[:500],
                    utc_now(),
                ),
            )


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
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _clean_token(value: str, *, default: str) -> str:
    cleaned = "".join(ch for ch in str(value or "").strip() if ch.isalnum() or ch in {"-", "_", ".", ":"})
    return cleaned[:100] or default


def _clean_event_type(value: str) -> str:
    return _clean_token(value, default="")
