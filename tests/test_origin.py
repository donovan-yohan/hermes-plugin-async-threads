import json
from types import SimpleNamespace

from async_threads import origin as origin_module
from async_threads.listeners import create_listener
from async_threads.origin import OriginIndex, remember_gateway_origin, resolve_current_origin
from async_threads.registry import AsyncThreadRegistry
from gateway.config import Platform
from gateway.session import SessionSource


class FakeStore:
    def __init__(self, entry_by_session_id=None, entry_by_key=None):
        self.entry_by_session_id = entry_by_session_id or {}
        self.entry_by_key = entry_by_key or {}

    def lookup_by_session_id(self, session_id):
        return self.entry_by_session_id.get(session_id)

    def get_session_by_key(self, session_key):
        return self.entry_by_key.get(session_key)


def _discord_source(**overrides):
    data = {
        "platform": Platform.DISCORD,
        "chat_id": "channel-1",
        "chat_type": "channel",
        "thread_id": "thread-1",
        "parent_chat_id": "parent-1",
        "guild_id": "guild-1",
        "user_id": "user-1",
        "user_name": "Kyle",
        "message_id": "msg-1",
    }
    data.update(overrides)
    return SessionSource(**data)


def _entry(source=None, session_key="agent:main:discord:channel:channel-1:thread-1", session_id="sid-1"):
    return SimpleNamespace(origin=source or _discord_source(), session_key=session_key, session_id=session_id)


def test_resolves_current_origin_from_explicit_trusted_source():
    source = _discord_source()

    resolution = resolve_current_origin(source=source, session_id="sid-explicit", session_key="key-explicit")

    assert resolution.ok is True
    assert resolution.source_kind == "explicit"
    assert resolution.session_id == "sid-explicit"
    assert resolution.session_key == "key-explicit"
    assert resolution.owner_user_id == "user-1"
    assert resolution.source_dict["platform"] == "discord"
    assert resolution.source_dict["chat_id"] == "channel-1"
    assert resolution.source_dict["thread_id"] == "thread-1"
    assert resolution.source_dict["parent_chat_id"] == "parent-1"
    assert resolution.source_dict["guild_id"] == "guild-1"


def test_resolves_current_origin_from_session_store_by_session_id():
    source = _discord_source(thread_id="review-thread")
    store = FakeStore(entry_by_session_id={"sid-store": _entry(source=source, session_id="sid-store", session_key="key-store")})

    resolution = resolve_current_origin(session_id="sid-store", session_store=store)

    assert resolution.ok is True
    assert resolution.source_kind == "session_store"
    assert resolution.session_id == "sid-store"
    assert resolution.session_key == "key-store"
    assert resolution.source_dict["thread_id"] == "review-thread"
    assert resolution.owner_user_id == "user-1"


def test_resolves_current_origin_from_session_store_by_session_key():
    store = FakeStore(entry_by_key={"key-store": _entry(session_id="sid-key", session_key="key-store")})

    resolution = resolve_current_origin(session_key="key-store", session_store=store)

    assert resolution.ok is True
    assert resolution.source_kind == "session_store"
    assert resolution.session_id == "sid-key"
    assert resolution.session_key == "key-store"
    assert resolution.source_dict["platform"] == "discord"


def test_resolves_current_origin_from_profile_sessions_json(tmp_path):
    sessions_file = tmp_path / "sessions.json"
    sessions_file.write_text(
        json.dumps(
            {
                "key-old": {"session_key": "key-old", "session_id": "sid-old", "origin": _discord_source(thread_id="old").to_dict()},
                "key-current": {
                    "session_key": "key-current",
                    "session_id": "sid-current",
                    "origin": _discord_source(thread_id="thread-current", parent_chat_id="parent-current").to_dict(),
                },
            }
        ),
        encoding="utf-8",
    )

    resolution = resolve_current_origin(session_id="sid-current", sessions_file=sessions_file)

    assert resolution.ok is True
    assert resolution.source_kind == "sessions_file"
    assert resolution.session_id == "sid-current"
    assert resolution.session_key == "key-current"
    assert resolution.source_dict["thread_id"] == "thread-current"
    assert resolution.source_dict["parent_chat_id"] == "parent-current"


def test_explicit_session_id_ignores_unrelated_session_context_key(monkeypatch, tmp_path):
    monkeypatch.setattr(
        origin_module,
        "_session_env",
        lambda name: {
            "HERMES_SESSION_ID": "env-session",
            "HERMES_SESSION_KEY": "env-key",
            "HERMES_SESSION_PLATFORM": "discord",
            "HERMES_SESSION_CHAT_ID": "env-channel",
        }.get(name, ""),
    )
    sessions_file = tmp_path / "sessions.json"
    sessions_file.write_text(
        json.dumps(
            {
                "key-current": {
                    "session_key": "key-current",
                    "session_id": "sid-current",
                    "origin": _discord_source(thread_id="thread-current").to_dict(),
                },
            }
        ),
        encoding="utf-8",
    )

    resolution = resolve_current_origin(session_id="sid-current", sessions_file=sessions_file)

    assert resolution.ok is True
    assert resolution.source_kind == "sessions_file"
    assert resolution.session_id == "sid-current"
    assert resolution.session_key == "key-current"
    assert resolution.source_dict["thread_id"] == "thread-current"


def test_explicit_session_key_ignores_unrelated_session_context_id(monkeypatch, tmp_path):
    monkeypatch.setattr(
        origin_module,
        "_session_env",
        lambda name: {
            "HERMES_SESSION_ID": "env-session",
            "HERMES_SESSION_KEY": "env-key",
            "HERMES_SESSION_PLATFORM": "discord",
            "HERMES_SESSION_CHAT_ID": "env-channel",
        }.get(name, ""),
    )
    store = FakeStore(entry_by_session_id={"env-session": _entry(session_id="env-session", session_key="env-key")})

    resolution = resolve_current_origin(
        session_key="missing-key",
        session_store=store,
        sessions_file=tmp_path / "none.json",
        origin_index=OriginIndex(),
    )

    assert resolution.ok is False
    assert resolution.public_error()["error"] == "source_unavailable"


def test_explicit_session_id_and_key_resolve_matching_store_entry(tmp_path):
    store = FakeStore(
        entry_by_session_id={"sid-requested": _entry(session_id="sid-requested", session_key="key-requested")},
        entry_by_key={"key-requested": _entry(session_id="sid-requested", session_key="key-requested")},
    )

    resolution = resolve_current_origin(
        session_id="sid-requested",
        session_key="key-requested",
        session_store=store,
        sessions_file=tmp_path / "none.json",
        origin_index=OriginIndex(),
    )

    assert resolution.ok is True
    assert resolution.session_id == "sid-requested"
    assert resolution.session_key == "key-requested"


def test_explicit_session_id_and_key_must_match_same_store_entry(tmp_path):
    store = FakeStore(
        entry_by_session_id={"sid-requested": _entry(session_id="sid-requested", session_key="different-key")},
        entry_by_key={"key-requested": _entry(session_id="different-sid", session_key="key-requested")},
    )

    resolution = resolve_current_origin(
        session_id="sid-requested",
        session_key="key-requested",
        session_store=store,
        sessions_file=tmp_path / "none.json",
        origin_index=OriginIndex(),
    )

    assert resolution.ok is False
    assert resolution.public_error()["error"] == "source_unavailable"


def test_origin_index_requires_matching_explicit_session_id_and_key():
    index = OriginIndex()
    index.remember(source=_discord_source(thread_id="wrong"), session_id="sid-requested", session_key="different-key")
    index.remember(source=_discord_source(thread_id="also-wrong"), session_id="different-sid", session_key="key-requested")

    resolution = resolve_current_origin(
        session_id="sid-requested",
        session_key="key-requested",
        origin_index=index,
        sessions_file="/no/such/file",
    )

    assert resolution.ok is False
    assert resolution.public_error()["error"] == "source_unavailable"


def test_explicit_missing_session_does_not_fall_back_to_session_context(monkeypatch, tmp_path):
    monkeypatch.setattr(
        origin_module,
        "_session_env",
        lambda name: {
            "HERMES_SESSION_ID": "env-session",
            "HERMES_SESSION_KEY": "env-key",
            "HERMES_SESSION_PLATFORM": "discord",
            "HERMES_SESSION_CHAT_ID": "env-channel",
            "HERMES_SESSION_USER_ID": "env-user",
        }.get(name, ""),
    )

    resolution = resolve_current_origin(
        session_id="missing",
        session_store=FakeStore(),
        sessions_file=tmp_path / "none.json",
        origin_index=OriginIndex(),
    )

    assert resolution.ok is False
    assert resolution.public_error()["error"] == "source_unavailable"


def test_missing_source_fails_closed_without_creating_handle(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")

    resolution = resolve_current_origin(session_id="missing", session_store=FakeStore(), sessions_file=tmp_path / "none.json", origin_index=OriginIndex())

    assert resolution.ok is False
    assert resolution.public_error()["error"] == "source_unavailable"
    assert registry.list_handles(owner_user_id="user-1") == []


def test_local_or_cli_context_is_not_gateway_routable():
    resolution = resolve_current_origin(
        source={"platform": "local", "chat_id": "terminal", "user_id": "user-1"},
        session_id="sid-local",
    )

    assert resolution.ok is False
    assert resolution.public_error()["error"] == "source_unavailable"


def test_does_not_trust_user_supplied_source_in_tool_arguments():
    user_args = {
        "source": {
            "platform": "discord",
            "chat_id": "attacker-channel",
            "thread_id": "attacker-thread",
            "user_id": "attacker",
        },
        "session_id": "missing",
    }

    resolution = resolve_current_origin(
        trusted_context=user_args,
        session_store=FakeStore(),
        origin_index=OriginIndex(),
    )

    assert resolution.ok is False
    assert resolution.public_error()["error"] == "source_unavailable"


def test_does_not_trust_user_supplied_source_aliases_in_tool_arguments():
    for key in ("gateway_source", "session_source"):
        user_args = {
            key: {
                "platform": "discord",
                "chat_id": "attacker-channel",
                "thread_id": "attacker-thread",
                "user_id": "attacker",
            },
            "session_id": "missing",
        }

        resolution = resolve_current_origin(
            trusted_context=user_args,
            session_store=FakeStore(),
            origin_index=OriginIndex(),
        )

        assert resolution.ok is False
        assert resolution.public_error()["error"] == "source_unavailable"


def test_local_platform_enum_mapping_source_is_not_gateway_routable():
    for platform in (Platform.LOCAL, Platform.API_SERVER):
        resolution = resolve_current_origin(
            source={"platform": platform, "chat_id": "terminal", "user_id": "user-1"},
            session_id="sid-local",
        )

        assert resolution.ok is False
        assert resolution.public_error()["error"] == "source_unavailable"


def test_origin_resolution_can_feed_shared_listener_service(tmp_path):
    registry = AsyncThreadRegistry(tmp_path / "ath.sqlite3")
    store = FakeStore(entry_by_session_id={"sid-store": _entry(session_id="sid-store", session_key="key-store")})
    resolution = resolve_current_origin(session_id="sid-store", session_store=store)
    assert resolution.ok is True

    result = create_listener(
        registry=registry,
        source=resolution.source,
        producer_id="demo-ci",
        allowed_event_types=["demo.finished"],
        session_key=resolution.session_key,
        session_id=resolution.session_id,
        owner_user_id=resolution.owner_user_id,
    )

    handle = registry.get_handle(result.thread_key)
    assert handle is not None
    assert handle.source["chat_id"] == "channel-1"
    assert handle.source["thread_id"] == "thread-1"
    assert handle.source["parent_chat_id"] == "parent-1"
    assert handle.session_key == "key-store"
    assert handle.session_id == "sid-store"
    assert handle.owner_user_id == "user-1"


def test_remember_gateway_origin_indexes_only_trusted_event_source():
    source = _discord_source()
    index = OriginIndex()
    gateway = SimpleNamespace(
        config=SimpleNamespace(group_sessions_per_user=True, thread_sessions_per_user=False),
        session_store=FakeStore(),
    )

    remembered = remember_gateway_origin(event=SimpleNamespace(source=source), gateway=gateway, origin_index=index)
    assert remembered.ok is True
    assert remembered.session_key

    resolution = resolve_current_origin(session_key=remembered.session_key, origin_index=index, sessions_file="/no/such/file")
    assert resolution.ok is True
    assert resolution.source_kind == "gateway_event"
    assert resolution.source_dict["thread_id"] == "thread-1"
