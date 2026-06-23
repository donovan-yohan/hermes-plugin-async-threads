from types import SimpleNamespace

from async_threads import ATH_LISTENER_RETIRE_ACTION, register_ath_finalizers
from async_threads.finalizers import AthFinalizerAdapter, build_ath_finalizer_adapter
from async_threads.registry import AsyncThreadRegistry
from async_threads.secrets import write_secret_artifact


def _registry(tmp_path):
    return AsyncThreadRegistry(tmp_path / "ath.sqlite3")


def _handle(registry, *, owner_user_id="owner-1"):
    return registry.create_handle(
        source={"platform": "discord", "chat_id": "channel-1", "thread_id": "thread-1", "user_id": owner_user_id},
        producer_id="qa-producer",
        label="QA listener",
        allowed_event_types=["qa.goal.finished"],
        owner_user_id=owner_user_id,
    )


def _context(thread_key, *, action=ATH_LISTENER_RETIRE_ACTION):
    return {
        "run_id": "run-1",
        "loop_name": "qa-loop",
        "trigger": "success",
        "resource": {
            "id": "listener-resource",
            "kind": "ath.listener",
            "handle": {"threadKey": thread_key},
        },
        "finalizer": {"id": "retire-listener", "action": action, "policy": "required"},
    }


def _enabled(registry, thread_key):
    handle = registry.get_handle(thread_key)
    assert handle is not None
    return handle.enabled


def test_ath_finalizer_retires_enabled_listener_and_removes_secret(tmp_path):
    registry = _registry(tmp_path)
    handle = _handle(registry)
    artifact = write_secret_artifact(handle, root=tmp_path / "secrets")
    assert artifact.secret_file.exists()
    assert artifact.contract_file.exists()

    adapter = AthFinalizerAdapter(registry=registry, secret_root=tmp_path / "secrets")
    result = adapter.retire_listener(_context(handle.thread_key))

    after = registry.get_handle(handle.thread_key)
    assert result["ok"] is True
    assert result["summary"] == "ATH listener retired"
    assert after is not None
    assert after.enabled is False
    assert not artifact.secret_file.exists()
    assert not artifact.contract_file.exists()
    evidence = result["evidence"][0]
    assert evidence["threadKey"] == handle.thread_key
    assert evidence["producerId"] == "qa-producer"
    assert evidence["wasEnabled"] is True
    assert evidence["enabledAfter"] is False
    assert evidence["secretMaterialRemoved"] is True
    assert handle.secret not in str(result)


def test_ath_finalizer_is_idempotent_for_already_disabled_or_absent_listener(tmp_path):
    registry = _registry(tmp_path)
    handle = _handle(registry)
    adapter = AthFinalizerAdapter(registry=registry, secret_root=tmp_path / "secrets")

    first = adapter(_context(handle.thread_key))
    second = adapter(_context(handle.thread_key))
    absent = adapter(_context("ath_missinglistener"))

    assert first["ok"] is True
    assert first["summary"] == "ATH listener retired"
    assert second["ok"] is True
    assert second["summary"] == "ATH listener already retired"
    assert second["evidence"][0]["wasEnabled"] is False
    assert absent["ok"] is True
    assert absent["summary"] == "ATH listener already absent"
    assert absent["evidence"][0]["found"] is False


def test_ath_finalizer_fails_closed_without_thread_key_or_on_owner_mismatch(tmp_path):
    registry = _registry(tmp_path)
    handle = _handle(registry, owner_user_id="owner-1")
    artifact = write_secret_artifact(handle, root=tmp_path / "secrets")
    adapter = AthFinalizerAdapter(registry=registry, secret_root=tmp_path / "secrets", owner_user_id="owner-2")

    missing = adapter.retire_listener({"resource": {"handle": {}}, "finalizer": {"action": ATH_LISTENER_RETIRE_ACTION}})
    mismatch = adapter.retire_listener(_context(handle.thread_key))

    assert missing["ok"] is False
    assert "requires resource.handle.threadKey" in missing["error"]
    assert mismatch["ok"] is False
    assert "owner does not match" in mismatch["error"]
    assert mismatch["evidence"][0]["secretMaterialRemoved"] is False
    assert artifact.secret_file.exists()
    assert artifact.contract_file.exists()
    assert _enabled(registry, handle.thread_key) is True


def test_ath_finalizer_rejects_unsupported_action_without_mutation(tmp_path):
    registry = _registry(tmp_path)
    handle = _handle(registry)
    adapter = AthFinalizerAdapter(registry=registry, secret_root=tmp_path / "secrets")

    result = adapter.retire_listener(_context(handle.thread_key, action="relay.session.close"))

    assert result["ok"] is False
    assert "unsupported ATH finalizer action" in result["error"]
    assert _enabled(registry, handle.thread_key) is True


def test_register_ath_finalizers_uses_dynamic_workflows_style_registry_without_dependency(tmp_path):
    registry = _registry(tmp_path)
    handle = _handle(registry)
    calls = []

    class FakeFinalizerRegistry:
        def register(self, action, handler, *, replace=False):
            calls.append((action, replace, handler))
            return self

    finalizers = FakeFinalizerRegistry()
    returned = register_ath_finalizers(finalizers, registry=registry, secret_root=tmp_path / "secrets", replace=True)

    assert returned is finalizers
    assert calls[0][0] == ATH_LISTENER_RETIRE_ACTION
    assert calls[0][1] is True
    result = calls[0][2](_context(handle.thread_key))
    assert result["ok"] is True
    assert _enabled(registry, handle.thread_key) is False


def test_build_ath_finalizer_adapter_from_config(tmp_path):
    config = SimpleNamespace(extra={"registry_path": str(tmp_path / "ath.sqlite3"), "secret_root": str(tmp_path / "secrets")})
    adapter = build_ath_finalizer_adapter(config=config)
    handle = _handle(adapter.registry)

    result = adapter(_context(handle.thread_key))

    assert result["ok"] is True
    assert _enabled(adapter.registry, handle.thread_key) is False
