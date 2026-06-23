"""Dynamic Workflows resource-finalizer handlers for Async Threads.

This module intentionally does not import ``hermes_workflows``. Dynamic
Workflows owns the generic registry/runner contract; ATH owns concrete ATH
cleanup actions. Hosts can register ``AthFinalizerAdapter.retire_listener`` with
``ResourceFinalizerRegistry`` under ``ath.listener.retire``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .registry import AsyncThreadRegistry
from .secrets import remove_secret_artifact, secret_root_from_config

ATH_LISTENER_RETIRE_ACTION = "ath.listener.retire"
ATH_FINALIZER_ACTIONS: tuple[str, ...] = (ATH_LISTENER_RETIRE_ACTION,)


@dataclass(frozen=True)
class AthFinalizerAdapter:
    """Callable ATH cleanup adapter for Dynamic Workflows finalizers.

    Parameters are intentionally ATH-native. The adapter returns the bounded
    ``{"ok", "summary", "evidence"}`` shape expected by Dynamic Workflows but
    does not depend on that package at import time.
    """

    registry: AsyncThreadRegistry
    secret_root: str | Path | None = None
    owner_user_id: str = ""

    def __call__(self, context: dict[str, Any]) -> dict[str, Any]:
        return self.retire_listener(context)

    def retire_listener(self, context: dict[str, Any]) -> dict[str, Any]:
        context_map = _mapping(context)
        finalizer = _mapping(context_map.get("finalizer"))
        action = str(finalizer.get("action") or "")
        if action and action != ATH_LISTENER_RETIRE_ACTION:
            return _failed(f"unsupported ATH finalizer action {action!r}", action=action)

        thread_key = _thread_key_from_context(context_map)
        if not thread_key:
            return _failed("ATH listener finalizer requires resource.handle.threadKey", action=ATH_LISTENER_RETIRE_ACTION)

        handle = self.registry.get_handle(thread_key)
        if handle is None:
            # If this adapter is owner-scoped, an absent registry row means
            # ownership cannot be established. Leave any stale local artifacts in
            # place for an unscoped/admin cleanup path instead of deleting files
            # for a possibly foreign listener.
            removed_secret_material = (
                False if self.owner_user_id else remove_secret_artifact(thread_key, root=self.secret_root)
            )
            return {
                "ok": True,
                "summary": "ATH listener already absent",
                "evidence": [
                    {
                        "kind": "ath.listener.retire",
                        "threadKey": thread_key,
                        "found": False,
                        "wasEnabled": False,
                        "enabledAfter": False,
                        "retired": False,
                        "secretMaterialRemoved": removed_secret_material,
                    }
                ],
            }

        if self.owner_user_id and handle.owner_user_id != self.owner_user_id:
            return _failed(
                "ATH listener owner does not match finalizer adapter owner",
                action=ATH_LISTENER_RETIRE_ACTION,
                evidence={
                    "kind": "ath.listener.retire",
                    "threadKey": thread_key,
                    "found": True,
                    "ownerMatched": False,
                    "wasEnabled": bool(handle.enabled),
                    "secretMaterialRemoved": False,
                },
            )

        removed_secret_material = remove_secret_artifact(thread_key, root=self.secret_root)
        was_enabled = bool(handle.enabled)
        retired = self.registry.set_enabled(thread_key, False) if was_enabled else True
        after = self.registry.get_handle(thread_key)
        enabled_after = bool(after.enabled) if after is not None else False
        ok = bool(retired and not enabled_after)
        summary = "ATH listener retired" if was_enabled else "ATH listener already retired"
        evidence = {
            "kind": "ath.listener.retire",
            "threadKey": thread_key,
            "found": True,
            "producerId": handle.producer_id,
            "allowedEventTypes": list(handle.allowed_event_types),
            "wasEnabled": was_enabled,
            "retired": bool(retired),
            "enabledAfter": enabled_after,
            "secretMaterialRemoved": removed_secret_material,
        }
        if ok:
            return {"ok": True, "summary": summary, "evidence": [evidence]}
        return _failed("ATH listener could not be retired", action=ATH_LISTENER_RETIRE_ACTION, evidence=evidence)


def build_ath_finalizer_adapter(
    *,
    registry: AsyncThreadRegistry | None = None,
    config: Any | None = None,
    secret_root: str | Path | None = None,
    owner_user_id: str = "",
) -> AthFinalizerAdapter:
    """Build an ATH finalizer adapter from explicit registry or plugin config."""

    if registry is not None:
        resolved_registry = registry
    else:
        # ``async_threads.adapter`` depends on Hermes gateway modules. Keep that
        # import lazy so lightweight producer scripts can import ``async_threads``
        # or ``async_threads.finalizers`` for help/metadata without requiring the
        # gateway package on sys.path.
        from .adapter import registry_from_config

        resolved_registry = registry_from_config(config)
    resolved_secret_root = secret_root if secret_root is not None else secret_root_from_config(config)
    return AthFinalizerAdapter(
        registry=resolved_registry,
        secret_root=resolved_secret_root,
        owner_user_id=owner_user_id,
    )


def register_ath_finalizers(
    finalizer_registry: Any,
    *,
    registry: AsyncThreadRegistry | None = None,
    config: Any | None = None,
    secret_root: str | Path | None = None,
    owner_user_id: str = "",
    replace: bool = False,
) -> Any:
    """Register ATH cleanup actions on a Dynamic Workflows-style registry.

    ``finalizer_registry`` only needs a ``register(action, handler, replace=...)``
    method. This avoids a hard dependency on ``hermes_workflows`` while still
    giving hosts a one-line integration helper.
    """

    adapter = build_ath_finalizer_adapter(
        registry=registry,
        config=config,
        secret_root=secret_root,
        owner_user_id=owner_user_id,
    )
    finalizer_registry.register(ATH_LISTENER_RETIRE_ACTION, adapter.retire_listener, replace=replace)
    return finalizer_registry


def _thread_key_from_context(context: Mapping[str, Any]) -> str:
    resource = _mapping(context.get("resource"))
    finalizer = _mapping(context.get("finalizer"))
    handle = _mapping(resource.get("handle"))
    args = _mapping(finalizer.get("args"))
    for value in (
        handle.get("threadKey"),
        handle.get("thread_key"),
        resource.get("threadKey"),
        resource.get("thread_key"),
        args.get("threadKey"),
        args.get("thread_key"),
    ):
        if isinstance(value, str):
            cleaned = _safe_thread_key(value)
            if cleaned:
                return cleaned
    return ""


def _safe_thread_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) > 128:
        return ""
    if any(not (ch.isalnum() or ch in {"-", "_"}) for ch in text):
        return ""
    return text


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _failed(message: str, *, action: str, evidence: Mapping[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"ok": False, "summary": message, "error": message}
    if evidence:
        payload["evidence"] = [dict(evidence)]
    elif action:
        payload["evidence"] = [{"kind": action, "status": "failed"}]
    return payload
