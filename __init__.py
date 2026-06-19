"""Hermes async-thread continuation plugin."""

from __future__ import annotations

try:  # Hermes loads directory plugins as packages under hermes_plugins.*
    from .async_threads.adapter import AsyncThreadsAdapter, check_requirements, validate_config
    from .async_threads.commands import handle_pre_gateway_dispatch, ath_help
except ImportError:  # pytest may import this root __init__.py as a top-level module
    from async_threads.adapter import AsyncThreadsAdapter, check_requirements, validate_config
    from async_threads.commands import handle_pre_gateway_dispatch, ath_help


def register(ctx) -> None:
    """Register the async-thread receiver platform and /ath gateway hook."""
    ctx.register_platform(
        name="async_threads",
        label="Async Threads",
        adapter_factory=lambda cfg: AsyncThreadsAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        emoji="🧵",
        allow_update_command=False,
    )
    ctx.register_hook("pre_gateway_dispatch", handle_pre_gateway_dispatch)
    # This is mostly for command discoverability in CLI/help surfaces. The real
    # gateway implementation intercepts /ath in pre_gateway_dispatch so it can
    # capture event.source for "listen here".
    ctx.register_command(
        "ath",
        ath_help,
        description="Manage async-thread listeners",
        args_hint="listen|list|revoke|pause|resume|inspect",
    )
