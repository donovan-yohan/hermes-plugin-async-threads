"""Hermes plugin registration for async-thread continuation."""

from __future__ import annotations

from .adapter import AsyncThreadsAdapter, check_requirements, validate_config
from .commands import ath_help, handle_pre_gateway_dispatch


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
        args_hint="listen|list|status|events|inspect|pause|resume|revoke",
    )
