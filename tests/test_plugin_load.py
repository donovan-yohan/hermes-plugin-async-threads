from __future__ import annotations

import importlib.util
from pathlib import Path

from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry


class FakePluginContext:
    def __init__(self):
        self.platforms = []
        self.hooks = []
        self.commands = []
        self.tools = []

    def register_platform(self, **kwargs):
        self.platforms.append(kwargs)

    def register_hook(self, name, handler):
        self.hooks.append((name, handler))

    def register_command(self, name, handler, **kwargs):
        self.commands.append((name, handler, kwargs))

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)


def _load_root_plugin():
    root = Path(__file__).resolve().parents[1] / "__init__.py"
    spec = importlib.util.spec_from_file_location("hermes_plugins.async_threads_test", root)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ensure_async_threads_platform_registered(adapter_factory):
    if not platform_registry.is_registered("async_threads"):
        platform_registry.register(
            PlatformEntry(
                name="async_threads",
                label="Async Threads",
                adapter_factory=adapter_factory,
                check_fn=lambda: True,
            )
        )


def test_pip_entrypoint_module_exposes_register():
    import hermes_plugin_async_threads

    assert callable(hermes_plugin_async_threads.register)


def test_root_plugin_registers_platform_hook_and_command():
    plugin = _load_root_plugin()
    ctx = FakePluginContext()

    plugin.register(ctx)

    assert [platform["name"] for platform in ctx.platforms] == ["async_threads"]
    platform = ctx.platforms[0]
    assert platform["label"] == "Async Threads"
    assert callable(platform["adapter_factory"])
    assert callable(platform["check_fn"])
    assert callable(platform["validate_config"])
    assert platform["check_fn"]() is True
    assert platform["validate_config"](PlatformConfig(enabled=True, extra={"port": "8765"})) is True
    assert platform["validate_config"](PlatformConfig(enabled=True, extra={"port": "bad"})) is False

    _ensure_async_threads_platform_registered(platform["adapter_factory"])
    adapter = platform["adapter_factory"](PlatformConfig(enabled=True, extra={"port": 0}))
    assert adapter.platform.value == "async_threads"

    assert [(name, handler.__name__) for name, handler in ctx.hooks] == [
        ("pre_gateway_dispatch", "handle_pre_gateway_dispatch")
    ]
    assert [(name, kwargs["description"]) for name, _handler, kwargs in ctx.commands] == [
        ("ath", "Manage async-thread listeners")
    ]
    assert {tool["name"] for tool in ctx.tools} == {
        "ath_create_listener",
        "ath_list_listeners",
        "ath_get_listener",
        "ath_retire_listener",
        "ath_rotate_listener_secret",
        "ath_generate_producer_handoff",
        "ath_trace_event",
        "ath_get_event_payload",
        "ath_create_source_binding",
        "ath_list_source_bindings",
        "ath_get_source_binding",
        "ath_set_source_binding_status",
        "ath_dry_run_source_binding",
    }
    assert {tool["toolset"] for tool in ctx.tools} == {"plugin_async_threads"}
