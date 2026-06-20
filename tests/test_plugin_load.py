from __future__ import annotations

import importlib.util
from pathlib import Path

from gateway.config import PlatformConfig


class FakePluginContext:
    def __init__(self):
        self.platforms = []
        self.hooks = []
        self.commands = []

    def register_platform(self, **kwargs):
        self.platforms.append(kwargs)

    def register_hook(self, name, handler):
        self.hooks.append((name, handler))

    def register_command(self, name, handler, **kwargs):
        self.commands.append((name, handler, kwargs))


def _load_root_plugin():
    root = Path(__file__).resolve().parents[1] / "__init__.py"
    spec = importlib.util.spec_from_file_location("hermes_plugins.async_threads_test", root)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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

    adapter = platform["adapter_factory"](PlatformConfig(enabled=True, extra={"port": 0}))
    assert adapter.platform.value == "async_threads"

    assert [(name, handler.__name__) for name, handler in ctx.hooks] == [
        ("pre_gateway_dispatch", "handle_pre_gateway_dispatch")
    ]
    assert [(name, kwargs["description"]) for name, _handler, kwargs in ctx.commands] == [
        ("ath", "Manage async-thread listeners")
    ]
