"""Hermes async-thread continuation plugin."""

from __future__ import annotations

try:  # Hermes loads directory plugins as packages under hermes_plugins.*
    from .async_threads.plugin import register
except ImportError:  # pytest may import this root __init__.py as a top-level module
    from async_threads.plugin import register

__all__ = ["register"]
