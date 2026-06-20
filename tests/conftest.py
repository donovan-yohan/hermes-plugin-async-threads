"""Test bootstrap for the Hermes plugin repository.

The plugin imports Hermes gateway modules. In a checked-out development setup
those modules usually live in a sibling `hermes-agent` checkout or the profile's
managed Hermes checkout. Allow `uv run pytest` from this repo to discover that
checkout without requiring each developer to remember `PYTHONPATH=...`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _candidate_hermes_paths() -> list[Path]:
    here = Path(__file__).resolve()
    repo = here.parents[1]
    candidates = []
    env_path = os.environ.get("HERMES_AGENT_PATH")
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.extend(
        [
            repo.parent / "hermes-agent",
            Path.home() / ".hermes" / "hermes-agent",
        ]
    )
    return candidates


for candidate in _candidate_hermes_paths():
    if (candidate / "gateway" / "config.py").exists():
        path = str(candidate)
        if path not in sys.path:
            sys.path.insert(0, path)
        break
