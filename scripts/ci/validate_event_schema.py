#!/usr/bin/env python3
"""Validate async-thread public event-schema docs.

This script is intentionally gateway-free: it imports only async_threads.security
and can run in public CI without a Hermes checkout.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from async_threads.security import extract_envelope_fields

SCHEMA_PATH = ROOT / "docs" / "schemas" / "async-thread-event-v1.schema.json"
DOCS = [ROOT / "docs" / "EVENT_CONTRACT.md", ROOT / "docs" / "QUICKSTART.md", ROOT / "docs" / "BRIDGE_RECIPES.md"]

FENCE_RE = re.compile(r"```json\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)


def _loads_json(text: str, *, source: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:  # pragma: no cover - assertion path
        raise AssertionError(f"invalid JSON fence in {source}: {exc}") from exc


def main() -> None:
    schema = _loads_json(SCHEMA_PATH.read_text(encoding="utf-8"), source=str(SCHEMA_PATH))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)

    checked = 0
    for doc in DOCS:
        text = doc.read_text(encoding="utf-8")
        for idx, body in enumerate(FENCE_RE.findall(text), start=1):
            data = _loads_json(body, source=f"{doc} fence #{idx}")
            if not isinstance(data, dict) or data.get("version") != "async-thread-event/v1":
                continue
            errors = sorted(validator.iter_errors(data), key=lambda err: list(err.path))
            if errors:
                formatted = "\n".join(f"- {list(err.path)}: {err.message}" for err in errors)
                raise AssertionError(f"schema validation failed for {doc} fence #{idx}:\n{formatted}")
            extract_envelope_fields(data)
            checked += 1

    if checked == 0:
        raise AssertionError("no async-thread-event/v1 JSON examples found in public docs")
    print(f"event schema/docs validation ok: {checked} async-thread-event/v1 examples")


if __name__ == "__main__":
    main()
