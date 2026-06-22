"""Secret material artifacts for async-thread producers.

The receiver still stores the HMAC secret in SQLite because it must validate
inbound events. This module controls the producer-facing copy of that secret:
files live under Hermes profile data by default, are chmod 0600 on POSIX, and
callers only render references/paths, never literal secret values.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .registry import AsyncThreadHandle


@dataclass(frozen=True)
class SecretArtifact:
    thread_key: str
    directory: Path
    secret_file: Path
    contract_file: Path

    def public_ref(self) -> dict[str, Any]:
        return {
            "available": True,
            "returned": False,
            "secretFile": str(self.secret_file),
            "contractFile": str(self.contract_file),
            "env": {"ATH_SECRET_FILE": str(self.secret_file)},
            "permissions": "0600",
        }


def write_secret_artifact(
    handle: AsyncThreadHandle,
    *,
    event_url: str = "",
    root: str | Path | None = None,
) -> SecretArtifact:
    """Write producer-facing secret material outside the checkout."""

    base = _secret_root(root)
    directory = base / _safe_path_token(handle.thread_key)
    _mkdir_private(base)
    _mkdir_private(directory)
    secret_file = directory / "secret.txt"
    contract_file = directory / "contract.json"
    _write_private_text(secret_file, handle.secret + "\n")
    contract = {
        "version": "async-thread-secret/v1",
        "threadKey": handle.thread_key,
        "producerId": handle.producer_id,
        "allowedEventTypes": list(handle.allowed_event_types),
        "eventUrl": event_url,
        "secretFile": str(secret_file),
        "requiredHeaders": ["Content-Type: application/json", "X-Hermes-Signature-256: sha256=<hex>"],
        "signature": "hex_hmac_sha256(secret_file_contents, exact_utf8_request_body_bytes)",
        "notes": [
            "Do not paste the secret into chat, prompts, logs, issue comments, or event payloads.",
            "Use ATH_SECRET_FILE or another local secret manager reference when running producer code.",
        ],
    }
    _write_private_text(contract_file, json.dumps(contract, indent=2, sort_keys=True) + "\n")
    return SecretArtifact(
        thread_key=handle.thread_key,
        directory=directory,
        secret_file=secret_file,
        contract_file=contract_file,
    )


def describe_secret_artifact(
    handle: AsyncThreadHandle,
    *,
    event_url: str = "",
    root: str | Path | None = None,
    ensure: bool = True,
) -> dict[str, Any]:
    """Return a non-secret producer-facing reference for a handle."""

    artifact = write_secret_artifact(handle, event_url=event_url, root=root) if ensure else _artifact_for(handle, root=root)
    return artifact.public_ref()


def remove_secret_artifact(thread_key: str, *, root: str | Path | None = None) -> bool:
    """Best-effort removal of producer-facing secret material."""

    directory = _secret_root(root) / _safe_path_token(thread_key)
    removed = False
    for name in ("secret.txt", "contract.json"):
        path = directory / name
        try:
            path.unlink()
            removed = True
        except FileNotFoundError:
            pass
    try:
        directory.rmdir()
    except OSError:
        pass
    return removed


def secret_root_from_config(config: Any | None = None) -> Path:
    extra = getattr(config, "extra", {}) or {}
    configured = extra.get("secret_root") or extra.get("secrets_root") or extra.get("emitter_root")
    return _secret_root(configured)


def _artifact_for(handle: AsyncThreadHandle, *, root: str | Path | None = None) -> SecretArtifact:
    directory = _secret_root(root) / _safe_path_token(handle.thread_key)
    return SecretArtifact(
        thread_key=handle.thread_key,
        directory=directory,
        secret_file=directory / "secret.txt",
        contract_file=directory / "contract.json",
    )


def _secret_root(root: str | Path | None = None) -> Path:
    if root:
        return Path(root).expanduser().resolve()
    try:
        from hermes_constants import get_hermes_home

        home = get_hermes_home()
    except Exception:
        home = Path.home() / ".hermes"
    return (home / "data" / "async-threads" / "emitters").expanduser().resolve()


def _safe_path_token(value: str) -> str:
    cleaned = "".join(ch for ch in str(value or "") if ch.isalnum() or ch in {"-", "_"})
    return cleaned or "unknown"


def _mkdir_private(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    _chmod(path, 0o700)


def _write_private_text(path: Path, content: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
    finally:
        _chmod(path, 0o600)


def _chmod(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
    except (AttributeError, NotImplementedError, PermissionError, OSError):
        pass
