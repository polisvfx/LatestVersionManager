"""
Per-project trust fingerprinting for pre/post-promote shell hooks.

Opening a shared project JSON or applying a template can introduce shell
commands (`pre_promote_cmd` / `post_promote_cmd`). This module computes a
stable SHA-256 fingerprint of ``(project_path, hooks)`` so the GUI can require
explicit user approval before running unfamiliar commands and remember trusted
commands per project.
"""

__all__ = [
    "collect_source_hooks",
    "hook_fingerprint",
    "is_hook_trusted",
    "add_trusted_hook",
]

import hashlib
import json
import os
from pathlib import Path
from typing import Iterable, Sequence

from .models import WatchedSource


def _canonical_project_key(project_path: str) -> str:
    if not project_path:
        return ""
    try:
        resolved = str(Path(project_path).resolve())
    except OSError:
        resolved = os.path.abspath(project_path)
    return os.path.normcase(resolved)


def collect_source_hooks(sources: Sequence[WatchedSource]) -> list[tuple[str, str, str]]:
    """Return non-empty ``(source_name, pre_cmd, post_cmd)`` triples for *sources*."""
    out: list[tuple[str, str, str]] = []
    for s in sources:
        pre = (getattr(s, "pre_promote_cmd", "") or "").strip()
        post = (getattr(s, "post_promote_cmd", "") or "").strip()
        if pre or post:
            out.append((s.name, pre, post))
    return out


def hook_fingerprint(project_path: str, pre_cmd: str, post_cmd: str) -> str:
    """Compute a deterministic SHA-256 hex digest for a project's hook pair."""
    payload = json.dumps(
        [
            _canonical_project_key(project_path),
            (pre_cmd or "").strip(),
            (post_cmd or "").strip(),
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def is_hook_trusted(
    project_path: str,
    pre_cmd: str,
    post_cmd: str,
    trusted_fingerprints: Iterable[str],
) -> bool:
    """Return True if *pre_cmd*/*post_cmd* are empty or already trusted."""
    if not (pre_cmd or "").strip() and not (post_cmd or "").strip():
        return True
    fp = hook_fingerprint(project_path, pre_cmd, post_cmd)
    return fp in set(trusted_fingerprints or ())


def add_trusted_hook(
    project_path: str,
    pre_cmd: str,
    post_cmd: str,
    trusted_fingerprints: Sequence[str],
    max_entries: int = 500,
) -> list[str]:
    """Return an updated trusted-fingerprint list containing this hook pair."""
    existing = list(trusted_fingerprints or ())
    if not (pre_cmd or "").strip() and not (post_cmd or "").strip():
        return existing
    fp = hook_fingerprint(project_path, pre_cmd, post_cmd)
    if fp in existing:
        existing.remove(fp)
    existing.append(fp)
    if len(existing) > max_entries:
        existing = existing[-max_entries:]
    return existing
