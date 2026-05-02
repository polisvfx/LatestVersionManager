"""
Conflict detection for overlapping latest targets.

Detects when two watched sources would actually clobber each other on
promote — either by writing to the same history sidecar or by producing
the same output filename pattern in their target directory. Sources that
share a target *directory* but write distinct filenames into it (e.g. a
folder of per-shot .mov outputs) are not flagged.
"""

__all__ = ["detect_target_conflicts", "check_target_ownership"]

import logging
import re
from pathlib import Path
from typing import Optional

from .models import ProjectConfig
from .task_tokens import derive_source_tokens, FRAME_EXT_RE
from .config import _expand_group_token

logger = logging.getLogger(__name__)

# Strip versioning when no rename template is set — mirrors the fallback
# branch in promoter._remap_filename so conflict detection sees the same
# output names that promotion would actually write.
_VERSION_RE = re.compile(r"[._\-]v\d+", re.IGNORECASE)
_DOUBLE_DIVIDER_RE = re.compile(r"([_.\-]){2,}")


def _resolved(path: str) -> str:
    try:
        return str(Path(path).resolve())
    except (OSError, ValueError):
        return path


def _output_pattern(source, task_tokens: list) -> Optional[str]:
    """Return the canonical output filename a source would write.

    For frame sequences the frame number is replaced with ``*`` so that two
    sources whose stems differ don't get merged just because they happen to
    share frame ranges. Returns ``None`` when the source has no
    ``sample_filename`` (in that case callers fall back to a directory-level
    comparison so unknown writers still collide on the same dir).
    """
    sample = source.sample_filename or ""
    if not sample:
        return None

    sequence = FRAME_EXT_RE.search(sample) is not None
    ext = Path(sample).suffix.lstrip(".")

    template = source.file_rename_template
    date_fmt = getattr(source, "date_format", "") or ""

    if not template:
        # No rename template — promoter falls back to stripping version (and
        # date when configured) from the original filename.
        result = _VERSION_RE.sub("", sample, count=1)
        if date_fmt:
            from .task_tokens import strip_date
            p = Path(result)
            stem = strip_date(p.stem, date_fmt)
            result = stem + p.suffix if p.suffix else stem
        result = _DOUBLE_DIVIDER_RE.sub(r"\1", result)
        # For sequences, replace the frame digits in `result` with '*'.
        if sequence:
            m2 = FRAME_EXT_RE.search(result)
            if m2:
                # Replace just the digit run with '*'.
                return result[:m2.start(1)] + "*" + result[m2.end(1):]
        return result

    # Template path — mirrors promoter._remap_filename's template branch
    tokens = derive_source_tokens(
        sample, task_tokens or [], date_fmt, source_title=source.name,
    )
    base = template
    base = base.replace("{source_title}", tokens["source_title"])
    base = base.replace("{source_name}", tokens["source_name"])
    base = base.replace("{source_basename}", tokens["source_basename"])
    base = base.replace("{source_fullname}", tokens["source_fullname"])
    base = _expand_group_token(base, source.group)

    if sequence:
        return f"{base}.*.{ext}" if ext else f"{base}.*"
    return f"{base}.{ext}" if ext else base


def detect_target_conflicts(
    config: ProjectConfig, task_tokens: Optional[list] = None,
) -> list[tuple[str, str, str]]:
    """Find sources that would actually clobber each other on promote.

    A conflict is reported when two sources share an effective write path —
    either the same history sidecar (target_dir + history_filename) or the
    same output filename pattern (target_dir + remapped sample, with frame
    numbers wildcarded). Sources writing different filenames into the same
    target directory are not flagged.

    Returns a list of ``(path, source_a_name, source_b_name)`` tuples — one
    per conflicting pair. ``path`` is the colliding write path (history file
    or output pattern).
    """
    if task_tokens is None:
        task_tokens = list(getattr(config, "task_tokens", None) or [])

    history_map: dict[str, list[str]] = {}
    output_map: dict[str, list[str]] = {}

    for source in config.watched_sources:
        if not source.latest_target:
            continue
        target_dir = _resolved(source.latest_target)

        history_name = source.history_filename or ".latest_history.json"
        history_path = str(Path(target_dir) / history_name)
        history_map.setdefault(history_path, []).append(source.name)

        pattern = _output_pattern(source, task_tokens)
        if pattern is None:
            # No sample_filename — use directory as a coarse signature so
            # unknown writers in the same dir still collide.
            output_key = f"{target_dir}::<unknown>"
        else:
            output_key = str(Path(target_dir) / pattern)
        output_map.setdefault(output_key, []).append(source.name)

    conflicts: list[tuple[str, str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()

    for sig_map in (history_map, output_map):
        for sig, names in sig_map.items():
            if len(names) < 2:
                continue
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    pair = tuple(sorted((names[i], names[j])))
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    conflicts.append((sig, names[i], names[j]))

    return conflicts


def check_target_ownership(
    target_dir: str, source_name: str, config: ProjectConfig
) -> Optional[str]:
    """Check if another source also targets this directory.

    Directory-level check used by validation paths that don't have access to
    a specific filename to test. Returns the name of the first conflicting
    source, or ``None``.
    """
    normalized = _resolved(target_dir)
    for source in config.watched_sources:
        if source.name == source_name:
            continue
        if not source.latest_target:
            continue
        if _resolved(source.latest_target) == normalized:
            return source.name
    return None
