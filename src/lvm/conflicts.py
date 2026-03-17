"""
Conflict detection for overlapping latest targets.

Detects when multiple watched sources share the same resolved latest_target
directory, which would cause promotions to silently overwrite each other.
"""

__all__ = ["detect_target_conflicts", "check_target_ownership"]

import logging
from pathlib import Path
from typing import Optional

from .models import ProjectConfig

logger = logging.getLogger(__name__)


def detect_target_conflicts(config: ProjectConfig) -> list[tuple[str, str, str]]:
    """Find sources that share the same resolved latest_target.

    Returns a list of (target_path, source_name_a, source_name_b) tuples
    for each pair of conflicting sources.
    """
    target_map: dict[str, list[str]] = {}
    for source in config.watched_sources:
        if not source.latest_target:
            continue
        # Normalize path for comparison
        try:
            normalized = str(Path(source.latest_target).resolve())
        except (OSError, ValueError):
            normalized = source.latest_target
        target_map.setdefault(normalized, []).append(source.name)

    conflicts = []
    for target, names in target_map.items():
        if len(names) > 1:
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    conflicts.append((target, names[i], names[j]))
    return conflicts


def check_target_ownership(
    target_dir: str, source_name: str, config: ProjectConfig
) -> Optional[str]:
    """Check if another source also targets this directory.

    Returns the name of the conflicting source, or None if no conflict.
    """
    try:
        normalized = str(Path(target_dir).resolve())
    except (OSError, ValueError):
        normalized = target_dir

    for source in config.watched_sources:
        if source.name == source_name:
            continue
        if not source.latest_target:
            continue
        try:
            other = str(Path(source.latest_target).resolve())
        except (OSError, ValueError):
            other = source.latest_target
        if other == normalized:
            return source.name
    return None
