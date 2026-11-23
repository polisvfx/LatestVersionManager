"""
Auto-promote decision logic.

Decides whether the highest version of a watched source should be promoted
automatically after the file watcher reported a change. Everything here
touches the filesystem (rescan, history sidecar, obsolete-layer listing,
container render check, link-mode probe), so the GUI runs it on a worker
thread and only acts on the returned :class:`AutoPromoteDecision`.

No Qt dependency: usable from tests and a future CLI watch mode.
"""

__all__ = [
    "AutoPromoteDecision", "evaluate_auto_promote", "normalize_frame_range",
    "PROMOTE", "SKIP", "WAIT_RENDER", "NOOP", "ERROR",
]

import logging
from dataclasses import dataclass, field
from typing import Optional

from .elevation import check_link_mode_available
from .models import VersionInfo, version_strings_match

logger = logging.getLogger(__name__)

# Decision actions
PROMOTE = "promote"          # all checks passed — promote `version`
SKIP = "skip"                # a check failed; `reason` says which
WAIT_RENDER = "wait_render"  # a container is still being written; retry later
NOOP = "noop"                # nothing to do (no versions, already on highest)
ERROR = "error"              # the check itself failed (I/O error etc.)


@dataclass
class AutoPromoteDecision:
    source_name: str
    action: str
    reason: str = ""
    version: Optional[VersionInfo] = None          # the candidate (highest) version
    versions: Optional[list] = None                # fresh scan result, if the scan ran
    waiting_file: str = ""                         # WAIT_RENDER: name of the unfinished file
    # Set by the caller: this check was a retry while waiting on a render
    # (used to avoid logging the same "waiting" line every retry).
    waiting_on_render: bool = False
    # Extra detail for logs only (kept out of status-bar text).
    log_detail: str = field(default="", repr=False)
    log_level: int = logging.INFO


def normalize_frame_range(frame_range: Optional[str]) -> Optional[str]:
    """Extract the core frame range, stripping gap annotations.

    '1001-1120 (95/120 frames, gaps detected)' -> '1001-1120'
    '1001-1120' -> '1001-1120'
    None -> None
    """
    if frame_range is None:
        return None
    return frame_range.split(" ")[0].split("(")[0].strip()


def evaluate_auto_promote(promoter, scanner, settle_seconds: float = 0.0) -> AutoPromoteDecision:
    """Run every auto-promote check for one source and return the decision.

    Checks, in order (the first one that fails decides):

    1. Rescan; no versions -> NOOP
    2. A previous promotion must exist (auto-promote never makes the first one)
    3. Already on the highest version -> NOOP
    4. Frame range must match the last promoted version (gap notes ignored)
    5. No obsolete layers in the target (that needs a user decision)
    6. Container files must have finished rendering -> otherwise WAIT_RENDER
    7. The source's link mode must be available on this machine

    Never raises: unexpected errors come back as an ERROR decision.
    *promoter* and *scanner* should be private to the calling thread.
    """
    name = promoter.source.name
    try:
        return _evaluate(name, promoter, scanner, settle_seconds)
    except Exception as e:
        logger.debug("Auto-promote check failed for %s", name, exc_info=True)
        return AutoPromoteDecision(name, ERROR, reason=f"check failed: {e}",
                                   log_level=logging.WARNING)


def _evaluate(name, promoter, scanner, settle_seconds) -> AutoPromoteDecision:
    versions = scanner.scan()
    if not versions:
        return AutoPromoteDecision(name, NOOP, versions=versions)
    highest = versions[-1]

    current = promoter.get_current_version()
    if not current:
        return AutoPromoteDecision(
            name, SKIP, reason="no previous promotion exists",
            log_detail="promote manually first", version=highest, versions=versions,
        )

    if version_strings_match(highest.version_string, current.version, highest.version_number):
        return AutoPromoteDecision(name, NOOP, version=highest, versions=versions)

    prev_range = normalize_frame_range(current.frame_range)
    new_range = normalize_frame_range(highest.frame_range)
    if prev_range != new_range:
        return AutoPromoteDecision(
            name, SKIP, reason=f"frame range changed ({prev_range} → {new_range})",
            version=highest, versions=versions,
        )

    obsolete = promoter.detect_obsolete_layers(highest)
    if obsolete:
        layer_names = ", ".join(layer["name"] for layer in obsolete)
        return AutoPromoteDecision(
            name, SKIP, reason=f"obsolete layers in target ({layer_names})",
            version=highest, versions=versions,
        )

    # Don't publish a QuickTime/MXF the renderer is still writing. The
    # watcher only fires when the file is created, so the caller polls.
    unfinished = promoter.find_unfinished_renders(highest, settle_seconds=settle_seconds)
    if unfinished:
        path, why = unfinished[0]
        return AutoPromoteDecision(
            name, WAIT_RENDER, reason=why, waiting_file=path.name,
            version=highest, versions=versions,
        )

    available, reason = check_link_mode_available(promoter.source.link_mode)
    if not available:
        return AutoPromoteDecision(
            name, SKIP, reason=reason, version=highest, versions=versions,
            log_level=logging.WARNING,
        )

    return AutoPromoteDecision(name, PROMOTE, version=highest, versions=versions)
