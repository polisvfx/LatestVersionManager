"""
Auto-promote decision logic.

Decides whether the highest version of a watched source should be promoted
automatically after the file watcher reported a change. Everything here
touches the filesystem (rescan, history sidecar, obsolete-layer listing,
render checks, link-mode probe), so the GUI runs it on a worker thread and
only acts on the returned :class:`AutoPromoteDecision`.

Renders arrive over time, and the watcher only sees the first sign of a new
version (a new folder, or a new file in a flat layout), so the caller keeps
re-checking while a version is incomplete (``WAIT_RENDER``) and passes the
previous check's :class:`SettleState` back in. A version counts as settled
only when two consecutive checks saw the same file count, total size and
frame range. That comparison uses no clocks, so it is immune to clock skew
between the workstation and the file server.

No Qt dependency: usable from tests and a future CLI watch mode.
"""

__all__ = [
    "AutoPromoteDecision", "SettleState", "evaluate_auto_promote",
    "normalize_frame_range", "DEFAULT_MAX_WAIT_SECONDS",
    "PROMOTE", "SKIP", "WAIT_RENDER", "NOOP", "ERROR",
]

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .elevation import check_link_mode_available
from .models import VersionInfo, version_strings_match
from .promoter import has_frame_gaps

logger = logging.getLogger(__name__)

# Decision actions
PROMOTE = "promote"          # all checks passed — promote `version`
SKIP = "skip"                # a check failed; `reason` says which
WAIT_RENDER = "wait_render"  # the version is still being written; re-check later
NOOP = "noop"                # nothing to do (no versions, already on highest)
ERROR = "error"              # the check itself failed (I/O error etc.)

# Stop re-checking a version that stays incomplete for this long (a render
# that died, or a version that was abandoned half-way).
DEFAULT_MAX_WAIT_SECONDS = 8 * 3600


@dataclass(frozen=True)
class SettleState:
    """What one check saw of the candidate version; fed into the next check."""
    version_string: str
    file_count: int
    total_size_bytes: int
    frame_range: Optional[str]
    first_seen: float  # time.monotonic() of the first check of this version

    @property
    def key(self) -> tuple:
        return (self.version_string, self.file_count, self.total_size_bytes, self.frame_range)


@dataclass
class AutoPromoteDecision:
    source_name: str
    action: str
    reason: str = ""
    version: Optional[VersionInfo] = None          # the candidate (highest) version
    versions: Optional[list] = None                # fresh scan result, if the scan ran
    waiting_file: str = ""                         # WAIT_RENDER: file still being written, if known
    settle: Optional[SettleState] = None           # pass back into the next check
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


def _parse_range(frame_range: Optional[str]) -> Optional[tuple[int, int]]:
    core = normalize_frame_range(frame_range)
    if not core or "-" not in core:
        return None
    first, _, last = core.partition("-")
    try:
        return int(first), int(last)
    except ValueError:
        return None


def _is_strict_subrange(new: Optional[str], prev: Optional[str]) -> bool:
    """True if *new* lies inside *prev* without covering it, e.g. a render
    that has written 1001-1050 of 1001-1120 so far."""
    n, p = _parse_range(new), _parse_range(prev)
    if not n or not p or n == p:
        return False
    return p[0] <= n[0] and n[1] <= p[1]


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


def _format_duration(seconds: float) -> str:
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} h"
    return f"{seconds / 60:.0f} min"


def evaluate_auto_promote(promoter, scanner, settle_seconds: float = 0.0,
                          previous: Optional[SettleState] = None,
                          now: Optional[float] = None,
                          max_wait_seconds: float = DEFAULT_MAX_WAIT_SECONDS,
                          ) -> AutoPromoteDecision:
    """Run every auto-promote check for one source and return the decision.

    Checks, in order (the first one that fails decides):

    1. Rescan; no versions -> NOOP
    2. A previous promotion must exist (auto-promote never makes the first one)
    3. Already on the highest version -> NOOP
    4. The version must be complete -> otherwise WAIT_RENDER (re-check later):
       unchanged since the *previous* check, no frame gaps (when gaps block
       promotion), its frame range not a strict part of the last promoted
       one, and its newest frames not open for writing. After
       *max_wait_seconds* of waiting on one version this becomes a SKIP.
    5. Frame range must match the last promoted version (gap notes ignored)
    6. No obsolete layers in the target (that needs a user decision)
    7. Container files must have finished rendering -> otherwise WAIT_RENDER
    8. The source's link mode must be available on this machine

    Never raises: unexpected errors come back as an ERROR decision.
    *promoter* and *scanner* should be private to the calling thread.
    """
    name = promoter.source.name
    if now is None:
        now = time.monotonic()
    try:
        return _evaluate(name, promoter, scanner, settle_seconds, previous, now,
                         max_wait_seconds)
    except Exception as e:
        logger.debug("Auto-promote check failed for %s", name, exc_info=True)
        return AutoPromoteDecision(name, ERROR, reason=f"check failed: {e}",
                                   log_level=logging.WARNING)


def _evaluate(name, promoter, scanner, settle_seconds, previous, now,
              max_wait_seconds) -> AutoPromoteDecision:
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

    # -- 4. completeness / settle --------------------------------------
    same_version = previous is not None and previous.version_string == highest.version_string
    settle = SettleState(
        version_string=highest.version_string,
        file_count=highest.file_count,
        total_size_bytes=highest.total_size_bytes,
        frame_range=highest.frame_range,
        first_seen=previous.first_seen if same_version else now,
    )

    def done(action, reason="", **kw):
        return AutoPromoteDecision(name, action, reason=reason, version=highest,
                                   versions=versions, settle=settle, **kw)

    prev_range = normalize_frame_range(current.frame_range)
    new_range = normalize_frame_range(highest.frame_range)
    waiting_file = ""
    if previous is None or previous.key != settle.key:
        incomplete = (f"waiting for files to settle "
                      f"({_plural(highest.file_count, 'file')} so far)")
    elif (getattr(promoter.source, "block_incomplete_sequences", True)
            and has_frame_gaps(highest)):
        incomplete = f"frames missing ({highest.frame_range})"
    elif _is_strict_subrange(new_range, prev_range):
        incomplete = f"frames {new_range} of {prev_range} rendered so far"
    else:
        waiting_file = promoter.newest_frames_open_for_writing(highest) or ""
        incomplete = f"{waiting_file} is still being written" if waiting_file else ""

    if incomplete:
        waited = now - settle.first_seen
        if waited > max_wait_seconds:
            return done(SKIP, f"stopped waiting after {_format_duration(waited)}: {incomplete}")
        return done(WAIT_RENDER, incomplete, waiting_file=waiting_file)

    # -- 5.-8. --------------------------------------------------------
    if prev_range != new_range:
        return done(SKIP, f"frame range changed ({prev_range} → {new_range})")

    obsolete = promoter.detect_obsolete_layers(highest)
    if obsolete:
        layer_names = ", ".join(layer["name"] for layer in obsolete)
        return done(SKIP, f"obsolete layers in target ({layer_names})")

    # Don't publish a QuickTime/MXF the renderer is still writing.
    unfinished = promoter.find_unfinished_renders(highest, settle_seconds=settle_seconds)
    if unfinished:
        path, why = unfinished[0]
        return done(WAIT_RENDER, why, waiting_file=path.name)

    available, reason = check_link_mode_available(promoter.source.link_mode)
    if not available:
        return done(SKIP, reason, log_level=logging.WARNING)

    return done(PROMOTE)
