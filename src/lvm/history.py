"""
History Manager - manages the .latest_history.json sidecar file.

Tracks which version is currently promoted to "latest" and
maintains a full history of all promotions.
"""

__all__ = ["HistoryManager", "has_newer_versions_since", "MAX_HISTORY_ENTRIES"]

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from .fileutil import atomic_write_json
from .models import HistoryEntry, version_strings_match

logger = logging.getLogger(__name__)

MAX_HISTORY_ENTRIES = 100


class HistoryManager:
    """Reads and writes the promotion history sidecar file."""

    def __init__(self, history_path: str):
        """
        Args:
            history_path: Full path to the history JSON file,
                          e.g. /online/hero_comp_latest/.latest_history.json
        """
        self.path = Path(history_path)
        self._cache: Optional[dict] = None
        self._cache_stat: Optional[tuple[int, int]] = None

    def load(self, bypass_cache: bool = False) -> dict:
        """
        Load the history file. Returns a dict with 'current' and 'history' keys.
        Returns empty structure if file doesn't exist.

        Results are cached by ``(st_mtime_ns, st_size)`` to avoid redundant disk
        reads when get_current() and get_history() are called in quick succession.

        Raises OSError when the file exists but cannot be read (permission
        denied, network share dropped). That is deliberately *not* reported
        as "no history": a caller that then promoted would overwrite the
        real history with a single entry.
        """
        try:
            st = self.path.stat()
            current_stat: Optional[tuple[int, int]] = (st.st_mtime_ns, st.st_size)
        except (FileNotFoundError, NotADirectoryError):
            self._cache = None
            self._cache_stat = None
            return {"current": None, "history": []}

        if (not bypass_cache
                and self._cache is not None
                and current_stat is not None
                and current_stat == self._cache_stat):
            return {
                "current": self._cache["current"],
                "history": list(self._cache["history"]),
            }

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError(f"expected a JSON object, got {type(data).__name__}")
            result = {
                "current": HistoryEntry.from_dict(data["current"]) if data.get("current") else None,
                "history": [HistoryEntry.from_dict(h) for h in data.get("history", [])],
            }
            self._cache = {
                "current": result["current"],
                "history": list(result["history"]),
            }
            self._cache_stat = current_stat
            return result
        except (ValueError, KeyError, TypeError) as e:  # incl. JSON/Unicode decode errors
            logger.error(f"Failed to parse history file {self.path}: {e}")
            # Back up the corrupt file and start fresh. os.replace (not
            # Path.rename) so an older .bak doesn't raise FileExistsError on
            # Windows.
            backup = self.path.with_suffix(".json.bak")
            try:
                os.replace(self.path, backup)
                logger.info(f"Backed up corrupt history to {backup}")
            except FileNotFoundError:
                pass
            except OSError as be:
                logger.error(f"Could not back up corrupt history {self.path}: {be}")
            self._cache = None
            self._cache_stat = None
            return {"current": None, "history": []}

    def save(self, current: HistoryEntry, history: list[HistoryEntry]):
        """Write the history file to disk."""
        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Trim history to a reasonable length
        trimmed = list(history[:MAX_HISTORY_ENTRIES])

        data = {
            "current": current.to_dict(),
            "history": [h.to_dict() for h in trimmed],
        }

        # Atomic: uniquely named temp file in the same dir, fsync, os.replace
        try:
            atomic_write_json(self.path, data)
            logger.info(f"History saved: {current.version} -> {self.path}")

            # Update cache to avoid re-reading the file we just wrote
            try:
                st = self.path.stat()
                new_stat: Optional[tuple[int, int]] = (st.st_mtime_ns, st.st_size)
            except OSError:
                new_stat = None
            self._cache = {
                "current": current,
                "history": list(trimmed),
            }
            self._cache_stat = new_stat
        except OSError as e:
            logger.error(f"Failed to save history file: {e}")
            self._cache = None
            self._cache_stat = None
            raise

    def get_current(self) -> Optional[HistoryEntry]:
        """Get the currently promoted version, or None."""
        data = self.load()
        return data["current"]

    def get_history(self) -> list[HistoryEntry]:
        """Get full promotion history, newest first."""
        data = self.load()
        return list(data["history"])

    def get_previous(self, n: int = 1) -> Optional[HistoryEntry]:
        """Return the entry that was current n promotes ago.

        n=1 → the entry immediately before the current one (history[1]).
        Returns None when the history doesn't go back that far. Raises
        ValueError for n < 1.
        """
        if n < 1:
            raise ValueError(f"n must be >= 1, got {n}")
        history = self.get_history()
        if len(history) <= n:
            return None
        return history[n]

    def record_promotion(self, entry: HistoryEntry):
        """
        Record a new promotion: set as current and prepend to history.
        """
        # Bypass cache so an external writer on a coarse-mtime filesystem
        # cannot be clobbered by a stale cached history list.
        data = self.load(bypass_cache=True)
        history = data.get("history", [])
        if isinstance(history, list):
            history = list(history)
        else:
            history = []

        # Prepend new entry to history
        history.insert(0, entry)

        self.save(current=entry, history=history)

    def verify_integrity(self, actual_files: list[str]) -> dict:
        """
        Check if the files on disk match what the history says should be there.

        Args:
            actual_files: List of filenames currently in the latest target folder.

        Returns:
            Dict with 'valid' bool and 'message' string.
        """
        current = self.get_current()
        if current is None:
            if actual_files:
                return {
                    "valid": False,
                    "message": "Files exist in latest folder but no history record found. "
                               "Someone may have manually placed files here.",
                }
            return {"valid": True, "message": "No history and no files - clean state."}

        if not actual_files:
            return {
                "valid": False,
                "message": f"History says {current.version} should be loaded, "
                           f"but no files found in latest folder.",
            }

        if current.file_count > 0 and len(actual_files) != current.file_count:
            return {
                "valid": False,
                "message": f"History says {current.file_count} files for {current.version}, "
                           f"but found {len(actual_files)} files on disk.",
            }

        return {"valid": True, "message": f"Current: {current.version} - files match."}


def has_newer_versions_since(current: HistoryEntry, versions: list) -> bool:
    """Check if any version higher than the current one appeared after promotion.

    Compares the promotion timestamp against the modification time of
    higher-version source paths. Returns True if at least one higher
    version was created/modified *after* the promotion — meaning the
    user didn't deliberately skip it.

    A 2-second tolerance is applied because set_at is stored with
    second-level precision while filesystem timestamps have sub-second
    resolution.
    """
    if not current or not current.set_at or not versions:
        return False

    try:
        promoted_at = datetime.fromisoformat(current.set_at)
    except (ValueError, TypeError):
        return False

    # Add tolerance for timestamp rounding (set_at truncates to seconds)
    threshold = promoted_at + timedelta(seconds=2)

    current_rank: Optional[tuple[int, int]] = None
    for v in versions:
        if version_strings_match(v.version_string, current.version, v.version_number):
            current_rank = (getattr(v, "date_sortable", 0) or 0, v.version_number)
            break
    if current_rank is None:
        # Current version may have been archived/deleted from disk — infer
        # its rank from the version string so newer arrivals are still spotted.
        import re
        cur_ver = current.version or ""
        m_v = re.search(r"v(\d+)", cur_ver, re.IGNORECASE)
        m_iso = re.search(r"(\d{4})-(\d{2})-(\d{2})", cur_ver)
        m_dmy = re.search(r"(\d{2})-(\d{2})-(\d{4})", cur_ver)
        inferred_date = 0
        if m_iso:
            inferred_date = int(f"{m_iso.group(1)}{m_iso.group(2)}{m_iso.group(3)}")
        elif m_dmy:
            inferred_date = int(f"{m_dmy.group(3)}{m_dmy.group(2)}{m_dmy.group(1)}")
        inferred_ver = int(m_v.group(1)) if m_v else 0
        if inferred_date or m_v:
            current_rank = (inferred_date, inferred_ver)
        else:
            return False

    for v in versions:
        rank = (getattr(v, "date_sortable", 0) or 0, v.version_number)
        if rank <= current_rank:
            continue
        # Check when this higher version's source path was last modified
        try:
            source_path = Path(v.source_path)
            mtime = datetime.fromtimestamp(source_path.stat().st_mtime)
            if mtime > threshold:
                return True
        except (OSError, ValueError):
            continue

    return False
