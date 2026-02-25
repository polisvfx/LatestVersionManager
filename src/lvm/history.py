"""
History Manager - manages the .latest_history.json sidecar file.

Tracks which version is currently promoted to "latest" and
maintains a full history of all promotions.
"""

import json
import logging
from pathlib import Path
from typing import Optional

from .models import HistoryEntry

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
        self._cache_mtime: Optional[float] = None

    def load(self) -> dict:
        """
        Load the history file. Returns a dict with 'current' and 'history' keys.
        Returns empty structure if file doesn't exist.

        Results are cached by file mtime to avoid redundant disk reads when
        get_current() and get_history() are called in quick succession.
        """
        if not self.path.exists():
            self._cache = None
            self._cache_mtime = None
            return {"current": None, "history": []}

        try:
            current_mtime = self.path.stat().st_mtime
        except OSError:
            current_mtime = None

        if (self._cache is not None
                and current_mtime is not None
                and current_mtime == self._cache_mtime):
            return self._cache

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            result = {
                "current": HistoryEntry.from_dict(data["current"]) if data.get("current") else None,
                "history": [HistoryEntry.from_dict(h) for h in data.get("history", [])],
            }
            self._cache = result
            self._cache_mtime = current_mtime
            return result
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error(f"Failed to parse history file {self.path}: {e}")
            # Back up the corrupt file and start fresh
            backup = self.path.with_suffix(".json.bak")
            if self.path.exists():
                self.path.rename(backup)
                logger.info(f"Backed up corrupt history to {backup}")
            self._cache = None
            self._cache_mtime = None
            return {"current": None, "history": []}

    def save(self, current: HistoryEntry, history: list[HistoryEntry]):
        """Write the history file to disk."""
        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Trim history to a reasonable length
        trimmed = history[:MAX_HISTORY_ENTRIES]

        data = {
            "current": current.to_dict(),
            "history": [h.to_dict() for h in trimmed],
        }

        # Write atomically: write to temp file, then rename
        tmp_path = self.path.with_suffix(".json.tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Atomic rename (works on both platforms for same-directory moves)
            tmp_path.replace(self.path)
            logger.info(f"History saved: {current.version} -> {self.path}")

            # Update cache to avoid re-reading the file we just wrote
            try:
                new_mtime = self.path.stat().st_mtime
            except OSError:
                new_mtime = None
            self._cache = {
                "current": current,
                "history": trimmed,
            }
            self._cache_mtime = new_mtime
        except OSError as e:
            logger.error(f"Failed to save history file: {e}")
            self._cache = None
            self._cache_mtime = None
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def get_current(self) -> Optional[HistoryEntry]:
        """Get the currently promoted version, or None."""
        data = self.load()
        return data["current"]

    def get_history(self) -> list[HistoryEntry]:
        """Get full promotion history, newest first."""
        data = self.load()
        return data["history"]

    def record_promotion(self, entry: HistoryEntry):
        """
        Record a new promotion: set as current and prepend to history.
        """
        data = self.load()
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
