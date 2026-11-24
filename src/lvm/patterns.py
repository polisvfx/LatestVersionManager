"""Shared compiled regex patterns, filename sanitization, and subprocess helpers.

Centralizes pattern definitions that were previously duplicated across
``task_tokens.py``, ``promoter.py``, ``conflicts.py``, ``timecode.py``,
``thumbnail.py``, and ``app/``.
"""

__all__ = [
    "DIVIDERS",
    "DIVIDER_RE",
    "VERSION_RE",
    "VERSION_STRIP_RE",
    "FRAME_EXT_RE",
    "FRAME_EXT_SPLIT_RE",
    "DOUBLE_DIVIDER_RE",
    "DATE_RE",
    "sanitize_filename",
    "subprocess_kwargs",
]

import re
import subprocess
import sys

# Characters that act as word boundaries in VFX filenames
DIVIDERS = "_.-"
DIVIDER_RE = r"[_.\-]"

# Version patterns: _v01, .v002, -v0051, _V004
VERSION_RE = re.compile(r"[._\-]v(\d+)", re.IGNORECASE)
VERSION_STRIP_RE = re.compile(r"[._\-]v\d+", re.IGNORECASE)

# Frame number + extension at end of filename: name.1001.exr, name_1001.exr
# FRAME_EXT_RE captures (digits) for legacy callers; FRAME_EXT_SPLIT_RE captures (sep, digits, ext).
FRAME_EXT_RE = re.compile(r"[._](\d+)\.\w+$")
FRAME_EXT_SPLIT_RE = re.compile(r"([._])(\d+)\.(\w+)$")

# Repeated divider runs: __, .., --
DOUBLE_DIVIDER_RE = re.compile(r"([_.\-]){2,}")

# Date patterns in filenames: 6-digit (DDMMYY/YYMMDD) or 8-digit (DDMMYYYY/YYYYMMDD)
DATE_RE = re.compile(r"(?:^|(?<=[._\-]))(\d{6}|\d{8})(?=[._\-]|$)")


def sanitize_filename(name: str, fallback: str = "project") -> str:
    """Turn an arbitrary label (e.g. project or source name) into a safe filename stem."""
    safe = re.sub(r"[^\w\-]", "_", (name or "").strip()).strip("_")
    return safe or fallback


def subprocess_kwargs() -> dict:
    """Return platform-specific subprocess kwargs to suppress console windows on Windows."""
    kwargs: dict = {}
    if sys.platform == "win32":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = subprocess.SW_HIDE
        kwargs["startupinfo"] = si
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    return kwargs
