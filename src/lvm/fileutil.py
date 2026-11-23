"""
Small file helpers shared by config, history and cache writers.
"""

__all__ = ["atomic_write_json"]

import json
import os
import sys
import tempfile
import time
from pathlib import Path

_REPLACE_RETRIES = 5
_REPLACE_BACKOFF_BASE = 0.02  # seconds


def _apply_posix_permissions(tmp_name: str, target_path: Path) -> None:
    """Ensure the temp file is readable according to target mode or process umask.

    ``tempfile.mkstemp`` creates files with mode ``0o600``, which locks out
    other artists sharing a project/sidecar on a POSIX NAS.
    """
    if os.name == "nt":
        return
    try:
        mode = target_path.stat().st_mode & 0o777
    except OSError:
        umask = os.umask(0)
        os.umask(umask)
        mode = 0o666 & ~umask
    try:
        os.chmod(tmp_name, mode)
    except OSError:
        pass


def atomic_write_json(path, data, indent: int = 2) -> None:
    """Write *data* as JSON to *path* without ever leaving a truncated file.

    The JSON goes to a uniquely named temp file in the same directory, is
    flushed to disk, and then replaces *path* with ``os.replace`` (atomic on
    the same volume, including SMB shares). On Windows, brief retries handle
    transient ``PermissionError`` when another reader briefly holds *path*.
    On any failure the temp file is removed and the previous *path* is left
    untouched.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        _apply_posix_permissions(tmp_name, path)

        if sys.platform == "win32":
            last_err: OSError | None = None
            for attempt in range(_REPLACE_RETRIES):
                try:
                    os.replace(tmp_name, path)
                    last_err = None
                    break
                except PermissionError as e:
                    last_err = e
                    if attempt < _REPLACE_RETRIES - 1:
                        time.sleep(_REPLACE_BACKOFF_BASE * (attempt + 1))
            if last_err is not None:
                raise last_err
        else:
            os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
