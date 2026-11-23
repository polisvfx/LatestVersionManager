"""
Small file helpers shared by config, history and cache writers.
"""

__all__ = ["atomic_write_json"]

import json
import os
import tempfile
from pathlib import Path


def atomic_write_json(path, data, indent: int = 2) -> None:
    """Write *data* as JSON to *path* without ever leaving a truncated file.

    The JSON goes to a uniquely named temp file in the same directory, is
    flushed to disk, and then replaces *path* with ``os.replace`` (atomic on
    the same volume, including SMB shares). On any failure the temp file is
    removed and the previous *path* is left untouched.
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
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
