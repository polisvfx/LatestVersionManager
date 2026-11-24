"""
Render-completion checks for container files (QuickTime, MP4, MXF, AVI, ...).

A frame sequence grows one finished frame at a time, but a container is a
single file that a renderer keeps open and appends to for the whole render
and then finalises (QuickTime/MP4 write the ``moov`` index last, MXF closes
its header and writes a footer, AVI patches its RIFF sizes). Promoting such
a file mid-render publishes a truncated, usually unplayable clip.

Three independent signals are combined:

1. **Open for writing** (Windows only): opening the file for reading while
   refusing to share write access fails with a sharing violation exactly
   when another process still holds a write handle, which is what every
   renderer does until it finishes.
2. **Container structure**: cheap header/trailer parsing (no full read) that
   recognises the "not finalised yet" state of MOV/MP4, MXF and AVI.
   Unrecognised data is reported as *unknown*, never as incomplete, so a
   file this module cannot parse is never blocked on structure alone.
3. **Settle time** (opt-in): when a file's structure is unknown, a
   modification time newer than *settle_seconds* is treated as still being
   written. Used by auto-promote, where waiting a few seconds is harmless.
"""

__all__ = [
    "CONTAINER_EXTENSIONS", "is_container_file", "is_open_for_writing",
    "container_structure_status", "render_in_progress_reason",
    "find_unfinished_renders",
    "COMPLETE", "INCOMPLETE", "UNKNOWN",
]

import logging
import os
import struct
import sys
import time
from pathlib import Path
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

CONTAINER_EXTENSIONS = frozenset({
    ".mov", ".mp4", ".m4v", ".mxf", ".avi", ".mkv", ".webm",
})

COMPLETE = "complete"
INCOMPLETE = "incomplete"
UNKNOWN = "unknown"

_QT_EXTENSIONS = frozenset({".mov", ".mp4", ".m4v"})

# Top-level QuickTime / ISO-BMFF atoms. The first atom must be one of these
# for the file to be treated as QuickTime at all.
_QT_TOP_LEVEL_ATOMS = frozenset({
    b"ftyp", b"moov", b"mdat", b"free", b"skip", b"wide", b"pnot", b"uuid",
    b"junk", b"pdin", b"meta", b"moof", b"mfra", b"sidx", b"styp", b"udta",
    b"PICT", b"prfl",
})

# MXF partition pack keys share this 13-byte prefix; byte 13 is the
# partition kind (0x02 header, 0x03 body, 0x04 footer), byte 14 its status.
_MXF_PARTITION_PREFIX = bytes.fromhex("060E2B34020501010D01020101")
_MXF_RIP_KEY = bytes.fromhex("060E2B34020501010D0102010111")
_MXF_RUN_IN_MAX = 65536
_MXF_FOOTER_SEARCH_BYTES = 4 * 1024 * 1024


def is_container_file(path: Path) -> bool:
    return Path(path).suffix.lower() in CONTAINER_EXTENSIONS


# ---------------------------------------------------------------------------
# Signal 1: another process holds a write handle
# ---------------------------------------------------------------------------

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    _GENERIC_READ = 0x80000000
    _FILE_SHARE_READ = 0x00000001
    _FILE_SHARE_DELETE = 0x00000004
    _OPEN_EXISTING = 3
    _FILE_ATTRIBUTE_NORMAL = 0x80
    _ERROR_SHARING_VIOLATION = 32
    _ERROR_LOCK_VIOLATION = 33
    _INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _CreateFileW = _kernel32.CreateFileW
    _CreateFileW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
        wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    ]
    _CreateFileW.restype = wintypes.HANDLE
    _CloseHandle = _kernel32.CloseHandle
    _CloseHandle.argtypes = [wintypes.HANDLE]
    _CloseHandle.restype = wintypes.BOOL

    def _win32_long_path(path: Path) -> str:
        s = str(path)
        if s.startswith("\\\\?\\") or len(s) < 260:
            return s
        s = os.path.abspath(s)
        if s.startswith("\\\\?\\"):
            return s
        if s.startswith("\\\\"):
            return "\\\\?\\UNC\\" + s[2:]
        return "\\\\?\\" + s

    def is_open_for_writing(path: Path) -> bool:
        """True if another process has *path* open with write access.

        We ask for read access while sharing read + delete: Windows refuses
        that with a sharing violation if any existing handle has write
        access. Plain readers (players, Resolve) don't conflict. Works on
        local disks and SMB shares.
        """
        handle = _CreateFileW(
            _win32_long_path(path),
            _GENERIC_READ,
            _FILE_SHARE_READ | _FILE_SHARE_DELETE,
            None,
            _OPEN_EXISTING,
            _FILE_ATTRIBUTE_NORMAL,
            None,
        )
        if handle == _INVALID_HANDLE_VALUE or handle is None:
            err = ctypes.get_last_error()
            return err in (_ERROR_SHARING_VIOLATION, _ERROR_LOCK_VIOLATION)
        _CloseHandle(handle)
        return False
else:
    def is_open_for_writing(path: Path) -> bool:
        """POSIX has no mandatory share modes; rely on the other signals."""
        return False


# ---------------------------------------------------------------------------
# Signal 2: container structure
# ---------------------------------------------------------------------------

def _qt_status(f, size: int) -> tuple[str, str]:
    """Walk top-level atoms. A finished file has a ``moov`` atom and its
    atoms tile the file exactly; a file mid-render has no ``moov`` yet
    (it is written last) and often an ``mdat`` sized 0 or past EOF."""
    if 0 < size < 8:
        hdr = f.read(size)
        if len(hdr) > 4:
            tag_prefix = hdr[4:]
            if any(atom.startswith(tag_prefix) for atom in _QT_TOP_LEVEL_ATOMS):
                return INCOMPLETE, "QuickTime file header is truncated"
        elif hdr and hdr[0] == 0:
            return INCOMPLETE, "QuickTime file header is truncated"
        return UNKNOWN, ""
    pos = 0
    seen_moov = False
    first = True
    while pos < size:
        if size - pos < 8:
            # A few trailing pad bytes after the last atom are tolerated.
            break
        f.seek(pos)
        header = f.read(16)
        if len(header) < 8:
            break
        atom_size, atom_type = struct.unpack(">I4s", header[:8])
        if first and atom_type not in _QT_TOP_LEVEL_ATOMS:
            return UNKNOWN, ""
        first = False
        if not all(32 <= b < 127 for b in atom_type):
            return UNKNOWN, ""
        if atom_type in (b"moof", b"mfra", b"sidx", b"styp"):
            # Fragmented MP4: playable while growing, completeness unknowable.
            return UNKNOWN, ""
        if atom_type == b"moov":
            seen_moov = True

        if atom_size == 1:
            if len(header) < 16:
                return INCOMPLETE, "QuickTime atom header is truncated"
            atom_size = struct.unpack(">Q", header[8:16])[0]
            # A 64-bit size of 0 is a placeholder, handled like size 0 below.
            if 0 < atom_size < 16:
                return UNKNOWN, ""
        if atom_size == 0:
            # "Extends to end of file" — what writers use as a placeholder
            # for mdat until they patch in the real size at the end.
            if not seen_moov:
                return INCOMPLETE, (
                    f"'{atom_type.decode('latin-1')}' atom size not finalised "
                    f"and no movie index (moov) yet"
                )
            return COMPLETE, ""
        if atom_size < 8:
            return UNKNOWN, ""

        if pos + atom_size > size:
            return INCOMPLETE, (
                f"'{atom_type.decode('latin-1')}' atom runs past end of file "
                f"({pos + atom_size:,} > {size:,} bytes)"
            )
        pos += atom_size

    if first:
        return UNKNOWN, ""
    if not seen_moov:
        return INCOMPLETE, "no movie index (moov atom) written yet"
    return COMPLETE, ""


def _mxf_status(f, size: int) -> tuple[str, str]:
    """A finished MXF has a closed/complete header partition, a Random
    Index Pack at the very end, or a footer partition near the end. A file
    mid-render has an open/incomplete header and nothing at the tail."""
    f.seek(0)
    head = f.read(_MXF_RUN_IN_MAX + 16)
    idx = head.find(_MXF_PARTITION_PREFIX)
    if idx < 0 or idx + 15 > len(head) or head[idx + 13] != 0x02:
        return UNKNOWN, ""
    header_status = head[idx + 14]
    if header_status in (0x03, 0x04):  # open/closed *complete*
        return COMPLETE, ""
    if header_status not in (0x01, 0x02):
        return UNKNOWN, ""

    # Random Index Pack: last 4 bytes are its total length.
    if size >= 20:
        f.seek(size - 4)
        rip_len = struct.unpack(">I", f.read(4))[0]
        if 20 <= rip_len <= size:
            f.seek(size - rip_len)
            if f.read(len(_MXF_RIP_KEY)) == _MXF_RIP_KEY:
                return COMPLETE, ""

    # Footer partition pack somewhere in the tail.
    tail_start = max(idx + 16, size - _MXF_FOOTER_SEARCH_BYTES)
    f.seek(tail_start)
    tail = f.read(size - tail_start)
    search = 0
    while True:
        k = tail.find(_MXF_PARTITION_PREFIX, search)
        if k < 0 or k + 14 > len(tail):
            break
        if tail[k + 13] == 0x04:
            return COMPLETE, ""
        search = k + 1

    return INCOMPLETE, "MXF header is still open and no footer has been written"


def _avi_status(f, size: int) -> tuple[str, str]:
    """RIFF chunk sizes are patched when the writer finishes; until then
    they are 0 or don't add up to the file size."""
    if 0 < size < 12:
        hdr = f.read(size)
        if len(hdr) <= 4:
            if b"RIFF".startswith(hdr):
                return INCOMPLETE, "AVI RIFF header is truncated"
        elif hdr.startswith(b"RIFF"):
            if len(hdr) <= 8 or b"AVI ".startswith(hdr[8:]):
                return INCOMPLETE, "AVI RIFF header is truncated"
        return UNKNOWN, ""
    pos = 0
    first = True
    while pos < size:
        if size - pos < 12:
            break
        f.seek(pos)
        hdr = f.read(12)
        tag, chunk_size, form = struct.unpack("<4sI4s", hdr)
        if first and (tag != b"RIFF" or form != b"AVI "):
            return UNKNOWN, ""
        first = False
        if tag != b"RIFF":
            # Trailing junk after the last RIFF (some writers pad).
            break
        if chunk_size == 0:
            return INCOMPLETE, "AVI RIFF size not finalised"
        end = pos + 8 + chunk_size + (chunk_size & 1)
        if end > size + 1:  # tolerate a missing final pad byte
            return INCOMPLETE, (
                f"AVI RIFF chunk runs past end of file ({end:,} > {size:,} bytes)"
            )
        pos = end
    if first:
        return UNKNOWN, ""
    return COMPLETE, ""


def container_structure_status(path: Path) -> tuple[str, str]:
    """Return ``(COMPLETE | INCOMPLETE | UNKNOWN, reason)`` for *path*.

    Only headers/trailers are read, so this is cheap even over SMB.
    """
    path = Path(path)
    ext = path.suffix.lower()
    if ext in _QT_EXTENSIONS:
        parser = _qt_status
    elif ext == ".mxf":
        parser = _mxf_status
    elif ext == ".avi":
        parser = _avi_status
    else:
        return UNKNOWN, ""
    try:
        with open(path, "rb") as f:
            size = os.fstat(f.fileno()).st_size
            if size == 0:
                return INCOMPLETE, "file is empty"
            return parser(f, size)
    except OSError as e:
        logger.debug(f"Structure check skipped for {path}: {e}")
        return UNKNOWN, ""
    except struct.error:
        return UNKNOWN, ""


# ---------------------------------------------------------------------------
# Combined verdict
# ---------------------------------------------------------------------------

def render_in_progress_reason(path: Path, settle_seconds: float = 0.0,
                              now: Optional[float] = None) -> Optional[str]:
    """Return why *path* looks like it is still being rendered, or None."""
    path = Path(path)
    if not is_container_file(path):
        return None
    try:
        if is_open_for_writing(path):
            return "still open for writing by another process"
    except OSError:
        pass

    status, reason = container_structure_status(path)
    if status == INCOMPLETE:
        return reason
    if status == COMPLETE:
        return None

    if settle_seconds > 0:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return None
        age = (time.time() if now is None else now) - mtime
        # Large negative ages mean server clock skew; don't block on those.
        if -settle_seconds < age < settle_seconds:
            return f"modified {max(age, 0):.0f}s ago (waiting {settle_seconds:.0f}s for it to settle)"
    return None


def find_unfinished_renders(paths: Iterable[Path],
                            settle_seconds: float = 0.0) -> list[tuple[Path, str]]:
    """Return ``(path, reason)`` for each container file that is not finished."""
    now = time.time()
    unfinished = []
    for p in paths:
        reason = render_in_progress_reason(p, settle_seconds=settle_seconds, now=now)
        if reason:
            unfinished.append((Path(p), reason))
    return unfinished
