"""
Platform-native fast copy — drop-in replacement for shutil.copy2().

Uses OS-level APIs that enable optimizations Python's shutil cannot trigger:

- **Windows**: CopyFileExW via ctypes — automatically uses SMB server-side
  copy (FSCTL_SRV_COPYCHUNK) when both paths are on the same SMB 3.0+ server.
  Also faster than shutil locally (kernel-level, no Python buffering).

- **macOS**: clonefile() for instant APFS Copy-on-Write clones on the same
  volume, then copyfile(COPYFILE_CLONE | COPYFILE_ALL) which tries CoW first
  and falls back to native copy with metadata.

- **Linux**: os.copy_file_range() for kernel-level copy acceleration,
  including NFS 4.2+ and CIFS server-side copy.

All paths fall back gracefully to shutil.copy2() if native APIs are
unavailable or fail.
"""

__all__ = ["smart_copy", "CopyCancelled", "is_same_smb_server"]

import os
import re
import sys
import shutil
import logging
import threading
from pathlib import Path
from typing import Optional, Callable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# UNC path utilities (Windows)
# ---------------------------------------------------------------------------

_UNC_SERVER_RE = re.compile(r"^\\\\([^\\]+)\\")


def _parse_unc_server(path: Path) -> Optional[str]:
    """Extract the server name from a UNC path.

    Returns the lowercase server name, or None if not a UNC path.
    Handles both ``\\\\server\\share`` and ``\\\\?\\UNC\\server\\share`` forms.
    """
    s = str(path)
    # Extended-length UNC: \\?\UNC\server\share
    if s.startswith("\\\\?\\UNC\\"):
        remainder = s[8:]
        sep = remainder.find("\\")
        if sep > 0:
            return remainder[:sep].lower()
        return remainder.lower() if remainder else None
    # Standard UNC: \\server\share
    m = _UNC_SERVER_RE.match(s)
    if m:
        return m.group(1).lower()
    return None


def is_same_smb_server(src: Path, dst: Path) -> bool:
    """Return True if both paths are on the same SMB server."""
    src_server = _parse_unc_server(src)
    dst_server = _parse_unc_server(dst)
    if src_server and dst_server:
        return src_server == dst_server
    return False


# ---------------------------------------------------------------------------
# Windows: CopyFileExW via ctypes
# ---------------------------------------------------------------------------

if sys.platform == "win32":
    import ctypes
    import ctypes.wintypes as wintypes

    # CopyFileExW flags
    _COPY_FILE_FAIL_IF_EXISTS = 0x00000001
    _COPY_FILE_NO_BUFFERING = 0x00001000

    # Progress callback return values
    _PROGRESS_CONTINUE = 0
    _PROGRESS_CANCEL = 1

    # Callback reason
    _CALLBACK_CHUNK_FINISHED = 0x00000000

    _LARGE_INTEGER = ctypes.c_int64

    # LPPROGRESS_ROUTINE signature
    _LPPROGRESS_ROUTINE = ctypes.WINFUNCTYPE(
        wintypes.DWORD,    # return
        _LARGE_INTEGER,    # TotalFileSize
        _LARGE_INTEGER,    # TotalBytesTransferred
        _LARGE_INTEGER,    # StreamSize
        _LARGE_INTEGER,    # StreamBytesTransferred
        wintypes.DWORD,    # dwStreamNumber
        wintypes.DWORD,    # dwCallbackReason
        wintypes.HANDLE,   # hSourceFile
        wintypes.HANDLE,   # hDestinationFile
        ctypes.c_void_p,   # lpData
    )

    _CopyFileExW = None
    _win32_available: Optional[bool] = None

    def _noop_progress(*args):
        return _PROGRESS_CONTINUE

    _NOOP_CALLBACK = _LPPROGRESS_ROUTINE(_noop_progress)

    def _setup_win32_copy() -> bool:
        """One-time initialization of CopyFileExW binding."""
        global _CopyFileExW, _win32_available
        if _win32_available is not None:
            return _win32_available
        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            _CopyFileExW = kernel32.CopyFileExW
            _CopyFileExW.argtypes = [
                wintypes.LPCWSTR,
                wintypes.LPCWSTR,
                _LPPROGRESS_ROUTINE,
                ctypes.c_void_p,
                ctypes.POINTER(wintypes.BOOL),
                wintypes.DWORD,
            ]
            _CopyFileExW.restype = wintypes.BOOL
            _win32_available = True
            logger.debug("Win32 CopyFileExW binding initialized")
        except (OSError, AttributeError) as exc:
            logger.debug("Win32 CopyFileExW not available: %s", exc)
            _win32_available = False
        return _win32_available

    def _prepare_win32_path(path: Path) -> str:
        """Prepend extended-length prefix for paths exceeding MAX_PATH."""
        s = str(path)
        if len(s) < 260:
            return s
        if s.startswith("\\\\"):
            return "\\\\?\\UNC\\" + s[2:]
        return "\\\\?\\" + s

    def _win32_copy_file(
        src: Path,
        dst: Path,
        cancel_event: Optional[threading.Event] = None,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> bool:
        """Copy a file using CopyFileExW. Returns False on failure."""
        if not _setup_win32_copy():
            return False

        cancel_flag = wintypes.BOOL(False)
        # Must hold a reference to prevent GC during the copy
        callback_ref = _NOOP_CALLBACK

        if progress_cb or cancel_event:
            def _progress(
                total_size, transferred, _ss, _st, _sn, reason, _hs, _hd, _d
            ):
                if cancel_event and cancel_event.is_set():
                    cancel_flag.value = True
                    return _PROGRESS_CANCEL
                if progress_cb and reason == _CALLBACK_CHUNK_FINISHED:
                    progress_cb(int(transferred), int(total_size))
                return _PROGRESS_CONTINUE

            callback_ref = _LPPROGRESS_ROUTINE(_progress)

        try:
            result = _CopyFileExW(
                _prepare_win32_path(src),
                _prepare_win32_path(dst),
                callback_ref,
                None,
                ctypes.byref(cancel_flag),
                0,
            )
            if not result:
                err = ctypes.get_last_error()
                # 1235 = ERROR_REQUEST_ABORTED (user cancelled)
                if err == 1235 and cancel_event and cancel_event.is_set():
                    raise CopyCancelled("Copy cancelled by user")
                logger.debug(
                    "CopyFileExW failed for %s (error %d)", src.name, err
                )
                return False
            return True
        except CopyCancelled:
            raise
        except Exception as exc:
            logger.debug("CopyFileExW exception for %s: %s", src.name, exc)
            return False


# ---------------------------------------------------------------------------
# macOS: clonefile + copyfile via ctypes
# ---------------------------------------------------------------------------

if sys.platform == "darwin":
    import ctypes
    import ctypes.util

    # copyfile flags (from <copyfile.h>)
    _COPYFILE_ACL = 1 << 0
    _COPYFILE_STAT = 1 << 1
    _COPYFILE_XATTR = 1 << 2
    _COPYFILE_DATA = 1 << 3
    _COPYFILE_SECURITY = _COPYFILE_ACL | _COPYFILE_STAT
    _COPYFILE_METADATA = _COPYFILE_SECURITY | _COPYFILE_XATTR
    _COPYFILE_ALL = _COPYFILE_METADATA | _COPYFILE_DATA
    _COPYFILE_CLONE = 1 << 24
    _COPYFILE_CLONE_FORCE = 1 << 25

    _libsystem = None
    _clonefile = None
    _copyfile_func = None
    _macos_available: Optional[bool] = None

    def _setup_macos_copy() -> bool:
        """One-time initialization of macOS copy bindings."""
        global _libsystem, _clonefile, _copyfile_func, _macos_available
        if _macos_available is not None:
            return _macos_available
        try:
            _libsystem = ctypes.CDLL("libSystem.B.dylib", use_errno=True)

            _clonefile = _libsystem.clonefile
            _clonefile.argtypes = [
                ctypes.c_char_p,  # src
                ctypes.c_char_p,  # dst
                ctypes.c_int,     # flags
            ]
            _clonefile.restype = ctypes.c_int

            _copyfile_func = _libsystem.copyfile
            _copyfile_func.argtypes = [
                ctypes.c_char_p,  # from
                ctypes.c_char_p,  # to
                ctypes.c_void_p,  # state (NULL)
                ctypes.c_uint32,  # flags
            ]
            _copyfile_func.restype = ctypes.c_int

            _macos_available = True
            logger.debug("macOS clonefile/copyfile bindings initialized")
        except (OSError, AttributeError) as exc:
            logger.debug("macOS native copy not available: %s", exc)
            _macos_available = False
        return _macos_available

    def _macos_clonefile(src: Path, dst: Path) -> bool:
        """Try an APFS CoW clone. Only works on same APFS volume."""
        if not _setup_macos_copy():
            return False
        try:
            result = _clonefile(
                str(src).encode("utf-8"),
                str(dst).encode("utf-8"),
                0,
            )
            if result == 0:
                return True
            errno_val = ctypes.get_errno()
            # ENOTSUP (45) = filesystem doesn't support cloning
            # EXDEV (18) = cross-device (different volumes)
            logger.debug(
                "clonefile failed for %s (errno %d)", src.name, errno_val
            )
            return False
        except Exception as exc:
            logger.debug("clonefile exception for %s: %s", src.name, exc)
            return False

    def _macos_copyfile(src: Path, dst: Path) -> bool:
        """Copy using macOS copyfile() with COPYFILE_CLONE | COPYFILE_ALL.

        Tries CoW clone first, falls back to native copy with full metadata.
        Faster than shutil.copy2() because it avoids Python-level buffering.
        """
        if not _setup_macos_copy():
            return False
        try:
            flags = _COPYFILE_CLONE | _COPYFILE_ALL
            result = _copyfile_func(
                str(src).encode("utf-8"),
                str(dst).encode("utf-8"),
                None,
                flags,
            )
            if result == 0:
                return True
            errno_val = ctypes.get_errno()
            logger.debug(
                "copyfile failed for %s (errno %d)", src.name, errno_val
            )
            return False
        except Exception as exc:
            logger.debug("copyfile exception for %s: %s", src.name, exc)
            return False


# ---------------------------------------------------------------------------
# Linux: os.copy_file_range
# ---------------------------------------------------------------------------

def _linux_copy_file_range(src: Path, dst: Path) -> bool:
    """Copy using os.copy_file_range() on Linux (Python 3.8+).

    Enables kernel-level acceleration for NFS 4.2+, CIFS, btrfs reflinks, etc.
    """
    if not hasattr(os, "copy_file_range"):
        return False
    try:
        src_size = src.stat().st_size
        if src_size == 0:
            # copy_file_range doesn't handle empty files — just create it
            dst.touch()
            return True
        chunk = 128 * 1024 * 1024  # 128 MB
        with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
            copied = 0
            while copied < src_size:
                written = os.copy_file_range(
                    fsrc.fileno(), fdst.fileno(), min(chunk, src_size - copied)
                )
                if written == 0:
                    break
                copied += written
        return True
    except OSError as exc:
        logger.debug("copy_file_range failed for %s: %s", src.name, exc)
        return False


# ---------------------------------------------------------------------------
# Metadata preservation
# ---------------------------------------------------------------------------

def _preserve_metadata(src: Path, dst: Path):
    """Copy timestamps and permissions from src to dst.

    CopyFileExW and copy_file_range don't fully replicate shutil.copy2's
    metadata handling. macOS copyfile with COPYFILE_ALL does, so skip for that.
    """
    try:
        shutil.copystat(str(src), str(dst))
    except OSError as exc:
        logger.debug("Could not preserve metadata for %s: %s", dst.name, exc)


# ---------------------------------------------------------------------------
# Cancellation exception
# ---------------------------------------------------------------------------

class CopyCancelled(Exception):
    """Raised when a copy operation is cancelled via cancel_event."""
    pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def smart_copy(
    src: Path,
    dst: Path,
    cancel_event: Optional[threading.Event] = None,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> None:
    """Copy a file using the fastest available platform method.

    Falls back to shutil.copy2() if native APIs are unavailable or fail.

    Args:
        src: Source file path.
        dst: Destination file path.
        cancel_event: Optional threading.Event for cancellation.
        progress_cb: Optional callback(bytes_copied, total_bytes).
                     Only effective on Windows (CopyFileExW progress routine).

    Raises:
        CopyCancelled: If cancelled via cancel_event.
        shutil.Error / OSError: If all copy methods fail.
    """
    if sys.platform == "win32":
        same_server = is_same_smb_server(src, dst)
        if same_server:
            logger.info("Server-side copy (same SMB server): %s", src.name)
        else:
            logger.debug("Native copy (CopyFileExW): %s", src.name)

        if _win32_copy_file(src, dst, cancel_event, progress_cb):
            _preserve_metadata(src, dst)
            return
        logger.debug("Falling back to shutil.copy2 for %s", src.name)

    elif sys.platform == "darwin":
        # Try instant APFS clone first, then native copyfile
        if _macos_clonefile(src, dst):
            logger.debug("APFS clone succeeded: %s", src.name)
            return
        if _macos_copyfile(src, dst):
            logger.debug("macOS copyfile succeeded: %s", src.name)
            # copyfile with COPYFILE_ALL preserves metadata — no extra step
            return
        logger.debug("Falling back to shutil.copy2 for %s", src.name)

    elif sys.platform == "linux":
        if _linux_copy_file_range(src, dst):
            logger.debug("copy_file_range succeeded: %s", src.name)
            _preserve_metadata(src, dst)
            return
        logger.debug("Falling back to shutil.copy2 for %s", src.name)

    # Universal fallback
    shutil.copy2(str(src), str(dst))
