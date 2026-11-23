"""
Filesystem facts the file watcher needs: is a path on a network share?

Native change notification is unreliable on network filesystems: macOS
FSEvents and Linux inotify do not see changes that *other machines* make on
an SMB/NFS mount (a render node writing into the share), while Windows
ReadDirectoryChangesW does work over SMB. ``choose_watch_backend`` turns
that into a per-source decision.

Cheap and local on every platform: no access to the share itself, so a dead
or slow share can't block the caller.
"""

__all__ = [
    "is_network_path", "choose_watch_backend", "parse_proc_mounts",
    "parse_macos_mount_output", "WATCH_MODES", "NATIVE", "POLLING",
]

import logging
import os
import subprocess
import sys
import threading
import time
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

NATIVE = "native"
POLLING = "polling"
WATCH_MODES = ("auto", NATIVE, POLLING)

# Filesystem types (Linux /proc/mounts, macOS `mount`) that live on another machine.
NETWORK_FS_TYPES = frozenset({
    "cifs", "smb3", "smbfs", "smb", "nfs", "nfs4", "afpfs", "webdav", "davfs",
    "fuse.sshfs", "sshfs", "fuse.rclone", "9p", "ceph", "glusterfs",
    "fuse.glusterfs", "lustre", "beegfs", "gpfs",
})

_DRIVE_REMOTE = 4  # GetDriveTypeW


def parse_proc_mounts(text: str) -> list[tuple[str, str]]:
    """``[(mount_point, fs_type)]`` from the contents of /proc/mounts."""
    mounts = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            # /proc/mounts escapes spaces as \040
            mounts.append((parts[1].replace("\\040", " "), parts[2]))
    return mounts


def parse_macos_mount_output(text: str) -> list[tuple[str, str]]:
    """``[(mount_point, fs_type)]`` from macOS ``mount`` output, e.g.
    ``//user@nas/show on /Volumes/show (smbfs, nodev, nosuid, mounted by user)``."""
    mounts = []
    for line in text.splitlines():
        if " on " not in line or "(" not in line:
            continue
        rest = line.split(" on ", 1)[1]
        mount_point, _, opts = rest.rpartition(" (")
        fs_type = opts.split(",", 1)[0].strip(" )")
        if mount_point:
            mounts.append((mount_point, fs_type))
    return mounts


class _MountTable:
    """Mount table cached for a minute (shares get mounted while LVM runs)."""
    TTL = 60.0

    def __init__(self):
        self._lock = threading.Lock()
        self._mounts: Optional[list[tuple[str, str]]] = None
        self._loaded_at = 0.0

    def get(self) -> list[tuple[str, str]]:
        with self._lock:
            if self._mounts is None or time.monotonic() - self._loaded_at > self.TTL:
                self._mounts = self._load()
                self._loaded_at = time.monotonic()
            return self._mounts

    @staticmethod
    def _load() -> list[tuple[str, str]]:
        try:
            if sys.platform.startswith("linux"):
                with open("/proc/mounts", "r", encoding="utf-8", errors="replace") as f:
                    return parse_proc_mounts(f.read())
            if sys.platform == "darwin":
                out = subprocess.run(["/sbin/mount"], capture_output=True, text=True,
                                     timeout=5).stdout
                return parse_macos_mount_output(out)
        except (OSError, subprocess.SubprocessError) as e:
            logger.debug("Could not read mount table: %s", e)
        return []


_MOUNT_TABLE = _MountTable()


def _fs_type_for(path: str, mounts: Iterable[tuple[str, str]]) -> Optional[str]:
    """fs type of the longest mount point containing *path*."""
    best, best_len = None, -1
    for mount_point, fs_type in mounts:
        mp = mount_point.rstrip("/") or "/"
        if path == mp or path.startswith(mp + "/") or mp == "/":
            if len(mp) > best_len:
                best, best_len = fs_type, len(mp)
    return best


def _windows_is_network(path: str) -> bool:
    if path.startswith(("\\\\", "//")):
        return True  # UNC path
    drive = os.path.splitdrive(path)[0]
    if not drive:
        return False
    try:
        import ctypes
        return ctypes.windll.kernel32.GetDriveTypeW(drive + "\\") == _DRIVE_REMOTE
    except (OSError, AttributeError):
        return False


def is_network_path(path, *, platform: Optional[str] = None,
                    mounts: Optional[Iterable[tuple[str, str]]] = None) -> bool:
    """True if *path* is on a network filesystem (SMB, NFS, AFP, ...).

    *platform* and *mounts* exist for tests; by default the running
    platform and its (cached) mount table are used. Unknown means False.
    """
    platform = platform or sys.platform
    p = os.path.abspath(str(path)) if platform == sys.platform else str(path)
    if platform == "win32":
        return _windows_is_network(p)
    if mounts is None:
        mounts = _MOUNT_TABLE.get()
    fs_type = _fs_type_for(p, mounts)
    return bool(fs_type) and fs_type.lower() in NETWORK_FS_TYPES


def choose_watch_backend(path, mode: str = "auto", *,
                         platform: Optional[str] = None,
                         mounts: Optional[Iterable[tuple[str, str]]] = None) -> str:
    """``NATIVE`` or ``POLLING`` for watching *path* under watch *mode*.

    ``auto`` polls network paths on macOS/Linux, where native events miss
    changes made by other machines, and uses native events everywhere else
    (including SMB on Windows, where they work).
    """
    if mode == NATIVE:
        return NATIVE
    if mode == POLLING:
        return POLLING
    platform = platform or sys.platform
    if platform != "win32" and is_network_path(path, platform=platform, mounts=mounts):
        return POLLING
    return NATIVE
