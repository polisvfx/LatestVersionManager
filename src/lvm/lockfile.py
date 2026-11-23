"""
Inter-process lock for promotion target directories.

A promotion writes into a shared "latest" directory that other LVM
processes may also write into — the GUI and CLI on one machine, or two
artists' machines over SMB. PromoteLock serializes those writers with an
atomically-created lock file inside the target directory.

The lock is leased, not permanent: the holder refreshes the lock file's
mtime from a heartbeat thread, so a crashed promotion goes stale after
``stale_after`` seconds and the next promotion takes the lock over. A
dead PID on the same host is taken over immediately. Each acquisition
embeds a unique UUID token so stale-takeover, heartbeat, and release
operations never interfere with a different holder's live lock.
"""

__all__ = ["PromoteLock", "LockHeldError", "LOCK_FILENAME"]

import json
import logging
import os
import socket
import tempfile
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

LOCK_FILENAME = ".lvm_promote.lock"
DEFAULT_STALE_AFTER = 300.0  # seconds without a heartbeat before takeover
HEARTBEAT_INTERVAL = 30.0


def _current_user() -> str:
    try:
        return os.getlogin()
    except OSError:
        return os.environ.get("USER", os.environ.get("USERNAME", "unknown"))


class LockHeldError(Exception):
    """Raised when the target directory is locked by another promotion."""

    def __init__(self, message: str, info: Optional[dict] = None):
        super().__init__(message)
        self.info = info or {}


class PromoteLock:
    """Lease-style lock file guarding one promotion target directory.

    Usage::

        lock = PromoteLock(target_dir)
        lock.acquire()          # raises LockHeldError when busy
        try:
            ...                 # promote
        finally:
            lock.release()

    Also usable as a context manager.
    """

    def __init__(self, directory, *,
                 stale_after: float = DEFAULT_STALE_AFTER,
                 heartbeat_interval: float = HEARTBEAT_INTERVAL):
        self._directory = Path(directory)
        self.stale_after = stale_after
        self.heartbeat_interval = heartbeat_interval
        self._acquired = False
        self._token: Optional[str] = None
        self._hb_thread: Optional[threading.Thread] = None
        self._hb_stop: Optional[threading.Event] = None

    @property
    def path(self) -> Path:
        return self._directory / LOCK_FILENAME

    # -- acquisition ---------------------------------------------------------

    def acquire(self) -> None:
        """Take the lock or raise LockHeldError with the holder's info."""
        if self._acquired:
            return
        if self._try_create():
            return
        info = self._read_info()
        if self._is_stale(info):
            logger.warning(
                "Taking over stale promote lock %s (holder: %s)", self.path, info
            )
            self._evict_stale_lock(info)
            # Single retry — if another process won the takeover race,
            # correctly defer to it.
            if self._try_create():
                return
            info = self._read_info()
        raise LockHeldError(
            f"Promotion target is locked: {self.path}", info
        )

    def release(self) -> None:
        """Drop the lock if still owned by this instance. Safe when not held."""
        if not self._acquired:
            return
        self._stop_heartbeat()
        token = self._token
        self._token = None
        self._acquired = False
        try:
            if token is not None and not self._matches_token(self.path, token):
                logger.warning(
                    "Promote lock %s was taken over by another holder; leaving it intact",
                    self.path,
                )
                return
            self.path.unlink(missing_ok=True)
        except OSError as e:
            logger.warning("Could not remove promote lock %s: %s", self.path, e)

    def __enter__(self) -> "PromoteLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    # -- internals -----------------------------------------------------------

    def _try_create(self) -> bool:
        token = uuid.uuid4().hex
        payload = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "user": _current_user(),
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "token": token,
        }
        try:
            from . import __version__
            payload["lvm_version"] = __version__
        except Exception:
            pass
        try:
            fd = os.open(
                str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644
            )
        except FileExistsError:
            return False
        try:
            os.write(fd, json.dumps(payload).encode("utf-8"))
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            try:
                self.path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        os.close(fd)
        self._token = token
        self._acquired = True
        self._start_heartbeat()
        return True

    def _evict_stale_lock(self, stale_info: Optional[dict]) -> None:
        """Remove a stale lock without unlinking a fresh lock acquired concurrently.

        Renames the lock to a unique tombstone first; if the tombstone turns out
        to belong to a *different* holder (token changed in the race window),
        restore it best-effort rather than deleting another live lock.
        """
        tombstone = self._directory / f"{LOCK_FILENAME}.stale.{uuid.uuid4().hex}"
        try:
            os.replace(self.path, tombstone)
        except OSError:
            return
        try:
            moved_info = self._read_info_from(tombstone)
            stale_tok = stale_info.get("token") if isinstance(stale_info, dict) else None
            moved_tok = moved_info.get("token") if isinstance(moved_info, dict) else None
            if moved_tok and moved_tok != stale_tok and not self._is_info_dead(moved_info):
                # Another process won the race right before our rename — put it back
                try:
                    os.replace(tombstone, self.path)
                    return
                except OSError:
                    pass
        finally:
            try:
                tombstone.unlink(missing_ok=True)
            except OSError:
                pass

    @staticmethod
    def _read_info_from(path: Path) -> Optional[dict]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
        except (OSError, ValueError):
            return None

    def _read_info(self) -> Optional[dict]:
        return self._read_info_from(self.path)

    @classmethod
    def _matches_token(cls, path: Path, token: str) -> bool:
        info = cls._read_info_from(path)
        return isinstance(info, dict) and info.get("token") == token

    def _is_info_dead(self, info: Optional[dict]) -> bool:
        if info and info.get("host") == socket.gethostname():
            pid = info.get("pid")
            if isinstance(pid, int) and self._pid_alive(pid) is False:
                return True
        return False

    def _server_now(self) -> float:
        """Estimate the filesystem's current wall clock via a quick probe file.

        Falls back to local ``time.time()`` if the directory isn't writable.
        Prevents SMB/NFS client-vs-server clock skew from treating active
        locks from other machines as immediately stale.
        """
        try:
            fd, probe = tempfile.mkstemp(
                prefix=".lvm_clock_", suffix=".tmp", dir=str(self._directory)
            )
            try:
                os.close(fd)
                return os.stat(probe).st_mtime
            finally:
                try:
                    os.unlink(probe)
                except OSError:
                    pass
        except OSError:
            return time.time()

    def _is_stale(self, info: Optional[dict]) -> bool:
        # A dead process on this host can never release its lock — take over.
        if self._is_info_dead(info):
            return True
        # Heartbeat rule: the holder refreshes mtime every heartbeat_interval,
        # so an mtime older than stale_after means the holder is gone. This
        # also covers foreign hosts and corrupt/unreadable lock files.
        try:
            mtime = self.path.stat().st_mtime
        except OSError:
            return True  # vanished — free to retry
        # Compare against the filesystem's own clock to tolerate SMB clock skew
        now = self._server_now()
        return (now - mtime) > self.stale_after

    @staticmethod
    def _pid_alive(pid: int) -> Optional[bool]:
        """True/False when determinable, None when unknown."""
        if pid <= 0:
            return None
        try:
            if os.name == "nt":
                import ctypes
                from ctypes import wintypes

                PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                ERROR_ACCESS_DENIED = 5
                ERROR_INVALID_PARAMETER = 87  # PID does not exist
                STILL_ACTIVE = 259

                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel32.OpenProcess.argtypes = [
                    wintypes.DWORD, wintypes.BOOL, wintypes.DWORD
                ]
                kernel32.OpenProcess.restype = wintypes.HANDLE
                kernel32.GetExitCodeProcess.argtypes = [
                    wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)
                ]
                kernel32.GetExitCodeProcess.restype = wintypes.BOOL
                kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
                kernel32.CloseHandle.restype = wintypes.BOOL

                handle = kernel32.OpenProcess(
                    PROCESS_QUERY_LIMITED_INFORMATION, False, pid
                )
                if handle:
                    try:
                        exit_code = wintypes.DWORD(0)
                        if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                            return exit_code.value == STILL_ACTIVE
                        return True
                    finally:
                        kernel32.CloseHandle(handle)
                err = ctypes.get_last_error()
                if err == ERROR_ACCESS_DENIED:
                    return True
                if err == ERROR_INVALID_PARAMETER:
                    return False
                return None
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return None

    def _start_heartbeat(self) -> None:
        self._hb_stop = threading.Event()
        stop = self._hb_stop
        path = self.path
        interval = self.heartbeat_interval
        token = self._token

        def _beat():
            while not stop.wait(interval):
                try:
                    # Only touch the lock while we still own it
                    if token is not None and not self._matches_token(path, token):
                        return
                    os.utime(path)
                except OSError:
                    pass

        self._hb_thread = threading.Thread(
            target=_beat, daemon=True, name="lvm-promote-lock-heartbeat"
        )
        self._hb_thread.start()

    def _stop_heartbeat(self) -> None:
        if self._hb_stop is not None:
            self._hb_stop.set()
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=5.0)
        self._hb_thread = None
        self._hb_stop = None
