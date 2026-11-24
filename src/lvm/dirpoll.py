"""
Lightweight directory poller — the watcher's fallback where native change
events can't be trusted (network shares on macOS/Linux, a native watch that
died, a share that wasn't reachable when watching started).

Each poll lists every watched directory once with ``os.scandir`` and
compares entry *names* with the previous poll. LVM only reacts to new
entries (a new version folder or file, or one renamed into place), so no
per-entry ``stat`` is needed — unlike watchdog's PollingObserver, which
stats every entry and costs one round-trip per file on SMB.

No Qt dependency. The callback runs on the poller thread.
"""

__all__ = ["DirectoryPoller"]

import logging
import os
import threading
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class DirectoryPoller:
    """Polls directories on a background thread and calls
    ``callback(key)`` when new entries appear in the directory for *key*.

    A directory that can't be listed (share unmounted, permission denied) is
    logged once and retried every poll; when it becomes readable again the
    callback fires once as a catch-up, since anything may have arrived in
    the meantime.
    """

    def __init__(self, callback: Callable[[str], None], interval: float = 10.0):
        self._callback = callback
        self.interval = max(0.5, float(interval))
        self._dirs: dict[str, str] = {}          # key -> path
        self._names: dict[str, Optional[frozenset]] = {}  # key -> last names, None = unreadable
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- configuration ------------------------------------------------------

    def add(self, key: str, path: str) -> None:
        """Start polling *path* for *key*. The first poll only records the
        current entries; nothing that already exists triggers the callback."""
        with self._lock:
            self._dirs[key] = str(path)
            self._names.pop(key, None)

    def remove(self, key: str) -> None:
        with self._lock:
            self._dirs.pop(key, None)
            self._names.pop(key, None)

    @property
    def keys(self) -> list[str]:
        with self._lock:
            return list(self._dirs)

    def is_reachable(self, key: str) -> Optional[bool]:
        """False while *key*'s directory can't be listed, None before the first poll."""
        with self._lock:
            if key not in self._names:
                return None
            return self._names[key] is not None

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        if self.is_alive():
            return
        # Allocate a fresh stop event per thread so a previously-blocked
        # thread from an earlier stop() never sees its event cleared.
        stop_ev = threading.Event()
        self._stop = stop_ev
        self._thread = threading.Thread(target=self._run, args=(stop_ev,), daemon=True,
                                        name="lvm-dir-poller")
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._thread = None

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # -- polling ------------------------------------------------------------

    def _run(self, stop_ev: threading.Event) -> None:
        while not stop_ev.is_set():
            self.poll_once(_stop_ev=stop_ev)
            stop_ev.wait(self.interval)

    def poll_once(self, _stop_ev: Optional[threading.Event] = None) -> list[str]:
        """Poll every directory once; returns the keys that changed (the
        callback has already been called for them)."""
        stop_ev = _stop_ev if _stop_ev is not None else self._stop
        with self._lock:
            targets = list(self._dirs.items())
        changed = []
        for key, path in targets:
            if stop_ev.is_set():
                break
            names = self._list(path)
            with self._lock:
                if self._dirs.get(key) != path:
                    continue  # removed or repointed meanwhile
                first_poll = key not in self._names
                previous = self._names.get(key)
                self._names[key] = names
            if names is None:
                if first_poll or previous is not None:
                    logger.warning("Watcher: cannot read %s — will keep retrying", path)
                continue
            if first_poll:
                continue
            if previous is None:
                logger.info("Watcher: %s is reachable again — rescanning", path)
                changed.append(key)
            elif names - previous:
                changed.append(key)
        for key in changed:
            if stop_ev.is_set():
                break
            try:
                self._callback(key)
            except Exception:
                logger.exception("Directory poller callback failed for %s", key)
        return changed

    @staticmethod
    def _list(path: str) -> Optional[frozenset]:
        try:
            with os.scandir(path) as it:
                return frozenset(e.name for e in it)
        except OSError:
            return None
