"""
File Watcher - monitors source directories for new versions.

Emits Qt signals when new version folders or files appear so the GUI
can update without manual refresh.

Two backends, chosen per source (see :func:`fsinfo.choose_watch_backend`):

- **native** — watchdog's OS change notifications. Instant, but on macOS and
  Linux they don't see changes other machines make on a network share.
- **polling** — :class:`dirpoll.DirectoryPoller`, one directory listing per
  source per interval. Used for network shares on macOS/Linux, for sources
  whose directory wasn't reachable when watching started, and as the
  fallback when a native watch dies (share dropped, buffer overflow).
"""

__all__ = ["SourceWatcher"]

import logging

from PySide6.QtCore import QObject, Signal, QTimer, Slot, QMetaObject, Qt, Q_ARG
from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler, DirCreatedEvent, FileCreatedEvent,
    DirMovedEvent, FileMovedEvent,
)

from .dirpoll import DirectoryPoller
from .fsinfo import NATIVE, POLLING, choose_watch_backend
from .models import WatchedSource

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 10.0
HEALTH_CHECK_INTERVAL_MS = 15_000


class _FolderEventHandler(FileSystemEventHandler):
    """Watchdog handler that forwards events to a Qt callback."""

    def __init__(self, callback, watched_source: WatchedSource):
        super().__init__()
        self.callback = callback
        self.source = watched_source

    def on_created(self, event):
        if isinstance(event, (DirCreatedEvent, FileCreatedEvent)):
            self.callback(self.source.name)

    def on_moved(self, event):
        # Renders that write to a temp name and rename into place (or a
        # version folder moved in from elsewhere on the share) arrive as a
        # move, not a create.
        if isinstance(event, (DirMovedEvent, FileMovedEvent)):
            self.callback(self.source.name)


class SourceWatcher(QObject):
    """
    Watches multiple source directories and emits a signal when changes
    are detected, so the GUI can re-scan.

    Uses a debounce timer to avoid flooding the UI when a large render
    is being written (many files arriving in quick succession).
    """

    # Emitted with the source name when new files/folders appear
    source_changed = Signal(str)

    # Emitted when watching starts/stops (for status bar)
    watch_status_changed = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._observer = None
        self._poller = None
        self._handlers = {}
        self._native_watches = {}  # source name -> ObservedWatch
        self._source_dirs = {}     # source name -> directory
        self._running = False
        self._debounce_timer = QTimer(self)
        self._debounce_timer.setSingleShot(True)
        self._debounce_timer.setInterval(2000)  # 2 second debounce
        self._debounce_timer.timeout.connect(self._flush_pending)
        self._pending_sources = set()
        self._health_timer = QTimer(self)
        self._health_timer.setInterval(HEALTH_CHECK_INTERVAL_MS)
        self._health_timer.timeout.connect(self._check_health)

    def start(self, sources: list[WatchedSource], mode: str = "auto",
              poll_interval: float = DEFAULT_POLL_INTERVAL):
        """Start watching all configured source directories.

        *mode* is ``auto`` (poll network shares on macOS/Linux, native
        events elsewhere), ``native`` or ``polling``.
        """
        self.stop()

        self._observer = Observer()
        self._poller = DirectoryPoller(self._on_change, interval=poll_interval)
        # Start the observer *before* scheduling: on a running observer each
        # schedule() opens its directory right away and raises on its own,
        # whereas one missing directory makes Observer.start() fail for all.
        self._observer.start()

        for source in sources:
            if not source.source_dir:
                continue
            path = str(source.source_dir)
            self._source_dirs[source.name] = path
            backend = choose_watch_backend(path, mode)
            if backend == NATIVE and self._schedule_native(source):
                continue
            self._poller.add(source.name, path)

        native = len(self._native_watches)
        polled = len(self._poller.keys)
        if not native:
            self._observer.stop()
            self._observer.join(timeout=5)
        if polled:
            self._poller.start()
        self._running = bool(native or polled)
        if self._running:
            self._health_timer.start()
            # Catch watches whose thread died right at startup quickly.
            QTimer.singleShot(2000, self._check_health)
        self._emit_status()

    def _schedule_native(self, source: WatchedSource) -> bool:
        handler = _FolderEventHandler(self._on_change, source)
        try:
            watch = self._observer.schedule(handler, str(source.source_dir), recursive=False)
        except Exception as e:
            # Missing or unreachable directory — the poller retries it.
            logger.warning(f"Native watch failed for {source.source_dir} ({e}); polling instead")
            return False
        self._handlers[source.name] = handler
        self._native_watches[source.name] = watch
        logger.info(f"Watching: {source.source_dir}")
        return True

    def stop(self):
        """Stop all watchers."""
        self._running = False
        self._health_timer.stop()
        if self._observer and self._observer.is_alive():
            self._observer.stop()
            self._observer.join(timeout=5)
        self._observer = None
        if self._poller is not None:
            self._poller.stop()
        self._poller = None
        self._handlers.clear()
        self._native_watches.clear()
        self._source_dirs.clear()
        self._pending_sources.clear()
        self.watch_status_changed.emit("Watcher stopped")

    # -- health -------------------------------------------------------------

    def _check_health(self):
        """Move sources whose native watch has died to the poller.

        A watchdog emitter thread ends when its directory disappears or the
        share drops; nothing else notices, so the source would silently stop
        being watched. Only thread liveness is checked here — no filesystem
        access on the GUI thread.
        """
        if not self._running or self._observer is None:
            return
        observer_alive = self._observer.is_alive()
        alive_watches = set()
        if observer_alive:
            alive_watches = {e.watch for e in list(self._observer.emitters) if e.is_alive()}
        dead = [name for name, watch in self._native_watches.items()
                if not observer_alive or watch not in alive_watches]
        if not dead:
            if self._poller is not None and self._poller.keys and not self._poller.is_alive():
                logger.warning("Directory poller stopped — restarting it")
                self._poller.start()
            return

        for name in dead:
            watch = self._native_watches.pop(name)
            self._handlers.pop(name, None)
            if observer_alive:
                try:
                    self._observer.unschedule(watch)
                except Exception:
                    pass
            logger.warning(
                f"Native watch for '{name}' stopped ({self._source_dirs.get(name)}); "
                f"switching to polling"
            )
            self._poller.add(name, self._source_dirs[name])
            # Anything may have arrived while the watch was dead.
            self._on_change_main_thread(name)
        if not self._poller.is_alive():
            self._poller.start()
        self._emit_status()

    def _emit_status(self):
        native = len(self._native_watches)
        polled = len(self._poller.keys) if self._poller is not None else 0
        total = native + polled
        if not total:
            self.watch_status_changed.emit("No directories to watch")
            return
        msg = f"Watching {total} source(s)"
        if polled:
            msg += f" ({polled} by polling every {self._poller.interval:g}s)"
        self.watch_status_changed.emit(msg)

    def backend_for(self, source_name: str):
        """``"native"``, ``"polling"`` or None — for status display and tests."""
        if source_name in self._native_watches:
            return NATIVE
        if self._poller is not None and source_name in self._poller.keys:
            return POLLING
        return None

    # -- events -------------------------------------------------------------

    def _on_change(self, source_name: str):
        """Called by watchdog / the poller (background thread) — marshal to main thread.

        Thread safety: QueuedConnection ensures _on_change_main_thread runs on
        the main/GUI thread, so _pending_sources and _debounce_timer are only
        ever accessed from a single thread.  No additional locking is needed.
        """
        QMetaObject.invokeMethod(self, "_on_change_main_thread",
                                 Qt.ConnectionType.QueuedConnection,
                                 Q_ARG(str, source_name))

    @Slot(str)
    def _on_change_main_thread(self, source_name: str):
        """Runs on the main thread — debounces before emitting signal."""
        self._pending_sources.add(source_name)
        # Restart the debounce timer
        self._debounce_timer.start()

    def _flush_pending(self):
        """Emit signals for all sources that changed during the debounce window."""
        for name in self._pending_sources:
            self.source_changed.emit(name)
        self._pending_sources.clear()

    @property
    def is_running(self) -> bool:
        if not self._running:
            return False
        native_ok = self._observer is not None and self._observer.is_alive()
        polling_ok = self._poller is not None and self._poller.is_alive()
        return native_ok or polling_ok
