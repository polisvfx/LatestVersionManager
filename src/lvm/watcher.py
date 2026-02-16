"""
File Watcher - monitors source directories for new versions using watchdog.

Emits Qt signals when new version folders or files appear so the GUI
can update without manual refresh.
"""

import logging
from pathlib import Path

from PySide6.QtCore import QObject, Signal, QTimer, Slot, QMetaObject, Qt, Q_ARG
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, DirCreatedEvent, FileCreatedEvent

from .models import WatchedSource

logger = logging.getLogger(__name__)


class _FolderEventHandler(FileSystemEventHandler):
    """Watchdog handler that forwards events to a Qt callback."""

    def __init__(self, callback, watched_source: WatchedSource):
        super().__init__()
        self.callback = callback
        self.source = watched_source
        self._debounce_pending = set()

    def on_created(self, event):
        if isinstance(event, (DirCreatedEvent, FileCreatedEvent)):
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
        self._handlers = {}
        self._debounce_timer = QTimer(self)
        self._debounce_timer.setSingleShot(True)
        self._debounce_timer.setInterval(2000)  # 2 second debounce
        self._debounce_timer.timeout.connect(self._flush_pending)
        self._pending_sources = set()

    def start(self, sources: list[WatchedSource]):
        """Start watching all configured source directories."""
        self.stop()

        self._observer = Observer()
        watched_count = 0

        for source in sources:
            source_path = Path(source.source_dir)
            if not source_path.exists():
                logger.warning(f"Cannot watch non-existent directory: {source_path}")
                continue

            handler = _FolderEventHandler(self._on_change, source)
            self._handlers[source.name] = handler

            try:
                self._observer.schedule(handler, str(source_path), recursive=False)
                watched_count += 1
                logger.info(f"Watching: {source_path}")
            except Exception as e:
                logger.error(f"Failed to watch {source_path}: {e}")

        if watched_count > 0:
            self._observer.start()
            self.watch_status_changed.emit(f"Watching {watched_count} source(s)")
        else:
            self.watch_status_changed.emit("No directories to watch")

    def stop(self):
        """Stop all watchers."""
        if self._observer and self._observer.is_alive():
            self._observer.stop()
            self._observer.join(timeout=5)
        self._observer = None
        self._handlers.clear()
        self._pending_sources.clear()
        self.watch_status_changed.emit("Watcher stopped")

    def _on_change(self, source_name: str):
        """Called by watchdog handler (background thread) — marshal to main thread."""
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
        return self._observer is not None and self._observer.is_alive()
