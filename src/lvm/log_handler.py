"""
Qt-aware logging handler that emits signals for GUI log display.

Captures Python logging records and forwards them as Qt signals
so the GUI can display them in a log panel.
"""

import logging
import threading
from collections import deque

from PySide6.QtCore import QObject, Signal


class QtLogHandler(logging.Handler):
    """Logging handler that emits a Qt signal for each log record."""

    class _Emitter(QObject):
        log_record = Signal(str, str)  # level_name, formatted_message

    def __init__(self, max_buffer: int = 1000):
        super().__init__()
        self.emitter = self._Emitter()
        self._buffer = deque(maxlen=max_buffer)
        self._buf_lock = threading.Lock()
        self._closed = False

    def emit(self, record: logging.LogRecord):
        if self._closed:
            return
        msg = self.format(record)
        with self._buf_lock:
            self._buffer.append((record.levelname, msg))
        try:
            self.emitter.log_record.emit(record.levelname, msg)
        except RuntimeError:
            # Qt QObject already destroyed during shutdown
            self._closed = True

    def close(self):
        self._closed = True
        super().close()

    @property
    def log_record(self):
        """Convenience accessor for the signal."""
        return self.emitter.log_record

    def get_buffer(self) -> list[tuple[str, str]]:
        """Return a thread-safe snapshot of all buffered log entries."""
        with self._buf_lock:
            return list(self._buffer)

    def clear_buffer(self):
        """Clear the in-memory buffer."""
        with self._buf_lock:
            self._buffer.clear()
