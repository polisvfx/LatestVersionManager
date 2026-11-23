"""Desktop notifications for auto-promote outcomes (system tray messages).

Auto-promote runs unattended, and its results otherwise only reach the
status bar and the log dock. With notifications on (per project: Settings ->
General -> Auto-promote notifications) the user gets an OS notification
through a tray icon:

* ``promoted`` - a version was auto-promoted
* ``waiting``  - a version is waiting for its render to finish (first time)
* ``skipped``  - auto-promote declined a version (frame range, layers, ...)
* ``failed``   - the check or the promotion failed

Level ``"problems"`` sends only ``skipped`` and ``failed``. Nothing is shown
while the LVM window is the active window (the status bar covers that), and
the same message for the same source isn't repeated until something else
was reported for it.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from PySide6.QtCore import QObject
from PySide6.QtWidgets import QSystemTrayIcon

logger = logging.getLogger(__name__)

NOTIFY_LEVELS = ("off", "problems", "all")
PROMOTED, WAITING, SKIPPED, FAILED = "promoted", "waiting", "skipped", "failed"
_PROBLEMS = {SKIPPED, FAILED}
_TITLES = {
    PROMOTED: "Auto-promoted",
    WAITING: "Auto-promote waiting",
    SKIPPED: "Auto-promote skipped",
    FAILED: "Auto-promote failed",
}


class AutoPromoteNotifier(QObject):
    """Owns the tray icon; shows one message per auto-promote event."""

    def __init__(self, window, on_clicked: Optional[Callable[[str], None]] = None):
        super().__init__(window)
        self._window = window
        self._on_clicked = on_clicked
        self._tray: Optional[QSystemTrayIcon] = None
        self._level = "off"
        self._last: dict[str, tuple[str, str]] = {}
        self._last_source: Optional[str] = None

    @staticmethod
    def available() -> bool:
        return QSystemTrayIcon.isSystemTrayAvailable()

    @property
    def level(self) -> str:
        return self._level

    def set_level(self, level: str) -> None:
        self._level = level if level in NOTIFY_LEVELS else "off"
        if self._level == "off":
            self._last.clear()
            if self._tray is not None:
                self._tray.hide()
            return
        if self._tray is None and self.available():
            self._tray = QSystemTrayIcon(self._window.windowIcon(), self)
            self._tray.setToolTip("Latest Version Manager")
            self._tray.messageClicked.connect(self._message_clicked)
            self._tray.activated.connect(lambda _reason: self._raise_window())
        if self._tray is not None:
            self._tray.show()

    def notify(self, source: str, kind: str, message: str) -> bool:
        """Show *message* for *source* if the level allows it. Returns True
        when a notification was shown."""
        if self._level == "off" or (self._level == "problems" and kind not in _PROBLEMS):
            return False
        key = (kind, message)
        if self._last.get(source) == key:
            return False  # same event again (e.g. the same skip on every check)
        self._last[source] = key
        if self._window.isActiveWindow():
            return False  # the user is looking at the status bar
        if self._tray is None:
            return False
        icon = (QSystemTrayIcon.Warning if kind in _PROBLEMS
                else QSystemTrayIcon.Information)
        self._tray.showMessage(_TITLES.get(kind, "LVM"), message, icon, 8000)
        self._last_source = source
        logger.debug("Notification (%s) for %s: %s", kind, source, message)
        return True

    def _raise_window(self) -> None:
        w = self._window
        if w.isMinimized():
            w.showNormal()
        w.raise_()
        w.activateWindow()

    def _message_clicked(self) -> None:
        self._raise_window()
        if self._on_clicked and self._last_source:
            self._on_clicked(self._last_source)
