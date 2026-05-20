"""Shared imports, constants, and helpers for the app/ package."""
"""
Latest Version Manager - PySide6 GUI Application.
"""

import os
import re
import sys
import json
import logging
import platform
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QListWidget, QListWidgetItem, QTreeWidget, QTreeWidgetItem,
    QPushButton, QLabel, QStatusBar, QFileDialog, QMessageBox,
    QProgressBar, QGroupBox, QCheckBox, QLineEdit, QComboBox,
    QDialog, QFormLayout, QDialogButtonBox, QHeaderView, QMenu,
    QToolBar, QSizePolicy, QFrame, QAbstractItemView,
    QColorDialog, QInputDialog, QStyledItemDelegate, QStyle,
    QTextEdit, QDockWidget, QPlainTextEdit, QScrollArea,
    QToolButton,
)
from PySide6.QtCore import Qt, QThread, Signal, QSize, QSettings, QUrl, QMimeData, QTimer
from PySide6.QtGui import QAction, QFont, QColor, QIcon, QPalette, QPainter, QPen, QBrush, QFontMetrics, QPixmap, QKeySequence
from PySide6.QtSvg import QSvgRenderer

from src.lvm.models import ProjectConfig, WatchedSource, VersionInfo, HistoryEntry, make_relative, DEFAULT_FILE_EXTENSIONS, version_strings_match
from src.lvm.config import load_config, save_config, create_example_config, create_project, apply_project_defaults, _expand_group_token, _resolve_group_root
from src.lvm.scanner import VersionScanner, detect_sequence_from_file, scan_directory_as_version, create_manual_version
from src.lvm.promoter import Promoter, PromotionError, generate_report
from src.lvm.history import has_newer_versions_since
from src.lvm.elevation import (
    is_admin, can_create_symlinks, can_create_hardlinks,
    restart_elevated, check_link_mode_available, LINK_MODES,
)
from src.lvm.watcher import SourceWatcher
from src.lvm.discovery import discover, DiscoveryResult
from src.lvm.timecode import populate_timecodes, populate_timecodes_parallel
from src.lvm.task_tokens import (
    compute_source_name, derive_source_tokens, get_naming_options, strip_task_tokens
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source list rendering constants — hoisted to module level so we don't
# re-allocate dicts and QColor objects on every row build. _make_source_item
# runs once per source per list rebuild, so 500 sources × N rebuilds add up.
# ---------------------------------------------------------------------------

_STATUS_MARKERS = {
    "newer": "▼! ",
    "stale": "↻ ",
    "deliberate": "* ",
    "integrity_fail": "⚠ ",
}

_STATUS_LABELS = {
    "newer": "Newer Available",
    "stale": "Stale",
    "deliberate": "Pinned",
    "highest": "Latest",
    "integrity_fail": "Integrity Fail",
    "no_version": "Not Promoted",
    "no_target": "No Target",
}

# QColor instances are immutable from our perspective — share one per status.
_STATUS_COLORS = {
    "newer": QColor("#cc8833"),
    "stale": QColor("#e8a040"),
    "deliberate": QColor("#7abbe0"),
    "highest": QColor("#4ec9a0"),
    "integrity_fail": QColor("#ffaa00"),
    "no_version": QColor("#8c8c8c"),
    "no_target": QColor("#555555"),
}

_OVERRIDE_COLOR = QColor("#88aaff")
_CONFLICT_COLOR = QColor("#ff8c00")
_DEFAULT_GROUP_COLOR_HEX = "#8c8c8c"
_GROUP_COLOR_CACHE: dict[str, QColor] = {}


def _group_qcolor(hex_str: str) -> QColor:
    """Return a cached QColor for a hex string. Group colours are user-defined
    so we can't pre-build the dict, but caching avoids re-parsing the same
    hex on every row build."""
    qc = _GROUP_COLOR_CACHE.get(hex_str)
    if qc is None:
        qc = QColor(hex_str)
        _GROUP_COLOR_CACHE[hex_str] = qc
    return qc

APP_NAME = "Latest Version Manager"
from src.lvm import __version__ as APP_VERSION

# When frozen by PyInstaller, data files live under sys._MEIPASS.
# When running from source, they live next to this file.
_BASE_DIR = Path(getattr(sys, '_MEIPASS', Path(__file__).parent))
LOGO_PATH = _BASE_DIR / "resources" / "mp_logo.svg"


def _load_app_icon() -> QIcon:
    """Load the app icon, preferring the pre-rendered PNG for reliability.

    Falls back to SVG rendering if no PNG is available (e.g. dev environment
    before icons have been generated).  Using a rasterised PNG avoids the
    QSvgRenderer initialisation overhead on every launch and is more robust
    inside a frozen bundle.
    """
    # Prefer a pre-rendered 256×256 PNG (bundled by PyInstaller on all platforms)
    png_path = _BASE_DIR / "resources" / "mp_logo_256.png"
    if png_path.exists():
        icon = QIcon()
        for size in (16, 32, 48, 64, 128, 256):
            pixmap = QPixmap(str(png_path))
            if not pixmap.isNull():
                icon.addPixmap(pixmap.scaled(size, size, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        return icon

    # Fallback: render from SVG (works in dev, requires QtSvg)
    icon = QIcon()
    if LOGO_PATH.exists():
        renderer = QSvgRenderer(str(LOGO_PATH))
        for size in (16, 32, 48, 64, 128, 256):
            pixmap = QPixmap(size, size)
            pixmap.fill(Qt.transparent)
            painter = QPainter(pixmap)
            renderer.render(painter)
            painter.end()
            icon.addPixmap(pixmap)
    return icon


# ---------------------------------------------------------------------------
# Cross-platform file browser helpers
# ---------------------------------------------------------------------------

_PLATFORM = platform.system()
if _PLATFORM == "Darwin":
    _REVEAL_LABEL = "Reveal in Finder"
elif _PLATFORM == "Windows":
    _REVEAL_LABEL = "Reveal in Explorer"
else:
    _REVEAL_LABEL = "Open in File Browser"


# Container/single-file extensions that don't carry a frame number in their
# output filename. Used by the resolved-path preview to decide whether to
# render the trailing sample as ".####.<ext>" or just ".<ext>".
_SINGLE_FILE_EXTS = {
    ".mov", ".mxf", ".mp4", ".m4v", ".avi", ".mkv",
    ".wav", ".aiff", ".aif", ".mp3", ".flac",
}


def _preview_sample_suffix(source) -> str:
    """Build the trailing filename for a resolved-path preview.

    Sequence sources (e.g. .exr frames) render as ".####.<ext>". Single-file
    sources (e.g. .mov containers) render as plain ".<ext>". Decision uses the
    source's *sample_filename* when available — a frame number in the sample
    indicates a sequence — and falls back to the first configured extension.
    """
    from src.lvm.task_tokens import FRAME_EXT_RE

    sample = getattr(source, "sample_filename", "") or ""
    if sample:
        m = FRAME_EXT_RE.search(sample)
        if m:
            return f".####{sample[m.end(1):]}"
        sample_ext = Path(sample).suffix
        if sample_ext:
            return sample_ext

    extensions = getattr(source, "file_extensions", []) or []
    if extensions:
        ext = extensions[0]
        if not ext.startswith("."):
            ext = "." + ext
        if ext.lower() in _SINGLE_FILE_EXTS:
            return ext
        return f".####{ext}"

    return ".####.exr"


def reveal_in_file_browser(path: str) -> None:
    """Open the containing folder in the native file browser and select the item."""
    p = Path(path)
    if not p.exists():
        # Fall back to parent if the exact path doesn't exist
        p = p.parent
        if not p.exists():
            return
    target = str(p)
    if _PLATFORM == "Windows":
        if p.is_dir():
            os.startfile(target)
        else:
            subprocess.Popen(["explorer", "/select,", target])
    elif _PLATFORM == "Darwin":
        subprocess.Popen(["open", "-R", target])
    else:
        # Linux / other — xdg-open opens the containing folder
        folder = target if p.is_dir() else str(p.parent)
        subprocess.Popen(["xdg-open", folder])



def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(APP_VERSION)

    if LOGO_PATH.exists():
        app.setWindowIcon(_load_app_icon())

    # Dark palette
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(28, 28, 28))
    palette.setColor(QPalette.WindowText, QColor(240, 240, 240))
    palette.setColor(QPalette.Base, QColor(18, 18, 18))
    palette.setColor(QPalette.AlternateBase, QColor(22, 22, 22))
    palette.setColor(QPalette.Text, QColor(240, 240, 240))
    palette.setColor(QPalette.Button, QColor(36, 36, 36))
    palette.setColor(QPalette.ButtonText, QColor(240, 240, 240))
    palette.setColor(QPalette.Highlight, QColor(51, 102, 153))
    palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
    palette.setColor(QPalette.Link, QColor(102, 153, 204))
    palette.setColor(QPalette.LinkVisited, QColor(68, 119, 170))
    palette.setColor(QPalette.ToolTipBase, QColor(28, 28, 28))
    palette.setColor(QPalette.ToolTipText, QColor(240, 240, 240))
    palette.setColor(QPalette.PlaceholderText, QColor(100, 100, 100))
    app.setPalette(palette)

    # Global stylesheet
    app.setStyleSheet("""
        QMainWindow { background-color: #1c1c1c; }
        QGroupBox {
            font-weight: bold;
            border: 1px solid #2a2a2a;
            border-radius: 4px;
            margin-top: 8px;
            padding-top: 12px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 4px;
        }
        QTreeWidget {
            border: 1px solid #2a2a2a;
            border-radius: 2px;
        }
        QTreeWidget::item:selected {
            background-color: #336699;
        }
        QListWidget {
            border: 1px solid #2a2a2a;
            border-radius: 2px;
        }
        QListWidget::item {
            padding: 2px 6px;
        }
        QListWidget::item:selected {
            background-color: #336699;
        }
        QPushButton {
            padding: 5px 12px;
            border: 1px solid #333333;
            border-radius: 3px;
            background-color: #242424;
        }
        QPushButton:hover {
            background-color: #2e2e2e;
        }
        QPushButton:pressed {
            background-color: #1a1a1a;
        }
        QPushButton:disabled {
            color: #555555;
        }
        QProgressBar {
            border: 1px solid #2a2a2a;
            border-radius: 3px;
            text-align: center;
            background-color: #121212;
        }
        QProgressBar::chunk {
            background-color: #336699;
        }
        QToolBar {
            spacing: 4px;
            padding: 4px;
            border-bottom: 1px solid #2a2a2a;
        }
        QStatusBar {
            border-top: 1px solid #2a2a2a;
        }
        QSplitter::handle {
            background-color: #2a2a2a;
        }
        QCheckBox::indicator {
            width: 14px;
            height: 14px;
            border: 1px solid #333333;
            border-radius: 2px;
            background-color: #121212;
        }
        QCheckBox::indicator:hover {
            border-color: #6699cc;
        }
        QCheckBox::indicator:checked {
            background-color: #336699;
            border-color: #4d7aae;
        }
        QCheckBox::indicator:checked:hover {
            background-color: #4d7aae;
        }
        QCheckBox::indicator:disabled {
            border-color: #2a2a2a;
            background-color: #1a1a1a;
        }
        QComboBox {
            border: 1px solid #333333;
            border-radius: 3px;
            background-color: #242424;
            padding: 2px 6px;
        }
        QComboBox:hover {
            border-color: #6699cc;
        }
        QComboBox QAbstractItemView {
            border: 1px solid #333333;
            background-color: #1c1c1c;
            selection-background-color: #336699;
            selection-color: #f0f0f0;
        }
        QAbstractItemView {
            outline: none;
        }
        QAbstractItemView::item:focus {
            outline: none;
        }
        QAbstractItemView::item:selected {
            background-color: #336699;
            color: #f0f0f0;
        }
        QRadioButton::indicator {
            width: 14px;
            height: 14px;
            border: 1px solid #333333;
            border-radius: 7px;
            background-color: #121212;
        }
        QRadioButton::indicator:hover {
            border-color: #6699cc;
        }
        QRadioButton::indicator:checked {
            background-color: #336699;
            border-color: #4d7aae;
        }
        QRadioButton::indicator:checked:hover {
            background-color: #4d7aae;
        }
        QRadioButton::indicator:disabled {
            border-color: #2a2a2a;
            background-color: #1a1a1a;
        }
        QLineEdit {
            border: 1px solid #333333;
            border-radius: 3px;
            padding: 2px 4px;
            background-color: #1c1c1c;
        }
        QLineEdit:focus {
            border-color: #6699cc;
        }
        QLineEdit:disabled {
            color: #555555;
            background-color: #1a1a1a;
        }
        QSpinBox, QDoubleSpinBox {
            border: 1px solid #333333;
            border-radius: 3px;
            padding: 2px 4px;
            background-color: #1c1c1c;
        }
        QSpinBox:focus, QDoubleSpinBox:focus {
            border-color: #6699cc;
        }
        QTabBar::tab {
            background-color: #242424;
            border: 1px solid #2a2a2a;
            border-bottom: none;
            padding: 4px 14px;
        }
        QTabBar::tab:selected {
            background-color: #1c1c1c;
            border-bottom: 2px solid #6699cc;
        }
        QTabBar::tab:hover:!selected {
            background-color: #2e2e2e;
        }
        QScrollBar:vertical {
            background: #1c1c1c;
            width: 10px;
            border: none;
            margin: 0;
        }
        QScrollBar::handle:vertical {
            background: #3a3a4a;
            min-height: 24px;
            border-radius: 4px;
            margin: 1px;
        }
        QScrollBar::handle:vertical:hover {
            background: #4d7aae;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background: none; }
        QScrollBar:horizontal {
            background: #1c1c1c;
            height: 10px;
            border: none;
            margin: 0;
        }
        QScrollBar::handle:horizontal {
            background: #3a3a4a;
            min-width: 24px;
            border-radius: 4px;
            margin: 1px;
        }
        QScrollBar::handle:horizontal:hover {
            background: #4d7aae;
        }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0; }
        QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal { background: none; }
    """)

    from app.main_window import MainWindow
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
