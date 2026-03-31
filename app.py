"""
Latest Version Manager - PySide6 GUI Application.
"""

import os
import sys
import json
import logging
import platform
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
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

from src.lvm.models import ProjectConfig, WatchedSource, VersionInfo, HistoryEntry, make_relative, DEFAULT_FILE_EXTENSIONS
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
from src.lvm.timecode import populate_timecodes
from src.lvm.task_tokens import (
    compute_source_name, derive_source_tokens, get_naming_options, strip_task_tokens
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

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


# ---------------------------------------------------------------------------
# Worker thread for promotions (so the UI doesn't freeze during copy)
# ---------------------------------------------------------------------------

class PromoteWorker(QThread):
    """Runs the file copy in a background thread."""
    progress = Signal(int, int, str)   # current, total, filename
    finished = Signal(object)          # HistoryEntry on success
    error = Signal(str)                # error message

    def __init__(self, promoter: Promoter, version: VersionInfo, parent=None,
                 force=False, pinned=False, keep_layers=None):
        super().__init__(parent)
        self.promoter = promoter
        self.version = version
        self.force = force
        self.pinned = pinned
        self.keep_layers = keep_layers

    def cancel(self):
        """Request cancellation of the running promotion."""
        self.promoter.cancel()

    def run(self):
        try:
            entry = self.promoter.promote(
                self.version,
                progress_callback=self._on_progress,
                force=self.force,
                pinned=self.pinned,
                keep_layers=self.keep_layers,
            )
            self.finished.emit(entry)
        except PromotionError as e:
            self.error.emit(str(e))
        except Exception as e:
            self.error.emit(f"Unexpected error: {e}")

    def _on_progress(self, current, total, filename):
        self.progress.emit(current, total, filename)


# ---------------------------------------------------------------------------
# Worker thread for thumbnail generation
# ---------------------------------------------------------------------------

class ThumbnailWorker(QThread):
    finished = Signal(str)  # path to thumbnail or empty string

    def __init__(self, source_path, version_string, extensions, cache_dir, parent=None):
        super().__init__(parent)
        self.source_path = source_path
        self.version_string = version_string
        self.extensions = extensions
        self.cache_dir = cache_dir

    def run(self):
        from src.lvm.thumbnail import get_thumbnail
        result = get_thumbnail(self.source_path, self.version_string, self.extensions, self.cache_dir)
        self.finished.emit(result or "")


# ---------------------------------------------------------------------------
# Worker threads for update checking / downloading
# ---------------------------------------------------------------------------

class UpdateCheckWorker(QThread):
    """Checks GitHub for a newer release in a background thread."""
    finished = Signal(object)  # ReleaseInfo or None
    error = Signal(str)

    def __init__(self, current_version: str, parent=None):
        super().__init__(parent)
        self.current_version = current_version

    def run(self):
        try:
            from src.lvm.updater import check_for_update
            release = check_for_update(self.current_version)
            self.finished.emit(release)
        except Exception as e:
            self.error.emit(str(e))


class UpdateDownloadWorker(QThread):
    """Downloads the update ZIP in a background thread."""
    progress = Signal(int, int)    # bytes_downloaded, total_bytes
    finished = Signal(str)         # path to downloaded ZIP
    error = Signal(str)

    def __init__(self, release_info, dest_dir: str, parent=None):
        super().__init__(parent)
        self.release_info = release_info
        self.dest_dir = dest_dir

    def run(self):
        try:
            from src.lvm.updater import download_update
            zip_path = download_update(
                self.release_info,
                self.dest_dir,
                progress_callback=lambda current, total: self.progress.emit(current, total),
            )
            self.finished.emit(str(zip_path))
        except Exception as e:
            self.error.emit(str(e))


class ScanWorker(QThread):
    """Scans project sources in a background thread."""
    progress = Signal(int, int, str)  # current_index, total, source_name
    finished = Signal(dict)           # {source_name: (versions, status_info)}
    error = Signal(str)

    def __init__(self, config: ProjectConfig, sources=None, previous_cache: dict[str, list] = None, parent=None):
        super().__init__(parent)
        self.config = config
        self._sources = sources or config.watched_sources
        self.previous_cache = previous_cache or {}

    def run(self):
        try:
            results = {}
            total = len(self._sources)
            tc_mode = self.config.timecode_mode

            def _scan_one(source):
                scanner = VersionScanner(source, self.config.task_tokens)
                versions = scanner.scan()
                if tc_mode == "always":
                    populate_timecodes(versions)
                return source.name, versions

            worker_count = min(8, total)
            if worker_count > 1:
                completed = 0
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    futures = {
                        executor.submit(_scan_one, s): s
                        for s in self._sources
                    }
                    for future in as_completed(futures):
                        name, versions = future.result()
                        results[name] = versions
                        completed += 1
                        self.progress.emit(completed, total, name)
            else:
                for i, source in enumerate(self._sources):
                    self.progress.emit(i + 1, total, source.name)
                    name, versions = _scan_one(source)
                    results[name] = versions

            self.finished.emit(results)
        except Exception as e:
            self.error.emit(str(e))


class StatusWorker(QThread):
    """Computes source statuses (verify, conflicts) in a background thread.

    Runs Promoter.verify() and conflict detection off the main thread so the
    UI stays responsive after scanning completes.  Per-source work is
    parallelised with a ThreadPoolExecutor for I/O-bound speedup.
    """
    finished = Signal(dict, dict, dict, dict)  # source_status, target_conflicts, promoters, scanners

    def __init__(self, config: ProjectConfig, versions_cache: dict,
                 sources=None, parent=None):
        super().__init__(parent)
        self._config = config
        self._versions_cache = versions_cache
        self._sources = sources or config.watched_sources

    def run(self):
        from src.lvm.conflicts import detect_target_conflicts

        def _compute_one(source):
            versions = self._versions_cache.get(source.name, [])
            scanner = VersionScanner(source, self._config.task_tokens)
            highest_ver = versions[-1].version_string if versions else None
            current = None
            status = "no_target"
            integrity = None
            promoter = None

            if source.latest_target:
                promoter = Promoter(source, self._config.task_tokens, self._config.project_name)
                current = promoter.get_current_version()

                if not current:
                    status = "no_version"
                elif current.version == highest_ver:
                    integrity = promoter.verify()
                    if integrity["valid"]:
                        status = "highest"
                    elif "modified since promotion" in integrity.get("message", ""):
                        status = "stale"
                    else:
                        status = "integrity_fail"
                elif getattr(current, 'pinned', False) and not has_newer_versions_since(current, versions):
                    integrity = promoter.verify()
                    if not integrity["valid"] and "modified since promotion" in integrity.get("message", ""):
                        status = "stale"
                    else:
                        status = "deliberate"
                else:
                    integrity = promoter.verify()
                    if not integrity["valid"] and "modified since promotion" in integrity.get("message", ""):
                        status = "stale"
                    else:
                        status = "newer"

            status_info = {
                "current": current,
                "status": status,
                "has_overrides": source.has_overrides,
                "integrity": integrity,
            }
            return source.name, status_info, promoter, scanner

        source_status = {}
        promoters = {}
        scanners = {}

        sources = self._sources
        worker_count = min(8, len(sources))
        if worker_count > 1:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = {executor.submit(_compute_one, s): s for s in sources}
                for future in as_completed(futures):
                    name, status_info, promoter, scanner = future.result()
                    source_status[name] = status_info
                    if promoter:
                        promoters[name] = promoter
                    scanners[name] = scanner
        else:
            for source in sources:
                name, status_info, promoter, scanner = _compute_one(source)
                source_status[name] = status_info
                if promoter:
                    promoters[name] = promoter
                scanners[name] = scanner

        conflicts = detect_target_conflicts(self._config)
        target_conflicts = {}
        for target, name_a, name_b in conflicts:
            target_conflicts.setdefault(name_a, []).append(name_b)
            target_conflicts.setdefault(name_b, []).append(name_a)

        self.finished.emit(source_status, target_conflicts, promoters, scanners)


# ---------------------------------------------------------------------------
# Version tree with drag-and-drop support for manual version import
# ---------------------------------------------------------------------------

class VersionTreeWidget(QTreeWidget):
    """QTreeWidget subclass that accepts file/directory drops for manual version import."""

    files_dropped = Signal(list)  # list of Path objects (files and/or directories)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            super().dragEnterEvent(event)

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            super().dragMoveEvent(event)

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            paths = []
            for url in event.mimeData().urls():
                local = url.toLocalFile()
                if local:
                    paths.append(Path(local))
            if paths:
                self.files_dropped.emit(paths)
            event.acceptProposedAction()
        else:
            super().dropEvent(event)


# ---------------------------------------------------------------------------
# Dry-run preview dialog
# ---------------------------------------------------------------------------

class DryRunDialog(QDialog):
    """Preview dialog showing the file mapping before promotion."""

    def __init__(self, dry_run_data: dict, version: VersionInfo,
                 source: WatchedSource, current: HistoryEntry = None,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Promote Preview — {source.name} → {version.version_string}")
        self.setMinimumSize(750, 500)
        self.resize(850, 550)

        layout = QVBoxLayout(self)

        # Summary header
        header = QLabel(
            f"<b>{source.name}</b> → <b>{version.version_string}</b> &nbsp; "
            f"({dry_run_data['total_files']} files, "
            f"{version.total_size_human}, {dry_run_data['link_mode']} mode)"
        )
        header.setStyleSheet("font-size: 13pt; padding: 4px;")
        layout.addWidget(header)

        target_label = QLabel(f"Target: {dry_run_data['target_dir']}")
        target_label.setStyleSheet("color: #8c8c8c; font-size: 11pt; padding: 0 4px 4px;")
        layout.addWidget(target_label)

        # Frame range mismatch warning
        self._warnings = []
        if current:
            if (current.frame_range and version.frame_range
                    and current.frame_range != version.frame_range):
                self._warnings.append(
                    f"Frame range changed: {current.frame_range} → {version.frame_range}"
                )
            if (current.frame_count and version.frame_count
                    and current.frame_count != version.frame_count):
                self._warnings.append(
                    f"Frame count changed: {current.frame_count} → {version.frame_count}"
                )
            if (current.start_timecode and version.start_timecode
                    and current.start_timecode != version.start_timecode):
                self._warnings.append(
                    f"Timecode changed: {current.start_timecode} → {version.start_timecode}"
                )

        if self._warnings:
            warn_frame = QFrame()
            warn_frame.setStyleSheet(
                "QFrame { background-color: #3a3a1a; border: 1px solid #6a6a2d; "
                "border-radius: 4px; padding: 6px; }"
            )
            warn_layout = QVBoxLayout(warn_frame)
            warn_layout.setContentsMargins(8, 4, 8, 4)
            for w in self._warnings:
                warn_label = QLabel(f"\u26a0 {w}")
                warn_label.setStyleSheet("color: #ffaa00; font-size: 12pt;")
                warn_layout.addWidget(warn_label)
            layout.addWidget(warn_frame)

        # File mapping tree
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Source File", "→ Target Name", "Size"])
        self.tree.setRootIsDecorated(False)
        self.tree.setAlternatingRowColors(True)
        header_view = self.tree.header()
        header_view.setStretchLastSection(False)
        header_view.setSectionResizeMode(0, QHeaderView.Stretch)
        header_view.setSectionResizeMode(1, QHeaderView.Stretch)
        header_view.setSectionResizeMode(2, QHeaderView.ResizeToContents)

        for item in dry_run_data["file_map"]:
            src_name = os.path.basename(item["source"])
            size = item["size_bytes"]
            for unit in ("B", "KB", "MB", "GB"):
                if size < 1024:
                    size_str = f"{size:.1f} {unit}"
                    break
                size /= 1024
            else:
                size_str = f"{size:.1f} TB"

            tree_item = QTreeWidgetItem([src_name, item["target_name"], size_str])
            # Highlight if name actually changed
            if src_name != item["target_name"]:
                tree_item.setForeground(1, QColor("#4ec9a0"))
            self.tree.addTopLevelItem(tree_item)

        layout.addWidget(self.tree)

        # Keep / Replace info
        if current:
            replace_label = QLabel(
                f"This will replace the current version ({current.version})."
            )
            replace_label.setStyleSheet("color: #cc8833; font-size: 11pt; padding: 4px;")
            layout.addWidget(replace_label)

        # Buttons
        btn_box = QDialogButtonBox()
        self.btn_promote = btn_box.addButton("Promote", QDialogButtonBox.AcceptRole)
        self.btn_promote.setStyleSheet(
            "QPushButton { background-color: #336699; color: white; padding: 8px 20px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #4d7aae; }"
        )
        btn_box.addButton(QDialogButtonBox.Cancel)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)


# ---------------------------------------------------------------------------
# Source editor dialog (with override toggles)
# ---------------------------------------------------------------------------

class SourceDialog(QDialog):
    """Dialog for adding or editing a watched source."""

    def __init__(self, source: WatchedSource = None, project_config: ProjectConfig = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Source" if source else "Add Source")
        self.setMinimumWidth(550)
        self._project_config = project_config

        layout = QFormLayout(self)
        layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.name_edit = QLineEdit()
        layout.addRow("Name:", self.name_edit)

        # Source directory
        self.source_dir_edit = QLineEdit()
        self.source_browse_btn = QPushButton("Browse...")
        self.source_browse_btn.clicked.connect(self._browse_source)
        source_row = QHBoxLayout()
        source_row.addWidget(self.source_dir_edit, 1)
        source_row.addWidget(self.source_browse_btn)
        layout.addRow("Source Directory:", source_row)

        # --- Overrideable fields ---
        override_label = QLabel("Fields below inherit project defaults unless overridden:")
        override_label.setStyleSheet("color: #8c8c8c; font-size: 11pt; margin-top: 8px;")
        layout.addRow("", override_label)

        # Latest target
        self.override_latest_check = QCheckBox("Override")
        self.target_dir_edit = QLineEdit()
        self.target_browse_btn = QPushButton("Browse...")
        self.target_browse_btn.clicked.connect(self._browse_target)
        self.override_latest_check.toggled.connect(lambda on: self._toggle_override(
            on, self.target_dir_edit, self.target_browse_btn,
            self._get_default_latest()))
        target_row = QHBoxLayout()
        target_row.addWidget(self.override_latest_check)
        target_row.addWidget(self.target_dir_edit, 1)
        target_row.addWidget(self.target_browse_btn)
        layout.addRow("Latest Target:", target_row)

        # Version pattern
        self.override_pattern_check = QCheckBox("Override")
        self.pattern_edit = QLineEdit()
        self.override_pattern_check.toggled.connect(lambda on: self._toggle_override(
            on, self.pattern_edit, None,
            project_config.default_version_pattern if project_config else "_v{version}"))
        pattern_row = QHBoxLayout()
        pattern_row.addWidget(self.override_pattern_check)
        pattern_row.addWidget(self.pattern_edit, 1)
        layout.addRow("Version Pattern:", pattern_row)

        # Date format
        self.override_date_format_check = QCheckBox("Override")
        self.date_format_combo = QComboBox()
        self.date_format_combo.addItems(["(none)", "DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD"])
        self.override_date_format_check.toggled.connect(lambda on: self.date_format_combo.setEnabled(on))
        date_row = QHBoxLayout()
        date_row.addWidget(self.override_date_format_check)
        date_row.addWidget(self.date_format_combo, 1)
        layout.addRow("Date Format:", date_row)

        # File extensions
        self.override_ext_check = QCheckBox("Override")
        self.extensions_edit = QLineEdit()
        default_ext_str = " ".join(project_config.default_file_extensions if project_config else DEFAULT_FILE_EXTENSIONS)
        self.override_ext_check.toggled.connect(lambda on: self._toggle_override(
            on, self.extensions_edit, None, default_ext_str))
        ext_row = QHBoxLayout()
        ext_row.addWidget(self.override_ext_check)
        ext_row.addWidget(self.extensions_edit, 1)
        layout.addRow("File Extensions:", ext_row)

        # Link mode
        self.override_link_mode_check = QCheckBox("Override")
        self.link_mode_combo = QComboBox()
        self.link_mode_combo.addItems(["copy", "hardlink", "symlink"])
        self.override_link_mode_check.toggled.connect(lambda on: self.link_mode_combo.setEnabled(on))
        link_row = QHBoxLayout()
        link_row.addWidget(self.override_link_mode_check)
        link_row.addWidget(self.link_mode_combo, 1)
        layout.addRow("Link Mode:", link_row)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

        # Populate if editing
        if source:
            self.name_edit.setText(source.name)
            self.source_dir_edit.setText(source.source_dir)

            self.override_latest_check.setChecked(source.override_latest_target)
            self.target_dir_edit.setText(source.latest_target)

            self.override_pattern_check.setChecked(source.override_version_pattern)
            self.pattern_edit.setText(source.version_pattern)

            self.override_ext_check.setChecked(source.override_file_extensions)
            self.extensions_edit.setText(" ".join(source.file_extensions))

            self.override_date_format_check.setChecked(source.override_date_format)
            if source.date_format:
                self.date_format_combo.setCurrentText(source.date_format)

            self.override_link_mode_check.setChecked(source.override_link_mode)
            self.link_mode_combo.setCurrentText(source.link_mode)
        else:
            # New source — start with defaults, overrides off
            self.override_latest_check.setChecked(False)
            self.override_pattern_check.setChecked(False)
            self.override_date_format_check.setChecked(False)
            self.override_ext_check.setChecked(False)
            self.override_link_mode_check.setChecked(False)

        # Apply initial toggle state
        self._toggle_override(self.override_latest_check.isChecked(),
                              self.target_dir_edit, self.target_browse_btn,
                              self._get_default_latest())
        self._toggle_override(self.override_pattern_check.isChecked(),
                              self.pattern_edit, None,
                              project_config.default_version_pattern if project_config else "_v{version}")
        self._toggle_override(self.override_ext_check.isChecked(),
                              self.extensions_edit, None, default_ext_str)
        self.date_format_combo.setEnabled(self.override_date_format_check.isChecked())
        self.link_mode_combo.setEnabled(self.override_link_mode_check.isChecked())

    def _get_default_latest(self) -> str:
        if self._project_config and self._project_config.latest_path_template:
            return self._project_config.latest_path_template
        return "(set in Project Settings)"

    def _toggle_override(self, enabled: bool, line_edit: QLineEdit, browse_btn=None, default_text=""):
        line_edit.setEnabled(enabled)
        if browse_btn:
            browse_btn.setEnabled(enabled)
        if not enabled:
            line_edit.setPlaceholderText(f"Default: {default_text}")

    def _browse_source(self):
        d = QFileDialog.getExistingDirectory(self, "Select Source Directory")
        if d:
            self.source_dir_edit.setText(d)

    def _browse_target(self):
        d = QFileDialog.getExistingDirectory(self, "Select Latest Target Directory")
        if d:
            self.target_dir_edit.setText(d)

    def get_source(self) -> WatchedSource:
        exts_text = self.extensions_edit.text().strip()
        exts = exts_text.split() if exts_text else list(DEFAULT_FILE_EXTENSIONS)

        pc = self._project_config
        date_fmt_text = self.date_format_combo.currentText()
        date_fmt = "" if date_fmt_text == "(none)" else date_fmt_text
        return WatchedSource(
            name=self.name_edit.text().strip() or "Untitled",
            source_dir=self.source_dir_edit.text().strip(),
            version_pattern=self.pattern_edit.text().strip() or (pc.default_version_pattern if pc else "_v{version}"),
            file_extensions=exts,
            latest_target=self.target_dir_edit.text().strip(),
            link_mode=self.link_mode_combo.currentText(),
            date_format=date_fmt,
            override_version_pattern=self.override_pattern_check.isChecked(),
            override_date_format=self.override_date_format_check.isChecked(),
            override_file_extensions=self.override_ext_check.isChecked(),
            override_latest_target=self.override_latest_check.isChecked(),
            override_link_mode=self.override_link_mode_check.isChecked(),
        )


# ---------------------------------------------------------------------------
# Project Setup Dialog (simplified: name + dir + filters)
# ---------------------------------------------------------------------------

class ProjectSetupDialog(QDialog):
    """Dialog for setting up a new LVM project."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("New Project")
        self.setMinimumWidth(500)

        layout = QFormLayout(self)
        layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("My VFX Project")
        layout.addRow("Project Name:", self.name_edit)

        # Template dropdown (Feature #17)
        self.template_combo = QComboBox()
        self.template_combo.addItem("(none)", "")
        try:
            from src.lvm.templates import list_templates
            for t in list_templates():
                self.template_combo.addItem(f"{t['name']} [{t['location']}]", t["path"])
        except Exception:
            logger.warning("Could not load templates", exc_info=True)
        layout.addRow("From Template:", self.template_combo)

        # Project Root — the logical root of the project
        self.root_edit = QLineEdit()
        self.root_browse_btn = QPushButton("Browse...")
        self.root_browse_btn.clicked.connect(self._browse_root)
        root_row = QHBoxLayout()
        root_row.addWidget(self.root_edit, 1)
        root_row.addWidget(self.root_browse_btn)
        root_help = QLabel("The root directory of the project (used for {project_root} token).")
        root_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        layout.addRow("Project Root:", root_row)
        layout.addRow("", root_help)

        # Project Save — where the JSON config is saved (hidden until root is set)
        self.save_label = QLabel("Project Save:")
        self.save_edit = QLineEdit()
        self.save_browse_btn = QPushButton("Browse...")
        self.save_browse_btn.clicked.connect(self._browse_save)
        save_row = QHBoxLayout()
        save_row.addWidget(self.save_edit, 1)
        save_row.addWidget(self.save_browse_btn)
        save_help = QLabel("Where the project file (.json) is saved. Defaults to Project Root.")
        save_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        self._save_label = self.save_label
        self._save_row_widget = QWidget()
        self._save_row_widget.setLayout(save_row)
        self._save_help = save_help
        layout.addRow(self.save_label, self._save_row_widget)
        layout.addRow("", save_help)

        # Initially hide save location until root is chosen
        self._set_save_visible(False)
        self._save_user_edited = False
        self.root_edit.textChanged.connect(self._on_root_changed)
        self.save_edit.textChanged.connect(self._on_save_edited)

        # Filters
        filter_label = QLabel(
            "Filters are used when discovering versioned content.\n"
            "Whitelist: only include folders matching these keywords.\n"
            "Blacklist: skip folders matching these keywords."
        )
        filter_label.setStyleSheet("color: #8c8c8c; font-size: 11pt; margin-top: 6px;")
        layout.addRow("", filter_label)

        self.whitelist_edit = TagInputWidget(placeholder="Type and press comma to add...")
        layout.addRow("Whitelist:", self.whitelist_edit)

        self.blacklist_edit = TagInputWidget(placeholder="Type and press comma to add...")
        layout.addRow("Blacklist:", self.blacklist_edit)

        # Task tokens
        task_sep = QLabel("Task Names")
        task_sep.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addRow("", task_sep)

        task_help = QLabel(
            "Common task names in your pipeline that can be stripped\n"
            "from filenames. Use % as a counted wildcard (e.g. comp_%% matches comp_mp)."
        )
        task_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        layout.addRow("", task_help)

        self.tasks_edit = QLineEdit()
        self.tasks_edit.setPlaceholderText("comp, grade, dmp, fx, roto, paint")
        layout.addRow("Task Names:", self.tasks_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def _set_save_visible(self, visible: bool):
        self._save_label.setVisible(visible)
        self._save_row_widget.setVisible(visible)
        self._save_help.setVisible(visible)

    def _on_root_changed(self, text: str):
        root = text.strip()
        if root:
            self._set_save_visible(True)
            # Auto-populate save with root if user hasn't manually edited it
            if not self._save_user_edited:
                self.save_edit.blockSignals(True)
                self.save_edit.setText(root)
                self.save_edit.blockSignals(False)
        else:
            self._set_save_visible(False)

    def _on_save_edited(self):
        # Mark as user-edited only when save differs from root
        if self.save_edit.text().strip() != self.root_edit.text().strip():
            self._save_user_edited = True

    def _browse_root(self):
        d = QFileDialog.getExistingDirectory(self, "Select Project Root")
        if d:
            self.root_edit.setText(d)

    def _browse_save(self):
        start = self.save_edit.text().strip() or self.root_edit.text().strip()
        d = QFileDialog.getExistingDirectory(self, "Select Project Save Location", start)
        if d:
            self.save_edit.setText(d)

    def get_project_info(self) -> dict:
        wl = self.whitelist_edit.tags()
        bl = self.blacklist_edit.tags()
        tasks = [t.strip() for t in self.tasks_edit.text().split(",") if t.strip()]
        root = self.root_edit.text().strip()
        save = self.save_edit.text().strip() or root
        template_path = self.template_combo.currentData() or ""
        return {
            "project_name": self.name_edit.text().strip() or "Untitled",
            "project_root": root,
            "save_dir": save,
            "project_dir": save,  # backwards compat — save location
            "name_whitelist": wl,
            "name_blacklist": bl,
            "task_tokens": tasks,
            "template_path": template_path,
        }


# ---------------------------------------------------------------------------
# Tag Input Widget (for whitelist / blacklist)
# ---------------------------------------------------------------------------

class FlowLayout(QVBoxLayout):
    """Simple flow layout that wraps widgets into rows."""

    def __init__(self, parent=None, spacing=4):
        super().__init__(parent)
        self._rows: list[QHBoxLayout] = []
        self._spacing = spacing
        self.setSpacing(spacing)
        self.setContentsMargins(0, 0, 0, 0)
        self._add_row()

    def _add_row(self):
        row = QHBoxLayout()
        row.setSpacing(self._spacing)
        row.setContentsMargins(0, 0, 0, 0)
        row.addStretch()
        super().addLayout(row)
        self._rows.append(row)
        return row

    def addWidget(self, widget):
        row = self._rows[-1]
        row.insertWidget(row.count() - 1, widget)

    def removeWidget(self, widget):
        for row in self._rows:
            row.removeWidget(widget)
        widget.setParent(None)


class TagWidget(QFrame):
    """A single removable tag pill with an X button on the right."""

    removed = Signal(str)

    def __init__(self, text: str, parent=None):
        super().__init__(parent)
        self._text = text
        self.setFrameShape(QFrame.NoFrame)
        self.setStyleSheet(
            "TagWidget {"
            "  background: #1e2530; border: 1px solid #333333; border-radius: 10px;"
            "  padding: 1px 6px 1px 2px;"
            "}"
            "TagWidget:hover { background: #28333f; }"
        )

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 1, 2, 1)
        layout.setSpacing(2)

        label = QLabel(text)
        label.setStyleSheet("background: transparent; border: none; color: #e0e0e0; padding: 0;")
        layout.addWidget(label)

        close_btn = QPushButton("\u00d7")
        close_btn.setFixedSize(16, 16)
        close_btn.setCursor(Qt.PointingHandCursor)
        close_btn.setStyleSheet(
            "QPushButton {"
            "  background: transparent; border: none; color: #8c8c8c;"
            "  font-size: 13pt; font-weight: bold; padding: 0; margin: 0;"
            "}"
            "QPushButton:hover { color: #ff6b6b; }"
        )
        close_btn.clicked.connect(lambda: self.removed.emit(self._text))
        layout.addWidget(close_btn)

        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    @property
    def text(self):
        return self._text


class TagInputWidget(QWidget):
    """Container that displays tags as removable pills with a text input.

    Typing a comma converts the preceding text into a tag.
    """

    tagsChanged = Signal()  # Emitted when tags are added or removed

    def __init__(self, initial_tags: list[str] = None, placeholder: str = "", parent=None):
        super().__init__(parent)
        self._tags: list[str] = []

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(4)

        self._tag_container = QWidget()
        self._flow = FlowLayout(self._tag_container, spacing=4)
        outer.addWidget(self._tag_container)

        self._input = QLineEdit()
        self._input.setPlaceholderText(placeholder)
        self._input.textChanged.connect(self._on_text_changed)
        self._input.returnPressed.connect(self._commit_input)
        outer.addWidget(self._input)

        if initial_tags:
            for tag in initial_tags:
                self._add_tag(tag)
        self._update_container_visibility()

    def _on_text_changed(self, text: str):
        if "," in text:
            parts = text.split(",")
            for part in parts[:-1]:
                word = part.strip()
                if word:
                    self._add_tag(word)
            self._input.setText(parts[-1].lstrip())

    def _commit_input(self):
        word = self._input.text().strip().rstrip(",")
        if word:
            self._add_tag(word)
            self._input.clear()

    def _add_tag(self, text: str):
        text = text.strip()
        if not text or text in self._tags:
            return
        self._tags.append(text)
        tag_w = TagWidget(text)
        tag_w.removed.connect(self._remove_tag)
        self._flow.addWidget(tag_w)
        self._update_container_visibility()
        self.tagsChanged.emit()

    def _remove_tag(self, text: str):
        if text in self._tags:
            self._tags.remove(text)
        for i in range(self._tag_container.layout().count()):
            item = self._tag_container.layout().itemAt(i)
            if item and isinstance(item, QHBoxLayout):
                for j in range(item.count()):
                    sub = item.itemAt(j)
                    if sub and sub.widget() and isinstance(sub.widget(), TagWidget):
                        if sub.widget().text == text:
                            self._flow.removeWidget(sub.widget())
                            self._update_container_visibility()
                            self.tagsChanged.emit()
                            return

    def _update_container_visibility(self):
        self._tag_container.setVisible(bool(self._tags))

    def tags(self) -> list[str]:
        """Return the current list of tags, including any uncommitted input."""
        result = list(self._tags)
        pending = self._input.text().strip().rstrip(",")
        if pending and pending not in result:
            result.append(pending)
        return result


# ---------------------------------------------------------------------------
# Collapsible Section Widget
# ---------------------------------------------------------------------------

class CollapsibleSection(QWidget):
    """A collapsible section with a toggle header and animated content area."""

    def __init__(self, title: str, parent=None, collapsed: bool = False):
        super().__init__(parent)
        self._collapsed = collapsed

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Header button
        self._toggle_btn = QToolButton()
        self._toggle_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self._toggle_btn.setText(title)
        self._toggle_btn.setCheckable(True)
        self._toggle_btn.setChecked(not collapsed)
        self._toggle_btn.setArrowType(Qt.DownArrow if not collapsed else Qt.RightArrow)
        self._toggle_btn.setStyleSheet(
            "QToolButton { border: none; font-weight: bold; font-size: 12pt;"
            " padding: 6px 4px; color: #e0e0e0; }"
            "QToolButton:hover { color: #fff; background: #242424; }"
        )
        self._toggle_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._toggle_btn.clicked.connect(self._on_toggle)

        # Separator line under header
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setStyleSheet("color: #2a2a2a;")

        # Content area
        self._content = QWidget()
        self._content_layout = QFormLayout(self._content)
        self._content_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        self._content_layout.setContentsMargins(8, 4, 4, 8)
        self._content.setVisible(not collapsed)

        main_layout.addWidget(self._toggle_btn)
        main_layout.addWidget(separator)
        main_layout.addWidget(self._content)

    def content_layout(self) -> QFormLayout:
        """Return the QFormLayout inside the collapsible content area."""
        return self._content_layout

    def _on_toggle(self, checked: bool = None):
        if checked is None:
            checked = self._toggle_btn.isChecked()
        self._collapsed = not checked
        self._toggle_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        self._content.setVisible(checked)


# ---------------------------------------------------------------------------
# Project Settings Dialog
# ---------------------------------------------------------------------------

class ProjectSettingsDialog(QDialog):
    """Dialog for editing project-wide settings."""

    _last_geometry = None  # remember size/position within session

    def __init__(self, config: ProjectConfig, selected_source: WatchedSource = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Project Settings")
        self.setMinimumWidth(620)
        self.setMinimumHeight(400)
        if ProjectSettingsDialog._last_geometry:
            self.restoreGeometry(ProjectSettingsDialog._last_geometry)
        else:
            self.resize(700, 600)
        self._config = config
        self._selected_source = selected_source
        self._naming_reset = False

        # Outer layout with scroll area
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll_widget = QWidget()
        top_layout = QVBoxLayout(scroll_widget)
        top_layout.setContentsMargins(12, 12, 12, 12)
        top_layout.setSpacing(12)

        # ==================================================================
        # GENERAL (always visible, not collapsible)
        # ==================================================================
        general_form = QFormLayout()
        general_form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        general_form.setContentsMargins(0, 0, 0, 0)

        self.name_edit = QLineEdit(config.project_name)
        general_form.addRow("Project Name:", self.name_edit)

        self.root_edit = QLineEdit(config.effective_project_root)
        self.root_edit.textChanged.connect(self._update_path_preview)
        self.root_browse_btn = QPushButton("Browse...")
        self.root_browse_btn.clicked.connect(self._browse_root)
        root_row = QHBoxLayout()
        root_row.addWidget(self.root_edit, 1)
        root_row.addWidget(self.root_browse_btn)
        root_help = QLabel("The root directory of the project (used for {project_root} token).")
        root_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        general_form.addRow("Project Root:", root_row)
        general_form.addRow("", root_help)

        top_layout.addLayout(general_form)

        # ==================================================================
        # OUTPUT PATHS
        # ==================================================================
        paths_section = CollapsibleSection("Output Paths")
        paths = paths_section.content_layout()

        template_help = QLabel(
            "Relative paths resolve from each source's directory.\n"
            "Tokens: {source_dir}, {source_title}, {source_name}, {source_basename},\n"
            "{source_fullname}, {source_filename}, {project_root}, {group}, {group_root}\n"
            "If {group} is empty, the token and its trailing divider are omitted.\n"
            "{group_root} resolves to the group's root directory (falls back to {project_root}).\n"
            "{source_title} is the source's in-project display name.\n"
            "Examples: {group_root}/online/{source_name}  |  latest/{group}/{source_basename}_latest"
        )
        template_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        paths.addRow("", template_help)

        self.latest_template_edit = QLineEdit(config.latest_path_template)
        self.latest_template_edit.setPlaceholderText("latest/{source_basename}_latest")
        self.latest_template_edit.textChanged.connect(self._update_path_preview)
        paths.addRow("Latest Path Template:", self.latest_template_edit)

        rename_help = QLabel(
            "Controls the output filename (without frame/ext).\n"
            "Tokens: {source_title}, {source_name}, {source_basename}, {source_fullname}, {group}"
        )
        rename_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        paths.addRow("", rename_help)

        self.rename_template_edit = QLineEdit(config.default_file_rename_template)
        self.rename_template_edit.setPlaceholderText("{source_basename}_latest")
        self.rename_template_edit.textChanged.connect(self._update_path_preview)
        paths.addRow("File Rename Template:", self.rename_template_edit)

        self.path_preview_label = QLabel("")
        self.path_preview_label.setStyleSheet("color: #3aaa88; font-size: 11pt;")
        self.path_preview_label.setWordWrap(True)
        self.path_preview_label.setMinimumWidth(50)
        self.path_preview_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        paths.addRow("Resolved Preview:", self.path_preview_label)

        top_layout.addWidget(paths_section)

        # ==================================================================
        # SOURCE NAMING & DETECTION
        # ==================================================================
        naming_section = CollapsibleSection("Source Naming && Detection")
        naming = naming_section.content_layout()

        # --- Naming rule (improved display) ---
        naming_row = QHBoxLayout()
        self.naming_label = QLabel()
        self.naming_label.setWordWrap(True)
        self._format_naming_label(config.default_naming_rule)
        naming_row.addWidget(self.naming_label, 1)
        self.reset_naming_btn = QPushButton("Reset")
        self.reset_naming_btn.setToolTip("Reset naming convention so it is re-asked on next ingest")
        self.reset_naming_btn.clicked.connect(self._reset_naming)
        naming_row.addWidget(self.reset_naming_btn)
        naming.addRow("Naming Rule:", naming_row)

        # --- Task names ---
        task_help = QLabel(
            "Task names stripped from filenames to produce cleaner source names.\n"
            "Each % matches one character (e.g. comp_%% matches comp_mp). Bounded by: _ - ."
        )
        task_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        naming.addRow("", task_help)

        self.tasks_edit = QLineEdit(", ".join(config.task_tokens))
        self.tasks_edit.setPlaceholderText("comp, grade, dmp, fx, roto, paint")
        self.tasks_edit.textChanged.connect(self._update_path_preview)
        naming.addRow("Task Names:", self.tasks_edit)

        # --- Version pattern ---
        self.pattern_edit = QLineEdit(config.default_version_pattern)
        naming.addRow("Version Pattern:", self.pattern_edit)

        # --- Date format ---
        self.date_format_combo = QComboBox()
        self.date_format_combo.addItems(["(none)", "DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD"])
        if config.default_date_format:
            self.date_format_combo.setCurrentText(config.default_date_format)
        naming.addRow("Date Format:", self.date_format_combo)

        # --- File extensions ---
        self.extensions_edit = QLineEdit(" ".join(config.default_file_extensions))
        naming.addRow("File Extensions:", self.extensions_edit)

        top_layout.addWidget(naming_section)

        # ==================================================================
        # DISCOVERY FILTERS
        # ==================================================================
        filters_section = CollapsibleSection("Discovery Filters")
        filters = filters_section.content_layout()

        self.whitelist_edit = TagInputWidget(config.name_whitelist, placeholder="Type and press comma to add...")
        filters.addRow("Whitelist:", self.whitelist_edit)

        self.blacklist_edit = TagInputWidget(config.name_blacklist, placeholder="Type and press comma to add...")
        filters.addRow("Blacklist:", self.blacklist_edit)

        top_layout.addWidget(filters_section)

        # ==================================================================
        # ADVANCED (collapsed by default)
        # ==================================================================
        advanced_section = CollapsibleSection("Advanced", collapsed=True)
        adv = advanced_section.content_layout()

        # Link mode
        self.link_mode_combo = QComboBox()
        self.link_mode_combo.addItems(["copy", "hardlink", "symlink"])
        self.link_mode_combo.setCurrentText(config.default_link_mode)
        adv.addRow("Default Link Mode:", self.link_mode_combo)

        # Timecode mode
        self.timecode_combo = QComboBox()
        self.timecode_combo.addItems(["always", "lazy", "never"])
        self.timecode_combo.setCurrentText(config.timecode_mode)
        tc_help = QLabel(
            "Always: read timecodes during scan (slower, all TCs visible immediately)\n"
            "Lazy: read on demand when a source is viewed (fast scan)\n"
            "Never: skip timecode extraction entirely (fastest)"
        )
        tc_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        adv.addRow("Timecode Mode:", self.timecode_combo)
        adv.addRow("", tc_help)

        # Promotion hooks
        hooks_header = QLabel("Promotion Hooks")
        hooks_header.setStyleSheet("font-weight: bold; margin-top: 6px;")
        adv.addRow("", hooks_header)

        hooks_help = QLabel(
            "Shell commands to run before/after each promotion.\n"
            "Leave empty to disable. Tokens: {source_name}, {version}, {target_dir}"
        )
        hooks_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        adv.addRow("", hooks_help)

        self.pre_promote_edit = QLineEdit(getattr(config, 'pre_promote_cmd', '') or '')
        self.pre_promote_edit.setPlaceholderText("e.g. echo 'Starting promotion of {source_name}'")
        adv.addRow("Pre-Promote Command:", self.pre_promote_edit)

        self.post_promote_edit = QLineEdit(getattr(config, 'post_promote_cmd', '') or '')
        self.post_promote_edit.setPlaceholderText("e.g. python notify.py --source {source_name} --version {version}")
        adv.addRow("Post-Promote Command:", self.post_promote_edit)

        # Sequence validation
        seq_header = QLabel("Sequence Validation")
        seq_header.setStyleSheet("font-weight: bold; margin-top: 6px;")
        adv.addRow("", seq_header)

        self.block_incomplete_cb = QCheckBox("Block promotion of incomplete sequences (warn on frame gaps)")
        self.block_incomplete_cb.setChecked(getattr(config, 'block_incomplete_sequences', False))
        adv.addRow("", self.block_incomplete_cb)

        # Network / SMB performance
        net_header = QLabel("Network Performance")
        net_header.setStyleSheet("font-weight: bold; margin-top: 6px;")
        adv.addRow("", net_header)

        self.skip_resolve_cb = QCheckBox("Skip symlink resolution during discovery (faster over SMB)")
        self.skip_resolve_cb.setChecked(getattr(config, 'skip_resolve', True))
        adv.addRow("", self.skip_resolve_cb)

        skip_resolve_help = QLabel(
            "Skips Path.resolve() on each directory, eliminating extra network\n"
            "round-trips. Safe for SMB/NFS shares which rarely use symlinks.\n"
            "Disable only if your source directories contain symlink loops."
        )
        skip_resolve_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        adv.addRow("", skip_resolve_help)

        top_layout.addWidget(advanced_section)

        # ==================================================================
        # Footer (Save as Template + OK/Cancel)
        # ==================================================================
        top_layout.addStretch()
        scroll.setWidget(scroll_widget)
        outer.addWidget(scroll, 1)

        # Bottom bar outside scroll
        bottom = QHBoxLayout()
        bottom.setContentsMargins(12, 6, 12, 10)
        save_tpl_btn = QPushButton("Save as Template...")
        save_tpl_btn.clicked.connect(self._save_as_template)
        bottom.addWidget(save_tpl_btn)
        bottom.addStretch()
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        bottom.addWidget(buttons)
        outer.addLayout(bottom)

        # Compute initial preview now that all fields exist
        self._update_path_preview()

    def _update_path_preview(self):
        """Update the resolved path preview based on the current templates.

        If a source was selected when opening the dialog, preview that source.
        Otherwise, preview the last 3 added sources.
        """
        tpl = self.latest_template_edit.text().strip()
        rename_tpl = self.rename_template_edit.text().strip() or "{source_basename}_latest"
        if not tpl:
            self.path_preview_label.setText("(no template set)")
            self.path_preview_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
            return

        config = self._config
        # Read live values from edit fields
        live_task_tokens = [t.strip() for t in self.tasks_edit.text().split(",") if t.strip()]
        live_root = self.root_edit.text().strip() or config.effective_project_root or "<project_root>"
        previews = []
        # Pick which sources to preview
        if self._selected_source:
            sources = [self._selected_source]
        else:
            # Last 3 added sources (tail of the list)
            sources = config.watched_sources[-3:] if config.watched_sources else []
        if sources:
            for source in sources:
                tokens = derive_source_tokens(source.sample_filename or source.name,
                                              live_task_tokens, source_title=source.name)
                resolved = tpl
                resolved = resolved.replace("{project_root}", live_root)
                resolved = resolved.replace("{group_root}", _resolve_group_root(config, source.group) or "<project_root>")
                resolved = resolved.replace("{source_title}", tokens["source_title"])
                resolved = resolved.replace("{source_name}", tokens["source_name"])
                resolved = resolved.replace("{source_basename}", tokens["source_basename"])
                resolved = resolved.replace("{source_fullname}", tokens["source_fullname"])
                resolved = resolved.replace("{source_filename}", tokens["source_filename"])
                resolved = resolved.replace("{source_dir}", source.source_dir)
                resolved = _expand_group_token(resolved, source.group)
                # Relative paths resolve from the source directory
                p = Path(resolved)
                if not p.is_absolute() and source.source_dir:
                    p = Path(source.source_dir) / p
                elif not p.is_absolute() and config.project_dir:
                    p = Path(config.project_dir) / p
                try:
                    dir_str = str(p.resolve())
                except OSError:
                    dir_str = str(p)
                # Build sample renamed file
                rename_resolved = rename_tpl
                rename_resolved = rename_resolved.replace("{source_title}", tokens["source_title"])
                rename_resolved = rename_resolved.replace("{source_name}", tokens["source_name"])
                rename_resolved = rename_resolved.replace("{source_basename}", tokens["source_basename"])
                rename_resolved = rename_resolved.replace("{source_fullname}", tokens["source_fullname"])
                rename_resolved = _expand_group_token(rename_resolved, source.group)
                sample_file = f"{rename_resolved}.####.exr"
                group_tag = f" [{source.group}]" if source.group else ""
                previews.append(f"{source.name}{group_tag}: {str(Path(dir_str) / sample_file)}")
            if not self._selected_source and len(config.watched_sources) > 3:
                previews.append(f"... and {len(config.watched_sources) - 3} more")
        else:
            resolved = tpl
            resolved = resolved.replace("{project_root}", live_root)
            resolved = resolved.replace("{group_root}", "<group_root>")
            resolved = resolved.replace("{source_title}", "<source_title>")
            resolved = resolved.replace("{source_name}", "<source_name>")
            resolved = resolved.replace("{source_basename}", "<source_basename>")
            resolved = resolved.replace("{source_fullname}", "<source_fullname>")
            resolved = resolved.replace("{source_filename}", "<source_filename>")
            resolved = resolved.replace("{source_dir}", "<source_dir>")
            resolved = resolved.replace("{group}", "<group>")
            p = Path(resolved)
            try:
                dir_str = str(p.resolve())
            except OSError:
                dir_str = str(p)
            rename_resolved = rename_tpl
            rename_resolved = rename_resolved.replace("{source_title}", "<source_title>")
            rename_resolved = rename_resolved.replace("{source_name}", "<source_name>")
            rename_resolved = rename_resolved.replace("{source_basename}", "<source_basename>")
            rename_resolved = rename_resolved.replace("{source_fullname}", "<source_fullname>")
            rename_resolved = rename_resolved.replace("{group}", "<group>")
            sample_file = f"{rename_resolved}.####.exr"
            previews.append(str(Path(dir_str) / sample_file))

        def _path_wrappable(p: str) -> str:
            # Insert zero-width spaces after path separators so Qt can wrap long paths.
            # Qt's rich text engine doesn't support <wbr>; \u200b works natively.
            return p.replace("/", "/\u200b").replace("\\", "\\\u200b")

        self.path_preview_label.setText("\n".join(_path_wrappable(p) for p in previews))
        self.path_preview_label.setStyleSheet("color: #3aaa88; font-size: 11pt;")

    def _browse_root(self):
        start = self.root_edit.text().strip()
        d = QFileDialog.getExistingDirectory(self, "Select Project Root", start)
        if d:
            self.root_edit.setText(d)

    def _format_naming_label(self, rule: str):
        """Format the naming rule label with a human-readable description."""
        if not rule:
            self.naming_label.setText("Not configured yet — will be set on first ingest")
            self.naming_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
            return

        descriptions = {
            "source_name": (
                "Source Name",
                "Filename without version, frame numbers, or extension",
                "hero_comp_v003.1001.exr  →  hero_comp",
            ),
            "source_basename": (
                "Base Name",
                "Filename without version, frames, extension, or task tokens",
                "hero_comp_v003.1001.exr  →  hero",
            ),
            "source_fullname": (
                "Full Name",
                "Filename without frame numbers or extension (keeps version)",
                "hero_comp_v003.1001.exr  →  hero_comp_v003",
            ),
        }

        if rule in descriptions:
            label, desc, example = descriptions[rule]
            text = (
                f'<b>{label}</b> <span style="color:#8c8c8c;">({rule})</span><br/>'
                f'<span style="color:#8c8c8c; font-size:11px;">{desc}</span><br/>'
                f'<span style="color:#3aaa88; font-size:11px;">e.g. {example}</span>'
            )
        elif rule.startswith("parent:"):
            depth = rule.split(":")[1]
            if depth == "0":
                level_desc = "immediate parent folder"
            elif depth == "1":
                level_desc = "grandparent folder"
            else:
                level_desc = f"ancestor folder (depth {depth})"
            text = (
                f'<b>Parent Directory</b> <span style="color:#8c8c8c;">({level_desc})</span><br/>'
                f'<span style="color:#8c8c8c; font-size:11px;">Source name comes from the {level_desc} of the version folder</span>'
            )
        else:
            text = f'<span style="color:#c0c0c0;">{rule}</span>'

        self.naming_label.setText(text)
        self.naming_label.setTextFormat(Qt.RichText)
        self.naming_label.setStyleSheet("")

    def _reset_naming(self):
        """Reset naming convention so it will be re-asked on next discovery ingest."""
        self._naming_reset = True
        self.naming_label.setTextFormat(Qt.PlainText)
        self.naming_label.setText("(will be re-asked on next ingest)")
        self.naming_label.setStyleSheet("color: #ffaa00;")

    def apply_to_config(self, config: ProjectConfig):
        """Apply dialog values back to the config."""
        config.project_name = self.name_edit.text().strip() or "Untitled"
        root = self.root_edit.text().strip()
        # Store project_root only when it differs from project_dir (JSON location)
        if root and root != config.project_dir:
            config.project_root = root
        else:
            config.project_root = ""
        config.latest_path_template = self.latest_template_edit.text().strip()
        config.default_file_rename_template = self.rename_template_edit.text().strip() or "{source_basename}_latest"
        config.default_version_pattern = self.pattern_edit.text().strip() or "_v{version}"

        date_fmt_text = self.date_format_combo.currentText()
        config.default_date_format = "" if date_fmt_text == "(none)" else date_fmt_text

        exts = self.extensions_edit.text().strip().split()
        config.default_file_extensions = exts if exts else list(DEFAULT_FILE_EXTENSIONS)

        config.default_link_mode = self.link_mode_combo.currentText()
        config.timecode_mode = self.timecode_combo.currentText()

        config.task_tokens = [t.strip() for t in self.tasks_edit.text().split(",") if t.strip()]

        if self._naming_reset:
            config.default_naming_rule = ""
            config.naming_configured = False

        config.name_whitelist = self.whitelist_edit.tags()
        config.name_blacklist = self.blacklist_edit.tags()

        # Hooks (Feature #2)
        config.pre_promote_cmd = self.pre_promote_edit.text().strip()
        config.post_promote_cmd = self.post_promote_edit.text().strip()

        # Sequence validation (Feature #11)
        config.block_incomplete_sequences = self.block_incomplete_cb.isChecked()

        # Network performance
        config.skip_resolve = self.skip_resolve_cb.isChecked()

        # Re-apply defaults to non-overridden sources
        apply_project_defaults(config)

    def done(self, result):
        ProjectSettingsDialog._last_geometry = self.saveGeometry()
        super().done(result)

    def _save_as_template(self):
        """Save the current config as a reusable template (Feature #17)."""
        from src.lvm.templates import save_template
        name, ok = QInputDialog.getText(self, "Save Template", "Template name:")
        if ok and name.strip():
            path = save_template(self._config, name.strip())
            QMessageBox.information(self, "Template Saved", f"Template saved to:\n{path}")


# ---------------------------------------------------------------------------
# Latest Path Dialog (shown when no latest path template is configured)
# ---------------------------------------------------------------------------

class LatestPathDialog(QDialog):
    """Dialog for defining the latest path template.

    Shown when the user tries to promote or add sources but no latest path
    template has been configured. Provides a live preview of the resolved path.
    """

    def __init__(self, config: ProjectConfig, source: WatchedSource = None,
                 discovery_results: list = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Set Latest Path")
        self.setMinimumWidth(600)
        self._config = config
        self._source = source  # optional: specific source for preview
        self._discovery_results = discovery_results or []  # DiscoveryResults for preview

        layout = QVBoxLayout(self)

        # Header
        header = QLabel("A latest path template is required before promotion.")
        header.setStyleSheet("font-size: 14pt; font-weight: bold; margin-bottom: 4px;")
        layout.addWidget(header)

        desc = QLabel(
            "This defines where promoted files are placed. The template is applied\n"
            "to each source, so you can use tokens to create unique paths per source."
        )
        desc.setStyleSheet("color: #8c8c8c; font-size: 11pt; margin-bottom: 8px;")
        layout.addWidget(desc)

        # Template input
        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        token_help = QLabel(
            "Tokens: {source_dir}, {source_title}, {source_name}, {source_basename},\n"
            "{source_fullname}, {source_filename}, {project_root}, {group}, {group_root}\n"
            "If {group} is empty, the token and its trailing divider are omitted.\n"
            "{group_root} resolves to the group's root directory (falls back to {project_root}).\n"
            "{source_title} is the source's in-project display name.\n"
            "Examples: ../online  |  {group_root}/latest/{source_name}  |  online/{group}/{source_name}"
        )
        token_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        form.addRow("", token_help)

        default_template = config.latest_path_template or "{source_dir}/../{source_name}_latest"
        self.template_edit = QLineEdit(default_template)
        self.template_edit.setPlaceholderText("{source_dir}/../{source_name}_latest")
        self.template_edit.textChanged.connect(self._update_preview)
        form.addRow("Latest Path Template:", self.template_edit)

        # Browse button
        browse_row = QHBoxLayout()
        browse_row.addStretch()
        browse_btn = QPushButton("Browse for Directory...")
        browse_btn.clicked.connect(self._browse)
        browse_row.addWidget(browse_btn)
        form.addRow("", browse_row)

        # File rename template
        rename_help = QLabel(
            "Controls the output filename (frame number and extension are preserved).\n"
            "Tokens: {source_title}, {source_name}, {source_basename}, {source_fullname}"
        )
        rename_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        form.addRow("", rename_help)

        default_rename = config.default_file_rename_template or "{source_basename}_latest"
        self.rename_edit = QLineEdit(default_rename)
        self.rename_edit.setPlaceholderText("{source_basename}_latest")
        self.rename_edit.textChanged.connect(self._update_preview)
        form.addRow("File Rename Template:", self.rename_edit)

        layout.addLayout(form)

        # Preview
        preview_header = QLabel("Resolved Preview:")
        preview_header.setStyleSheet("font-weight: bold; margin-top: 8px;")
        layout.addWidget(preview_header)

        self.preview_label = QLabel("")
        self.preview_label.setStyleSheet("color: #3aaa88; font-size: 11pt;")
        self.preview_label.setWordWrap(True)
        self.preview_label.setMinimumHeight(60)
        layout.addWidget(self.preview_label)

        # Buttons
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.ok_btn = btn_box.button(QDialogButtonBox.Ok)
        self.ok_btn.setEnabled(False)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

        self._update_preview()

    def _browse(self):
        d = QFileDialog.getExistingDirectory(self, "Select Latest Target Directory")
        if d:
            # Try to make relative to project_dir for portability
            if self._config.project_dir:
                try:
                    rel = os.path.relpath(d, self._config.project_dir).replace("\\", "/")
                    self.template_edit.setText(rel)
                    return
                except ValueError:
                    pass
            self.template_edit.setText(d)

    def _update_preview(self):
        tpl = self.template_edit.text().strip()
        rename_tpl = self.rename_edit.text().strip() or "{source_basename}_latest"
        self.ok_btn.setEnabled(bool(tpl))
        if not tpl:
            self.preview_label.setText("(enter a template above)")
            self.preview_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
            return

        config = self._config
        previews = []

        # If a specific source was given, show it first; also include
        # discovery results so the preview is meaningful before sources are
        # actually added to the project.
        sources = []
        if self._source:
            sources.append(self._source)
        sources.extend(s for s in config.watched_sources if s != self._source)
        # Build lightweight preview sources from discovery results
        if self._discovery_results:
            naming_rule = config.default_naming_rule or "parent:0"
            for dr in self._discovery_results:
                name = compute_source_name(dr, naming_rule, config.task_tokens)
                preview_src = WatchedSource(
                    name=name,
                    source_dir=dr.path,
                    sample_filename=dr.sample_filename or "",
                )
                if preview_src not in sources:
                    sources.append(preview_src)
        sources = sources[:4]

        if sources:
            for source in sources:
                tokens = derive_source_tokens(source.sample_filename or source.name,
                                              config.task_tokens, source_title=source.name)
                resolved = tpl
                resolved = resolved.replace("{project_root}", config.effective_project_root or "<project_root>")
                resolved = resolved.replace("{group_root}", _resolve_group_root(config, source.group) or "<project_root>")
                resolved = resolved.replace("{source_title}", tokens["source_title"])
                resolved = resolved.replace("{source_name}", tokens["source_name"])
                resolved = resolved.replace("{source_basename}", tokens["source_basename"])
                resolved = resolved.replace("{source_fullname}", tokens["source_fullname"])
                resolved = resolved.replace("{source_filename}", tokens["source_filename"])
                resolved = resolved.replace("{source_dir}", source.source_dir)
                resolved = _expand_group_token(resolved, source.group)
                # Relative paths resolve from the source directory
                p = Path(resolved)
                if not p.is_absolute() and source.source_dir:
                    p = Path(source.source_dir) / p
                elif not p.is_absolute() and config.project_dir:
                    p = Path(config.project_dir) / p
                try:
                    dir_str = str(p.resolve())
                except OSError:
                    dir_str = str(p)
                # Build sample renamed file
                rename_resolved = rename_tpl
                rename_resolved = rename_resolved.replace("{source_title}", tokens["source_title"])
                rename_resolved = rename_resolved.replace("{source_name}", tokens["source_name"])
                rename_resolved = rename_resolved.replace("{source_basename}", tokens["source_basename"])
                rename_resolved = rename_resolved.replace("{source_fullname}", tokens["source_fullname"])
                rename_resolved = _expand_group_token(rename_resolved, source.group)
                sample_file = f"{rename_resolved}.####.exr"
                group_tag = f" [{source.group}]" if source.group else ""
                previews.append(f"{source.name}{group_tag}: {str(Path(dir_str) / sample_file)}")
            if len(config.watched_sources) > 4:
                previews.append(f"... and {len(config.watched_sources) - 4} more")
        else:
            resolved = tpl
            resolved = resolved.replace("{project_root}", config.effective_project_root or "<project_root>")
            resolved = resolved.replace("{group_root}", "<group_root>")
            resolved = resolved.replace("{source_title}", "<source_title>")
            resolved = resolved.replace("{source_name}", "<source_name>")
            resolved = resolved.replace("{source_basename}", "<source_basename>")
            resolved = resolved.replace("{source_fullname}", "<source_fullname>")
            resolved = resolved.replace("{source_filename}", "<source_filename>")
            resolved = resolved.replace("{source_dir}", "<source_dir>")
            resolved = resolved.replace("{group}", "<group>")
            p = Path(resolved)
            try:
                dir_str = str(p.resolve())
            except OSError:
                dir_str = str(p)
            rename_resolved = rename_tpl
            rename_resolved = rename_resolved.replace("{source_title}", "<source_title>")
            rename_resolved = rename_resolved.replace("{source_name}", "<source_name>")
            rename_resolved = rename_resolved.replace("{source_basename}", "<source_basename>")
            rename_resolved = rename_resolved.replace("{source_fullname}", "<source_fullname>")
            rename_resolved = rename_resolved.replace("{group}", "<group>")
            sample_file = f"{rename_resolved}.####.exr"
            previews.append(str(Path(dir_str) / sample_file))

        self.preview_label.setText("\n".join(previews))
        self.preview_label.setStyleSheet("color: #3aaa88; font-size: 11pt;")

    def get_template(self) -> str:
        return self.template_edit.text().strip()

    def get_rename_template(self) -> str:
        return self.rename_edit.text().strip() or "{source_basename}_latest"


# ---------------------------------------------------------------------------
# Naming Rule Dialog (first-ingest naming convention choice)
# ---------------------------------------------------------------------------

class NamingRuleDialog(QDialog):
    """Dialog for choosing how sources should be named during ingest.

    Shown on the first discovery add when naming_configured is False.
    Presents naming options derived from a representative DiscoveryResult.
    """

    def __init__(self, results: list, task_patterns: list = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Choose Source Naming Convention")
        self.setMinimumWidth(600)
        self._results = results
        self._task_patterns = task_patterns or []
        self._chosen_rule = ""

        layout = QVBoxLayout(self)

        # Header
        header = QLabel("How should discovered sources be named?")
        header.setStyleSheet("font-size: 14pt; font-weight: bold; margin-bottom: 8px;")
        layout.addWidget(header)

        # Show example path from first result
        if results:
            result = results[0]
            path_label = QLabel(f"Example path: {result.path}")
            path_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
            path_label.setWordWrap(True)
            layout.addWidget(path_label)

            if result.sample_filename:
                file_label = QLabel(f"Example file: {result.sample_filename}")
                file_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
                layout.addWidget(file_label)

        # Radio buttons for naming options
        options_group = QGroupBox("Naming Options")
        options_layout = QVBoxLayout(options_group)

        self._radio_buttons = []

        if results:
            options = get_naming_options(results[0], self._task_patterns)
            for opt in options:
                radio = QPushButton()
                radio.setCheckable(True)
                radio.setStyleSheet(
                    "QPushButton { text-align: left; padding: 8px 12px; "
                    "border: 1px solid #2a2a2a; border-radius: 4px; }"
                    "QPushButton:checked { background-color: #336699; border-color: #5588bb; }"
                    "QPushButton:hover { background-color: #242424; }"
                )
                radio.setText(opt["label"])
                radio.clicked.connect(lambda checked, r=opt["rule"]: self._select_rule(r))
                options_layout.addWidget(radio)
                self._radio_buttons.append((radio, opt["rule"]))

        layout.addWidget(options_group)

        # Preview section
        preview_label = QLabel("Preview (names for all selected sources):")
        preview_label.setStyleSheet("font-weight: bold; margin-top: 8px;")
        layout.addWidget(preview_label)

        self.preview_list = QListWidget()
        self.preview_list.setMaximumHeight(120)
        self.preview_list.setStyleSheet("color: #88aaff;")
        layout.addWidget(self.preview_list)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.ok_btn = QPushButton("OK")
        self.ok_btn.setEnabled(False)
        self.ok_btn.clicked.connect(self.accept)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        # Auto-select first option if available
        if self._radio_buttons:
            self._radio_buttons[0][0].setChecked(True)
            self._select_rule(self._radio_buttons[0][1])

    def _select_rule(self, rule: str):
        """Update selection and preview when a naming rule is chosen."""
        self._chosen_rule = rule
        self.ok_btn.setEnabled(True)

        # Uncheck other buttons
        for radio, r in self._radio_buttons:
            if r != rule:
                radio.setChecked(False)
            else:
                radio.setChecked(True)

        # Update preview for all selected results
        self.preview_list.clear()
        for result in self._results:
            name = compute_source_name(result, rule, self._task_patterns)
            self.preview_list.addItem(f"  {name}")

    def get_chosen_rule(self) -> str:
        """Return the selected naming rule string."""
        return self._chosen_rule


# ---------------------------------------------------------------------------
# Discovery Dialog (multi-select + add to project)
# ---------------------------------------------------------------------------

class DiscoveryWorker(QThread):
    """Runs directory discovery scan in background."""
    finished = Signal(list)   # list of DiscoveryResult
    error = Signal(str)
    progress = Signal(str, int, int)  # current_path, dirs_scanned, estimated_total

    def __init__(self, root_dir: str, max_depth: int = 4, extensions=None,
                 whitelist=None, blacklist=None, skip_resolve=True, parent=None):
        super().__init__(parent)
        self.root_dir = root_dir
        self.max_depth = max_depth
        self.extensions = extensions
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.skip_resolve = skip_resolve

    def _on_progress(self, current_path: str, dirs_scanned: int, estimated_total: int):
        self.progress.emit(current_path, dirs_scanned, estimated_total)

    def run(self):
        try:
            results = discover(
                root_dir=self.root_dir,
                max_depth=self.max_depth,
                extensions=self.extensions,
                whitelist=self.whitelist,
                blacklist=self.blacklist,
                progress_callback=self._on_progress,
                skip_resolve=self.skip_resolve,
            )
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(str(e))


class DiscoveryDialog(QDialog):
    """Dialog showing discovery scan results with multi-select and add-to-project."""

    sources_added = Signal(int)  # number of sources added

    def __init__(self, config: ProjectConfig = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Discover Versions")
        self.resize(850, 550)
        self._worker = None
        self._config = config
        self._results = []  # store DiscoveryResults for add-to-project
        self._timecodes_populated = False
        self._ignored_paths: set[str] = set()          # ignored source directory paths
        self._ignored_versions: set[tuple[str, int]] = set()  # (path, version_number)
        self._filtered_by_whitelist: set[str] = set()  # paths filtered by whitelist
        self._filtered_by_blacklist: set[str] = set()  # paths filtered by blacklist

        layout = QVBoxLayout(self)

        # Directory picker (editable combo with search history)
        pick_row = QHBoxLayout()
        pick_row.addWidget(QLabel("Directory:"))
        self.dir_combo = QComboBox()
        self.dir_combo.setEditable(True)
        self.dir_combo.setInsertPolicy(QComboBox.NoInsert)
        self.dir_combo.lineEdit().setPlaceholderText("Select a directory to scan...")
        self.dir_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        # Populate history from project config
        if config and config.discovery_search_history:
            self.dir_combo.addItems(config.discovery_search_history)
        # Default to project root if available
        if config and config.effective_project_root:
            project_root = config.effective_project_root
            # If project root is already in history, select it; otherwise prepend it
            idx = self.dir_combo.findText(project_root)
            if idx >= 0:
                self.dir_combo.setCurrentIndex(idx)
            else:
                self.dir_combo.insertItem(0, project_root)
                self.dir_combo.setCurrentIndex(0)
        pick_row.addWidget(self.dir_combo, 1)
        self.browse_btn = QPushButton("Browse...")
        self.browse_btn.clicked.connect(self._browse)
        pick_row.addWidget(self.browse_btn)
        self.scan_btn = QPushButton("Scan")
        self.scan_btn.clicked.connect(self._start_scan)
        pick_row.addWidget(self.scan_btn)
        layout.addLayout(pick_row)

        # Whitelist/Blacklist filters (live filtering)
        filters_section = CollapsibleSection("Filters", collapsed=True)
        filters_layout = filters_section.content_layout()
        filters_layout.setSpacing(6)

        # Whitelist label and input
        self.discovery_whitelist = TagInputWidget(placeholder="Type and press comma to add...")
        filters_layout.addRow("Whitelist (include only):", self.discovery_whitelist)
        self.discovery_whitelist.tagsChanged.connect(self._on_discovery_filters_changed)

        # Blacklist label and input
        self.discovery_blacklist = TagInputWidget(placeholder="Type and press comma to add...")
        filters_layout.addRow("Blacklist (exclude):", self.discovery_blacklist)
        self.discovery_blacklist.tagsChanged.connect(self._on_discovery_filters_changed)

        layout.addWidget(filters_section)

        # Results tree (multi-select)
        self.result_tree = QTreeWidget()
        self.result_tree.setHeaderLabels(["Location / Version", "Files", "Size", "Frame Range", "Timecode", "Pattern"])
        self.result_tree.setRootIsDecorated(True)
        self.result_tree.setAlternatingRowColors(True)
        self.result_tree.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.result_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.result_tree.customContextMenuRequested.connect(self._tree_context_menu)
        header = self.result_tree.header()
        header.setStretchLastSection(True)
        header.resizeSection(0, 300)
        header.resizeSection(1, 60)
        header.resizeSection(2, 80)
        header.resizeSection(3, 140)
        header.resizeSection(4, 110)
        layout.addWidget(self.result_tree)

        # Filter row
        filter_row = QHBoxLayout()
        self.hide_existing_cb = QCheckBox("Hide already added sources")
        self.hide_existing_cb.setChecked(True)
        self.hide_existing_cb.setToolTip("Hide sources whose directory is already in the project")
        self.hide_existing_cb.toggled.connect(self._rebuild_tree)
        filter_row.addWidget(self.hide_existing_cb)
        self.show_ignored_cb = QCheckBox("Show ignored")
        self.show_ignored_cb.setChecked(False)
        self.show_ignored_cb.setToolTip("Show items you've marked as ignored")
        self.show_ignored_cb.toggled.connect(self._rebuild_tree)
        filter_row.addWidget(self.show_ignored_cb)
        filter_row.addStretch()
        layout.addLayout(filter_row)

        # Progress bar (hidden until scan starts)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        layout.addWidget(self.progress_bar)

        self.status_label = QLabel("Select a directory and click Scan.")
        self.status_label.setStyleSheet("color: #8c8c8c;")
        layout.addWidget(self.status_label)

        # Bottom buttons
        btn_row = QHBoxLayout()
        self.add_btn = QPushButton("Add Selected to Project")
        self.add_btn.setEnabled(False)
        self.add_btn.setStyleSheet(
            "QPushButton { background-color: #336699; color: white; padding: 6px 14px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #4d7aae; }"
            "QPushButton:disabled { background-color: #2a2a2a; color: #8c8c8c; }"
        )
        self.add_btn.clicked.connect(self._add_selected)
        btn_row.addWidget(self.add_btn)
        btn_row.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

        self.result_tree.itemSelectionChanged.connect(self._on_selection_changed)

    def _browse(self):
        start_dir = self.dir_combo.currentText().strip() or ""
        d = QFileDialog.getExistingDirectory(self, "Select Directory to Scan", start_dir)
        if d:
            self.dir_combo.setCurrentText(d)

    def _on_selection_changed(self):
        # Only count top-level (location) items, not version children
        selected = [item for item in self.result_tree.selectedItems()
                     if item.parent() is None]
        has_project = self._config is not None
        self.add_btn.setEnabled(has_project and len(selected) > 0)

    def _save_search_path(self, path: str):
        """Add path to the combo history and persist in project config."""
        MAX_SEARCH_HISTORY = 20
        # Add to combo if not already present
        idx = self.dir_combo.findText(path)
        if idx >= 0:
            # Move to top
            self.dir_combo.removeItem(idx)
        self.dir_combo.insertItem(0, path)
        self.dir_combo.setCurrentIndex(0)
        # Cap the dropdown size
        while self.dir_combo.count() > MAX_SEARCH_HISTORY:
            self.dir_combo.removeItem(self.dir_combo.count() - 1)
        # Persist in project config
        if self._config is not None:
            history = [self.dir_combo.itemText(i) for i in range(self.dir_combo.count())]
            self._config.discovery_search_history = history

    def _start_scan(self):
        root_dir = self.dir_combo.currentText().strip()
        if not root_dir:
            return

        # Save search path to history
        self._save_search_path(root_dir)

        self.result_tree.clear()
        self._results.clear()
        self.scan_btn.setEnabled(False)
        self.add_btn.setEnabled(False)
        self.status_label.setText("Scanning...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # indeterminate initially
        self.progress_bar.setFormat("Estimating scan size...")

        # Use project filters if available
        whitelist = self._config.name_whitelist if self._config else None
        blacklist = self._config.name_blacklist if self._config else None

        skip_resolve = self._config.skip_resolve if self._config else True

        self._worker = DiscoveryWorker(
            root_dir,
            whitelist=whitelist or None,
            blacklist=blacklist or None,
            skip_resolve=skip_resolve,
            parent=self,
        )
        self._worker.finished.connect(self._on_results)
        self._worker.error.connect(self._on_error)
        self._worker.progress.connect(self._on_scan_progress)
        self._worker.start()

    def _get_existing_sources(self) -> set:
        """Return a set of (resolved_source_dir, source_name) tuples already in the project."""
        existing = set()
        if self._config:
            for src in self._config.watched_sources:
                if src.source_dir:
                    existing.add((str(Path(src.source_dir).resolve()).lower(), src.name.lower()))
        return existing

    def _is_existing(self, result_path: str, existing_sources: set, result_name: str = "") -> bool:
        """Check if a discovery result matches an existing source.

        Compares both the source directory path AND the result name to handle
        multi-shot directories where multiple sources share the same parent.
        """
        resolved = str(Path(result_path).resolve()).lower()
        # Check if this specific (path, name) combination exists
        if (resolved, result_name.lower()) in existing_sources:
            return True
        # Also check path-only match for backward compatibility (single-shot dirs)
        return any(d == resolved for d, _ in existing_sources)

    def _on_scan_progress(self, current_path: str, dirs_scanned: int, estimated_total: int):
        """Update progress bar during discovery scan."""
        if estimated_total > 0 and dirs_scanned > 0:
            # Switch to determinate mode with percentage
            self.progress_bar.setRange(0, estimated_total)
            # Clamp to 95% if we exceed estimate; final 100% comes on completion
            value = min(dirs_scanned, int(estimated_total * 0.95))
            self.progress_bar.setValue(value)
            self.progress_bar.setFormat(f"%p%  ({dirs_scanned}/{estimated_total} directories)")
        # Show abbreviated path in status label
        display_path = current_path
        if len(display_path) > 80:
            display_path = "..." + display_path[-77:]
        self.status_label.setText(f"Scanning: {display_path}")

    def _on_results(self, results: list):
        self._worker = None
        self.scan_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        self._results = results
        self._timecodes_populated = False

        if not results:
            self.status_label.setText("No versioned content found.")
            return

        # Populate timecodes once on new results rather than on every tree rebuild
        tc_mode = self._config.timecode_mode if self._config else "lazy"
        if tc_mode != "never":
            for result in self._results:
                populate_timecodes(result.versions_found)
            self._timecodes_populated = True

        self._rebuild_tree()

    def _rebuild_tree(self):
        """Rebuild the results tree, respecting filters (existing, ignored).

        Wraps the rebuild in setUpdatesEnabled(False/True) to avoid costly
        per-item repaints when the tree has hundreds of results.
        """
        self.result_tree.setUpdatesEnabled(False)
        try:
            self._rebuild_tree_inner()
        finally:
            self.result_tree.setUpdatesEnabled(True)

    def _rebuild_tree_inner(self):
        self.result_tree.clear()

        if not self._results:
            return

        root_dir = self.dir_combo.currentText().strip()
        root = Path(root_dir).resolve() if root_dir else None

        existing_sources = self._get_existing_sources()
        hide_existing = self.hide_existing_cb.isChecked()
        show_ignored = self.show_ignored_cb.isChecked()

        shown = 0
        hidden = 0

        for i, result in enumerate(self._results):
            if self._config and self._config.default_naming_rule:
                expected_name = compute_source_name(result, self._config.default_naming_rule, self._config.task_tokens)
            else:
                expected_name = result.name
            is_existing = self._is_existing(result.path, existing_sources, expected_name)
            is_ignored = result.path in self._ignored_paths
            is_filtered_whitelist = result.path in self._filtered_by_whitelist
            is_filtered_blacklist = result.path in self._filtered_by_blacklist

            if is_existing and hide_existing:
                hidden += 1
                continue

            if is_ignored and not show_ignored:
                hidden += 1
                continue

            # Apply whitelist/blacklist filters (always hide, not subject to show_ignored)
            if is_filtered_whitelist or is_filtered_blacklist:
                hidden += 1
                continue

            try:
                display_path = str(Path(result.path).relative_to(root)) if root else result.path
            except ValueError:
                display_path = result.path

            if result.sample_filename:
                display_path += "  \u2014  " + result.sample_filename

            # Mark existing/ignored sources with a suffix
            if is_existing:
                display_path += "  (already added)"
            elif is_ignored:
                display_path += "  (ignored)"

            parent_item = QTreeWidgetItem([
                display_path,
                str(len(result.versions_found)),
                "",
                "",
                "",
                result.suggested_pattern,
            ])
            parent_item.setData(0, Qt.UserRole, i)  # store index into _results
            parent_item.setExpanded(True)
            if result.sample_filename:
                parent_item.setToolTip(0, f"Sample file: {result.sample_filename}")

            # Style existing sources: gray and italic, non-selectable
            if is_existing:
                gray = QColor("#8c8c8c")
                italic_font = QFont()
                italic_font.setItalic(True)
                for col in range(6):
                    parent_item.setForeground(col, gray)
                    parent_item.setFont(col, italic_font)
                parent_item.setFlags(parent_item.flags() & ~Qt.ItemIsSelectable)

            # Style ignored sources: gray, italic, strikethrough, non-selectable
            if is_ignored:
                gray = QColor("#8c8c8c")
                strike_font = QFont()
                strike_font.setStrikeOut(True)
                strike_font.setItalic(True)
                for col in range(6):
                    parent_item.setForeground(col, gray)
                    parent_item.setFont(col, strike_font)
                parent_item.setFlags(parent_item.flags() & ~Qt.ItemIsSelectable)

            for vi_idx, v in enumerate(result.versions_found):
                version_key = (result.path, v.version_number)
                is_version_ignored = version_key in self._ignored_versions

                # Skip ignored versions if "Show ignored" is not checked
                if is_version_ignored and not show_ignored:
                    continue

                label = f"  {v.version_string}"
                if is_version_ignored:
                    label += "  (ignored)"

                frame_display = v.frame_range or ""
                if v.sub_sequences:
                    frame_display += f" (+{len(v.sub_sequences)} layer{'s' if len(v.sub_sequences) > 1 else ''})"
                child = QTreeWidgetItem([
                    label,
                    str(v.file_count),
                    v.total_size_human,
                    frame_display,
                    v.start_timecode or "",
                    "",
                ])
                # Store version index for context menu mapping
                child.setData(0, Qt.UserRole, vi_idx)
                # Make children non-selectable
                child.setFlags(child.flags() & ~Qt.ItemIsSelectable)

                # Style: gray/italic for existing or ignored parent, strikethrough for ignored version
                if is_existing or is_ignored or is_version_ignored:
                    gray = QColor("#8c8c8c")
                    style_font = QFont()
                    style_font.setItalic(True)
                    if is_version_ignored:
                        style_font.setStrikeOut(True)
                    for col in range(6):
                        child.setForeground(col, gray)
                        child.setFont(col, style_font)

                parent_item.addChild(child)

            self.result_tree.addTopLevelItem(parent_item)
            shown += 1

        total_versions = sum(len(r.versions_found) for r in self._results)
        status = f"Found {len(self._results)} location(s) with {total_versions} version(s)."
        if hidden:
            status += f" ({hidden} hidden.)"
        ignored_count = len(self._ignored_paths) + len(self._ignored_versions)
        if ignored_count:
            status += f" {ignored_count} item(s) ignored."
        if shown:
            status += " Select locations and click 'Add Selected to Project'."
        self.status_label.setText(status)

    def _on_error(self, msg: str):
        self._worker = None
        self.scan_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.status_label.setText(f"Error: {msg}")

    def reject(self):
        """Clean up any running discovery worker before closing."""
        if self._worker is not None:
            try:
                self._worker.disconnect()
            except RuntimeError:
                pass
            if self._worker.isRunning():
                self._worker.quit()
                self._worker.wait(2000)
            self._worker = None
        super().reject()

    # --- Ignore / context menu ---

    def _tree_context_menu(self, pos):
        """Show context menu for ignoring sources or versions."""
        item = self.result_tree.itemAt(pos)
        if not item:
            return

        menu = QMenu(self)
        is_top_level = item.parent() is None

        if is_top_level:
            idx = item.data(0, Qt.UserRole)
            if idx is not None and idx < len(self._results):
                result = self._results[idx]
                result_path = result.path

                menu.addAction(
                    _REVEAL_LABEL,
                    lambda p=result_path: reveal_in_file_browser(p),
                )
                menu.addSeparator()

                if result_path in self._ignored_paths:
                    menu.addAction("Unignore this source",
                        lambda p=result_path: self._unignore_source(p))
                else:
                    menu.addAction("Ignore this source",
                        lambda p=result_path: self._ignore_source(p))

                if self._config is not None:
                    menu.addSeparator()
                    menu.addAction("Add to project blacklist",
                        lambda r=result: self._add_to_blacklist(r))
        else:
            parent_item = item.parent()
            parent_idx = parent_item.data(0, Qt.UserRole)
            if parent_idx is not None and parent_idx < len(self._results):
                result = self._results[parent_idx]
                vi_idx = item.data(0, Qt.UserRole)
                if vi_idx is not None and vi_idx < len(result.versions_found):
                    version = result.versions_found[vi_idx]
                    key = (result.path, version.version_number)

                    menu.addAction(
                        _REVEAL_LABEL,
                        lambda p=version.source_path: reveal_in_file_browser(p),
                    )
                    menu.addSeparator()

                    if key in self._ignored_versions:
                        menu.addAction(f"Unignore {version.version_string}",
                            lambda k=key: self._unignore_version(k))
                    else:
                        menu.addAction(f"Ignore {version.version_string}",
                            lambda k=key: self._ignore_version(k))

        if menu.actions():
            menu.exec(self.result_tree.viewport().mapToGlobal(pos))

    def _ignore_source(self, path: str):
        self._ignored_paths.add(path)
        self._rebuild_tree()

    def _unignore_source(self, path: str):
        self._ignored_paths.discard(path)
        self._rebuild_tree()

    def _ignore_version(self, key: tuple):
        self._ignored_versions.add(key)
        self._rebuild_tree()

    def _unignore_version(self, key: tuple):
        self._ignored_versions.discard(key)
        self._rebuild_tree()

    def _on_discovery_filters_changed(self):
        """Handle live filtering when whitelist/blacklist tags change."""
        # Recalculate filtered sets based on current tags
        self._filtered_by_whitelist.clear()
        self._filtered_by_blacklist.clear()

        whitelist_tags = self.discovery_whitelist.tags()
        blacklist_tags = self.discovery_blacklist.tags()

        # Apply whitelist: only include results that match at least one whitelist tag
        if whitelist_tags:
            for result in self._results:
                parts = [result.name, result.path]
                if result.sample_filename:
                    parts.append(result.sample_filename)
                search_text = " ".join(parts).lower()

                # Check if any whitelist tag is in the search text
                matches_whitelist = any(tag.lower() in search_text for tag in whitelist_tags)
                if not matches_whitelist:
                    self._filtered_by_whitelist.add(result.path)

        # Apply blacklist: exclude results that match any blacklist tag
        if blacklist_tags:
            for result in self._results:
                parts = [result.name, result.path]
                if result.sample_filename:
                    parts.append(result.sample_filename)
                search_text = " ".join(parts).lower()

                # Check if any blacklist tag is in the search text
                matches_blacklist = any(tag.lower() in search_text for tag in blacklist_tags)
                if matches_blacklist:
                    self._filtered_by_blacklist.add(result.path)

        self._rebuild_tree()

    def _apply_blacklist_keyword(self, keyword: str):
        """
        Filter all results that contain the given blacklist keyword.
        Add matching results to _ignored_paths for immediate filtering.
        """
        keyword_lower = keyword.lower()
        for result in self._results:
            # Build search text from name and path, similar to discovery._apply_filters
            parts = [result.name, result.path]
            if result.sample_filename:
                parts.append(result.sample_filename)
            search_text = " ".join(parts).lower()

            # If keyword is found in the result, mark it as ignored
            if keyword_lower in search_text:
                self._ignored_paths.add(result.path)

    def _add_to_blacklist(self, result):
        """Add a keyword to the project's name_blacklist (persistent)."""
        keyword, ok = QInputDialog.getText(
            self, "Add to Blacklist",
            "Enter keyword to blacklist:",
            text=result.name,
        )
        if ok and keyword.strip():
            keyword = keyword.strip()
            if keyword not in self._config.name_blacklist:
                self._config.name_blacklist.append(keyword)
            # Apply the blacklist keyword to all results immediately
            self._apply_blacklist_keyword(keyword)
            self._rebuild_tree()

    def _add_selected(self):
        if not self._config:
            return

        selected_items = [item for item in self.result_tree.selectedItems()
                          if item.parent() is None]
        if not selected_items:
            return

        # Gather selected DiscoveryResults
        selected_results = []
        for item in selected_items:
            idx = item.data(0, Qt.UserRole)
            if idx is not None and idx < len(self._results):
                selected_results.append(self._results[idx])

        if not selected_results:
            return

        # If naming convention is not yet configured, show the naming dialog
        if not self._config.naming_configured:
            naming_dlg = NamingRuleDialog(
                results=selected_results,
                task_patterns=self._config.task_tokens,
                parent=self,
            )
            if naming_dlg.exec() != QDialog.Accepted:
                return
            self._config.default_naming_rule = naming_dlg.get_chosen_rule()
            self._config.naming_configured = True

        # If no latest path template is set, prompt the user to define one
        if not self._config.latest_path_template:
            path_dlg = LatestPathDialog(self._config, discovery_results=selected_results, parent=self)
            if path_dlg.exec() == QDialog.Accepted:
                self._config.latest_path_template = path_dlg.get_template()
                self._config.default_file_rename_template = path_dlg.get_rename_template()

        # Add sources using the naming rule
        added = 0
        for result in selected_results:
            source_name = compute_source_name(
                result,
                self._config.default_naming_rule,
                self._config.task_tokens,
            )

            suggested_date_fmt = getattr(result, "suggested_date_format", "")
            from datetime import datetime as _dt
            source = WatchedSource(
                name=source_name,
                source_dir=result.path,
                version_pattern=result.suggested_pattern or self._config.default_version_pattern,
                file_extensions=result.suggested_extensions or list(self._config.default_file_extensions),
                sample_filename=result.sample_filename or "",
                date_format=suggested_date_fmt or self._config.default_date_format,
                # Override pattern and extensions since they come from discovery
                override_version_pattern=bool(result.suggested_pattern),
                override_file_extensions=bool(result.suggested_extensions),
                override_date_format=bool(suggested_date_fmt),
                added_at=_dt.now().isoformat(timespec="seconds"),
            )

            # Compute latest_target from project template if available
            if self._config.latest_path_template:
                tokens = derive_source_tokens(
                    result.sample_filename or source_name,
                    self._config.task_tokens,
                    source_title=source_name,
                )
                tpl = self._config.latest_path_template
                tpl = tpl.replace("{project_root}", self._config.effective_project_root)
                tpl = tpl.replace("{group_root}", _resolve_group_root(self._config, source.group))
                tpl = tpl.replace("{source_title}", tokens["source_title"])
                tpl = tpl.replace("{source_name}", tokens["source_name"])
                tpl = tpl.replace("{source_basename}", tokens["source_basename"])
                tpl = tpl.replace("{source_fullname}", tokens["source_fullname"])
                tpl = tpl.replace("{source_filename}", tokens["source_filename"])
                tpl = tpl.replace("{source_dir}", source.source_dir)
                tpl = _expand_group_token(tpl, source.group)
                # Relative paths resolve from the source directory
                resolved = Path(tpl)
                if not resolved.is_absolute() and source.source_dir:
                    resolved = Path(source.source_dir) / resolved
                elif not resolved.is_absolute() and self._config.project_dir:
                    resolved = Path(self._config.project_dir) / resolved
                source.latest_target = str(resolved.resolve())
                # Don't mark as override — it came from the project default template

            self._config.watched_sources.append(source)
            added += 1

        if added:
            self.sources_added.emit(added)
            QMessageBox.information(self, "Sources Added", f"Added {added} source(s) to the project.")
            # Rebuild tree so newly-added sources get marked/hidden
            self._rebuild_tree()


# ---------------------------------------------------------------------------
# Source list delegate — renders group tags as colored pills
# ---------------------------------------------------------------------------

class SourceItemDelegate(QStyledItemDelegate):
    """Custom delegate that renders group tags as colored labels in the source list."""

    # Role to store (group_name, group_color) tuple
    GROUP_ROLE = Qt.UserRole + 1

    def paint(self, painter: QPainter, option, index):
        group_data = index.data(self.GROUP_ROLE)

        # If no group data, just use default painting
        if not group_data:
            super().paint(painter, option, index)
            return

        # Draw everything except the text via the base style
        opt = option.__class__(option)
        self.initStyleOption(opt, index)

        # Strip the bullet+group from the display text for base measurement
        full_text = opt.text or ""
        marker = "  \u2022"
        if marker in full_text:
            main_text = full_text[:full_text.index(marker)]
        else:
            main_text = full_text

        # Let base class draw selection background + icon, but with truncated text
        opt.text = main_text
        style = opt.widget.style() if opt.widget else QApplication.style()
        style.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, painter, opt.widget)

        # Now draw the group pill on top
        group_name, group_color_str = group_data
        group_color = QColor(group_color_str)

        painter.save()

        font = opt.font
        fm = QFontMetrics(font)
        text_rect = style.subElementRect(QStyle.SubElement.SE_ItemViewItemText, opt, opt.widget)

        tag_font = QFont(font)
        base_size = font.pointSize()
        if base_size <= 0:
            base_size = font.pixelSize()
            if base_size <= 0:
                base_size = 9
        tag_font.setPointSize(max(base_size - 1, 7))
        tag_fm = QFontMetrics(tag_font)
        tag_width = tag_fm.horizontalAdvance(f" {group_name} ") + 6
        tag_height = tag_fm.height() + 2
        tag_x = text_rect.right() - tag_width - 2
        tag_y = text_rect.center().y() - tag_height // 2

        # Draw rounded pill background
        pill_color = QColor(group_color)
        pill_color.setAlpha(60)
        painter.setBrush(QBrush(pill_color))
        painter.setPen(QPen(group_color, 1))
        painter.setRenderHint(QPainter.Antialiasing)
        from PySide6.QtCore import QRectF
        painter.drawRoundedRect(QRectF(tag_x, tag_y, tag_width, tag_height), 4, 4)

        # Draw tag text
        painter.setFont(tag_font)
        painter.setPen(QPen(group_color))
        painter.drawText(
            int(tag_x), int(tag_y), int(tag_width), int(tag_height),
            Qt.AlignCenter, group_name,
        )

        painter.restore()


# ---------------------------------------------------------------------------
# Manage Groups Dialog
# ---------------------------------------------------------------------------

# Default palette for auto-assigning colors to new groups
_GROUP_COLOR_PALETTE = [
    "#4a90d9", "#d94a4a", "#4ad94a", "#d9a64a", "#9b59b6",
    "#1abc9c", "#e67e22", "#e74c3c", "#3498db", "#2ecc71",
]


class ManageGroupsDialog(QDialog):
    """Dialog for managing source groups (add, rename, recolor, delete)."""

    def __init__(self, config: ProjectConfig, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Groups")
        self.setMinimumWidth(450)
        self.setMinimumHeight(350)
        self._config = config
        # Work on a copy so we can cancel
        self._groups: dict[str, dict] = {k: dict(v) for k, v in config.groups.items()}

        layout = QVBoxLayout(self)

        self.group_list = QListWidget()
        self.group_list.setAlternatingRowColors(True)
        layout.addWidget(self.group_list)

        btn_row = QHBoxLayout()
        self.btn_add = QPushButton("Add Group")
        self.btn_add.clicked.connect(self._add_group)
        btn_row.addWidget(self.btn_add)

        self.btn_rename = QPushButton("Rename")
        self.btn_rename.clicked.connect(self._rename_group)
        btn_row.addWidget(self.btn_rename)

        self.btn_color = QPushButton("Change Color")
        self.btn_color.clicked.connect(self._change_color)
        btn_row.addWidget(self.btn_color)

        self.btn_root = QPushButton("Set Root Dir...")
        self.btn_root.clicked.connect(self._set_root_dir)
        btn_row.addWidget(self.btn_root)

        self.btn_clear_root = QPushButton("Clear Root")
        self.btn_clear_root.clicked.connect(self._clear_root_dir)
        btn_row.addWidget(self.btn_clear_root)

        self.btn_delete = QPushButton("Delete")
        self.btn_delete.clicked.connect(self._delete_group)
        btn_row.addWidget(self.btn_delete)

        layout.addLayout(btn_row)

        # Help text
        root_help = QLabel(
            "{group_root} token resolves to the group's root directory.\n"
            "If unset, {group_root} falls back to {project_root}."
        )
        root_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        layout.addWidget(root_help)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.group_list.currentItemChanged.connect(self._on_group_selected)
        self._rebuild_list()

    def _on_group_selected(self):
        item = self.group_list.currentItem()
        if item:
            name = item.data(Qt.UserRole)
            has_root = bool(self._groups[name].get("root_dir", ""))
            self.btn_clear_root.setEnabled(has_root)
        else:
            self.btn_clear_root.setEnabled(False)

    def _next_color(self) -> str:
        used = {v.get("color", "") for v in self._groups.values()}
        for c in _GROUP_COLOR_PALETTE:
            if c not in used:
                return c
        return _GROUP_COLOR_PALETTE[len(self._groups) % len(_GROUP_COLOR_PALETTE)]

    def _rebuild_list(self):
        self.group_list.clear()
        for name, props in sorted(self._groups.items()):
            color = props.get("color", "#8c8c8c")
            root = props.get("root_dir", "")
            count = sum(1 for s in self._config.watched_sources if s.group == name)
            root_label = f"  \u2502 root: {root}" if root else ""
            item = QListWidgetItem(f"  {name}  ({count} sources){root_label}")
            item.setData(Qt.UserRole, name)
            item.setForeground(QColor(color))
            if root:
                item.setToolTip(f"Root directory: {root}")
            self.group_list.addItem(item)
        self._on_group_selected()

    def _add_group(self):
        name, ok = QInputDialog.getText(self, "New Group", "Group name:")
        if not ok or not name.strip():
            return
        name = name.strip()
        if name in self._groups:
            QMessageBox.warning(self, "Duplicate", f"Group '{name}' already exists.")
            return
        self._groups[name] = {"color": self._next_color()}
        self._rebuild_list()

    def _rename_group(self):
        item = self.group_list.currentItem()
        if not item:
            return
        old_name = item.data(Qt.UserRole)
        new_name, ok = QInputDialog.getText(self, "Rename Group", "New name:", text=old_name)
        if not ok or not new_name.strip() or new_name.strip() == old_name:
            return
        new_name = new_name.strip()
        if new_name in self._groups:
            QMessageBox.warning(self, "Duplicate", f"Group '{new_name}' already exists.")
            return
        self._groups[new_name] = self._groups.pop(old_name)
        # Update sources referencing old name
        for source in self._config.watched_sources:
            if source.group == old_name:
                source.group = new_name
        self._rebuild_list()

    def _change_color(self):
        item = self.group_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        current = QColor(self._groups[name].get("color", "#8c8c8c"))
        color = QColorDialog.getColor(current, self, f"Color for {name}")
        if color.isValid():
            self._groups[name]["color"] = color.name()
            self._rebuild_list()

    def _set_root_dir(self):
        item = self.group_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        current = self._groups[name].get("root_dir", "")
        path = QFileDialog.getExistingDirectory(
            self, f"Root Directory for '{name}'", current or ""
        )
        if path:
            self._groups[name]["root_dir"] = path
        self._rebuild_list()

    def _clear_root_dir(self):
        item = self.group_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        self._groups[name]["root_dir"] = ""
        self._rebuild_list()

    def _delete_group(self):
        item = self.group_list.currentItem()
        if not item:
            return
        name = item.data(Qt.UserRole)
        count = sum(1 for s in self._config.watched_sources if s.group == name)
        if count > 0:
            reply = QMessageBox.question(
                self, "Delete Group",
                f"Group '{name}' has {count} source(s) assigned.\n\n"
                f"Delete the group and unassign those sources?"
            )
            if reply != QMessageBox.Yes:
                return
            for source in self._config.watched_sources:
                if source.group == name:
                    source.group = ""
        del self._groups[name]
        self._rebuild_list()

    def apply_to_config(self, config: ProjectConfig):
        config.groups = dict(self._groups)


# ---------------------------------------------------------------------------
# Update Dialog
# ---------------------------------------------------------------------------

class UpdateDialog(QDialog):
    """Check for updates and optionally download + install."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Check for Updates")
        self.setMinimumSize(500, 350)
        self.resize(550, 420)

        self._release_info = None
        self._check_worker: Optional[UpdateCheckWorker] = None
        self._download_worker: Optional[UpdateDownloadWorker] = None
        self._downloaded_zip: Optional[str] = None
        self._temp_dir: Optional[str] = None

        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(20, 20, 20, 20)

        # Header
        self._header = QLabel(f"<b>{APP_NAME}</b> &mdash; v{APP_VERSION}")
        self._header.setStyleSheet("font-size: 14pt; padding: 4px;")
        layout.addWidget(self._header)

        # Status label
        self._status_label = QLabel("Checking for updates...")
        self._status_label.setWordWrap(True)
        layout.addWidget(self._status_label)

        # Release notes area (hidden initially)
        self._notes_group = QGroupBox("Release Notes")
        notes_layout = QVBoxLayout(self._notes_group)
        self._notes_text = QTextEdit()
        self._notes_text.setReadOnly(True)
        self._notes_text.setMaximumHeight(200)
        notes_layout.addWidget(self._notes_text)
        self._notes_group.setVisible(False)
        layout.addWidget(self._notes_group)

        # Progress bar (hidden initially)
        self._progress_bar = QProgressBar()
        self._progress_bar.setVisible(False)
        layout.addWidget(self._progress_bar)

        # Size label
        self._size_label = QLabel()
        self._size_label.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        self._size_label.setVisible(False)
        layout.addWidget(self._size_label)

        layout.addStretch()

        # Buttons
        self._btn_layout = QHBoxLayout()
        self._action_btn = QPushButton("Download && Install")
        self._action_btn.setVisible(False)
        self._action_btn.clicked.connect(self._on_action_clicked)
        self._btn_layout.addStretch()
        self._btn_layout.addWidget(self._action_btn)

        self._close_btn = QPushButton("Close")
        self._close_btn.clicked.connect(self.reject)
        self._btn_layout.addWidget(self._close_btn)
        layout.addLayout(self._btn_layout)

        # Start checking
        self._start_check()

    # -- Check phase --

    def _start_check(self):
        self._check_worker = UpdateCheckWorker(APP_VERSION, self)
        self._check_worker.finished.connect(self._on_check_finished)
        self._check_worker.error.connect(self._on_check_error)
        self._check_worker.start()

    def _on_check_finished(self, release_info):
        self._check_worker = None
        if release_info is None:
            self._status_label.setText(
                f"<span style='color:#4caf50;'>&#10004;</span> "
                f"You are running the latest version (v{APP_VERSION})."
            )
            self._close_btn.setText("OK")
            self._close_btn.setFocus()
            return

        self._release_info = release_info
        self._status_label.setText(
            f"<b>A new version is available: v{release_info.version}</b>"
        )

        # Show release notes
        if release_info.body:
            self._notes_text.setPlainText(release_info.body)
            self._notes_group.setVisible(True)

        # Show asset size
        size_mb = release_info.asset_size / (1024 * 1024)
        self._size_label.setText(f"Download size: {size_mb:.1f} MB")
        self._size_label.setVisible(True)

        # Show the action button
        from src.lvm.updater import is_frozen
        if is_frozen():
            self._action_btn.setText("Download && Install")
        else:
            self._action_btn.setText("View on GitHub")
        self._action_btn.setVisible(True)

    def _on_check_error(self, msg):
        self._check_worker = None
        self._status_label.setText(f"<span style='color:#e55;'>{msg}</span>")
        self._action_btn.setText("Retry")
        self._action_btn.setVisible(True)

    # -- Action button handler (context-sensitive) --

    def _on_action_clicked(self):
        btn_text = self._action_btn.text()

        if btn_text == "Retry":
            self._action_btn.setVisible(False)
            self._status_label.setText("Checking for updates...")
            self._start_check()

        elif btn_text == "View on GitHub":
            if self._release_info:
                from PySide6.QtGui import QDesktopServices
                QDesktopServices.openUrl(QUrl(self._release_info.html_url))

        elif btn_text.startswith("Download"):
            self._start_download()

        elif btn_text.startswith("Install"):
            self._start_install()

    # -- Download phase --

    def _start_download(self):
        if not self._release_info:
            return

        self._temp_dir = tempfile.mkdtemp(prefix="lvm_update_")
        self._action_btn.setEnabled(False)
        self._action_btn.setText("Downloading...")
        self._progress_bar.setValue(0)
        self._progress_bar.setVisible(True)

        self._download_worker = UpdateDownloadWorker(
            self._release_info, self._temp_dir, self
        )
        self._download_worker.progress.connect(self._on_download_progress)
        self._download_worker.finished.connect(self._on_download_finished)
        self._download_worker.error.connect(self._on_download_error)
        self._download_worker.start()

    def _on_download_progress(self, current, total):
        if total > 0:
            self._progress_bar.setMaximum(total)
            self._progress_bar.setValue(current)
            pct = int(current / total * 100)
            mb_done = current / (1024 * 1024)
            mb_total = total / (1024 * 1024)
            self._size_label.setText(
                f"Downloading: {mb_done:.1f} / {mb_total:.1f} MB ({pct}%)"
            )

    def _on_download_finished(self, zip_path):
        self._download_worker = None
        self._downloaded_zip = zip_path
        self._progress_bar.setVisible(False)
        self._status_label.setText(
            "Update downloaded. Click <b>Install &amp; Restart</b> to apply.\n\n"
            "The application will close, update, and relaunch automatically."
        )
        self._size_label.setVisible(False)
        self._action_btn.setText("Install && Restart")
        self._action_btn.setEnabled(True)

    def _on_download_error(self, msg):
        self._download_worker = None
        self._progress_bar.setVisible(False)
        self._status_label.setText(
            f"<span style='color:#e55;'>Download failed: {msg}</span>"
        )
        self._action_btn.setText("Download && Install")
        self._action_btn.setEnabled(True)

    # -- Install phase --

    def _start_install(self):
        if not self._downloaded_zip or not self._temp_dir:
            return

        from src.lvm.updater import (
            extract_update, create_updater_script, launch_updater, get_install_dir,
        )

        install_dir = get_install_dir()
        if not install_dir:
            QMessageBox.warning(
                self, "Update Error",
                "Cannot determine the installation directory."
            )
            return

        try:
            self._status_label.setText("Extracting update...")
            self._action_btn.setEnabled(False)
            QApplication.processEvents()

            extract_dir = Path(self._temp_dir) / "extracted"
            extracted = extract_update(Path(self._downloaded_zip), extract_dir)

            script = create_updater_script(
                extracted_dir=extracted,
                install_dir=install_dir,
                executable_path=Path(sys.executable),
                pid=os.getpid(),
            )

            launch_updater(script)

            # Quit the application so the updater can replace files
            QApplication.quit()

        except Exception as e:
            self._status_label.setText(
                f"<span style='color:#e55;'>Install failed: {e}</span>"
            )
            self._action_btn.setText("Download && Install")
            self._action_btn.setEnabled(True)

    # -- Cleanup on close --

    def reject(self):
        # Stop any running workers
        if self._download_worker and self._download_worker.isRunning():
            self._download_worker.terminate()
            self._download_worker.wait(2000)
        if self._check_worker and self._check_worker.isRunning():
            self._check_worker.terminate()
            self._check_worker.wait(2000)
        # Clean up temp dir if download wasn't installed
        if self._temp_dir and not self._downloaded_zip:
            import shutil
            try:
                shutil.rmtree(self._temp_dir, ignore_errors=True)
            except Exception:
                pass
        super().reject()


# ---------------------------------------------------------------------------
# About Dialog
# ---------------------------------------------------------------------------

class AboutDialog(QDialog):
    """About dialog showing application info and credits."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"About {APP_NAME}")
        self.setFixedSize(400, 340)

        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(24, 24, 24, 24)

        # Logo
        if LOGO_PATH.exists():
            logo_pixmap = QPixmap(64, 64)
            logo_pixmap.fill(Qt.transparent)
            renderer = QSvgRenderer(str(LOGO_PATH))
            painter = QPainter(logo_pixmap)
            renderer.render(painter)
            painter.end()
            logo_label = QLabel()
            logo_label.setPixmap(logo_pixmap)
            logo_label.setAlignment(Qt.AlignCenter)
            layout.addWidget(logo_label)

        # App name and version
        title = QLabel(f"<h2>{APP_NAME}</h2>")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        version = QLabel(f"Version {APP_VERSION}")
        version.setAlignment(Qt.AlignCenter)
        version.setStyleSheet("color: #8c8c8c;")
        layout.addWidget(version)

        # Separator
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("color: #2a2a2a;")
        layout.addWidget(line)

        # Author info
        info = QLabel(
            '<p style="text-align:center;">'
            '<b>Author:</b> Maris Polis<br>'
            '<a href="https://marispolis.com" style="color:#6699cc;">marispolis.com</a><br>'
            '<a href="mailto:mp@marispolis.com" style="color:#6699cc;">mp@marispolis.com</a><br><br>'
            '<a href="https://www.linkedin.com/in/maris-polis-2bb404191/" style="color:#6699cc;">LinkedIn</a>'
            '&nbsp;&nbsp;|&nbsp;&nbsp;'
            '<a href="https://github.com/polisvfx/LatestVersionManager" style="color:#6699cc;">GitHub</a>'
            '</p>'
        )
        info.setOpenExternalLinks(True)
        info.setAlignment(Qt.AlignCenter)
        layout.addWidget(info)

        layout.addStretch()

        # Close button
        btn_box = QDialogButtonBox(QDialogButtonBox.Close)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)


# ---------------------------------------------------------------------------
# Batch Promote Review Dialog
# ---------------------------------------------------------------------------

class BatchPromoteReviewDialog(QDialog):
    """Review table for batch promotion with per-source checkboxes."""

    def __init__(self, promote_list, source_status, promoters, already_current, skipped, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Batch Promotion Review")
        self.setMinimumSize(900, 500)

        layout = QVBoxLayout(self)

        summary = QLabel(f"<b>{len(promote_list)}</b> source(s) will be promoted")
        summary.setStyleSheet("font-size: 13pt; padding: 4px;")
        layout.addWidget(summary)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["", "Source", "Current", "Target Version", "Files", "Frame Range", "Timecode", "Status"])
        self.tree.setRootIsDecorated(False)
        self.tree.setAlternatingRowColors(True)
        header = self.tree.header()
        header.resizeSection(0, 30)

        for source, version in promote_list:
            promoter = promoters.get(source.name)
            current = promoter.get_current_version() if promoter else None
            current_ver = current.version if current else "---"

            row_status = "normal"
            status_text = "OK"
            if version.frame_range and "gaps detected" in version.frame_range:
                row_status = "red"
                status_text = "GAPS"
            elif current:
                if current.frame_range and version.frame_range and current.frame_range != version.frame_range:
                    row_status = "orange"
                    status_text = "Range changed"
                elif current.start_timecode and version.start_timecode and current.start_timecode != version.start_timecode:
                    row_status = "orange"
                    status_text = "TC changed"

            batch_frame_display = version.frame_range or "---"
            if version.sub_sequences:
                batch_frame_display += f" (+{len(version.sub_sequences)} layer{'s' if len(version.sub_sequences) > 1 else ''})"
            item = QTreeWidgetItem([
                "", source.name, current_ver, version.version_string,
                str(version.file_count), batch_frame_display,
                version.start_timecode or "---", status_text,
            ])
            item.setCheckState(0, Qt.Checked)
            item.setData(0, Qt.UserRole, (source, version))

            color_map = {"normal": "#4ec9a0", "orange": "#ffaa00", "red": "#ff6666"}
            color = QColor(color_map[row_status])
            for col in range(1, 8):
                item.setForeground(col, color)

            self.tree.addTopLevelItem(item)

        layout.addWidget(self.tree)

        if already_current or skipped:
            info_parts = []
            if already_current:
                info_parts.append(f"{len(already_current)} already current")
            if skipped:
                info_parts.append(f"{len(skipped)} skipped")
            info_label = QLabel(", ".join(info_parts))
            info_label.setStyleSheet("color: #8c8c8c; font-size: 11pt; padding: 4px;")
            layout.addWidget(info_label)

        btn_row = QHBoxLayout()
        btn_select_all = QPushButton("Select All")
        btn_select_all.clicked.connect(lambda: self._set_all_checked(True))
        btn_deselect_all = QPushButton("Deselect All")
        btn_deselect_all.clicked.connect(lambda: self._set_all_checked(False))
        btn_row.addWidget(btn_select_all)
        btn_row.addWidget(btn_deselect_all)
        btn_row.addStretch()

        btn_promote = QPushButton("Promote Selected")
        btn_promote.setStyleSheet(
            "QPushButton { background-color: #336699; color: white; padding: 8px 20px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #4d7aae; }"
        )
        btn_promote.clicked.connect(self.accept)
        btn_row.addWidget(btn_promote)

        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)

        layout.addLayout(btn_row)

    def _set_all_checked(self, checked):
        state = Qt.Checked if checked else Qt.Unchecked
        for i in range(self.tree.topLevelItemCount()):
            self.tree.topLevelItem(i).setCheckState(0, state)

    def get_selected(self):
        selected = []
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if item.checkState(0) == Qt.Checked:
                selected.append(item.data(0, Qt.UserRole))
        return selected


# ---------------------------------------------------------------------------
# Obsolete Layer Conflict Dialog
# ---------------------------------------------------------------------------

class ObsoleteLayerDialog(QDialog):
    """Asks the user what to do when the new version is missing layers
    that the previously promoted version had in the latest directory.

    The dialog presents the list of obsolete layers and offers three actions:
    - **Keep**: leave the old layer files in the latest directory
    - **Delete**: remove them (default promotion behaviour)
    - **Skip**: do not promote this source at all

    An "Apply to all" checkbox (on by default) lets the user apply the same
    decision to every subsequent source with a layer conflict in the current
    batch.
    """

    # Result codes matching the three buttons
    KEEP = 1
    DELETE = 2
    SKIP = 3

    def __init__(self, source_name: str, version_string: str,
                 obsolete_layers: list[dict], conflict_count: int,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle("Obsolete Layer Conflict")
        self.setMinimumWidth(480)
        self.choice = self.DELETE  # default

        layout = QVBoxLayout(self)

        # Header
        if conflict_count > 1:
            count_label = QLabel(
                f"<b>{conflict_count}</b> source(s) in this promotion have layer conflicts."
            )
            count_label.setStyleSheet("color: #ffaa00; font-size: 12pt; padding-bottom: 4px;")
            layout.addWidget(count_label)

        header = QLabel(
            f"<b>{source_name}</b> — promoting to <b>{version_string}</b>"
        )
        header.setWordWrap(True)
        layout.addWidget(header)

        desc = QLabel(
            "The new version is missing the following layers that are "
            "currently in the latest directory:"
        )
        desc.setWordWrap(True)
        desc.setStyleSheet("padding: 4px 0;")
        layout.addWidget(desc)

        # Layer list
        layer_list = QTreeWidget()
        layer_list.setHeaderLabels(["Layer", "Files"])
        layer_list.setRootIsDecorated(False)
        layer_list.setAlternatingRowColors(True)
        for layer in obsolete_layers:
            item = QTreeWidgetItem([layer["name"], str(layer["file_count"])])
            item.setForeground(0, QColor("#ffaa00"))
            layer_list.addTopLevelItem(item)
        layer_list.header().setStretchLastSection(False)
        layer_list.header().setSectionResizeMode(0, QHeaderView.Stretch)
        layer_list.header().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        layer_list.setMaximumHeight(min(30 + len(obsolete_layers) * 26, 200))
        layout.addWidget(layer_list)

        # Apply to all checkbox
        self.apply_all_cb = QCheckBox("Apply to all current layer conflicts")
        self.apply_all_cb.setChecked(True)
        layout.addWidget(self.apply_all_cb)

        # Buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()

        btn_keep = QPushButton("Keep")
        btn_keep.setToolTip("Leave old layer files in the latest directory")
        btn_keep.setStyleSheet(
            "QPushButton { padding: 6px 18px; }"
        )
        btn_keep.clicked.connect(lambda: self._finish(self.KEEP))
        btn_row.addWidget(btn_keep)

        btn_delete = QPushButton("Delete")
        btn_delete.setToolTip("Remove obsolete layer files from the latest directory")
        btn_delete.setStyleSheet(
            "QPushButton { background-color: #8b2500; color: white; padding: 6px 18px; "
            "border-radius: 3px; }"
            "QPushButton:hover { background-color: #a83200; }"
        )
        btn_delete.clicked.connect(lambda: self._finish(self.DELETE))
        btn_row.addWidget(btn_delete)

        btn_skip = QPushButton("Skip Promotion")
        btn_skip.setToolTip("Do not promote this source")
        btn_skip.setStyleSheet(
            "QPushButton { padding: 6px 18px; }"
        )
        btn_skip.clicked.connect(lambda: self._finish(self.SKIP))
        btn_row.addWidget(btn_skip)

        layout.addLayout(btn_row)

    @property
    def apply_to_all(self) -> bool:
        return self.apply_all_cb.isChecked()

    def _finish(self, choice):
        self.choice = choice
        self.accept()


# ---------------------------------------------------------------------------
# Main Window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(1100, 700)

        self.config: ProjectConfig = None
        self.config_path: str = None
        self._scanners: dict[str, VersionScanner] = {}
        self._promoters: dict[str, Promoter] = {}
        self._versions_cache: dict[str, list[VersionInfo]] = {}
        self._manual_versions: dict[str, list[VersionInfo]] = {}
        self._current_source: WatchedSource = None
        self._worker: PromoteWorker = None
        self._promoting_source_name: str = None
        self._promoting_version: VersionInfo = None
        self._fallback_original_mode: str = None  # original link_mode before copy fallback
        self._batch_promote_list: list = []
        self._batch_promote_index: int = 0
        self._batch_keep_layers: dict = {}
        self._force_promote: bool = False
        self._target_conflicts: dict = {}
        self._deferred_refresh_results: dict = None  # scan results deferred due to promotion in progress
        self._scan_worker: ScanWorker = None
        self._status_worker: StatusWorker = None
        self._reload_pending: bool = False
        self._rescan_after_cache: bool = False
        self._reload_select_source: str = None  # source to select after async _reload_ui
        self._refresh_select_source: str = None  # source name to re-select after background refresh
        self._thumb_worker: ThumbnailWorker = None
        self._io_executor = ThreadPoolExecutor(max_workers=1)
        self._dirty = False  # True when config has unsaved changes

        # File watcher
        self.watcher = SourceWatcher(self)
        self.watcher.source_changed.connect(self._on_watcher_change)
        self.watcher.watch_status_changed.connect(self._on_watch_status)

        self._settings = QSettings("LatestVersionManager", "LVM")

        self._build_ui()
        self._build_menu()
        self._build_shortcuts()

        # Log handler setup (Feature #19)
        from src.lvm.log_handler import QtLogHandler
        self._log_handler = QtLogHandler(max_buffer=1000)
        self._log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"))
        logging.getLogger().addHandler(self._log_handler)
        self._log_handler.log_record.connect(self._append_log_entry)

        # Allow DEBUG records to reach the Qt log handler; keep console at INFO
        logging.getLogger().setLevel(logging.DEBUG)
        for h in logging.getLogger().handlers:
            if isinstance(h, logging.StreamHandler) and h is not self._log_handler:
                h.setLevel(logging.INFO)

        self._restore_state()

    # --- UI Construction ---

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(8, 8, 8, 8)

        # Toolbar
        toolbar = QToolBar()
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(16, 16))

        self.btn_project_settings = QPushButton("Project Settings")
        self.btn_project_settings.clicked.connect(self._open_project_settings)
        self.btn_project_settings.setEnabled(False)
        toolbar.addWidget(self.btn_project_settings)

        toolbar.addSeparator()

        self.btn_discover = QPushButton("Discover Versions...")
        self.btn_discover.clicked.connect(self._open_discover)
        toolbar.addWidget(self.btn_discover)

        self.btn_manage_groups = QPushButton("Manage Groups")
        self.btn_manage_groups.clicked.connect(self._open_manage_groups)
        self.btn_manage_groups.setEnabled(False)
        toolbar.addWidget(self.btn_manage_groups)

        toolbar.addSeparator()

        self.btn_refresh = QPushButton("Refresh All")
        self.btn_refresh.clicked.connect(self._refresh_all)
        self.btn_refresh.setEnabled(False)
        toolbar.addWidget(self.btn_refresh)

        self.watch_toggle = QPushButton("Start Watching")
        self.watch_toggle.setCheckable(True)
        self.watch_toggle.clicked.connect(self._toggle_watcher)
        self.watch_toggle.setEnabled(False)
        toolbar.addWidget(self.watch_toggle)

        self.auto_promote_cb = QCheckBox("Auto-Promote")
        self.auto_promote_cb.setToolTip(
            "Automatically promote new versions when detected,\n"
            "only if frame range matches the last promoted version."
        )
        self.auto_promote_cb.setEnabled(False)
        toolbar.addWidget(self.auto_promote_cb)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        toolbar.addWidget(spacer)

        self.project_label = QLabel("No project loaded")
        self.project_label.setStyleSheet("color: #8c8c8c; font-style: italic;")
        toolbar.addWidget(self.project_label)

        main_layout.addWidget(toolbar)

        # Main splitter: left (sources) | right (versions + history)
        splitter = QSplitter(Qt.Horizontal)

        # --- Left: Source list ---
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(4)

        # Header row with label + filter
        header_row = QHBoxLayout()
        header_row.setSpacing(6)
        sources_label = QLabel("Sources")
        sources_label.setStyleSheet("font-weight: bold; font-size: 13pt;")
        header_row.addWidget(sources_label)
        header_row.addStretch()
        self.source_filter = QComboBox()
        self.source_filter.addItems(["All", "Newer Available", "Not on Highest", "Stale"])
        self.source_filter.setFixedWidth(130)
        self.source_filter.setToolTip("Filter sources by version status")
        self.source_filter.currentIndexChanged.connect(self._apply_source_filter)
        header_row.addWidget(self.source_filter)
        left_layout.addLayout(header_row)

        # Search box
        self.source_search = QLineEdit()
        self.source_search.setPlaceholderText("Search sources...")
        self.source_search.setClearButtonEnabled(True)
        self.source_search.textChanged.connect(self._apply_source_filter)
        left_layout.addWidget(self.source_search)

        # Group-by checkbox
        self.group_by_check = QCheckBox("Group by groups")
        self.group_by_check.setChecked(False)
        self.group_by_check.toggled.connect(self._apply_source_filter)
        left_layout.addWidget(self.group_by_check)

        # Source list column definitions
        # key → (header label, column index)
        self._source_col_keys = ["name", "group", "version", "layers", "frames", "filetype", "added_on", "last_promoted", "status"]
        self._source_col_labels = {
            "name": "Name", "group": "Group", "version": "Version", "layers": "Layers",
            "frames": "Frames", "filetype": "Filetype",
            "added_on": "Added On", "last_promoted": "Last Promoted", "status": "Status",
        }

        self.source_list = QTreeWidget()
        self.source_list.setHeaderLabels([self._source_col_labels[k] for k in self._source_col_keys])
        self.source_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.source_list.setRootIsDecorated(False)
        self.source_list.setAllColumnsShowFocus(True)
        self.source_list.setSortingEnabled(True)
        self.source_list.header().setSortIndicatorShown(True)
        self.source_list.header().setSectionsClickable(True)
        self.source_list.sortByColumn(0, Qt.AscendingOrder)
        self.source_list.currentItemChanged.connect(self._on_source_item_changed)
        self.source_list.itemSelectionChanged.connect(self._on_source_selection_changed)
        self.source_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.source_list.customContextMenuRequested.connect(self._source_context_menu)
        # Header context menu for column visibility
        self.source_list.header().setContextMenuPolicy(Qt.CustomContextMenu)
        self.source_list.header().customContextMenuRequested.connect(self._source_header_context_menu)
        # Default column widths
        self.source_list.header().resizeSection(0, 200)  # Name
        self.source_list.header().resizeSection(1, 100)  # Group
        self.source_list.header().resizeSection(2, 70)   # Version
        self.source_list.header().resizeSection(3, 55)   # Layers
        self.source_list.header().resizeSection(4, 65)   # Frames
        self.source_list.header().resizeSection(5, 65)   # Filetype
        self.source_list.header().resizeSection(6, 140)  # Added On
        self.source_list.header().resizeSection(7, 140)  # Last Promoted
        self.source_list.header().resizeSection(8, 100)  # Status
        self.source_list.header().setStretchLastSection(False)
        # All columns are user-resizable (Interactive); last column stretches to fill remaining space
        self.source_list.header().setSectionResizeMode(QHeaderView.Interactive)
        self.source_list.header().setSectionResizeMode(len(self._source_col_keys) - 1, QHeaderView.Stretch)
        left_layout.addWidget(self.source_list)

        # Promote All / Promote Selected button(s)
        promote_style_main = (
            "QPushButton { background-color: #336699; color: white; padding: 8px 16px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #4d7aae; }"
            "QPushButton:disabled { background-color: #2a2a2a; color: #8c8c8c; }"
        )
        promote_style_secondary = (
            "QPushButton { background-color: #336699; color: white; padding: 8px 10px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #4d7aae; }"
            "QPushButton:disabled { background-color: #2a2a2a; color: #8c8c8c; }"
        )
        self.promote_container = QWidget()
        promote_layout = QHBoxLayout(self.promote_container)
        promote_layout.setContentsMargins(0, 0, 0, 0)
        promote_layout.setSpacing(2)

        self.btn_promote_all = QPushButton("Promote All to Latest")
        self.btn_promote_all.setStyleSheet(promote_style_main)
        self.btn_promote_all.setEnabled(False)
        self.btn_promote_all.setToolTip(
            "Promotes sources that are not on their highest version.\n"
            "Hold Shift to force re-promote all sources."
        )
        self.btn_promote_all.clicked.connect(self._promote_all_or_selected)
        promote_layout.addWidget(self.btn_promote_all, stretch=1)

        self.btn_promote_split_all = QPushButton("All")
        self.btn_promote_split_all.setStyleSheet(promote_style_secondary)
        self.btn_promote_split_all.setToolTip(
            "Promote all sources to latest.\n"
            "Hold Shift to force re-promote all sources."
        )
        self.btn_promote_split_all.clicked.connect(self._promote_all_forced)
        self.btn_promote_split_all.setVisible(False)
        promote_layout.addWidget(self.btn_promote_split_all, stretch=0)

        left_layout.addWidget(self.promote_container)

        splitter.addWidget(left_panel)

        # --- Right: Versions + details ---
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        # Current version banner
        self.current_banner = QFrame()
        self.current_banner.setFrameShape(QFrame.StyledPanel)
        self.current_banner.setStyleSheet(
            "QFrame { background-color: #1a2a3a; border: 1px solid #336699; border-radius: 4px; padding: 8px; }"
        )
        banner_layout = QHBoxLayout(self.current_banner)
        banner_layout.setContentsMargins(12, 8, 12, 8)
        self.current_label = QLabel("No version loaded")
        self.current_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #4ec9a0;")
        banner_layout.addWidget(self.current_label)
        self.integrity_label = QLabel("")
        self.integrity_label.setStyleSheet("font-size: 11pt; color: #8c8c8c;")
        self.integrity_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        banner_layout.addWidget(self.integrity_label)
        right_layout.addWidget(self.current_banner)

        # Version + History vertical split
        self.ver_hist_splitter = QSplitter(Qt.Vertical)

        # Version tree
        ver_group = QGroupBox("Available Versions")
        ver_layout = QVBoxLayout(ver_group)

        self.version_tree = VersionTreeWidget()
        self.version_tree.setHeaderLabels(["Version", "Date", "Files", "Size", "Frame Range", "Timecode", "Path"])
        self.version_tree.setRootIsDecorated(False)
        self.version_tree.setAlternatingRowColors(True)
        self.version_tree.setSortingEnabled(True)
        self.version_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.version_tree.customContextMenuRequested.connect(self._version_context_menu)
        self.version_tree.files_dropped.connect(self._handle_version_drop)
        header = self.version_tree.header()
        header.setStretchLastSection(True)
        header.resizeSection(0, 80)
        header.resizeSection(1, 70)
        header.resizeSection(2, 60)
        header.resizeSection(3, 80)
        header.resizeSection(4, 160)
        header.resizeSection(5, 110)

        # Thumbnail/Preview panel (Feature #7) — collapsible, collapsed by default
        self._ver_content_splitter = QSplitter(Qt.Horizontal)
        self._ver_content_splitter.addWidget(self.version_tree)

        # Preview panel container with toggle button
        preview_frame = QWidget()
        preview_layout = QVBoxLayout(preview_frame)
        preview_layout.setContentsMargins(0, 0, 0, 0)

        self._preview_toggle = QPushButton("\u25b6 Preview")
        self._preview_toggle.setCheckable(True)
        self._preview_toggle.setChecked(False)
        self._preview_toggle.setFixedHeight(24)
        self._preview_toggle.setStyleSheet(
            "QPushButton { text-align: left; border: none; padding-left: 4px; }"
            " QPushButton:checked { font-weight: bold; }"
        )
        self._preview_toggle.toggled.connect(self._toggle_preview_panel)

        self.thumbnail_label = QLabel()
        self.thumbnail_label.setAlignment(Qt.AlignCenter)
        self.thumbnail_label.setMinimumWidth(160)
        self.thumbnail_label.setMaximumWidth(320)
        self.thumbnail_label.setStyleSheet("QLabel { background-color: #121212; border: 1px solid #2a2a2a; border-radius: 4px; padding: 4px; }")
        self.thumbnail_label.setText("No Preview")
        self.thumbnail_label.setVisible(False)  # Hidden by default

        preview_layout.addWidget(self._preview_toggle)
        preview_layout.addWidget(self.thumbnail_label, 1)

        self._ver_content_splitter.addWidget(preview_frame)
        self._ver_content_splitter.setCollapsible(0, False)  # Version tree not collapsible
        self._ver_content_splitter.setCollapsible(1, True)   # Preview panel collapsible
        self._ver_content_splitter.setSizes([600, 24])       # Only toggle button width

        ver_layout.addWidget(self._ver_content_splitter)

        # Connect version selection for thumbnail (lazy — only loads when visible)
        self.version_tree.currentItemChanged.connect(self._on_version_selected_thumbnail)

        # Promote controls
        promote_row = QHBoxLayout()
        self.btn_import_version = QPushButton("Import Version...")
        self.btn_import_version.setEnabled(False)
        self.btn_import_version.clicked.connect(self._import_version)
        self.btn_refresh_versions = QPushButton("Refresh Versions")
        self.btn_refresh_versions.setEnabled(False)
        self.btn_refresh_versions.clicked.connect(self._refresh_current_source)
        self.btn_promote = QPushButton("Promote Selected to Latest")
        self.btn_promote.setStyleSheet(self._PROMOTE_STYLE)
        self.btn_promote.setEnabled(False)
        self.btn_promote.clicked.connect(self._promote_selected)
        promote_row.addWidget(self.btn_import_version)
        promote_row.addWidget(self.btn_refresh_versions)
        promote_row.addStretch()
        promote_row.addWidget(self.btn_promote)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        self.btn_cancel_promote = QPushButton("Cancel")
        self.btn_cancel_promote.setVisible(False)
        self.btn_cancel_promote.setFixedWidth(70)
        self.btn_cancel_promote.clicked.connect(self._cancel_promotion)

        progress_row = QHBoxLayout()
        progress_row.addWidget(self.progress_bar)
        progress_row.addWidget(self.btn_cancel_promote)

        ver_layout.addLayout(promote_row)
        ver_layout.addLayout(progress_row)

        self.ver_hist_splitter.addWidget(ver_group)

        # History tree
        hist_group = QGroupBox("Promotion History")
        hist_layout = QVBoxLayout(hist_group)

        self.history_tree = QTreeWidget()
        self.history_tree.setHeaderLabels(["Date/Time", "Version", "By", "Frame Range", "Timecode", "Files"])
        self.history_tree.setRootIsDecorated(False)
        self.history_tree.setAlternatingRowColors(True)
        h_header = self.history_tree.header()
        h_header.setStretchLastSection(True)
        h_header.resizeSection(0, 170)
        h_header.resizeSection(1, 70)
        h_header.resizeSection(2, 80)
        h_header.resizeSection(3, 140)
        h_header.resizeSection(4, 110)

        # Revert + Export buttons
        revert_row = QHBoxLayout()
        self.btn_export_report = QPushButton("Export Report...")
        self.btn_export_report.clicked.connect(self._export_report)
        revert_row.addWidget(self.btn_export_report)
        revert_row.addStretch()
        self.btn_revert = QPushButton("Revert to Selected")
        self.btn_revert.setEnabled(False)
        self.btn_revert.clicked.connect(self._revert_selected)
        revert_row.addWidget(self.btn_revert)

        hist_layout.addWidget(self.history_tree)
        hist_layout.addLayout(revert_row)

        self.ver_hist_splitter.addWidget(hist_group)
        self.ver_hist_splitter.setSizes([400, 200])

        right_layout.addWidget(self.ver_hist_splitter)
        splitter.addWidget(right_panel)

        splitter.setSizes([250, 850])
        main_layout.addWidget(splitter)

        # Log panel (Feature #19)
        self.log_dock = QDockWidget("Log", self)
        self.log_dock.setAllowedAreas(Qt.BottomDockWidgetArea)
        log_widget = QWidget()
        log_layout = QVBoxLayout(log_widget)
        log_layout.setContentsMargins(4, 4, 4, 4)

        log_controls = QHBoxLayout()
        self.log_level_filter = QComboBox()
        self.log_level_filter.addItems(["ALL", "DEBUG", "INFO", "WARNING", "ERROR"])
        self.log_level_filter.setCurrentText("INFO")
        self.log_level_filter.currentTextChanged.connect(self._filter_log)
        log_controls.addWidget(QLabel("Level:"))
        log_controls.addWidget(self.log_level_filter)
        log_controls.addStretch()
        btn_clear_log = QPushButton("Clear")
        btn_clear_log.clicked.connect(self._clear_log)
        log_controls.addWidget(btn_clear_log)
        btn_copy_log = QPushButton("Copy")
        btn_copy_log.clicked.connect(self._copy_log)
        log_controls.addWidget(btn_copy_log)
        log_layout.addLayout(log_controls)

        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumBlockCount(1000)
        self.log_text.setStyleSheet("QPlainTextEdit { font-family: 'Consolas', 'Monaco', monospace; font-size: 11pt; }")
        log_layout.addWidget(self.log_text)

        self.log_dock.setWidget(log_widget)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.log_dock)
        self.log_dock.setVisible(False)

        # Status bar
        self._scan_indicator = QLabel("")
        self._scan_indicator.setStyleSheet("color: #8c8c8c; font-size: 11pt; margin-right: 8px;")
        self.statusBar().addPermanentWidget(self._scan_indicator)
        self.statusBar().showMessage("Ready")

    def _build_menu(self):
        menubar = self.menuBar()

        file_menu = menubar.addMenu("&File")

        new_action = QAction("&New Project...", self)
        new_action.setShortcut(QKeySequence.StandardKey.New)
        new_action.triggered.connect(self._new_project)
        file_menu.addAction(new_action)

        open_action = QAction("&Open Project...", self)
        open_action.setShortcut(QKeySequence.StandardKey.Open)
        open_action.triggered.connect(self._open_project)
        file_menu.addAction(open_action)

        save_action = QAction("&Save Project", self)
        save_action.setShortcut(QKeySequence.StandardKey.Save)
        save_action.triggered.connect(self._save_project)
        file_menu.addAction(save_action)

        save_as_action = QAction("Save Project &As...", self)
        save_as_action.setShortcut(QKeySequence.StandardKey.SaveAs)
        save_as_action.triggered.connect(self._save_project_as)
        file_menu.addAction(save_as_action)

        file_menu.addSeparator()

        settings_action = QAction("Project &Settings...", self)
        settings_action.setShortcut(QKeySequence("Ctrl+P"))
        settings_action.triggered.connect(self._open_project_settings)
        file_menu.addAction(settings_action)

        file_menu.addSeparator()

        recent_menu = file_menu.addMenu("Recent Projects")
        self._populate_recent_menu(recent_menu)

        file_menu.addSeparator()

        quit_action = QAction("&Quit", self)
        quit_action.setShortcut(QKeySequence("Ctrl+Q"))
        quit_action.triggered.connect(self.close)
        file_menu.addAction(quit_action)

        tools_menu = menubar.addMenu("&Tools")

        discover_action = QAction("&Discover Versions...", self)
        discover_action.setShortcut(QKeySequence("Ctrl+Shift+D"))
        discover_action.triggered.connect(self._open_discover)
        tools_menu.addAction(discover_action)

        manage_groups_action = QAction("&Manage Groups...", self)
        manage_groups_action.setShortcut(QKeySequence("Ctrl+G"))
        manage_groups_action.triggered.connect(self._open_manage_groups)
        tools_menu.addAction(manage_groups_action)

        tools_menu.addSeparator()

        export_report_action = QAction("&Export Report...", self)
        export_report_action.triggered.connect(self._export_report)
        tools_menu.addAction(export_report_action)

        validate_action = QAction("&Validate Config", self)
        validate_action.triggered.connect(self._validate_config)
        tools_menu.addAction(validate_action)

        view_menu = menubar.addMenu("&View")
        self.log_dock_action = self.log_dock.toggleViewAction()
        self.log_dock_action.setShortcut(QKeySequence("Ctrl+L"))
        view_menu.addAction(self.log_dock_action)

        source_menu = menubar.addMenu("&Sources")

        add_source_action = QAction("&Add Source...", self)
        add_source_action.triggered.connect(self._add_source)
        source_menu.addAction(add_source_action)

        refresh_action = QAction("&Refresh All", self)
        refresh_action.setShortcut(QKeySequence("Ctrl+R"))
        refresh_action.triggered.connect(self._refresh_all)
        source_menu.addAction(refresh_action)

        source_menu.addSeparator()

        promote_all_action = QAction("&Promote All to Latest", self)
        promote_all_action.setShortcut(QKeySequence("Ctrl+Alt+Up"))
        promote_all_action.triggered.connect(self._promote_all_or_selected)
        source_menu.addAction(promote_all_action)

        help_menu = menubar.addMenu("&Help")

        update_action = QAction("Check for &Updates...", self)
        update_action.triggered.connect(self._check_for_updates)
        help_menu.addAction(update_action)

        help_menu.addSeparator()

        about_action = QAction("&About...", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

    def _build_shortcuts(self):
        """Register keyboard shortcuts (Feature #10)."""
        # F5: Refresh all
        f5 = QAction(self)
        f5.setShortcut(QKeySequence("F5"))
        f5.triggered.connect(self._refresh_all)
        self.addAction(f5)

        # Ctrl+F: Focus search
        search_sc = QAction(self)
        search_sc.setShortcut(QKeySequence("Ctrl+F"))
        search_sc.triggered.connect(lambda: self.source_search.setFocus())
        self.addAction(search_sc)

        # Escape: Cancel promotion
        esc = QAction(self)
        esc.setShortcut(QKeySequence(Qt.Key_Escape))
        esc.triggered.connect(self._cancel_promotion)
        self.addAction(esc)

        # Delete: Remove selected sources (only when source_list focused)
        delete_sc = QAction(self)
        delete_sc.setShortcut(QKeySequence(Qt.Key_Delete))
        delete_sc.triggered.connect(self._delete_selected_sources)
        self.addAction(delete_sc)

        # Ctrl+D: Deselect all sources
        deselect_sc = QAction(self)
        deselect_sc.setShortcut(QKeySequence("Ctrl+D"))
        deselect_sc.triggered.connect(self.source_list.clearSelection)
        self.addAction(deselect_sc)

    def _promote_selected_if_version_focused(self):
        if self.version_tree.hasFocus() and self.version_tree.selectedItems():
            self._promote_selected()

    def _reveal_current_source(self):
        if self._current_source and self._current_source.source_dir:
            reveal_in_file_browser(self._current_source.source_dir)

    def _delete_selected_sources(self):
        if not self.source_list.hasFocus():
            return
        selected_items = self.source_list.selectedItems()
        if not selected_items:
            return
        # Use existing remove logic
        names = [item.data(0, Qt.UserRole) for item in selected_items]
        if len(names) == 1:
            reply = QMessageBox.question(self, "Remove Source", f"Remove '{names[0]}'?")
        else:
            reply = QMessageBox.question(self, "Remove Sources", f"Remove {len(names)} source(s)?")
        if reply != QMessageBox.Yes:
            return
        self.config.watched_sources = [s for s in self.config.watched_sources if s.name not in names]
        self._mark_dirty()
        if self.config_path:
            self._save_project()
        self._reload_ui()

    def _check_for_updates(self):
        dlg = UpdateDialog(self)
        dlg.exec()

    def _show_about(self):
        dlg = AboutDialog(self)
        dlg.exec()

    def _populate_recent_menu(self, menu: QMenu):
        recents = self._settings.value("recent_projects", [])
        if not recents:
            action = menu.addAction("(No recent projects)")
            action.setEnabled(False)
            return
        for path in recents[:10]:
            action = menu.addAction(path)
            action.triggered.connect(lambda checked, p=path: self._load_project(p))

    # --- Project management ---

    def _new_project(self):
        try:
            dlg = ProjectSetupDialog(parent=self)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to open New Project dialog:\n{e}")
            return
        if dlg.exec() != QDialog.Accepted:
            return

        info = dlg.get_project_info()
        project_root = info.get("project_root", "")
        if not project_root:
            QMessageBox.warning(self, "Missing Directory", "Please specify a project root directory.")
            return

        try:
            output_path = create_project(
                project_name=info["project_name"],
                project_dir=project_root,
                project_root=project_root,
                save_dir=info.get("save_dir", ""),
                name_whitelist=info["name_whitelist"],
                name_blacklist=info["name_blacklist"],
                task_tokens=info.get("task_tokens", []),
            )
            self._load_project(output_path)

            # Apply template if selected (Feature #17)
            template_path = info.get("template_path", "")
            if template_path and self.config:
                from src.lvm.templates import load_template, apply_template
                template_data = load_template(template_path)
                apply_template(self.config, template_data)
                if self.config_path:
                    self._save_project()
                self._reload_ui()

            self.statusBar().showMessage(f"Created project: {output_path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to create project:\n{e}")

    def _open_project(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open Project Config", "",
            "LVM Config (*.json);;All Files (*)"
        )
        if path:
            self._load_project(path)

    def _load_project(self, path: str):
        try:
            self.config = load_config(path)
            self.config_path = path
            self._add_to_recent(path)

            # Try cache-first for fast startup
            from src.lvm.scan_cache import load_cache
            cached = load_cache(path, self.config.watched_sources)

            if cached:
                # Show cached data quickly, then rescan in background.
                # _reload_ui(cached_versions=...) is async; the background
                # rescan is triggered in _on_reload_status_complete via
                # _trigger_background_rescan_after_cache flag.
                self._rescan_after_cache = True
                self._reload_ui(cached_versions=cached)
            else:
                self._reload_ui()

            self.project_label.setText(f"{self.config.project_name}")
            self.project_label.setStyleSheet("color: #c0c0c0; font-weight: bold;")
            self._dirty = False
            self._update_title()
            self.statusBar().showMessage(f"Loaded: {path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load config:\n{e}")

    def _mark_dirty(self):
        """Mark the project as having unsaved changes."""
        if not self._dirty:
            self._dirty = True
            self._update_title()

    def _update_title(self):
        """Update window title with project name and dirty indicator."""
        title = APP_NAME
        if self.config:
            title = f"{self.config.project_name} - {APP_NAME}"
        if self._dirty:
            title = f"* {title}"
        self.setWindowTitle(title)

    def _save_project(self):
        if not self.config:
            return
        if not self.config_path:
            self._save_project_as()
            return
        # Snapshot config data on the main thread, write in background
        config_snapshot = self.config.to_dict()
        config_path = self.config_path
        project_dir = str(Path(config_path).resolve().parent)

        def _write():
            try:
                # Relativise paths (same logic as save_config but on snapshot)
                for source_data in config_snapshot.get("watched_sources", []):
                    sd = source_data.get("source_dir", "")
                    if sd and Path(sd).is_absolute():
                        source_data["source_dir"] = make_relative(sd, project_dir)
                    lt = source_data.get("latest_target", "")
                    if lt and Path(lt).is_absolute():
                        source_data["latest_target"] = make_relative(lt, project_dir)
                pr = config_snapshot.get("project_root", "")
                if pr and Path(pr).is_absolute():
                    config_snapshot["project_root"] = make_relative(pr, project_dir)
                for grp_props in config_snapshot.get("groups", {}).values():
                    rd = grp_props.get("root_dir", "")
                    if rd and Path(rd).is_absolute():
                        grp_props["root_dir"] = make_relative(rd, project_dir)

                p = Path(config_path).resolve()
                p.parent.mkdir(parents=True, exist_ok=True)
                import json as _json
                with open(p, "w", encoding="utf-8") as f:
                    _json.dump(config_snapshot, f, indent=2, ensure_ascii=False)
                self.config.project_dir = project_dir
                logger.info(f"Saved project config to {p}")
            except Exception as e:
                logger.error(f"Failed to save project: {e}")

        self._io_executor.submit(_write)
        self._dirty = False
        self._update_title()
        self.statusBar().showMessage(f"Saved: {self.config_path}")

    def _save_project_as(self):
        if not self.config:
            return
        from src.lvm.config import _sanitize_filename
        default_name = f"{_sanitize_filename(self.config.project_name)}_lvm.json"
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Project Config", default_name,
            "LVM Config (*.json);;All Files (*)"
        )
        if path:
            self.config_path = path
            self._save_project()
            self._add_to_recent(path)

    def _add_to_recent(self, path: str):
        recents = self._settings.value("recent_projects", [])
        if not isinstance(recents, list):
            recents = []
        if path in recents:
            recents.remove(path)
        recents.insert(0, path)
        self._settings.setValue("recent_projects", recents[:10])

    def _open_project_settings(self):
        if not self.config:
            QMessageBox.information(self, "No Project", "Open or create a project first.")
            return
        dlg = ProjectSettingsDialog(self.config, selected_source=self._current_source, parent=self)
        if dlg.exec() == QDialog.Accepted:
            dlg.apply_to_config(self.config)
            self._mark_dirty()
            self.project_label.setText(f"{self.config.project_name}")
            if self.config_path:
                self._save_project()
            self._refresh_all_with_selection()
            self.statusBar().showMessage("Project settings updated")

    def _open_discover(self):
        dlg = DiscoveryDialog(config=self.config, parent=self)
        dlg.sources_added.connect(self._on_sources_added_from_discover)
        dlg.exec()

    def _on_sources_added_from_discover(self, count: int):
        """Called when DiscoveryDialog adds sources to the project."""
        if self.config_path:
            self._save_project()
        self._reload_ui()
        self.statusBar().showMessage(f"Added {count} source(s) from discovery")

    # --- Source management ---

    def _add_source(self):
        if not self.config:
            return
        draft = None
        while True:
            dlg = SourceDialog(source=draft, project_config=self.config, parent=self)
            if dlg.exec() != QDialog.Accepted:
                return
            draft = dlg.get_source()
            existing_names = [s.name for s in self.config.watched_sources]
            if draft.name in existing_names:
                QMessageBox.warning(
                    self, "Duplicate Name",
                    f"A source named '{draft.name}' already exists.\n"
                    f"Please choose a different name.",
                )
                continue
            break
        if not draft.added_at:
            from datetime import datetime
            draft.added_at = datetime.now().isoformat(timespec="seconds")
        self.config.watched_sources.append(draft)
        self._mark_dirty()
        if self.config_path:
            self._save_project()
        self._reload_ui()
        self.statusBar().showMessage(f"Added source: {draft.name}")

    def _edit_source(self, index: int):
        if not self.config or index < 0 or index >= len(self.config.watched_sources):
            return
        draft = self.config.watched_sources[index]
        while True:
            dlg = SourceDialog(source=draft, project_config=self.config, parent=self)
            if dlg.exec() != QDialog.Accepted:
                return
            draft = dlg.get_source()
            existing_names = [
                s.name for i, s in enumerate(self.config.watched_sources) if i != index
            ]
            if draft.name in existing_names:
                QMessageBox.warning(
                    self, "Duplicate Name",
                    f"A source named '{draft.name}' already exists.\n"
                    f"Please choose a different name.",
                )
                continue
            break
        # Preserve original added_at timestamp
        original = self.config.watched_sources[index]
        if original.added_at and not draft.added_at:
            draft.added_at = original.added_at
        self.config.watched_sources[index] = draft
        self._mark_dirty()
        if self.config_path:
            self._save_project()
        self._reload_ui()

    def _remove_source(self, index: int):
        self._remove_sources([index])

    def _remove_sources(self, indices: list):
        if not self.config:
            return
        indices = [i for i in indices if 0 <= i < len(self.config.watched_sources)]
        if not indices:
            return
        if len(indices) == 1:
            source = self.config.watched_sources[indices[0]]
            msg = f"Remove '{source.name}' from this project?\n\nThis does NOT delete any files on disk."
            reply = QMessageBox.question(self, "Remove Source", msg)
            confirmed = reply == QMessageBox.Yes
        else:
            # Custom dialog with scrollable list for many sources
            dlg = QDialog(self)
            dlg.setWindowTitle("Remove Sources")
            dlg.setMinimumWidth(400)
            layout = QVBoxLayout(dlg)

            layout.addWidget(QLabel(f"Remove {len(indices)} sources from this project?"))

            source_list = QListWidget()
            source_list.setSelectionMode(QAbstractItemView.NoSelection)
            source_list.setMaximumHeight(300)
            for i in indices:
                source_list.addItem(self.config.watched_sources[i].name)
            layout.addWidget(source_list)

            layout.addWidget(QLabel("This does NOT delete any files on disk."))

            buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            buttons.accepted.connect(dlg.accept)
            buttons.rejected.connect(dlg.reject)
            layout.addWidget(buttons)

            confirmed = dlg.exec() == QDialog.Accepted
        if confirmed:
            for i in sorted(indices, reverse=True):
                self.config.watched_sources.pop(i)
            self._mark_dirty()
            if self.config_path:
                self._save_project()
            self._reload_ui()

    def _resolve_source_index_from_item(self, item) -> int:
        """Map a QTreeWidgetItem to the actual index in config.watched_sources."""
        if not item:
            return -1
        source_name = item.data(0, Qt.UserRole)
        if not source_name:
            return -1
        for i, s in enumerate(self.config.watched_sources):
            if s.name == source_name:
                return i
        return -1

    def _source_context_menu(self, pos):
        selected_rows = self.source_list.selectedItems()
        if not selected_rows:
            return
        selected_indices = []
        for item in selected_rows:
            idx = self._resolve_source_index_from_item(item)
            if idx >= 0:
                selected_indices.append(idx)
        if not selected_indices:
            return
        menu = QMenu(self)
        if len(selected_indices) == 1:
            index = selected_indices[0]
            menu.addAction("Edit Source...", lambda: self._edit_source(index))
            menu.addAction("Remove Source", lambda: self._remove_sources(selected_indices))
        else:
            menu.addAction(f"Remove {len(selected_indices)} Sources", lambda: self._remove_sources(selected_indices))

        # Refresh selected
        menu.addSeparator()
        if len(selected_indices) == 1:
            menu.addAction("Refresh Source", lambda: self._refresh_selected_sources(selected_indices))
        else:
            menu.addAction(f"Refresh {len(selected_indices)} Sources", lambda: self._refresh_selected_sources(selected_indices))

        # Reveal in file browser
        menu.addSeparator()
        if len(selected_indices) == 1:
            source = self.config.watched_sources[selected_indices[0]]
            menu.addAction(
                f"{_REVEAL_LABEL} — Source",
                lambda: reveal_in_file_browser(source.source_dir),
            )
            if source.latest_target:
                menu.addAction(
                    f"{_REVEAL_LABEL} — Latest Target",
                    lambda: reveal_in_file_browser(source.latest_target),
                )

        # Group submenu
        menu.addSeparator()
        group_menu = menu.addMenu("Group")
        sources = [self.config.watched_sources[i] for i in selected_indices]
        current_groups = set(s.group for s in sources)
        single_group = current_groups.pop() if len(current_groups) == 1 else None

        if self.config.groups:
            for grp_name in sorted(self.config.groups.keys()):
                color = self.config.groups[grp_name].get("color", "#8c8c8c")
                action = group_menu.addAction(grp_name)
                action.setCheckable(True)
                action.setChecked(single_group == grp_name)
                # Colored icon via stylesheet workaround: set foreground
                action.triggered.connect(
                    lambda checked, g=grp_name: self._assign_group(selected_indices, g if checked else "")
                )
            group_menu.addSeparator()

        group_menu.addAction("Assign to New Group...", lambda: self._assign_new_group(selected_indices))
        if any(s.group for s in sources):
            group_menu.addAction("Remove from Group", lambda: self._assign_group(selected_indices, ""))

        menu.exec(self.source_list.mapToGlobal(pos))

    def _version_context_menu(self, pos):
        items = self.version_tree.selectedItems()
        if not items or not self._current_source:
            return
        version: VersionInfo = items[0].data(0, Qt.UserRole)
        source = self._current_source
        promoter = self._promoters.get(source.name) if source else None
        current = promoter.get_current_version() if promoter else None
        is_promoted = current and version.version_string == current.version

        menu = QMenu(self)

        # Reveal actions
        menu.addAction(
            f"{_REVEAL_LABEL} — Version",
            lambda: reveal_in_file_browser(version.source_path),
        )
        if source.latest_target:
            menu.addAction(
                f"{_REVEAL_LABEL} — Latest Target",
                lambda: reveal_in_file_browser(source.latest_target),
            )

        # Promote
        menu.addSeparator()
        if is_promoted:
            promote_action = menu.addAction("Keep This Version", self._promote_selected)
        else:
            promote_action = menu.addAction("Promote This Version", self._promote_selected)

        # Copy actions
        menu.addSeparator()
        menu.addAction("Copy Version Path", lambda: QApplication.clipboard().setText(version.source_path))
        if source.latest_target:
            menu.addAction("Copy Latest Target Path", lambda: QApplication.clipboard().setText(source.latest_target))
        menu.addAction("Copy Version Info", lambda: self._copy_version_info(version, source))

        # History
        menu.addSeparator()
        menu.addAction("View Promotion History", self._scroll_to_history)

        menu.exec(self.version_tree.mapToGlobal(pos))

    def _copy_version_info(self, version: VersionInfo, source: WatchedSource):
        """Copy a formatted summary of the version to the clipboard."""
        lines = [
            f"Source: {source.name}",
            f"Version: {version.version_string}",
        ]
        if getattr(version, "date_string", None):
            date_fmt = getattr(source, "date_format", "")
            if date_fmt:
                from src.lvm.task_tokens import format_date_display
                lines.append(f"Date: {format_date_display(version.date_string, date_fmt)}")
            else:
                lines.append(f"Date: {version.date_string}")
        lines += [
            f"Files: {version.file_count}",
            f"Size: {version.total_size_human}",
            f"Frame Range: {version.frame_range or 'N/A'}",
        ]
        if version.sub_sequences:
            for seq in version.sub_sequences:
                lines.append(f"  + {seq['name']}: {seq['frame_range']} ({seq['file_count']} files)")
        lines += [
            f"Timecode: {version.start_timecode or 'N/A'}",
            f"Path: {version.source_path}",
        ]
        QApplication.clipboard().setText("\n".join(lines))

    def _scroll_to_history(self):
        """Ensure the history panel is visible and scroll to it."""
        # Make sure the history panel has a reasonable size in the splitter
        sizes = self.ver_hist_splitter.sizes()
        if sizes[1] < 100:
            self.ver_hist_splitter.setSizes([sizes[0], max(200, sizes[0] // 2)])
        self.history_tree.scrollToTop()
        self.history_tree.setFocus()
        # Select the first (most recent) history entry if available
        if self.history_tree.topLevelItemCount() > 0:
            self.history_tree.setCurrentItem(self.history_tree.topLevelItem(0))

    def _assign_group(self, indices: list, group_name: str):
        """Assign or unassign sources to a group."""
        old_groups = set()
        for i in indices:
            old_groups.add(self.config.watched_sources[i].group)
            self.config.watched_sources[i].group = group_name
        self._mark_dirty()

        # Check if any old groups are now empty
        for old_grp in old_groups:
            if old_grp and old_grp != group_name:
                still_used = any(s.group == old_grp for s in self.config.watched_sources)
                if not still_used:
                    reply = QMessageBox.question(
                        self, "Empty Group",
                        f"Group '{old_grp}' has no more sources.\n\nDelete the group?",
                    )
                    if reply == QMessageBox.Yes:
                        self.config.groups.pop(old_grp, None)

        # Reapply defaults (group token may affect latest_target)
        apply_project_defaults(self.config)
        if self.config_path:
            self._save_project()
        self._reload_ui()

    def _assign_new_group(self, indices: list):
        """Create a new group and assign selected sources to it."""
        name, ok = QInputDialog.getText(self, "New Group", "Group name:")
        if not ok or not name.strip():
            return
        name = name.strip()
        if name not in self.config.groups:
            # Pick next available palette color
            used = {v.get("color", "") for v in self.config.groups.values()}
            color = "#8c8c8c"
            for c in _GROUP_COLOR_PALETTE:
                if c not in used:
                    color = c
                    break
            self.config.groups[name] = {"color": color}
        self._assign_group(indices, name)

    def _open_manage_groups(self):
        """Open the Manage Groups dialog."""
        if not self.config:
            QMessageBox.information(self, "No Project", "Open or create a project first.")
            return
        dlg = ManageGroupsDialog(self.config, parent=self)
        if dlg.exec() == QDialog.Accepted:
            dlg.apply_to_config(self.config)
            self._mark_dirty()
            apply_project_defaults(self.config)
            if self.config_path:
                self._save_project()
            self._reload_ui()
            self.statusBar().showMessage("Groups updated")

    @property
    def _is_promotion_active(self) -> bool:
        """Return True if a promotion is currently in progress."""
        return self._worker is not None or bool(self._batch_promote_list)

    def _on_source_selection_changed(self):
        """Update Promote All/Selected button based on source list selection."""
        selected = self.source_list.selectedItems()
        has_sources = self.config and len(self.config.watched_sources) > 0
        if len(selected) >= 1:
            label = f"Promote Selected ({len(selected)})" if len(selected) > 1 else "Promote Selected"
            self.btn_promote_all.setText(label)
            self.btn_promote_all.setEnabled(self._worker is None)
            self.btn_promote_split_all.setVisible(True)
            self.btn_promote_split_all.setEnabled(has_sources and self._worker is None)
        else:
            self.btn_promote_all.setText("Promote All to Latest")
            self.btn_promote_all.setEnabled(has_sources and self._worker is None)
            self.btn_promote_split_all.setVisible(False)

    def _promote_all_or_selected(self):
        """Promote highest version of all or selected sources.

        By default, only promotes sources that are not already on their
        highest version (status "newer" or "deliberate").  This avoids
        rewriting identical data, which would trigger unnecessary resyncs
        with file-sync solutions.

        Hold Shift while clicking to force-promote every source, including
        those already on the highest version.
        """
        if not self.config:
            return

        # Detect Shift modifier → force mode
        modifiers = QApplication.keyboardModifiers()
        force = bool(modifiers & Qt.ShiftModifier)

        # Check if any sources lack latest_target and no template is set
        any_missing = any(not s.latest_target for s in self.config.watched_sources)
        if any_missing and not self.config.latest_path_template:
            dlg = LatestPathDialog(self.config, parent=self)
            if dlg.exec() != QDialog.Accepted:
                return
            self.config.latest_path_template = dlg.get_template()
            self.config.default_file_rename_template = dlg.get_rename_template()
            self._mark_dirty()
            apply_project_defaults(self.config)
            if self.config_path:
                self._save_project()
            self._reload_ui()

        selected_items = self.source_list.selectedItems()
        if len(selected_items) >= 1:
            # Promote selected
            source_names = [item.data(0, Qt.UserRole) for item in selected_items]
        else:
            # Promote all
            source_names = [s.name for s in self.config.watched_sources]

        # Gather sources with their highest versions
        promote_list = []
        skipped = []
        already_current = []
        for name in source_names:
            source = next((s for s in self.config.watched_sources if s.name == name), None)
            if not source:
                continue
            if not source.latest_target:
                skipped.append(f"{name} (no latest target)")
                continue

            scanner = self._scanners.get(name)
            if not scanner:
                continue

            if name not in self._versions_cache:
                self._versions_cache[name] = scanner.scan()
            versions = self._versions_cache[name]
            if not versions:
                skipped.append(f"{name} (no versions found)")
                continue

            highest = versions[-1]  # versions are sorted ascending by version_number

            # Unless force mode, only promote sources with actual newer versions
            # Skip both "highest" (already current) and "deliberate" (user chose
            # a lower version on purpose — no new versions exist to update to)
            if not force:
                status = self._source_status.get(name, {}).get("status", "")
                if status == "highest":
                    already_current.append(f"{name} (already on {highest.version_string})")
                    continue
                if status == "deliberate":
                    already_current.append(f"{name} (pinned on lower version)")
                    continue

            promote_list.append((source, highest))

        if not promote_list:
            detail = ""
            if already_current:
                detail = (
                    "\n\nAll sources are already on their highest version.\n"
                    "Hold Shift and click to force re-promote."
                )
            QMessageBox.information(
                self, "Nothing to Promote",
                f"No sources need promoting.{detail}"
            )
            return

        # Show batch review dialog (Feature #9)
        dlg = BatchPromoteReviewDialog(promote_list, self._source_status, self._promoters, already_current, skipped, parent=self)
        if dlg.exec() != QDialog.Accepted:
            return
        promote_list = dlg.get_selected()
        if not promote_list:
            return

        # Detect layer conflicts for all sources and prompt the user
        self._batch_keep_layers: dict[str, set[str] | None] = {}
        batch_apply_all_choice = None  # set when user ticks "apply to all"

        # Pre-compute which sources have obsolete layers
        conflicts: dict[str, list[dict]] = {}
        for source, version in promote_list:
            promoter = self._promoters.get(source.name)
            if not promoter and source.latest_target:
                promoter = Promoter(source, self.config.task_tokens, self.config.project_name)
                self._promoters[source.name] = promoter
            if promoter:
                obsolete = promoter.detect_obsolete_layers(version)
                if obsolete:
                    conflicts[source.name] = obsolete
        conflict_count = len(conflicts)

        # Prompt for each source with conflicts (unless "apply to all" covers it)
        skip_sources: set[str] = set()
        for source, version in promote_list:
            if source.name not in conflicts:
                continue
            if batch_apply_all_choice is not None:
                # Apply previously chosen action
                if batch_apply_all_choice == ObsoleteLayerDialog.SKIP:
                    skip_sources.add(source.name)
                elif batch_apply_all_choice == ObsoleteLayerDialog.KEEP:
                    self._batch_keep_layers[source.name] = {
                        layer["prefix"] for layer in conflicts[source.name]
                    }
                # DELETE: keep_layers stays None (default)
                continue

            dlg = ObsoleteLayerDialog(
                source.name, version.version_string,
                conflicts[source.name], conflict_count=conflict_count,
                parent=self,
            )
            if dlg.exec() != QDialog.Accepted:
                return  # user closed dialog — cancel entire batch
            if dlg.apply_to_all:
                batch_apply_all_choice = dlg.choice
            if dlg.choice == ObsoleteLayerDialog.SKIP:
                skip_sources.add(source.name)
            elif dlg.choice == ObsoleteLayerDialog.KEEP:
                self._batch_keep_layers[source.name] = {
                    layer["prefix"] for layer in conflicts[source.name]
                }

        # Remove skipped sources from the promote list
        if skip_sources:
            promote_list = [(s, v) for s, v in promote_list if s.name not in skip_sources]
        if not promote_list:
            self.statusBar().showMessage("All sources skipped due to layer conflicts.")
            return

        self._batch_promote_list = promote_list
        self._batch_promote_index = 0
        self._batch_promote_next()

    def _promote_all_forced(self):
        """Promote all sources regardless of selection (used by split 'All' button)."""
        self.source_list.clearSelection()
        self._promote_all_or_selected()

    def _batch_promote_next(self):
        """Promote the next source in the batch list."""
        if self._batch_promote_index >= len(self._batch_promote_list):
            # All done — rescan only the sources that were promoted
            batch = self._batch_promote_list
            promoted_names = [s.name for s, _v in batch]
            count = len(batch)
            self._batch_promote_list = []
            self._batch_keep_layers = {}
            for name in promoted_names:
                self._versions_cache.pop(name, None)
            self._process_deferred_or_refresh(promoted_names)
            self.statusBar().showMessage(f"Batch promotion complete: {count} source(s)")
            return

        source, version = self._batch_promote_list[self._batch_promote_index]
        promoter = self._promoters.get(source.name)
        if not promoter:
            # Create promoter if needed
            if source.latest_target:
                promoter = Promoter(source, self.config.task_tokens, self.config.project_name)
                self._promoters[source.name] = promoter
            else:
                self._batch_promote_index += 1
                self._batch_promote_next()
                return

        self.statusBar().showMessage(
            f"Promoting {self._batch_promote_index + 1}/{len(self._batch_promote_list)}: {source.name}"
        )
        self._current_source = source
        keep_layers = getattr(self, '_batch_keep_layers', {}).get(source.name)
        self._start_promotion(promoter, version, keep_layers=keep_layers)

    # --- UI Updates ---

    def _reload_ui(self, cached_versions: dict = None):
        """Refresh everything from current config (non-blocking).

        When *cached_versions* is supplied the scan phase is skipped and the
        cached data is fed straight into status computation.  Otherwise a
        background ``ScanWorker`` runs first.
        """
        # If an async reload is already in progress, queue this one
        if self._scan_worker is not None or self._status_worker is not None:
            self._reload_pending = True
            return

        # Clear UI immediately so the user sees something is happening
        self.source_list.clear()
        self.version_tree.clear()
        self.history_tree.clear()
        self._scanners.clear()
        self._promoters.clear()
        self._versions_cache.clear()
        self._manual_versions.clear()
        # Restore persisted manual versions from config
        if self.config:
            for source in self.config.watched_sources:
                if source.manual_versions:
                    self._manual_versions[source.name] = [
                        VersionInfo.from_dict(mv) for mv in source.manual_versions
                    ]
        self._current_source = None

        enabled = self.config is not None
        self.btn_project_settings.setEnabled(enabled)
        self.btn_manage_groups.setEnabled(enabled)
        self.btn_refresh.setEnabled(False)
        self.btn_import_version.setEnabled(False)
        self.btn_refresh_versions.setEnabled(False)
        self.watch_toggle.setEnabled(enabled)
        self.btn_promote.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self.btn_promote_all.setEnabled(False)
        self.btn_promote_all.setText("Promote All to Latest")
        self.btn_promote_split_all.setVisible(False)

        if not self.config:
            self.current_label.setText("No project loaded")
            self.integrity_label.setText("")
            return

        if not self.config.watched_sources:
            # Nothing to scan/compute — just populate empty state
            self._source_status = {}
            self._target_conflicts = {}
            self.btn_refresh.setEnabled(True)
            self._populate_source_list()
            return

        self._scan_indicator.setText("Loading...")
        self._scan_indicator.setStyleSheet("color: #d4a849; font-size: 11pt; margin-right: 8px;")

        if cached_versions is not None:
            # Skip scan, go straight to status computation
            self._versions_cache = dict(cached_versions)
            self._start_status_worker(dict(cached_versions))
        else:
            # Phase 1: scan in background
            self._scan_worker = ScanWorker(self.config, parent=self)
            self._scan_worker.progress.connect(self._on_refresh_progress)
            self._scan_worker.finished.connect(self._on_reload_scan_complete)
            self._scan_worker.error.connect(self._on_reload_error)
            self._scan_worker.start()

    def _on_reload_scan_complete(self, scan_results: dict):
        """Phase 1 done — scan results ready, start status computation."""
        self._scan_worker = None
        self._versions_cache = dict(scan_results)
        self._start_status_worker(scan_results)

    def _on_reload_error(self, msg: str):
        """Handle errors during reload scan phase."""
        self._scan_worker = None
        self.btn_refresh.setEnabled(True)
        self._scan_indicator.setText("")
        self.statusBar().showMessage(f"Scan error: {msg}")
        logger.error(f"Reload scan error: {msg}")
        self._check_reload_pending()

    def _start_status_worker(self, versions_cache: dict):
        """Phase 2: compute statuses in background thread."""
        self._status_worker = StatusWorker(self.config, versions_cache, parent=self)
        self._status_worker.finished.connect(self._on_reload_status_complete)
        self._status_worker.start()

    def _on_reload_status_complete(self, source_status: dict, target_conflicts: dict,
                                   promoters: dict, scanners: dict):
        """Phase 2 done — populate caches and rebuild UI."""
        self._status_worker = None
        self._source_status = source_status
        self._target_conflicts = target_conflicts
        self._promoters = promoters
        self._scanners = scanners

        self.btn_refresh.setEnabled(True)
        has_sources = len(self.config.watched_sources) > 0
        self.btn_promote_all.setEnabled(has_sources and self._worker is None)

        self._populate_source_list()

        # Restore selection
        restored = False
        if self._reload_select_source:
            for i in range(self.source_list.topLevelItemCount()):
                if self.source_list.topLevelItem(i).data(0, Qt.UserRole) == self._reload_select_source:
                    self.source_list.setCurrentItem(self.source_list.topLevelItem(i))
                    restored = True
                    break
            self._reload_select_source = None
        if not restored and self.source_list.topLevelItemCount() > 0:
            self.source_list.setCurrentItem(self.source_list.topLevelItem(0))

        self._save_scan_cache()
        self._scan_indicator.setText("")

        # If this was a cache-first load, kick off a background rescan now
        if getattr(self, '_rescan_after_cache', False):
            self._rescan_after_cache = False
            self._trigger_background_rescan()
        else:
            self._check_reload_pending()

    def _check_reload_pending(self):
        """If another reload was requested while one was running, start it now."""
        if self._reload_pending:
            self._reload_pending = False
            self._reload_ui()

    # Keep old name as alias for the cache-first load path
    def _reload_ui_from_cache(self, cached_versions: dict):
        """Populate UI from cached version data (non-blocking)."""
        self._reload_ui(cached_versions=cached_versions)

    def _trigger_background_rescan(self):
        """Start a background rescan after loading from cache."""
        if not self.config or not self.config.watched_sources:
            return
        if self._scan_worker is not None:
            return
        if self._is_promotion_active:
            logger.debug("Deferring background rescan — promotion in progress")
            return
        self._scan_indicator.setText("Updating...")
        self._scan_indicator.setStyleSheet("color: #d4a849; font-size: 11pt; margin-right: 8px;")
        self._scan_worker = ScanWorker(self.config, previous_cache=dict(self._versions_cache), parent=self)
        self._scan_worker.progress.connect(self._on_refresh_progress)
        self._scan_worker.finished.connect(self._on_refresh_complete)
        self._scan_worker.error.connect(self._on_refresh_error)
        self._scan_worker.start()

    def _save_scan_cache(self):
        """Save current _versions_cache to disk (background I/O)."""
        if not self.config_path or not self.config:
            return
        from src.lvm.scan_cache import save_cache
        # Snapshot refs for the background thread
        config_path = self.config_path
        sources = list(self.config.watched_sources)
        cache = dict(self._versions_cache)

        def _write():
            try:
                save_cache(config_path, sources, cache)
            except Exception as e:
                logging.getLogger(__name__).warning("Failed to save scan cache: %s", e)

        self._io_executor.submit(_write)

    def _source_matches_search(self, source: WatchedSource, query: str) -> bool:
        """Check if a source matches the search query (name, filename, task)."""
        if not query:
            return True
        q = query.lower()
        # Match against display name
        if q in source.name.lower():
            return True
        # Match against sample filename (includes version and task)
        if source.sample_filename and q in source.sample_filename.lower():
            return True
        # Match against source directory basename (file-level name)
        dirname = Path(source.source_dir).name if source.source_dir else ""
        if q in dirname.lower():
            return True
        return False

    def _make_source_item(self, source: WatchedSource) -> QTreeWidgetItem:
        """Create a QTreeWidgetItem for a source with status coloring and multi-column data."""
        info = self._source_status.get(source.name, {})
        status = info.get("status", "no_target")
        current = info.get("current")
        has_overrides = info.get("has_overrides", False)

        ver_tag = current.version if current else ""

        # Status markers for name column
        status_markers = {
            "newer": "\u25bc! ", "stale": "\u21bb ", "deliberate": "* ",
            "integrity_fail": "\u26a0 ",
        }
        marker = status_markers.get(status, "")
        name_text = f"{marker}{source.name}"
        if source.name in self._target_conflicts:
            name_text += " [!]"
        group_text = source.group if source.group else ""

        # Status display text
        status_labels = {
            "newer": "Newer Available", "stale": "Stale", "deliberate": "Pinned",
            "highest": "Latest", "integrity_fail": "Integrity Fail",
            "no_version": "Not Promoted", "no_target": "No Target",
        }
        status_text = status_labels.get(status, status)

        # Layers: number of additional layers (sub_sequences) in the promoted version
        layers = str(len(current.sub_sequences)) if current and current.sub_sequences else ""

        # Frames: frame count or clip length for containers
        frames = ""
        if current:
            if current.clip_frame_count:
                frames = str(current.clip_frame_count)
            elif current.frame_count:
                frames = str(current.frame_count)

        # Filetype: primary file extension
        filetype = current.file_type.lstrip(".").upper() if current and current.file_type else ""

        # Added on timestamp
        added_on = ""
        if source.added_at:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(source.added_at)
                added_on = dt.strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                added_on = source.added_at

        # Last promoted timestamp
        last_promoted = ""
        if current and current.set_at:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(current.set_at)
                last_promoted = dt.strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                last_promoted = current.set_at

        item = QTreeWidgetItem([name_text, group_text, ver_tag, layers, frames, filetype, added_on, last_promoted, status_text])
        item.setData(0, Qt.UserRole, source.name)

        # Color coding
        color = None
        tooltip = ""
        if status == "newer":
            color = QColor("#cc8833")
            tooltip = f"Newer versions available since {ver_tag} was promoted"
        elif status == "stale":
            color = QColor("#e8a040")
            tooltip = f"Source files for {ver_tag} modified since promotion — may have been re-rendered"
        elif status == "deliberate":
            color = QColor("#7abbe0")
            tooltip = f"Pinned on {ver_tag} (Keep) — batch promote skips until a new version arrives"
        elif status == "highest":
            color = QColor("#4ec9a0")
            tooltip = f"On latest version: {ver_tag}"
        elif status == "integrity_fail":
            color = QColor("#ffaa00")
            tooltip = f"Integrity issue with {ver_tag}"
        elif status == "no_version":
            color = QColor("#8c8c8c")
            tooltip = "No version promoted yet"
        else:  # no_target
            color = QColor("#555555")
            tooltip = "No latest target path configured"

        # Override marker (blue tint on top)
        if has_overrides:
            color = QColor("#88aaff")
            tooltip += " | Custom settings"

        # Conflict warning
        if source.name in self._target_conflicts:
            conflict_names = ", ".join(self._target_conflicts[source.name])
            color = QColor("#ff8c00")
            tooltip += f" | TARGET CONFLICT with: {conflict_names}"

        if source.group:
            tooltip += f" | Group: {source.group}"

        # Apply color to all columns
        group_col_idx = self._source_col_keys.index("group")
        if color:
            for col in range(len(self._source_col_keys)):
                if col == group_col_idx:
                    continue  # Group column gets its own color
                item.setForeground(col, color)

        # Color the group column with the group's own color
        if source.group and self.config and source.group in self.config.groups:
            grp_color = self.config.groups[source.group].get("color", "#8c8c8c")
            item.setForeground(group_col_idx, QColor(grp_color))

        item.setToolTip(0, tooltip)

        return item

    def _populate_source_list(self):
        """Build source list items based on computed status, active filter, search query, and grouping."""
        # Temporarily disable sorting while populating to avoid re-sorts on every insert
        self.source_list.setSortingEnabled(False)
        self.source_list.clear()
        if not self.config:
            self.source_list.setSortingEnabled(True)
            return

        filter_mode = self.source_filter.currentText()
        search_query = self.source_search.text().strip() if hasattr(self, 'source_search') else ""
        group_by = self.group_by_check.isChecked() if hasattr(self, 'group_by_check') else False

        # Build filtered source list
        filtered = []
        for source in self.config.watched_sources:
            info = self._source_status.get(source.name, {})
            status = info.get("status", "no_target")

            # Apply status filter
            if filter_mode == "Newer Available" and status != "newer":
                continue
            if filter_mode == "Stale" and status != "stale":
                continue
            if filter_mode == "Not on Highest" and status not in ("newer", "deliberate", "stale", "no_version"):
                continue

            # Apply search filter
            if not self._source_matches_search(source, search_query):
                continue

            filtered.append(source)

        if group_by and self.config.groups:
            # Sort: grouped sources first (by group name, then source name), ungrouped last
            grouped: dict[str, list] = {}
            ungrouped = []
            for source in filtered:
                if source.group and source.group in self.config.groups:
                    grouped.setdefault(source.group, []).append(source)
                else:
                    ungrouped.append(source)

            for grp_name in sorted(grouped.keys()):
                color = self.config.groups[grp_name].get("color", "#8c8c8c")
                # Group header (non-selectable separator)
                header = QTreeWidgetItem([f"\u2500\u2500 {grp_name} \u2500\u2500"])
                header.setFlags(Qt.NoItemFlags)
                for col in range(len(self._source_col_keys)):
                    header.setForeground(col, QColor(color))
                font = header.font(0)
                font.setBold(True)
                header.setFont(0, font)
                self.source_list.addTopLevelItem(header)

                for source in sorted(grouped[grp_name], key=lambda s: s.name.lower()):
                    self.source_list.addTopLevelItem(self._make_source_item(source))

            if ungrouped:
                if grouped:
                    header = QTreeWidgetItem(["\u2500\u2500 Ungrouped \u2500\u2500"])
                    header.setFlags(Qt.NoItemFlags)
                    for col in range(len(self._source_col_keys)):
                        header.setForeground(col, QColor("#555555"))
                    font = header.font(0)
                    font.setBold(True)
                    header.setFont(0, font)
                    self.source_list.addTopLevelItem(header)
                for source in sorted(ungrouped, key=lambda s: s.name.lower()):
                    self.source_list.addTopLevelItem(self._make_source_item(source))
        else:
            # Alphabetical order (sorting will handle this once re-enabled)
            for source in sorted(filtered, key=lambda s: s.name.lower()):
                self.source_list.addTopLevelItem(self._make_source_item(source))

        # Apply column visibility — setSortingEnabled must come first; on Linux/Qt6
        # enabling sort triggers a QHeaderView section re-init that resets hidden states.
        self.source_list.setSortingEnabled(True)
        self._apply_source_column_visibility()

    def _apply_source_column_visibility(self):
        """Show/hide source list columns based on config."""
        if not self.config:
            enabled = ["version", "status"]
        else:
            enabled = list(self.config.source_list_columns)
            # Auto-show group column when groups exist
            if self.config.groups and "group" not in enabled:
                enabled.append("group")
                self.config.source_list_columns = enabled
                self._mark_dirty()
        for i, key in enumerate(self._source_col_keys):
            if key == "name":
                # Name column is always visible
                self.source_list.setColumnHidden(i, False)
            else:
                self.source_list.setColumnHidden(i, key not in enabled)

    def _source_header_context_menu(self, pos):
        """Show context menu on source list header to toggle column visibility."""
        menu = QMenu(self)
        enabled = self.config.source_list_columns if self.config else ["version", "status"]
        for key in self._source_col_keys:
            if key == "name":
                continue  # Name is always visible
            label = self._source_col_labels[key]
            action = menu.addAction(label)
            action.setCheckable(True)
            action.setChecked(key in enabled)
            action.triggered.connect(lambda checked, k=key: self._toggle_source_column(k, checked))
        menu.exec(self.source_list.header().mapToGlobal(pos))

    def _toggle_source_column(self, key: str, visible: bool):
        """Toggle visibility of a source list column and persist to config."""
        if not self.config:
            return
        cols = list(self.config.source_list_columns)
        if visible and key not in cols:
            cols.append(key)
        elif not visible and key in cols:
            cols.remove(key)
        self.config.source_list_columns = cols
        self._mark_dirty()
        # Defer apply so Linux/Qt6 header re-init events from menu close settle first.
        QTimer.singleShot(0, self._apply_source_column_visibility)
        if self.config_path:
            self._save_project()

    def _apply_source_filter(self):
        """Re-filter the source list without full reload."""
        if not self.config or not hasattr(self, '_source_status'):
            return
        prev_source = None
        if self.source_list.currentItem():
            prev_source = self.source_list.currentItem().data(0, Qt.UserRole)
        self._populate_source_list()
        # Try to re-select the previously selected source
        if prev_source:
            for i in range(self.source_list.topLevelItemCount()):
                if self.source_list.topLevelItem(i).data(0, Qt.UserRole) == prev_source:
                    self.source_list.setCurrentItem(self.source_list.topLevelItem(i))
                    return
        if self.source_list.topLevelItemCount() > 0:
            self.source_list.setCurrentItem(self.source_list.topLevelItem(0))

    def _refresh_sources_by_name(self, source_names: list[str], select_source: str = None):
        """Re-scan only the given sources (by name) using the background worker path.

        Delegates to _refresh_selected_sources which uses ScanWorker + StatusWorker
        so the UI stays responsive even on slow network paths.
        """
        if not self.config:
            return
        if self._scan_worker is not None or self._status_worker is not None:
            # A worker is already running — schedule a full refresh once it finishes
            # rather than silently dropping this request.
            logger.debug("Refresh worker busy — scheduling full rescan after current worker")
            self._rescan_after_cache = True
            self._refresh_select_source = select_source
            return

        # Resolve source names to indices
        indices = []
        for i, s in enumerate(self.config.watched_sources):
            if s.name in source_names:
                indices.append(i)
        if not indices:
            return

        self._refresh_select_source = select_source
        self._refresh_selected_sources(indices)

    def _refresh_all_with_selection(self, select_source: str = None):
        """Re-scan all sources in background, restoring the given source selection on completion."""
        if select_source is None and self._current_source:
            select_source = self._current_source.name
        self._refresh_select_source = select_source
        self._refresh_all()

    def _refresh_current_source(self):
        """Refresh versions for the currently selected source."""
        if not self._current_source:
            return
        for i, s in enumerate(self.config.watched_sources):
            if s.name == self._current_source.name:
                self._refresh_selected_sources([i])
                return

    def _refresh_selected_sources(self, indices: list[int]):
        """Re-scan only the specified sources in background thread."""
        if not self.config or self._scan_worker is not None or self._status_worker is not None:
            return
        sources = [self.config.watched_sources[i] for i in indices if i < len(self.config.watched_sources)]
        if not sources:
            return

        # Remember which source to re-select after refresh
        self._refresh_select_source = self._current_source.name if self._current_source else None

        self.btn_refresh.setEnabled(False)
        self.btn_refresh_versions.setEnabled(False)
        self.btn_promote.setEnabled(False)
        self.btn_promote_all.setEnabled(False)
        self.btn_promote_split_all.setEnabled(False)
        names = ", ".join(s.name for s in sources)
        self._scan_indicator.setText("Updating...")
        self._scan_indicator.setStyleSheet("color: #d4a849; font-size: 11pt; margin-right: 8px;")
        self.statusBar().showMessage(f"Scanning: {names}")

        self._scan_worker = ScanWorker(self.config, sources=sources, parent=self)
        self._scan_worker.progress.connect(self._on_refresh_progress)
        self._scan_worker.finished.connect(self._on_partial_refresh_complete)
        self._scan_worker.error.connect(self._on_refresh_error)
        self._scan_worker.start()

    def _on_partial_refresh_complete(self, scan_results: dict):
        """Called when a partial (selected sources) scan finishes. Delegate to StatusWorker."""
        self._scan_worker = None
        self._partial_scan_count = len(scan_results)

        # Merge new scan results into the existing versions cache
        for source_name, versions in scan_results.items():
            self._versions_cache[source_name] = versions

        # Only recompute status for the sources that were actually re-scanned
        changed_sources = [
            s for s in self.config.watched_sources if s.name in scan_results
        ]
        self._status_worker = StatusWorker(
            self.config, dict(self._versions_cache),
            sources=changed_sources, parent=self,
        )
        self._status_worker.finished.connect(self._on_partial_status_complete)
        self._status_worker.start()

    def _on_partial_status_complete(self, source_status: dict, target_conflicts: dict,
                                    promoters: dict, scanners: dict):
        """Status computation done after partial refresh — rebuild UI."""
        self._status_worker = None
        # Merge partial results into existing caches (not replace)
        self._source_status.update(source_status)
        self._promoters.update(promoters)
        self._scanners.update(scanners)
        # Recompute conflicts for all sources (cheap — just path comparison)
        from src.lvm.conflicts import detect_target_conflicts
        conflicts = detect_target_conflicts(self.config)
        self._target_conflicts = {}
        for target, name_a, name_b in conflicts:
            self._target_conflicts.setdefault(name_a, []).append(name_b)
            self._target_conflicts.setdefault(name_b, []).append(name_a)

        # Rebuild source list and restore selection
        self.source_list.clear()
        self.version_tree.clear()
        self.history_tree.clear()

        self.btn_refresh.setEnabled(True)
        self.btn_import_version.setEnabled(False)
        self.btn_refresh_versions.setEnabled(False)
        self.btn_promote.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self.btn_promote_all.setEnabled(len(self.config.watched_sources) > 0 and self._worker is None)

        self._populate_source_list()

        self._scan_indicator.setText("")
        count = getattr(self, '_partial_scan_count', 0)
        self.statusBar().showMessage(f"Refreshed {count} source{'s' if count != 1 else ''}", 3000)

        # Restore selection
        if self._refresh_select_source:
            for i in range(self.source_list.topLevelItemCount()):
                if self.source_list.topLevelItem(i).data(0, Qt.UserRole) == self._refresh_select_source:
                    self.source_list.setCurrentItem(self.source_list.topLevelItem(i))
                    self._refresh_select_source = None
                    self._check_reload_pending()
                    return
        self._refresh_select_source = None
        if self.source_list.topLevelItemCount() > 0:
            self.source_list.setCurrentItem(self.source_list.topLevelItem(0))

        self._check_reload_pending()

    def _refresh_all(self):
        """Re-scan all sources in background thread."""
        if not self.config or not self.config.watched_sources:
            self._reload_ui()
            return

        # If a scan or status computation is already running, let it complete
        if self._scan_worker is not None or self._status_worker is not None:
            return

        # Disable refresh during scan
        self.btn_refresh.setEnabled(False)
        self.btn_refresh_versions.setEnabled(False)
        self.btn_promote.setEnabled(False)
        self.btn_promote_all.setEnabled(False)
        self.btn_promote_split_all.setEnabled(False)
        self._scan_indicator.setText("Updating...")
        self._scan_indicator.setStyleSheet("color: #d4a849; font-size: 11pt; margin-right: 8px;")
        self.statusBar().showMessage("Scanning sources...")

        self._scan_worker = ScanWorker(self.config, previous_cache=dict(self._versions_cache), parent=self)
        self._scan_worker.progress.connect(self._on_refresh_progress)
        self._scan_worker.finished.connect(self._on_refresh_complete)
        self._scan_worker.error.connect(self._on_refresh_error)
        self._scan_worker.start()

    def _on_refresh_progress(self, current: int, total: int, source_name: str):
        self.statusBar().showMessage(f"Scanning source {current}/{total}: {source_name}")

    def _on_refresh_error(self, msg: str):
        self._scan_worker = None
        self.btn_refresh.setEnabled(True)
        self._scan_indicator.setText("")
        self.statusBar().showMessage(f"Scan error: {msg}")
        logger.error(f"Scan error: {msg}")

        self._check_reload_pending()

    def _on_refresh_complete(self, scan_results: dict):
        """Called when background scan finishes. Delegate to StatusWorker.

        If a promotion is currently running, defer processing these results
        until the promotion completes.  Clearing _promoters/_scanners/
        _current_source while a PromoteWorker is active would corrupt the
        promotion state and silently break subsequent promotions.
        """
        self._scan_worker = None

        if self._is_promotion_active:
            logger.debug("Deferring refresh results — promotion in progress")
            self._deferred_refresh_results = scan_results
            return

        self._apply_refresh_results(scan_results)

    def _apply_refresh_results(self, scan_results: dict):
        """Apply full refresh scan results: update caches and start StatusWorker."""
        # Store scanned versions and clear stale caches
        self._versions_cache = dict(scan_results)
        self._scanners.clear()
        self._promoters.clear()
        self._current_source = None
        self._source_status = {}

        # Phase 2: compute statuses in background
        self._status_worker = StatusWorker(self.config, scan_results, parent=self)
        self._status_worker.finished.connect(self._on_refresh_status_complete)
        self._status_worker.start()

    def _on_refresh_status_complete(self, source_status: dict, target_conflicts: dict,
                                    promoters: dict, scanners: dict):
        """Status computation done after _refresh_all — rebuild UI."""
        self._status_worker = None
        self._source_status = source_status
        self._target_conflicts = target_conflicts
        self._promoters = promoters
        self._scanners = scanners

        self.source_list.clear()
        self.version_tree.clear()
        self.history_tree.clear()

        self.btn_project_settings.setEnabled(True)
        self.btn_manage_groups.setEnabled(True)
        self.btn_refresh.setEnabled(True)
        self.btn_import_version.setEnabled(False)
        self.btn_refresh_versions.setEnabled(False)
        self.watch_toggle.setEnabled(True)
        self.btn_promote.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self.btn_promote_all.setEnabled(len(self.config.watched_sources) > 0 and self._worker is None)
        self.btn_promote_all.setText("Promote All to Latest")
        self.btn_promote_split_all.setVisible(False)

        self._populate_source_list()

        # Restore selection if a specific source was requested
        restored = False
        if self._refresh_select_source:
            for i in range(self.source_list.topLevelItemCount()):
                if self.source_list.topLevelItem(i).data(0, Qt.UserRole) == self._refresh_select_source:
                    self.source_list.setCurrentItem(self.source_list.topLevelItem(i))
                    restored = True
                    break
            self._refresh_select_source = None
        if not restored and self.source_list.topLevelItemCount() > 0:
            self.source_list.setCurrentItem(self.source_list.topLevelItem(0))

        # Save scan results to cache and clear indicator
        self._save_scan_cache()
        self._scan_indicator.setText("")
        self.statusBar().showMessage("Refreshed all sources")

        self._check_reload_pending()

    def _process_deferred_or_refresh(self, source_names: list[str],
                                      select_source: str = None):
        """After promotion completes, apply deferred scan results or do a targeted refresh.

        If a full background scan completed while the promotion was running,
        its results were stashed in ``_deferred_refresh_results``.  Applying
        them now gives us a complete, up-to-date view without another scan.

        Otherwise fall back to ``_refresh_sources_by_name`` which rescans only
        the named sources.
        """
        deferred = self._deferred_refresh_results
        self._deferred_refresh_results = None

        if deferred is not None:
            logger.debug("Applying deferred refresh results after promotion")
            self._refresh_select_source = select_source
            self._apply_refresh_results(deferred)
        else:
            self._refresh_sources_by_name(source_names, select_source=select_source)

    def _export_report(self):
        """Export a promotion report for the current source."""
        if not self.config or not self._current_source:
            QMessageBox.information(self, "No Source", "Select a source first.")
            return

        source = self._current_source
        promoter = self._promoters.get(source.name)
        if not promoter:
            QMessageBox.information(self, "No Target", f"{source.name} has no latest target set.")
            return

        current = promoter.get_current_version()
        if not current:
            QMessageBox.information(self, "No History",
                                    f"No promotion history for {source.name}.")
            return

        report = generate_report(current, source)
        filepath, _ = QFileDialog.getSaveFileName(
            self, "Export Promotion Report",
            f"{source.name}_report.json",
            "JSON (*.json);;All Files (*)",
        )
        if not filepath:
            return

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        self.statusBar().showMessage(f"Report exported to: {filepath}")

    def _validate_config(self):
        """Validate the current project config and show results."""
        import re as _re

        if not self.config:
            QMessageBox.information(self, "No Project", "Open or create a project first.")
            return

        issues = []
        warnings = []

        if not self.config.project_name or self.config.project_name == "Untitled":
            warnings.append("Project name is default/empty")

        for source in self.config.watched_sources:
            if not os.path.isdir(source.source_dir):
                issues.append(f"{source.name}: source_dir does not exist:\n  {source.source_dir}")
            if source.latest_target and not os.path.isdir(source.latest_target):
                warnings.append(f"{source.name}: latest_target does not exist yet:\n  {source.latest_target}")
            if not source.file_extensions:
                warnings.append(f"{source.name}: no file extensions configured")
            if not source.version_pattern:
                issues.append(f"{source.name}: no version pattern configured")
            if source.group and source.group not in self.config.groups:
                warnings.append(f"{source.name}: group '{source.group}' is not defined")

        if self.config.latest_path_template and "{" in self.config.latest_path_template:
            tokens_found = _re.findall(r"\{(\w+)\}", self.config.latest_path_template)
            known = {"project_root", "group_root", "source_title", "source_name",
                     "source_basename", "source_fullname", "source_filename",
                     "source_dir", "group"}
            unknown = set(tokens_found) - known
            if unknown:
                warnings.append(f"Unknown tokens in latest_path_template: {unknown}")

        if issues or warnings:
            msg = ""
            if issues:
                msg += "ERRORS:\n" + "\n".join(f"  \u2022 {i}" for i in issues) + "\n\n"
            if warnings:
                msg += "WARNINGS:\n" + "\n".join(f"  \u2022 {w}" for w in warnings)
            icon = QMessageBox.Warning if issues else QMessageBox.Information
            dlg = QMessageBox(icon, "Config Validation", msg, QMessageBox.Ok, self)
            dlg.exec()
        else:
            QMessageBox.information(
                self, "Config Validation",
                f"Config OK: {self.config.project_name}\n"
                f"{len(self.config.watched_sources)} source(s), "
                f"{len(self.config.groups)} group(s)"
            )

    def _on_source_item_changed(self, current, previous):
        """Bridge for currentItemChanged signal → _on_source_selected."""
        if current:
            source_name = current.data(0, Qt.UserRole)
            if source_name:
                self._on_source_selected_by_name(source_name)
            else:
                self._on_source_selected_by_name(None)
        else:
            self._on_source_selected_by_name(None)

    def _on_source_selected_by_name(self, source_name):
        """User selected a source — populate versions and history."""
        self.version_tree.clear()
        self.history_tree.clear()
        self.btn_promote.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self.btn_import_version.setEnabled(False)
        self.btn_refresh_versions.setEnabled(False)

        if not source_name or not self.config:
            self.current_label.setText("No version loaded")
            self.integrity_label.setText("")
            return

        source = None
        for s in self.config.watched_sources:
            if s.name == source_name:
                source = s
                break
        if not source:
            return
        self._current_source = source
        self.btn_import_version.setEnabled(True)
        self.btn_refresh_versions.setEnabled(True)

        scanner = self._scanners.get(source.name)
        promoter = self._promoters.get(source.name)
        if not scanner:
            return

        # Scan versions (use cache if available)
        if source.name not in self._versions_cache:
            self._versions_cache[source.name] = scanner.scan()
        scanned_versions = self._versions_cache[source.name]

        # Merge manual versions
        manual = self._manual_versions.get(source.name, [])
        versions = sorted(scanned_versions + manual,
                          key=lambda v: (getattr(v, "date_sortable", 0), v.version_number))
        # Track which source_paths are manual for UI indicators
        manual_paths = {v.source_path for v in manual}

        # Timecode loading based on project setting
        tc_mode = self.config.timecode_mode if self.config else "lazy"
        if tc_mode == "lazy":
            populate_timecodes(versions)
        # "always" — already populated during scan (see _reload_ui)
        # "never"  — leave as None

        # Use cached status from StatusWorker to avoid redundant I/O
        status_info = self._source_status.get(source.name, {})
        current = status_info.get("current") if status_info else (
            promoter.get_current_version() if promoter else None)
        current_ver = current.version if current else None

        # Update banner
        if not promoter:
            self.current_label.setText(f"No latest target set   ({source.name})")
            self.current_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #ff8888;")
            self.integrity_label.setText("")
            self.current_banner.setStyleSheet(
                "QFrame { background-color: #3a2020; border: 1px solid #5a3030; "
                "border-radius: 4px; padding: 8px; }"
            )
        elif current:
            # Check if current version is the highest available
            highest_ver = versions[-1].version_string if versions else None
            is_highest = (current.version == highest_ver)
            if is_highest:
                self.current_label.setText(f"Current: {current.version}   ({source.name})")
                self.current_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #4ec9a0;")
            else:
                # Check if this is a pinned (Keep) version with no new versions since
                is_pinned_deliberate = (
                    getattr(current, 'pinned', False)
                    and not has_newer_versions_since(current, versions)
                )
                if is_pinned_deliberate:
                    # Pinned via "Keep" — blue indicator
                    self.current_label.setText(f"Current: {current.version}*   ({source.name})")
                    self.current_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #7abbe0;")
                else:
                    # Unpinned or pin expired — newer versions available (orange)
                    self.current_label.setText(f"Current: {current.version} \u25bc!   ({source.name})")
                    self.current_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #cc8833;")
            # Use cached integrity from StatusWorker; fallback to live call if not yet computed
            integrity = status_info.get("integrity") if status_info else None
            if integrity is None and promoter:
                integrity = promoter.verify()
            if not integrity:
                integrity = {"valid": True, "message": ""}
            if integrity["valid"]:
                self.integrity_label.setText("\u2713 Verified")
                self.integrity_label.setStyleSheet("font-size: 11pt; color: #4ec9a0;")
                self.current_banner.setStyleSheet(
                    "QFrame { background-color: #1a2a3a; border: 1px solid #336699; "
                    "border-radius: 4px; padding: 8px; }"
                )
            else:
                self.integrity_label.setText(f"\u26a0 {integrity['message']}")
                self.integrity_label.setStyleSheet("font-size: 11pt; color: #ffaa00;")
                self.current_banner.setStyleSheet(
                    "QFrame { background-color: #3a3a1a; border: 1px solid #5a5a2d; "
                    "border-radius: 4px; padding: 8px; }"
                )
        else:
            self.current_label.setText(f"No version loaded   ({source.name})")
            self.current_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #8c8c8c;")
            self.integrity_label.setText("")
            self.current_banner.setStyleSheet(
                "QFrame { background-color: #1a1a1a; border: 1px solid #2a2a2a; "
                "border-radius: 4px; padding: 8px; }"
            )

        # Determine highest version number and whether new versions appeared
        highest_ver = versions[-1].version_number if versions else 0
        has_new = (
            current is not None
            and current_ver != (versions[-1].version_string if versions else None)
            and (not getattr(current, 'pinned', False) or has_newer_versions_since(current, versions))
        )

        # Populate version tree
        current_tc = current.start_timecode if current else None

        for v in reversed(versions):  # Newest first
            is_manual = v.source_path in manual_paths
            version_label = f"{v.version_string} [manual]" if is_manual else v.version_string

            # Date display from VersionInfo (empty dash if no date)
            date_display = ""
            if getattr(v, "date_string", None):
                from src.lvm.task_tokens import format_date_display
                date_fmt = getattr(source, "date_format", "")
                date_display = format_date_display(v.date_string, date_fmt) if date_fmt else v.date_string

            main_frame_display = v.frame_range or "\u2014"
            if v.sub_sequences:
                main_frame_display += f" (+{len(v.sub_sequences)} layer{'s' if len(v.sub_sequences) > 1 else ''})"
            item = QTreeWidgetItem([
                version_label,
                date_display or "\u2014",
                str(v.file_count),
                v.total_size_human,
                main_frame_display,
                v.start_timecode or "\u2014",
                v.source_path,
            ])
            item.setData(0, Qt.UserRole, v)

            # Tooltip with sub-sequence detail
            if v.sub_sequences:
                tooltip_lines = [f"Primary: {v.frame_range or 'N/A'}"]
                for seq in v.sub_sequences:
                    tooltip_lines.append(f"  {seq['name']}: {seq['frame_range']} ({seq['file_count']} files)")
                item.setToolTip(4, "\n".join(tooltip_lines))

            if is_manual:
                # Cyan tint for manually imported versions
                manual_color = QColor("#66cccc")
                for col in range(7):
                    item.setForeground(col, manual_color)

            if v.version_string == current_ver:
                is_highest = (v.version_number == highest_ver)
                if is_highest:
                    # Promoted version IS the highest — bright green
                    suffix = " [manual]" if is_manual else ""
                    item.setText(0, f"{v.version_string}{suffix} \u25c0")
                    color = QColor("#4ec9a0")
                elif has_new:
                    # New higher versions appeared after promotion — dark orange
                    suffix = " [manual]" if is_manual else ""
                    item.setText(0, f"{v.version_string}{suffix} \u25bc! \u25c0")
                    color = QColor("#cc8833")
                else:
                    # User deliberately promoted a lower version — muted green
                    suffix = " [manual]" if is_manual else ""
                    item.setText(0, f"{v.version_string}{suffix}* \u25c0")
                    color = QColor("#7abbe0")
                for col in range(7):
                    item.setForeground(col, color)

            # Highlight timecode changes vs current promoted version
            if (current_tc and v.start_timecode
                    and v.start_timecode != current_tc
                    and v.version_string != current_ver):
                item.setForeground(5, QColor("#ff9944"))

            self.version_tree.addTopLevelItem(item)

        self.version_tree.itemSelectionChanged.connect(self._on_version_selected)

        # Populate history
        if promoter:
            history = promoter.get_history()
            for i, h in enumerate(history):
                item = QTreeWidgetItem([
                    h.set_at,
                    h.version,
                    h.set_by,
                    h.frame_range or "\u2014",
                    h.start_timecode or "\u2014",
                    str(h.file_count),
                ])
                item.setData(0, Qt.UserRole, h)
                if i == 0:
                    for col in range(6):
                        item.setForeground(col, QColor("#4ec9a0"))
                self.history_tree.addTopLevelItem(item)

        self.history_tree.itemSelectionChanged.connect(self._on_history_selected)

    _PROMOTE_STYLE = (
        "QPushButton { background-color: #336699; color: white; padding: 8px 16px; "
        "border-radius: 4px; font-weight: bold; font-size: 13pt; }"
        "QPushButton:hover { background-color: #4d7aae; }"
        "QPushButton:disabled { background-color: #2a2a2a; color: #8c8c8c; }"
    )
    _KEEP_STYLE = (
        "QPushButton { background-color: #1e3a5a; color: white; padding: 8px 16px; "
        "border-radius: 4px; font-weight: bold; font-size: 13pt; }"
        "QPushButton:hover { background-color: #2a5070; }"
        "QPushButton:disabled { background-color: #2a2a2a; color: #8c8c8c; }"
    )

    def _on_version_selected(self):
        items = self.version_tree.selectedItems()
        has_selection = len(items) > 0 and self._worker is None
        self.btn_promote.setEnabled(has_selection)

        if has_selection:
            version: VersionInfo = items[0].data(0, Qt.UserRole)
            source = self._current_source
            promoter = self._promoters.get(source.name) if source else None
            current = promoter.get_current_version() if promoter else None

            if current and version.version_string == current.version:
                self.btn_promote.setText("Keep This Version")
                self.btn_promote.setStyleSheet(self._KEEP_STYLE)
            else:
                self.btn_promote.setText("Promote Selected to Latest")
                self.btn_promote.setStyleSheet(self._PROMOTE_STYLE)
        else:
            self.btn_promote.setText("Promote Selected to Latest")
            self.btn_promote.setStyleSheet(self._PROMOTE_STYLE)

    def _on_history_selected(self):
        items = self.history_tree.selectedItems()
        self.btn_revert.setEnabled(len(items) > 0 and self._worker is None)

    # --- Manual version import ---

    def _get_next_manual_version_number(self, source_name: str) -> int:
        """Determine the next version number for a manual import."""
        scanned = self._versions_cache.get(source_name, [])
        manual = self._manual_versions.get(source_name, [])
        all_versions = scanned + manual
        if all_versions:
            return max(v.version_number for v in all_versions) + 1
        return 1

    def _add_manual_version(self, source: WatchedSource, paths: list[Path]):
        """Process dropped/browsed paths and add as manual versions."""
        extensions = source.file_extensions
        added = 0

        for p in paths:
            if p.is_dir():
                files, frame_range, frame_count = scan_directory_as_version(p, extensions)
                if not files:
                    continue
                total_size = sum(f.stat().st_size for f in files)
                ver_num = self._get_next_manual_version_number(source.name)
                version = create_manual_version(
                    source_path=str(p),
                    version_number=ver_num,
                    file_count=len(files),
                    total_size_bytes=total_size,
                    frame_range=frame_range,
                    frame_count=frame_count,
                )
                self._manual_versions.setdefault(source.name, []).append(version)
                added += 1
            elif p.is_file():
                if p.suffix.lower() not in [e.lower() for e in extensions]:
                    continue
                files, frame_range, frame_count = detect_sequence_from_file(p, extensions)
                if not files:
                    continue
                total_size = sum(f.stat().st_size for f in files)
                # source_path is parent dir for sequences, file path for single files
                if len(files) > 1:
                    src_path = str(p.parent)
                else:
                    src_path = str(p)
                ver_num = self._get_next_manual_version_number(source.name)
                version = create_manual_version(
                    source_path=src_path,
                    version_number=ver_num,
                    file_count=len(files),
                    total_size_bytes=total_size,
                    frame_range=frame_range,
                    frame_count=frame_count,
                )
                self._manual_versions.setdefault(source.name, []).append(version)
                added += 1

        if added:
            # Persist manual versions to project config
            self._persist_manual_versions(source.name)
            # Refresh the version display
            current_item = self.source_list.currentItem()
            if current_item:
                source_name = current_item.data(0, Qt.UserRole)
                if source_name:
                    self._on_source_selected_by_name(source_name)
            self.statusBar().showMessage(
                f"Imported {added} manual version{'s' if added != 1 else ''}"
            )

    def _persist_manual_versions(self, source_name: str):
        """Save manual versions for a source into the project config on disk."""
        if not self.config or not self.config_path:
            return
        manual = self._manual_versions.get(source_name, [])
        for source in self.config.watched_sources:
            if source.name == source_name:
                source.manual_versions = [v.to_dict() for v in manual]
                break
        save_config(self.config, self.config_path)

    def _import_version(self):
        """Open a file browser to import an external version."""
        source = self._current_source
        if not source:
            return

        # Build extension filter
        exts = source.file_extensions
        ext_str = " ".join(f"*{e}" for e in exts)
        filter_str = f"Media Files ({ext_str});;All Files (*)"

        filepath, _ = QFileDialog.getOpenFileName(
            self,
            "Import Version — Select a file (sequences auto-detected)",
            "",
            filter_str,
        )
        if not filepath:
            return

        self._add_manual_version(source, [Path(filepath)])

    def _handle_version_drop(self, paths: list[Path]):
        """Handle files/directories dropped onto the version tree."""
        source = self._current_source
        if not source:
            self.statusBar().showMessage("Select a source before dropping files")
            return
        self._add_manual_version(source, paths)

    # --- Promotion ---

    def _ensure_latest_path(self, source: WatchedSource) -> bool:
        """Ensure the source has a latest_target. Shows dialog if not set.

        Returns True if a latest path is available, False if the user cancelled.
        """
        if source.latest_target:
            return True

        # Show the latest path dialog
        dlg = LatestPathDialog(self.config, source=source, parent=self)
        if dlg.exec() != QDialog.Accepted:
            return False

        # Apply the template to the project config
        self.config.latest_path_template = dlg.get_template()
        self.config.default_file_rename_template = dlg.get_rename_template()
        self._mark_dirty()
        apply_project_defaults(self.config)

        # Save and rebuild promoters
        if self.config_path:
            self._save_project()
        self._reload_ui()
        return True

    def _promote_selected(self):
        items = self.version_tree.selectedItems()
        if not items or not self._current_source:
            return

        self._force_promote = False
        version: VersionInfo = items[0].data(0, Qt.UserRole)
        source = self._current_source

        # Ensure latest path is set
        if not self._ensure_latest_path(source):
            return

        # Re-fetch promoter after possible reload
        promoter = self._promoters.get(source.name)
        if not promoter:
            return

        # Check for incomplete sequences (Feature #11)
        from src.lvm.promoter import has_frame_gaps
        block_incomplete = getattr(source, 'block_incomplete_sequences', False) or getattr(self.config, 'block_incomplete_sequences', False)
        if block_incomplete and has_frame_gaps(version):
            reply = QMessageBox.warning(
                self, "Incomplete Sequence",
                f"Sequence has frame gaps: {version.frame_range}\n\nPromote anyway?",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply != QMessageBox.Yes:
                return
            self._force_promote = True

        # Detect "keep" vs normal promote
        current = promoter.get_current_version()
        is_keep = current and version.version_string == current.version

        if is_keep:
            msg = (
                f"Keep {source.name} at {version.version_string}?\n\n"
                f"This marks {version.version_string} as the deliberate choice, "
                f"so newer versions won't be flagged as missed updates."
            )
            reply = QMessageBox.question(self, "Confirm Keep", msg)
            if reply != QMessageBox.Yes:
                return
        else:
            # Show dry-run preview dialog
            dry_run_data = promoter.dry_run(version)
            dlg = DryRunDialog(dry_run_data, version, source, current, parent=self)
            if dlg.exec() != QDialog.Accepted:
                return

        # Check for obsolete layers
        keep_layers = None
        if not is_keep:
            obsolete = promoter.detect_obsolete_layers(version)
            if obsolete:
                dlg = ObsoleteLayerDialog(
                    source.name, version.version_string,
                    obsolete, conflict_count=1, parent=self,
                )
                if dlg.exec() != QDialog.Accepted:
                    return
                if dlg.choice == ObsoleteLayerDialog.SKIP:
                    return
                if dlg.choice == ObsoleteLayerDialog.KEEP:
                    keep_layers = {layer["prefix"] for layer in obsolete}

        self._pinned_promote = is_keep
        self._start_promotion(promoter, version, keep_layers=keep_layers)

    def _revert_selected(self):
        """Revert to a version from history by re-scanning and promoting."""
        items = self.history_tree.selectedItems()
        if not items or not self._current_source:
            return

        entry: HistoryEntry = items[0].data(0, Qt.UserRole)
        source = self._current_source
        scanner = self._scanners.get(source.name)
        promoter = self._promoters.get(source.name)
        if not scanner or not promoter:
            return

        # Find the version in current scan results
        versions = self._versions_cache.get(source.name, scanner.scan())
        target_version = None
        for v in versions:
            if v.version_string == entry.version:
                target_version = v
                break

        if not target_version:
            QMessageBox.warning(
                self, "Cannot Revert",
                f"Version {entry.version} no longer exists in the source directory.\n"
                f"Original path: {entry.source}"
            )
            return

        reply = QMessageBox.question(
            self, "Confirm Revert",
            f"Revert {source.name} back to {entry.version}?\n\n"
            f"This will overwrite the current latest files."
        )
        if reply != QMessageBox.Yes:
            return

        self._start_promotion(promoter, target_version)

    def _start_promotion(self, promoter: Promoter, version: VersionInfo,
                          keep_layers: set[str] | None = None):
        """Start the promotion in a background thread, checking link mode availability first."""
        self._promoting_source_name = promoter.source.name
        self._promoting_version = version
        mode = promoter.source.link_mode
        available, reason = check_link_mode_available(mode)
        if not available and mode == "symlink":
            reply = QMessageBox.question(
                self, "Elevation Required",
                f"{reason}\n\nRestart with Administrator privileges?",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                if restart_elevated():
                    QApplication.quit()
                    return
                else:
                    QMessageBox.warning(self, "Elevation Failed",
                                        "Could not restart with elevated privileges.\n"
                                        "The UAC prompt may have been declined.")
            return
        elif not available:
            QMessageBox.warning(self, "Link Mode Unavailable", reason)
            return

        self.btn_promote.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.btn_cancel_promote.setVisible(True)
        self.btn_cancel_promote.setEnabled(True)
        self.btn_cancel_promote.setText("Cancel")

        pinned = getattr(self, '_pinned_promote', False)
        self._pinned_promote = False

        self._worker = PromoteWorker(promoter, version, self, force=self._force_promote,
                                     pinned=pinned, keep_layers=keep_layers)
        self._worker.progress.connect(self._on_promote_progress)
        self._worker.finished.connect(self._on_promote_finished)
        self._worker.error.connect(self._on_promote_error)
        self._worker.start()

    def _on_promote_progress(self, current, total, filename):
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)
        self.progress_bar.setFormat(f"{current}/{total} \u2014 {filename}")

    def _cancel_promotion(self):
        """Request cancellation of the running promotion."""
        if self._worker:
            self._worker.cancel()
            self.btn_cancel_promote.setEnabled(False)
            self.btn_cancel_promote.setText("Cancelling...")
            self.statusBar().showMessage("Cancelling promotion...")

    def _on_promote_finished(self, entry):
        self._worker = None
        self.progress_bar.setVisible(False)
        self.btn_cancel_promote.setVisible(False)

        # Restore original link_mode if we fell back to copy
        if self._fallback_original_mode and self._current_source:
            self._current_source.link_mode = self._fallback_original_mode
            self._fallback_original_mode = None

        # Check if this is part of a batch promotion
        if hasattr(self, '_batch_promote_list') and self._batch_promote_list:
            # Guard: _current_source may have been cleared by a concurrent
            # background refresh — use _promoting_source_name as fallback.
            source_name = (
                self._current_source.name if self._current_source
                else self._promoting_source_name or "unknown"
            )
            self._versions_cache.pop(source_name, None)
            self._batch_promote_index += 1
            self._batch_promote_next()
            return

        promoted_name = self._promoting_source_name or (
            self._current_source.name if self._current_source else "unknown"
        )
        self._promoting_source_name = None
        self.statusBar().showMessage(
            f"Promoted {promoted_name} \u2192 {entry.version}"
        )
        # Rescan only the promoted source instead of all sources
        self._versions_cache.pop(promoted_name, None)
        self._process_deferred_or_refresh([promoted_name], select_source=promoted_name)

    def _on_promote_error(self, error_msg):
        self._worker = None
        error_source_name = self._promoting_source_name
        self._promoting_source_name = None
        self.progress_bar.setVisible(False)
        self.btn_cancel_promote.setVisible(False)
        self.btn_promote.setEnabled(True)

        # Restore original link_mode if we fell back to copy
        if self._fallback_original_mode and self._current_source:
            self._current_source.link_mode = self._fallback_original_mode
            self._fallback_original_mode = None

        # Check for symlink/hardlink failure — offer fallback options
        symlink_failed = "Symlink creation failed" in error_msg
        hardlink_failed = "Hardlink creation failed" in error_msg
        if symlink_failed or hardlink_failed:
            source = self._current_source
            version = self._promoting_version
            if source and version:
                promoter = self._promoters.get(source.name)
                if promoter:
                    mode_label = source.link_mode.title()
                    dlg = QMessageBox(self)
                    dlg.setWindowTitle("Link Mode Failed")
                    dlg.setIcon(QMessageBox.Warning)
                    dlg.setText(
                        f"{mode_label} creation failed for '{source.name}'.\n\n"
                        f"This is common on network/UNC paths where the server "
                        f"doesn't support {source.link_mode}s.\n\n"
                        f"Retry with a different mode?"
                    )
                    copy_btn = dlg.addButton("Copy", QMessageBox.AcceptRole)
                    hardlink_btn = None
                    if symlink_failed:
                        hardlink_btn = dlg.addButton("Hardlink", QMessageBox.AcceptRole)
                    dlg.addButton(QMessageBox.Cancel)
                    dlg.exec()
                    clicked = dlg.clickedButton()
                    fallback_mode = None
                    if clicked == copy_btn:
                        fallback_mode = "copy"
                    elif hardlink_btn and clicked == hardlink_btn:
                        fallback_mode = "hardlink"
                    if fallback_mode:
                        self._fallback_original_mode = source.link_mode
                        source.link_mode = fallback_mode
                        self._start_promotion(promoter, version)
                        return

        # If batch promotion, ask whether to continue
        if hasattr(self, '_batch_promote_list') and self._batch_promote_list:
            source_name = (
                self._current_source.name if self._current_source
                else error_source_name or "Unknown"
            )
            reply = QMessageBox.warning(
                self, "Promotion Failed",
                f"Failed to promote {source_name}:\n{error_msg}\n\nContinue with remaining sources?",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                self._batch_promote_index += 1
                self._batch_promote_next()
            else:
                self._batch_promote_list = []
                self._batch_keep_layers = {}
                self._process_deferred_or_refresh([], select_source=None)
            return

        QMessageBox.critical(self, "Promotion Failed", error_msg)
        # Apply any deferred refresh results that accumulated during the failed promotion
        if self._deferred_refresh_results is not None:
            self._process_deferred_or_refresh([], select_source=None)

    # --- File Watcher ---

    def _toggle_watcher(self):
        if self.watcher.is_running:
            self.watcher.stop()
            self.watch_toggle.setText("Start Watching")
            self.watch_toggle.setChecked(False)
            self.auto_promote_cb.setEnabled(False)
        else:
            if self.config:
                self.watcher.start(self.config.watched_sources)
                self.watch_toggle.setText("Stop Watching")
                self.watch_toggle.setChecked(True)
                self.auto_promote_cb.setEnabled(True)

    def _on_watcher_change(self, source_name: str):
        """A watched source had new files — refresh only that source."""
        logger.info(f"Watcher detected changes in: {source_name}")
        self._versions_cache.pop(source_name, None)

        # Refresh only the changed source instead of all sources
        if self.config:
            self._refresh_sources_by_name([source_name])

        self.statusBar().showMessage(f"New version detected in: {source_name}")

        # Attempt auto-promotion if enabled
        self._try_auto_promote(source_name)

    def _on_watch_status(self, status: str):
        self.statusBar().showMessage(status)

    @staticmethod
    def _normalize_frame_range(frame_range):
        """Extract core frame range, stripping gap annotations.

        '1001-1120 (95/120 frames, gaps detected)' -> '1001-1120'
        '1001-1120' -> '1001-1120'
        None -> None
        """
        if frame_range is None:
            return None
        return frame_range.split(" ")[0].split("(")[0].strip()

    def _try_auto_promote(self, source_name: str):
        """Attempt auto-promotion for a source after watcher detected changes.

        Auto-promotes only when:
        - The Auto-Promote checkbox is checked
        - No promotion is already in progress
        - The source has a previous promotion (history entry)
        - A newer highest version exists
        - The new version's frame range matches the last promoted version
        """
        if not self.auto_promote_cb.isChecked():
            return

        if self._worker is not None:
            logger.info(f"Auto-promote skipped for {source_name}: promotion already in progress")
            self.statusBar().showMessage(
                f"Auto-promote skipped for {source_name}: promotion already in progress"
            )
            return

        # Find promoter (only exists for sources with a latest_target)
        promoter = self._promoters.get(source_name)
        if not promoter:
            return

        # Re-scan to pick up the new version
        scanner = self._scanners.get(source_name)
        if not scanner:
            return

        versions = scanner.scan()
        self._versions_cache[source_name] = versions
        if not versions:
            return

        highest = versions[-1]

        # Check last promoted version
        current_entry = promoter.get_current_version()
        if not current_entry:
            logger.info(f"Auto-promote skipped for {source_name}: no previous promotion (promote manually first)")
            self.statusBar().showMessage(
                f"Auto-promote skipped for {source_name}: no previous promotion exists"
            )
            return

        if current_entry.version == highest.version_string:
            return  # Already on highest

        # Compare frame ranges (normalized to strip gap annotations)
        prev_range = self._normalize_frame_range(current_entry.frame_range)
        new_range = self._normalize_frame_range(highest.frame_range)

        if prev_range != new_range:
            msg = (
                f"Auto-promote skipped for {source_name}: "
                f"frame range changed ({prev_range} \u2192 {new_range})"
            )
            logger.info(msg)
            self.statusBar().showMessage(msg)
            return

        # Check for obsolete layers (cannot show interactive dialog in auto path)
        obsolete = promoter.detect_obsolete_layers(highest)
        if obsolete:
            layer_names = ", ".join(l["name"] for l in obsolete)
            msg = (
                f"Auto-promote skipped for {source_name}: "
                f"obsolete layers in target ({layer_names})"
            )
            logger.info(msg)
            self.statusBar().showMessage(msg)
            return

        # Pre-check link mode (avoid modal dialogs in auto-promote path)
        mode = promoter.source.link_mode
        available, reason = check_link_mode_available(mode)
        if not available:
            logger.warning(f"Auto-promote skipped for {source_name}: {reason}")
            self.statusBar().showMessage(
                f"Auto-promote skipped for {source_name}: {reason}"
            )
            return

        # All checks passed — auto-promote
        logger.info(f"Auto-promoting {source_name}: {highest.version_string}")
        self.statusBar().showMessage(
            f"Auto-promoting {source_name} to {highest.version_string}..."
        )
        self._start_promotion(promoter, highest)

    # --- Log viewer helpers (Feature #19) ---

    _LOG_COLORS = {
        "DEBUG": "#8c8c8c",
        "INFO": "#cccccc",
        "WARNING": "#ffaa00",
        "ERROR": "#ff4444",
        "CRITICAL": "#ff0000",
    }

    def _append_log_entry(self, level: str, message: str):
        import html as _html
        color = self._LOG_COLORS.get(level, "#cccccc")
        min_level = self.log_level_filter.currentText()
        level_order = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if min_level != "ALL":
            if level_order.index(level) < level_order.index(min_level):
                return
        self.log_text.appendHtml(f'<span style="color:{color}">{_html.escape(message)}</span>')

    def _filter_log(self):
        self.log_text.clear()
        for level, msg in self._log_handler.get_buffer():
            self._append_log_entry(level, msg)

    def _clear_log(self):
        self.log_text.clear()
        self._log_handler.clear_buffer()

    def _copy_log(self):
        QApplication.clipboard().setText(self.log_text.toPlainText())

    # --- Thumbnail/Preview helpers (Feature #7) ---

    def _toggle_preview_panel(self, checked):
        """Show/hide the preview panel. Triggers thumbnail load if becoming visible."""
        self.thumbnail_label.setVisible(checked)
        self._preview_toggle.setText("\u25bc Preview" if checked else "\u25b6 Preview")
        if checked:
            # Expand the splitter to show the preview
            sizes = self._ver_content_splitter.sizes()
            if sizes[1] < 160:
                self._ver_content_splitter.setSizes([600, 200])
            # Trigger thumbnail load for currently selected version
            current = self.version_tree.currentItem()
            if current:
                self._on_version_selected_thumbnail(current, None)
        else:
            self._ver_content_splitter.setSizes([600, 24])

    def _on_version_selected_thumbnail(self, current, previous):
        # Only load thumbnails when preview panel is visible
        if not self.thumbnail_label.isVisible():
            return

        if not current:
            self.thumbnail_label.setPixmap(QPixmap())
            self.thumbnail_label.setText("No Preview")
            return

        version = current.data(0, Qt.UserRole)
        if not version or not self._current_source:
            return

        cache_dir = ""
        if self.config_path:
            cache_dir = str(Path(self.config_path).parent / ".lvm_cache")
        if not cache_dir:
            return

        self._thumb_worker = ThumbnailWorker(
            version.source_path, version.version_string,
            self._current_source.file_extensions, cache_dir, self
        )
        self._thumb_worker.finished.connect(self._on_thumbnail_ready)
        self._thumb_worker.start()

    def _on_thumbnail_ready(self, thumb_path):
        if thumb_path:
            pixmap = QPixmap(thumb_path)
            if not pixmap.isNull():
                scaled = pixmap.scaled(
                    self.thumbnail_label.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation
                )
                self.thumbnail_label.setPixmap(scaled)
                self.thumbnail_label.setText("")
                return
        self.thumbnail_label.setPixmap(QPixmap())
        self.thumbnail_label.setText("No Preview")

    # --- State persistence ---

    def _restore_state(self):
        last_project = self._settings.value("last_project", None)
        if last_project and os.path.exists(last_project):
            self._load_project(last_project)

    def closeEvent(self, event):
        # Prompt to save unsaved changes
        if self._dirty and self.config_path:
            reply = QMessageBox.question(
                self, "Unsaved Changes",
                "You have unsaved changes. Save before closing?",
                QMessageBox.Save | QMessageBox.Discard | QMessageBox.Cancel,
                QMessageBox.Save,
            )
            if reply == QMessageBox.Cancel:
                event.ignore()
                return
            if reply == QMessageBox.Save:
                self._save_project()

        if self.config_path:
            self._settings.setValue("last_project", self.config_path)

        # Disconnect signals and stop all background workers to avoid
        # callbacks firing into a half-destroyed window.
        for worker in (self._scan_worker, self._status_worker,
                        self._worker, self._thumb_worker):
            if worker is not None:
                try:
                    worker.disconnect()
                except RuntimeError:
                    pass
                if worker.isRunning():
                    worker.quit()
                    worker.wait(2000)

        self._io_executor.shutdown(wait=False)
        self.watcher.stop()
        event.accept()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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

    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
