"""
Latest Version Manager - PySide6 GUI Application.
"""

import os
import sys
import json
import logging
from pathlib import Path

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QListWidget, QListWidgetItem, QTreeWidget, QTreeWidgetItem,
    QPushButton, QLabel, QStatusBar, QFileDialog, QMessageBox,
    QProgressBar, QGroupBox, QCheckBox, QLineEdit, QComboBox,
    QDialog, QFormLayout, QDialogButtonBox, QHeaderView, QMenu,
    QToolBar, QSizePolicy, QFrame, QAbstractItemView,
    QColorDialog, QInputDialog, QStyledItemDelegate, QStyle,
)
from PySide6.QtCore import Qt, QThread, Signal, QSize, QSettings, QUrl, QMimeData
from PySide6.QtGui import QAction, QFont, QColor, QIcon, QPalette, QPainter, QPen, QBrush, QFontMetrics, QPixmap
from PySide6.QtSvg import QSvgRenderer

from src.lvm.models import ProjectConfig, WatchedSource, VersionInfo, HistoryEntry, make_relative, DEFAULT_FILE_EXTENSIONS
from src.lvm.config import load_config, save_config, create_example_config, create_project, apply_project_defaults, _expand_group_token, _resolve_group_root
from src.lvm.scanner import VersionScanner, detect_sequence_from_file, scan_directory_as_version, create_manual_version
from src.lvm.promoter import Promoter, PromotionError, generate_report
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
# Worker thread for promotions (so the UI doesn't freeze during copy)
# ---------------------------------------------------------------------------

class PromoteWorker(QThread):
    """Runs the file copy in a background thread."""
    progress = Signal(int, int, str)   # current, total, filename
    finished = Signal(object)          # HistoryEntry on success
    error = Signal(str)                # error message

    def __init__(self, promoter: Promoter, version: VersionInfo, parent=None):
        super().__init__(parent)
        self.promoter = promoter
        self.version = version

    def run(self):
        try:
            entry = self.promoter.promote(
                self.version,
                progress_callback=self._on_progress,
            )
            self.finished.emit(entry)
        except PromotionError as e:
            self.error.emit(str(e))
        except Exception as e:
            self.error.emit(f"Unexpected error: {e}")

    def _on_progress(self, current, total, filename):
        self.progress.emit(current, total, filename)


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
        header.setStyleSheet("font-size: 13px; padding: 4px;")
        layout.addWidget(header)

        target_label = QLabel(f"Target: {dry_run_data['target_dir']}")
        target_label.setStyleSheet("color: #aaa; font-size: 11px; padding: 0 4px 4px;")
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
                warn_label.setStyleSheet("color: #ffaa00; font-size: 12px;")
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
                tree_item.setForeground(1, QColor("#90ee90"))
            self.tree.addTopLevelItem(tree_item)

        layout.addWidget(self.tree)

        # Keep / Replace info
        if current:
            replace_label = QLabel(
                f"This will replace the current version ({current.version})."
            )
            replace_label.setStyleSheet("color: #cc8833; font-size: 11px; padding: 4px;")
            layout.addWidget(replace_label)

        # Buttons
        btn_box = QDialogButtonBox()
        self.btn_promote = btn_box.addButton("Promote", QDialogButtonBox.AcceptRole)
        self.btn_promote.setStyleSheet(
            "QPushButton { background-color: #2d5a2d; color: white; padding: 8px 20px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #3a7a3a; }"
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
        override_label.setStyleSheet("color: #999; font-size: 11px; margin-top: 8px;")
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

            self.override_link_mode_check.setChecked(source.override_link_mode)
            self.link_mode_combo.setCurrentText(source.link_mode)
        else:
            # New source — start with defaults, overrides off
            self.override_latest_check.setChecked(False)
            self.override_pattern_check.setChecked(False)
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
        return WatchedSource(
            name=self.name_edit.text().strip() or "Untitled",
            source_dir=self.source_dir_edit.text().strip(),
            version_pattern=self.pattern_edit.text().strip() or (pc.default_version_pattern if pc else "_v{version}"),
            file_extensions=exts,
            latest_target=self.target_dir_edit.text().strip(),
            link_mode=self.link_mode_combo.currentText(),
            override_version_pattern=self.override_pattern_check.isChecked(),
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

        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("My VFX Project")
        layout.addRow("Project Name:", self.name_edit)

        # Project Root — the logical root of the project
        self.root_edit = QLineEdit()
        self.root_browse_btn = QPushButton("Browse...")
        self.root_browse_btn.clicked.connect(self._browse_root)
        root_row = QHBoxLayout()
        root_row.addWidget(self.root_edit, 1)
        root_row.addWidget(self.root_browse_btn)
        root_help = QLabel("The root directory of the project (used for {project_root} token).")
        root_help.setStyleSheet("color: #999; font-size: 11px;")
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
        save_help.setStyleSheet("color: #999; font-size: 11px;")
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
        filter_label.setStyleSheet("color: #999; font-size: 11px; margin-top: 6px;")
        layout.addRow("", filter_label)

        self.whitelist_edit = QLineEdit()
        self.whitelist_edit.setPlaceholderText("comp, grade, final")
        layout.addRow("Whitelist:", self.whitelist_edit)

        self.blacklist_edit = QLineEdit()
        self.blacklist_edit.setPlaceholderText("denoise, prerender, wip, temp")
        layout.addRow("Blacklist:", self.blacklist_edit)

        # Task tokens
        task_sep = QLabel("Task Names")
        task_sep.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addRow("", task_sep)

        task_help = QLabel(
            "Common task names in your pipeline that can be stripped\n"
            "from filenames. Use % as a counted wildcard (e.g. comp_%% matches comp_mp)."
        )
        task_help.setStyleSheet("color: #999; font-size: 11px;")
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
        wl = [kw.strip() for kw in self.whitelist_edit.text().split(",") if kw.strip()]
        bl = [kw.strip() for kw in self.blacklist_edit.text().split(",") if kw.strip()]
        tasks = [t.strip() for t in self.tasks_edit.text().split(",") if t.strip()]
        root = self.root_edit.text().strip()
        save = self.save_edit.text().strip() or root
        return {
            "project_name": self.name_edit.text().strip() or "Untitled",
            "project_root": root,
            "save_dir": save,
            "project_dir": save,  # backwards compat — save location
            "name_whitelist": wl,
            "name_blacklist": bl,
            "task_tokens": tasks,
        }


# ---------------------------------------------------------------------------
# Project Settings Dialog
# ---------------------------------------------------------------------------

class ProjectSettingsDialog(QDialog):
    """Dialog for editing project-wide settings."""

    def __init__(self, config: ProjectConfig, selected_source: WatchedSource = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Project Settings")
        self.setMinimumWidth(600)
        self._config = config
        self._selected_source = selected_source

        layout = QFormLayout(self)

        self.name_edit = QLineEdit(config.project_name)
        layout.addRow("Project Name:", self.name_edit)

        # Project Root
        self.root_edit = QLineEdit(config.effective_project_root)
        self.root_edit.textChanged.connect(self._update_path_preview)
        self.root_browse_btn = QPushButton("Browse...")
        self.root_browse_btn.clicked.connect(self._browse_root)
        root_row = QHBoxLayout()
        root_row.addWidget(self.root_edit, 1)
        root_row.addWidget(self.root_browse_btn)
        root_help = QLabel("The root directory of the project (used for {project_root} token).")
        root_help.setStyleSheet("color: #999; font-size: 11px;")
        layout.addRow("Project Root:", root_row)
        layout.addRow("", root_help)

        # Latest path template
        template_help = QLabel(
            "Relative paths resolve from each source's directory.\n"
            "Tokens: {source_dir}, {source_name}, {source_basename},\n"
            "{source_fullname}, {source_filename}, {project_root}, {group}, {group_root}\n"
            "If {group} is empty, the token and its trailing divider are omitted.\n"
            "{group_root} resolves to the group's root directory (falls back to {project_root}).\n"
            "Examples: {group_root}/online/{source_name}  |  latest/{group}/{source_basename}_latest"
        )
        template_help.setStyleSheet("color: #999; font-size: 11px;")
        layout.addRow("", template_help)

        self.latest_template_edit = QLineEdit(config.latest_path_template)
        self.latest_template_edit.setPlaceholderText("latest/{source_basename}_latest")
        self.latest_template_edit.textChanged.connect(self._update_path_preview)
        layout.addRow("Latest Path Template:", self.latest_template_edit)

        # File rename template
        rename_help = QLabel(
            "Controls the output filename (without frame/ext).\n"
            "Tokens: {source_name}, {source_basename}, {source_fullname}, {group}"
        )
        rename_help.setStyleSheet("color: #999; font-size: 11px;")
        layout.addRow("", rename_help)

        self.rename_template_edit = QLineEdit(config.default_file_rename_template)
        self.rename_template_edit.setPlaceholderText("{source_basename}_latest")
        self.rename_template_edit.textChanged.connect(self._update_path_preview)
        layout.addRow("File Rename Template:", self.rename_template_edit)

        # Combined preview (path + filename)
        self.path_preview_label = QLabel("")
        self.path_preview_label.setStyleSheet("color: #88cc88; font-size: 11px;")
        self.path_preview_label.setWordWrap(True)
        layout.addRow("Resolved Preview:", self.path_preview_label)

        # Default version pattern
        self.pattern_edit = QLineEdit(config.default_version_pattern)
        layout.addRow("Default Version Pattern:", self.pattern_edit)

        # Default file extensions
        self.extensions_edit = QLineEdit(" ".join(config.default_file_extensions))
        layout.addRow("Default File Extensions:", self.extensions_edit)

        # Default link mode
        self.link_mode_combo = QComboBox()
        self.link_mode_combo.addItems(["copy", "hardlink", "symlink"])
        self.link_mode_combo.setCurrentText(config.default_link_mode)
        layout.addRow("Default Link Mode:", self.link_mode_combo)

        # Timecode mode
        self.timecode_combo = QComboBox()
        self.timecode_combo.addItems(["always", "lazy", "never"])
        self.timecode_combo.setCurrentText(config.timecode_mode)
        tc_help = QLabel(
            "Always: read timecodes during scan (slower, all TCs visible immediately)\n"
            "Lazy: read timecodes when a source is viewed (fast scan, TCs on demand)\n"
            "Never: skip timecode extraction entirely (fastest)"
        )
        tc_help.setStyleSheet("color: #999; font-size: 11px;")
        layout.addRow("", tc_help)
        layout.addRow("Timecode Mode:", self.timecode_combo)

        # Task tokens section
        task_sep = QLabel("Task Names")
        task_sep.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addRow("", task_sep)

        task_help = QLabel(
            "Task names are stripped from filenames to produce cleaner source names.\n"
            "Each % matches exactly one character (e.g. comp_%% matches comp_mp).\n"
            "Tokens are bounded by dividers: _ - ."
        )
        task_help.setStyleSheet("color: #999; font-size: 11px;")
        layout.addRow("", task_help)

        self.tasks_edit = QLineEdit(", ".join(config.task_tokens))
        self.tasks_edit.setPlaceholderText("comp, grade, dmp, fx, roto, paint")
        self.tasks_edit.textChanged.connect(self._update_path_preview)
        layout.addRow("Task Names:", self.tasks_edit)

        # Now that all fields affecting the preview exist, compute initial preview
        self._update_path_preview()

        # Naming rule display
        naming_sep = QLabel("Source Naming")
        naming_sep.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addRow("", naming_sep)

        naming_row = QHBoxLayout()
        rule_display = config.default_naming_rule or "(not yet configured — set on first ingest)"
        self.naming_label = QLabel(rule_display)
        self.naming_label.setStyleSheet("color: #ccc;")
        naming_row.addWidget(self.naming_label, 1)
        self.reset_naming_btn = QPushButton("Reset")
        self.reset_naming_btn.setToolTip("Reset naming convention so it is re-asked on next ingest")
        self.reset_naming_btn.clicked.connect(self._reset_naming)
        naming_row.addWidget(self.reset_naming_btn)
        layout.addRow("Naming Rule:", naming_row)

        self._naming_reset = False

        # Filters section
        filter_sep = QLabel("Discovery Filters")
        filter_sep.setStyleSheet("font-weight: bold; margin-top: 10px;")
        layout.addRow("", filter_sep)

        self.whitelist_edit = QLineEdit(", ".join(config.name_whitelist))
        self.whitelist_edit.setPlaceholderText("comp, grade, final")
        layout.addRow("Whitelist:", self.whitelist_edit)

        self.blacklist_edit = QLineEdit(", ".join(config.name_blacklist))
        self.blacklist_edit.setPlaceholderText("denoise, prerender, wip, temp")
        layout.addRow("Blacklist:", self.blacklist_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)

    def _update_path_preview(self):
        """Update the resolved path preview based on the current templates.

        If a source was selected when opening the dialog, preview that source.
        Otherwise, preview the last 3 added sources.
        """
        tpl = self.latest_template_edit.text().strip()
        rename_tpl = self.rename_template_edit.text().strip() or "{source_basename}_latest"
        if not tpl:
            self.path_preview_label.setText("(no template set)")
            self.path_preview_label.setStyleSheet("color: #888; font-size: 11px;")
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
                tokens = derive_source_tokens(source.sample_filename or source.name, live_task_tokens)
                resolved = tpl
                resolved = resolved.replace("{project_root}", live_root)
                resolved = resolved.replace("{group_root}", _resolve_group_root(config, source.group) or "<project_root>")
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
                rename_resolved = rename_resolved.replace("{source_name}", tokens["source_name"])
                rename_resolved = rename_resolved.replace("{source_basename}", tokens["source_basename"])
                rename_resolved = rename_resolved.replace("{source_fullname}", tokens["source_fullname"])
                rename_resolved = _expand_group_token(rename_resolved, source.group)
                sample_file = f"{rename_resolved}.####.exr"
                group_tag = f" [{source.group}]" if source.group else ""
                previews.append(f"{source.name}{group_tag}: {dir_str}\\{sample_file}")
            if not self._selected_source and len(config.watched_sources) > 3:
                previews.append(f"... and {len(config.watched_sources) - 3} more")
        else:
            resolved = tpl
            resolved = resolved.replace("{project_root}", live_root)
            resolved = resolved.replace("{group_root}", "<group_root>")
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
            rename_resolved = rename_resolved.replace("{source_name}", "<source_name>")
            rename_resolved = rename_resolved.replace("{source_basename}", "<source_basename>")
            rename_resolved = rename_resolved.replace("{source_fullname}", "<source_fullname>")
            rename_resolved = rename_resolved.replace("{group}", "<group>")
            sample_file = f"{rename_resolved}.####.exr"
            previews.append(f"{dir_str}\\{sample_file}")

        self.path_preview_label.setText("\n".join(previews))
        self.path_preview_label.setStyleSheet("color: #88cc88; font-size: 11px;")

    def _browse_root(self):
        start = self.root_edit.text().strip()
        d = QFileDialog.getExistingDirectory(self, "Select Project Root", start)
        if d:
            self.root_edit.setText(d)

    def _reset_naming(self):
        """Reset naming convention so it will be re-asked on next discovery ingest."""
        self._naming_reset = True
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

        exts = self.extensions_edit.text().strip().split()
        config.default_file_extensions = exts if exts else list(DEFAULT_FILE_EXTENSIONS)

        config.default_link_mode = self.link_mode_combo.currentText()
        config.timecode_mode = self.timecode_combo.currentText()

        config.task_tokens = [t.strip() for t in self.tasks_edit.text().split(",") if t.strip()]

        if self._naming_reset:
            config.default_naming_rule = ""
            config.naming_configured = False

        config.name_whitelist = [kw.strip() for kw in self.whitelist_edit.text().split(",") if kw.strip()]
        config.name_blacklist = [kw.strip() for kw in self.blacklist_edit.text().split(",") if kw.strip()]

        # Re-apply defaults to non-overridden sources
        apply_project_defaults(config)


# ---------------------------------------------------------------------------
# Latest Path Dialog (shown when no latest path template is configured)
# ---------------------------------------------------------------------------

class LatestPathDialog(QDialog):
    """Dialog for defining the latest path template.

    Shown when the user tries to promote or add sources but no latest path
    template has been configured. Provides a live preview of the resolved path.
    """

    def __init__(self, config: ProjectConfig, source: WatchedSource = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Set Latest Path")
        self.setMinimumWidth(600)
        self._config = config
        self._source = source  # optional: specific source for preview

        layout = QVBoxLayout(self)

        # Header
        header = QLabel("A latest path template is required before promotion.")
        header.setStyleSheet("font-size: 14px; font-weight: bold; margin-bottom: 4px;")
        layout.addWidget(header)

        desc = QLabel(
            "This defines where promoted files are placed. The template is applied\n"
            "to each source, so you can use tokens to create unique paths per source."
        )
        desc.setStyleSheet("color: #999; font-size: 11px; margin-bottom: 8px;")
        layout.addWidget(desc)

        # Template input
        form = QFormLayout()

        token_help = QLabel(
            "Tokens: {source_dir}, {source_name}, {source_basename},\n"
            "{source_fullname}, {source_filename}, {project_root}, {group}, {group_root}\n"
            "If {group} is empty, the token and its trailing divider are omitted.\n"
            "{group_root} resolves to the group's root directory (falls back to {project_root}).\n"
            "Examples: ../online  |  {group_root}/latest/{source_name}  |  online/{group}/{source_name}"
        )
        token_help.setStyleSheet("color: #999; font-size: 11px;")
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
            "Tokens: {source_name}, {source_basename}, {source_fullname}"
        )
        rename_help.setStyleSheet("color: #999; font-size: 11px;")
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
        self.preview_label.setStyleSheet("color: #88cc88; font-size: 11px;")
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
            self.preview_label.setStyleSheet("color: #888; font-size: 11px;")
            return

        config = self._config
        previews = []

        # If a specific source was given, show it first
        sources = []
        if self._source:
            sources.append(self._source)
        sources.extend(s for s in config.watched_sources if s != self._source)
        sources = sources[:4]

        if sources:
            for source in sources:
                tokens = derive_source_tokens(source.sample_filename or source.name, config.task_tokens)
                resolved = tpl
                resolved = resolved.replace("{project_root}", config.effective_project_root or "<project_root>")
                resolved = resolved.replace("{group_root}", _resolve_group_root(config, source.group) or "<project_root>")
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
                rename_resolved = rename_resolved.replace("{source_name}", tokens["source_name"])
                rename_resolved = rename_resolved.replace("{source_basename}", tokens["source_basename"])
                rename_resolved = rename_resolved.replace("{source_fullname}", tokens["source_fullname"])
                rename_resolved = _expand_group_token(rename_resolved, source.group)
                sample_file = f"{rename_resolved}.####.exr"
                group_tag = f" [{source.group}]" if source.group else ""
                previews.append(f"{source.name}{group_tag}: {dir_str}\\{sample_file}")
            if len(config.watched_sources) > 4:
                previews.append(f"... and {len(config.watched_sources) - 4} more")
        else:
            resolved = tpl
            resolved = resolved.replace("{project_root}", config.effective_project_root or "<project_root>")
            resolved = resolved.replace("{group_root}", "<group_root>")
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
            rename_resolved = rename_resolved.replace("{source_name}", "<source_name>")
            rename_resolved = rename_resolved.replace("{source_basename}", "<source_basename>")
            rename_resolved = rename_resolved.replace("{source_fullname}", "<source_fullname>")
            rename_resolved = rename_resolved.replace("{group}", "<group>")
            sample_file = f"{rename_resolved}.####.exr"
            previews.append(f"{dir_str}\\{sample_file}")

        self.preview_label.setText("\n".join(previews))
        self.preview_label.setStyleSheet("color: #88cc88; font-size: 11px;")

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
        header.setStyleSheet("font-size: 14px; font-weight: bold; margin-bottom: 8px;")
        layout.addWidget(header)

        # Show example path from first result
        if results:
            result = results[0]
            path_label = QLabel(f"Example path: {result.path}")
            path_label.setStyleSheet("color: #999; font-size: 11px;")
            path_label.setWordWrap(True)
            layout.addWidget(path_label)

            if result.sample_filename:
                file_label = QLabel(f"Example file: {result.sample_filename}")
                file_label.setStyleSheet("color: #999; font-size: 11px;")
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
                    "border: 1px solid #444; border-radius: 4px; }"
                    "QPushButton:checked { background-color: #2d5a2d; border-color: #4a8a4a; }"
                    "QPushButton:hover { background-color: #3a3a3a; }"
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

    def __init__(self, root_dir: str, max_depth: int = 4, extensions=None,
                 whitelist=None, blacklist=None, parent=None):
        super().__init__(parent)
        self.root_dir = root_dir
        self.max_depth = max_depth
        self.extensions = extensions
        self.whitelist = whitelist
        self.blacklist = blacklist

    def run(self):
        try:
            results = discover(
                root_dir=self.root_dir,
                max_depth=self.max_depth,
                extensions=self.extensions,
                whitelist=self.whitelist,
                blacklist=self.blacklist,
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
        self._ignored_paths: set[str] = set()          # ignored source directory paths
        self._ignored_versions: set[tuple[str, int]] = set()  # (path, version_number)

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

        self.status_label = QLabel("Select a directory and click Scan.")
        self.status_label.setStyleSheet("color: #999;")
        layout.addWidget(self.status_label)

        # Bottom buttons
        btn_row = QHBoxLayout()
        self.add_btn = QPushButton("Add Selected to Project")
        self.add_btn.setEnabled(False)
        self.add_btn.setStyleSheet(
            "QPushButton { background-color: #2d5a2d; color: white; padding: 6px 14px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #3a7a3a; }"
            "QPushButton:disabled { background-color: #444; color: #888; }"
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

        # Use project filters if available
        whitelist = self._config.name_whitelist if self._config else None
        blacklist = self._config.name_blacklist if self._config else None

        self._worker = DiscoveryWorker(
            root_dir,
            whitelist=whitelist or None,
            blacklist=blacklist or None,
            parent=self,
        )
        self._worker.finished.connect(self._on_results)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _get_existing_source_dirs(self) -> set:
        """Return a set of resolved source_dir paths already in the project."""
        existing = set()
        if self._config:
            for src in self._config.watched_sources:
                if src.source_dir:
                    existing.add(str(Path(src.source_dir).resolve()).lower())
        return existing

    def _is_existing(self, result_path: str, existing_dirs: set) -> bool:
        """Check if a discovery result path matches an existing source."""
        return str(Path(result_path).resolve()).lower() in existing_dirs

    def _on_results(self, results: list):
        self._worker = None
        self.scan_btn.setEnabled(True)
        self._results = results

        if not results:
            self.status_label.setText("No versioned content found.")
            return

        self._rebuild_tree()

    def _rebuild_tree(self):
        """Rebuild the results tree, respecting filters (existing, ignored)."""
        self.result_tree.clear()

        if not self._results:
            return

        # Load timecodes based on project setting (default to lazy if no config)
        tc_mode = self._config.timecode_mode if self._config else "lazy"
        if tc_mode != "never":
            for result in self._results:
                populate_timecodes(result.versions_found)

        root_dir = self.dir_combo.currentText().strip()
        root = Path(root_dir).resolve() if root_dir else None

        existing_dirs = self._get_existing_source_dirs()
        hide_existing = self.hide_existing_cb.isChecked()
        show_ignored = self.show_ignored_cb.isChecked()

        shown = 0
        hidden = 0

        for i, result in enumerate(self._results):
            is_existing = self._is_existing(result.path, existing_dirs)
            is_ignored = result.path in self._ignored_paths

            if is_existing and hide_existing:
                hidden += 1
                continue

            if is_ignored and not show_ignored:
                hidden += 1
                continue

            try:
                display_path = str(Path(result.path).relative_to(root)) if root else result.path
            except ValueError:
                display_path = result.path

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

            # Style existing sources: gray and italic, non-selectable
            if is_existing:
                gray = QColor("#888888")
                italic_font = QFont()
                italic_font.setItalic(True)
                for col in range(6):
                    parent_item.setForeground(col, gray)
                    parent_item.setFont(col, italic_font)
                parent_item.setFlags(parent_item.flags() & ~Qt.ItemIsSelectable)

            # Style ignored sources: gray, italic, strikethrough, non-selectable
            if is_ignored:
                gray = QColor("#888888")
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

                child = QTreeWidgetItem([
                    label,
                    str(v.file_count),
                    v.total_size_human,
                    v.frame_range or "",
                    v.start_timecode or "",
                    "",
                ])
                # Store version index for context menu mapping
                child.setData(0, Qt.UserRole, vi_idx)
                # Make children non-selectable
                child.setFlags(child.flags() & ~Qt.ItemIsSelectable)

                # Style: gray/italic for existing or ignored parent, strikethrough for ignored version
                if is_existing or is_ignored or is_version_ignored:
                    gray = QColor("#888888")
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
        self.status_label.setText(f"Error: {msg}")

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
            self._ignored_paths.add(result.path)
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
            path_dlg = LatestPathDialog(self._config, parent=self)
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

            source = WatchedSource(
                name=source_name,
                source_dir=result.path,
                version_pattern=result.suggested_pattern or self._config.default_version_pattern,
                file_extensions=result.suggested_extensions or list(self._config.default_file_extensions),
                sample_filename=result.sample_filename or "",
                # Override pattern and extensions since they come from discovery
                override_version_pattern=bool(result.suggested_pattern),
                override_file_extensions=bool(result.suggested_extensions),
            )

            # Compute latest_target from project template if available
            if self._config.latest_path_template:
                tokens = derive_source_tokens(
                    result.sample_filename or source_name,
                    self._config.task_tokens,
                )
                tpl = self._config.latest_path_template
                tpl = tpl.replace("{project_root}", self._config.effective_project_root)
                tpl = tpl.replace("{group_root}", _resolve_group_root(self._config, source.group))
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
        tag_font.setPointSize(max(font.pointSize() - 1, 7))
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

        self.btn_delete = QPushButton("Delete")
        self.btn_delete.clicked.connect(self._delete_group)
        btn_row.addWidget(self.btn_delete)

        layout.addLayout(btn_row)

        # Help text
        root_help = QLabel(
            "{group_root} token resolves to the group's root directory.\n"
            "If unset, {group_root} falls back to {project_root}."
        )
        root_help.setStyleSheet("color: #999; font-size: 11px;")
        layout.addWidget(root_help)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self._rebuild_list()

    def _next_color(self) -> str:
        used = {v.get("color", "") for v in self._groups.values()}
        for c in _GROUP_COLOR_PALETTE:
            if c not in used:
                return c
        return _GROUP_COLOR_PALETTE[len(self._groups) % len(_GROUP_COLOR_PALETTE)]

    def _rebuild_list(self):
        self.group_list.clear()
        for name, props in sorted(self._groups.items()):
            color = props.get("color", "#888888")
            root = props.get("root_dir", "")
            count = sum(1 for s in self._config.watched_sources if s.group == name)
            root_label = f"  \u2502 root: {root}" if root else ""
            item = QListWidgetItem(f"  {name}  ({count} sources){root_label}")
            item.setData(Qt.UserRole, name)
            item.setForeground(QColor(color))
            if root:
                item.setToolTip(f"Root directory: {root}")
            self.group_list.addItem(item)

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
        current = QColor(self._groups[name].get("color", "#888888"))
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
        elif path == "":
            # User may want to clear — only if they had one set and hit cancel
            # Do nothing on cancel; offer explicit clear via empty selection
            pass
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
        version.setStyleSheet("color: #aaa;")
        layout.addWidget(version)

        # Separator
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("color: #444;")
        layout.addWidget(line)

        # Author info
        info = QLabel(
            '<p style="text-align:center;">'
            '<b>Author:</b> Maris Polis<br>'
            '<a href="https://marispolis.com" style="color:#5b9bd5;">marispolis.com</a><br>'
            '<a href="mailto:mp@marispolis.com" style="color:#5b9bd5;">mp@marispolis.com</a><br><br>'
            '<a href="https://www.linkedin.com/in/maris-polis-2bb404191/" style="color:#5b9bd5;">LinkedIn</a>'
            '&nbsp;&nbsp;|&nbsp;&nbsp;'
            '<a href="https://github.com/polisvfx/LatestVersionManager" style="color:#5b9bd5;">GitHub</a>'
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
        self._batch_promote_list: list = []
        self._batch_promote_index: int = 0

        # File watcher
        self.watcher = SourceWatcher(self)
        self.watcher.source_changed.connect(self._on_watcher_change)
        self.watcher.watch_status_changed.connect(self._on_watch_status)

        self._settings = QSettings("LatestVersionManager", "LVM")

        self._build_ui()
        self._build_menu()
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

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        toolbar.addWidget(spacer)

        self.project_label = QLabel("No project loaded")
        self.project_label.setStyleSheet("color: #888; font-style: italic;")
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
        sources_label.setStyleSheet("font-weight: bold; font-size: 13px;")
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

        self.source_list = QListWidget()
        self.source_list.setItemDelegate(SourceItemDelegate(self.source_list))
        self.source_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.source_list.currentRowChanged.connect(self._on_source_selected)
        self.source_list.itemSelectionChanged.connect(self._on_source_selection_changed)
        self.source_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.source_list.customContextMenuRequested.connect(self._source_context_menu)
        left_layout.addWidget(self.source_list)

        # Promote All / Promote Selected button
        self.btn_promote_all = QPushButton("Promote All to Latest")
        self.btn_promote_all.setStyleSheet(
            "QPushButton { background-color: #2d5a2d; color: white; padding: 8px 16px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #3a7a3a; }"
            "QPushButton:disabled { background-color: #444; color: #888; }"
        )
        self.btn_promote_all.setEnabled(False)
        self.btn_promote_all.setToolTip(
            "Promotes sources that are not on their highest version.\n"
            "Hold Shift to force re-promote all sources."
        )
        self.btn_promote_all.clicked.connect(self._promote_all_or_selected)
        left_layout.addWidget(self.btn_promote_all)

        splitter.addWidget(left_panel)

        # --- Right: Versions + details ---
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        # Current version banner
        self.current_banner = QFrame()
        self.current_banner.setFrameShape(QFrame.StyledPanel)
        self.current_banner.setStyleSheet(
            "QFrame { background-color: #1a3a1a; border: 1px solid #2d5a2d; border-radius: 4px; padding: 8px; }"
        )
        banner_layout = QHBoxLayout(self.current_banner)
        banner_layout.setContentsMargins(12, 8, 12, 8)
        self.current_label = QLabel("No version loaded")
        self.current_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #90ee90;")
        banner_layout.addWidget(self.current_label)
        self.integrity_label = QLabel("")
        self.integrity_label.setStyleSheet("font-size: 11px; color: #aaa;")
        self.integrity_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        banner_layout.addWidget(self.integrity_label)
        right_layout.addWidget(self.current_banner)

        # Version + History vertical split
        ver_hist_splitter = QSplitter(Qt.Vertical)

        # Version tree
        ver_group = QGroupBox("Available Versions")
        ver_layout = QVBoxLayout(ver_group)

        self.version_tree = VersionTreeWidget()
        self.version_tree.setHeaderLabels(["Version", "Files", "Size", "Frame Range", "Timecode", "Path"])
        self.version_tree.setRootIsDecorated(False)
        self.version_tree.setAlternatingRowColors(True)
        self.version_tree.setSortingEnabled(True)
        self.version_tree.files_dropped.connect(self._handle_version_drop)
        header = self.version_tree.header()
        header.setStretchLastSection(True)
        header.resizeSection(0, 80)
        header.resizeSection(1, 60)
        header.resizeSection(2, 80)
        header.resizeSection(3, 160)
        header.resizeSection(4, 110)
        ver_layout.addWidget(self.version_tree)

        # Promote controls
        promote_row = QHBoxLayout()
        self.btn_import_version = QPushButton("Import Version...")
        self.btn_import_version.setEnabled(False)
        self.btn_import_version.clicked.connect(self._import_version)
        self.btn_promote = QPushButton("Promote Selected to Latest")
        self.btn_promote.setStyleSheet(self._PROMOTE_STYLE)
        self.btn_promote.setEnabled(False)
        self.btn_promote.clicked.connect(self._promote_selected)
        promote_row.addWidget(self.btn_import_version)
        promote_row.addStretch()
        promote_row.addWidget(self.btn_promote)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)

        ver_layout.addLayout(promote_row)
        ver_layout.addWidget(self.progress_bar)

        ver_hist_splitter.addWidget(ver_group)

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

        ver_hist_splitter.addWidget(hist_group)
        ver_hist_splitter.setSizes([400, 200])

        right_layout.addWidget(ver_hist_splitter)
        splitter.addWidget(right_panel)

        splitter.setSizes([250, 850])
        main_layout.addWidget(splitter)

        # Status bar
        self.statusBar().showMessage("Ready")

    def _build_menu(self):
        menubar = self.menuBar()

        file_menu = menubar.addMenu("&File")

        new_action = QAction("&New Project...", self)
        new_action.triggered.connect(self._new_project)
        file_menu.addAction(new_action)

        open_action = QAction("&Open Project...", self)
        open_action.triggered.connect(self._open_project)
        file_menu.addAction(open_action)

        save_action = QAction("&Save Project", self)
        save_action.triggered.connect(self._save_project)
        file_menu.addAction(save_action)

        save_as_action = QAction("Save Project &As...", self)
        save_as_action.triggered.connect(self._save_project_as)
        file_menu.addAction(save_as_action)

        file_menu.addSeparator()

        settings_action = QAction("Project &Settings...", self)
        settings_action.triggered.connect(self._open_project_settings)
        file_menu.addAction(settings_action)

        file_menu.addSeparator()

        recent_menu = file_menu.addMenu("Recent Projects")
        self._populate_recent_menu(recent_menu)

        file_menu.addSeparator()

        quit_action = QAction("&Quit", self)
        quit_action.triggered.connect(self.close)
        file_menu.addAction(quit_action)

        tools_menu = menubar.addMenu("&Tools")

        discover_action = QAction("&Discover Versions...", self)
        discover_action.triggered.connect(self._open_discover)
        tools_menu.addAction(discover_action)

        manage_groups_action = QAction("&Manage Groups...", self)
        manage_groups_action.triggered.connect(self._open_manage_groups)
        tools_menu.addAction(manage_groups_action)

        tools_menu.addSeparator()

        export_report_action = QAction("&Export Report...", self)
        export_report_action.triggered.connect(self._export_report)
        tools_menu.addAction(export_report_action)

        validate_action = QAction("&Validate Config", self)
        validate_action.triggered.connect(self._validate_config)
        tools_menu.addAction(validate_action)

        source_menu = menubar.addMenu("&Sources")

        add_source_action = QAction("&Add Source...", self)
        add_source_action.triggered.connect(self._add_source)
        source_menu.addAction(add_source_action)

        refresh_action = QAction("&Refresh All", self)
        refresh_action.triggered.connect(self._refresh_all)
        source_menu.addAction(refresh_action)

        help_menu = menubar.addMenu("&Help")

        about_action = QAction("&About...", self)
        about_action.triggered.connect(self._show_about)
        help_menu.addAction(about_action)

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
        dlg = ProjectSetupDialog(parent=self)
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
            self._reload_ui()
            self.project_label.setText(f"{self.config.project_name}")
            self.project_label.setStyleSheet("color: #ccc; font-weight: bold;")
            self.statusBar().showMessage(f"Loaded: {path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load config:\n{e}")

    def _save_project(self):
        if not self.config:
            return
        if not self.config_path:
            self._save_project_as()
            return
        try:
            save_config(self.config, self.config_path)
            self.statusBar().showMessage(f"Saved: {self.config_path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save:\n{e}")

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
            self.project_label.setText(f"{self.config.project_name}")
            if self.config_path:
                self._save_project()
            self._reload_ui()
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
        dlg = SourceDialog(project_config=self.config, parent=self)
        if dlg.exec() == QDialog.Accepted:
            source = dlg.get_source()
            self.config.watched_sources.append(source)
            if self.config_path:
                self._save_project()
            self._reload_ui()
            self.statusBar().showMessage(f"Added source: {source.name}")

    def _edit_source(self, index: int):
        if not self.config or index < 0 or index >= len(self.config.watched_sources):
            return
        source = self.config.watched_sources[index]
        dlg = SourceDialog(source=source, project_config=self.config, parent=self)
        if dlg.exec() == QDialog.Accepted:
            self.config.watched_sources[index] = dlg.get_source()
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
        else:
            names = "\n".join(f"  - {self.config.watched_sources[i].name}" for i in indices)
            msg = f"Remove {len(indices)} sources from this project?\n\n{names}\n\nThis does NOT delete any files on disk."
        reply = QMessageBox.question(self, "Remove Source", msg)
        if reply == QMessageBox.Yes:
            for i in sorted(indices, reverse=True):
                self.config.watched_sources.pop(i)
            if self.config_path:
                self._save_project()
            self._reload_ui()

    def _resolve_source_index(self, list_row: int) -> int:
        """Map a filtered list row to the actual index in config.watched_sources."""
        item = self.source_list.item(list_row)
        if not item:
            return -1
        source_name = item.data(Qt.UserRole)
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
            row = self.source_list.row(item)
            idx = self._resolve_source_index(row)
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

        # Group submenu
        menu.addSeparator()
        group_menu = menu.addMenu("Group")
        sources = [self.config.watched_sources[i] for i in selected_indices]
        current_groups = set(s.group for s in sources)
        single_group = current_groups.pop() if len(current_groups) == 1 else None

        if self.config.groups:
            for grp_name in sorted(self.config.groups.keys()):
                color = self.config.groups[grp_name].get("color", "#888888")
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

    def _assign_group(self, indices: list, group_name: str):
        """Assign or unassign sources to a group."""
        old_groups = set()
        for i in indices:
            old_groups.add(self.config.watched_sources[i].group)
            self.config.watched_sources[i].group = group_name

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
            color = "#888888"
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
            apply_project_defaults(self.config)
            if self.config_path:
                self._save_project()
            self._reload_ui()
            self.statusBar().showMessage("Groups updated")

    def _on_source_selection_changed(self):
        """Update Promote All/Selected button based on source list selection."""
        selected = self.source_list.selectedItems()
        has_sources = self.config and len(self.config.watched_sources) > 0
        if len(selected) > 1:
            self.btn_promote_all.setText(f"Promote Selected ({len(selected)})")
            self.btn_promote_all.setEnabled(self._worker is None)
        else:
            self.btn_promote_all.setText("Promote All to Latest")
            self.btn_promote_all.setEnabled(has_sources and self._worker is None)

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
            apply_project_defaults(self.config)
            if self.config_path:
                self._save_project()
            self._reload_ui()

        selected_items = self.source_list.selectedItems()
        if len(selected_items) > 1:
            # Promote selected
            source_names = [item.data(Qt.UserRole) for item in selected_items]
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
                    already_current.append(f"{name} (deliberately on lower version)")
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

        # Build confirmation message
        mode_label = " (force)" if force else ""
        msg = f"Promote {len(promote_list)} source(s) to their highest version{mode_label}?\n\n"
        for source, version in promote_list:
            msg += f"  {source.name}: {version.version_string} ({version.total_size_human})\n"
        if already_current and not force:
            msg += f"\nAlready current ({len(already_current)}):\n"
            for s in already_current:
                msg += f"  {s}\n"
        if skipped:
            msg += f"\nSkipped:\n"
            for s in skipped:
                msg += f"  {s}\n"

        reply = QMessageBox.question(self, "Confirm Batch Promotion", msg)
        if reply != QMessageBox.Yes:
            return

        self._batch_promote_list = promote_list
        self._batch_promote_index = 0
        self._batch_promote_next()

    def _batch_promote_next(self):
        """Promote the next source in the batch list."""
        if self._batch_promote_index >= len(self._batch_promote_list):
            # All done
            self._reload_ui()
            count = len(self._batch_promote_list)
            self._batch_promote_list = []
            self.statusBar().showMessage(f"Batch promotion complete: {count} source(s)")
            return

        source, version = self._batch_promote_list[self._batch_promote_index]
        promoter = self._promoters.get(source.name)
        if not promoter:
            # Create promoter if needed
            if source.latest_target:
                promoter = Promoter(source, self.config.task_tokens)
                self._promoters[source.name] = promoter
            else:
                self._batch_promote_index += 1
                self._batch_promote_next()
                return

        self.statusBar().showMessage(
            f"Promoting {self._batch_promote_index + 1}/{len(self._batch_promote_list)}: {source.name}"
        )
        self._current_source = source
        self._start_promotion(promoter, version)

    # --- UI Updates ---

    def _reload_ui(self):
        """Refresh everything from current config."""
        self.source_list.clear()
        self.version_tree.clear()
        self.history_tree.clear()
        self._scanners.clear()
        self._promoters.clear()
        self._versions_cache.clear()
        self._manual_versions.clear()
        self._current_source = None

        enabled = self.config is not None
        self.btn_project_settings.setEnabled(enabled)
        self.btn_manage_groups.setEnabled(enabled)
        self.btn_refresh.setEnabled(enabled)
        self.btn_import_version.setEnabled(False)
        self.watch_toggle.setEnabled(enabled)
        self.btn_promote.setEnabled(False)
        self.btn_revert.setEnabled(False)
        has_sources = self.config is not None and len(self.config.watched_sources) > 0
        self.btn_promote_all.setEnabled(has_sources and self._worker is None)
        self.btn_promote_all.setText("Promote All to Latest")

        if not self.config:
            self.current_label.setText("No project loaded")
            self.integrity_label.setText("")
            self.btn_promote_all.setEnabled(False)
            return

        # Compute status for each source (used by list + filter)
        # status: "highest", "newer", "deliberate", "no_version", "no_target", "integrity_fail"
        self._source_status: dict[str, dict] = {}

        tc_mode = self.config.timecode_mode

        for source in self.config.watched_sources:
            self._scanners[source.name] = VersionScanner(source, self.config.task_tokens)
            versions = self._scanners[source.name].scan()
            # "always" mode: populate timecodes eagerly during scan
            if tc_mode == "always":
                populate_timecodes(versions)
            self._versions_cache[source.name] = versions

            current = None
            status = "no_target"
            highest_ver = versions[-1].version_string if versions else None

            if source.latest_target:
                self._promoters[source.name] = Promoter(source, self.config.task_tokens)
                promoter = self._promoters[source.name]
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
                elif self._has_newer_versions_since(current, versions):
                    status = "newer"
                else:
                    # Even for deliberate/older versions, check staleness
                    integrity = promoter.verify()
                    if not integrity["valid"] and "modified since promotion" in integrity.get("message", ""):
                        status = "stale"
                    else:
                        status = "deliberate"

            self._source_status[source.name] = {
                "current": current,
                "status": status,
                "has_overrides": source.has_overrides,
            }

        self._populate_source_list()

        # Select first visible source
        if self.source_list.count() > 0:
            self.source_list.setCurrentRow(0)

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

    def _make_source_item(self, source: WatchedSource) -> QListWidgetItem:
        """Create a QListWidgetItem for a source with status coloring and group tag."""
        info = self._source_status.get(source.name, {})
        status = info.get("status", "no_target")
        current = info.get("current")
        has_overrides = info.get("has_overrides", False)

        ver_tag = current.version if current else ""

        # Status markers
        if status == "newer":
            text = f"\u25bc! {source.name}  [{ver_tag}]"
        elif status == "stale":
            text = f"  \u21bb {source.name}  [{ver_tag}]"
        elif status == "deliberate":
            text = f"  * {source.name}  [{ver_tag}]"
        elif status == "highest":
            text = f"    {source.name}  [{ver_tag}]"
        elif status == "integrity_fail":
            text = f"  \u26a0 {source.name}  [{ver_tag}]"
        elif current:
            text = f"    {source.name}  [{ver_tag}]"
        else:
            text = f"    {source.name}"

        # Append group tag
        if source.group:
            text += f"  \u2022{source.group}"

        item = QListWidgetItem(text)
        item.setData(Qt.UserRole, source.name)

        # Color coding
        if status == "newer":
            item.setForeground(QColor("#cc8833"))
            item.setToolTip(f"Newer versions available since {ver_tag} was promoted")
        elif status == "stale":
            item.setForeground(QColor("#e8a040"))
            item.setToolTip(f"Source files for {ver_tag} modified since promotion — may have been re-rendered")
        elif status == "deliberate":
            item.setForeground(QColor("#7abbe0"))
            item.setToolTip(f"Deliberately on {ver_tag} — higher versions existed at promotion time")
        elif status == "highest":
            item.setForeground(QColor("#90ee90"))
            item.setToolTip(f"On latest version: {ver_tag}")
        elif status == "integrity_fail":
            item.setForeground(QColor("#ffaa00"))
            item.setToolTip(f"Integrity issue with {ver_tag}")
        elif status == "no_version":
            item.setForeground(QColor("#888888"))
            item.setToolTip("No version promoted yet")
        else:  # no_target
            item.setForeground(QColor("#666666"))
            item.setToolTip("No latest target path configured")

        # Override marker (blue tint on top)
        if has_overrides:
            item.setForeground(QColor("#88aaff"))
            item.setToolTip(item.toolTip() + " | Custom settings")

        if source.group:
            item.setToolTip(item.toolTip() + f" | Group: {source.group}")
            grp_props = self.config.groups.get(source.group, {})
            grp_color = grp_props.get("color", "#888888")
            item.setData(SourceItemDelegate.GROUP_ROLE, (source.group, grp_color))

        return item

    def _populate_source_list(self):
        """Build source list items based on computed status, active filter, search query, and grouping."""
        self.source_list.clear()
        if not self.config:
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
            if filter_mode == "Not on Highest" and status not in ("newer", "deliberate", "stale"):
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
                color = self.config.groups[grp_name].get("color", "#888888")
                # Group header (non-selectable separator)
                header = QListWidgetItem(f"\u2500\u2500 {grp_name} \u2500\u2500")
                header.setFlags(Qt.NoItemFlags)
                header.setForeground(QColor(color))
                font = header.font()
                font.setBold(True)
                header.setFont(font)
                self.source_list.addItem(header)

                for source in sorted(grouped[grp_name], key=lambda s: s.name.lower()):
                    self.source_list.addItem(self._make_source_item(source))

            if ungrouped:
                if grouped:
                    header = QListWidgetItem("\u2500\u2500 Ungrouped \u2500\u2500")
                    header.setFlags(Qt.NoItemFlags)
                    header.setForeground(QColor("#666666"))
                    font = header.font()
                    font.setBold(True)
                    header.setFont(font)
                    self.source_list.addItem(header)
                for source in sorted(ungrouped, key=lambda s: s.name.lower()):
                    self.source_list.addItem(self._make_source_item(source))
        else:
            # Alphabetical order
            for source in sorted(filtered, key=lambda s: s.name.lower()):
                self.source_list.addItem(self._make_source_item(source))

    def _apply_source_filter(self):
        """Re-filter the source list without full reload."""
        if not self.config or not hasattr(self, '_source_status'):
            return
        prev_source = None
        if self.source_list.currentItem():
            prev_source = self.source_list.currentItem().data(Qt.UserRole)
        self._populate_source_list()
        # Try to re-select the previously selected source
        if prev_source:
            for i in range(self.source_list.count()):
                if self.source_list.item(i).data(Qt.UserRole) == prev_source:
                    self.source_list.setCurrentRow(i)
                    return
        if self.source_list.count() > 0:
            self.source_list.setCurrentRow(0)

    def _refresh_all(self):
        """Re-scan all sources."""
        self._versions_cache.clear()
        self._reload_ui()
        self.statusBar().showMessage("Refreshed all sources")

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
            known = {"project_root", "group_root", "source_name", "source_basename",
                     "source_fullname", "source_filename", "source_dir", "group"}
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

    def _on_source_selected(self, row: int):
        """User selected a source — populate versions and history."""
        self.version_tree.clear()
        self.history_tree.clear()
        self.btn_promote.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self.btn_import_version.setEnabled(False)

        if row < 0 or not self.config:
            self.current_label.setText("No version loaded")
            self.integrity_label.setText("")
            return

        # Look up source by name stored in item data (filter-safe)
        item = self.source_list.item(row)
        if not item:
            self.current_label.setText("No version loaded")
            self.integrity_label.setText("")
            return
        source_name = item.data(Qt.UserRole)
        source = None
        for s in self.config.watched_sources:
            if s.name == source_name:
                source = s
                break
        if not source:
            return
        self._current_source = source
        self.btn_import_version.setEnabled(True)

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
        versions = sorted(scanned_versions + manual, key=lambda v: v.version_number)
        # Track which source_paths are manual for UI indicators
        manual_paths = {v.source_path for v in manual}

        # Timecode loading based on project setting
        tc_mode = self.config.timecode_mode if self.config else "lazy"
        if tc_mode == "lazy":
            populate_timecodes(versions)
        # "always" — already populated during scan (see _reload_ui)
        # "never"  — leave as None

        current = promoter.get_current_version() if promoter else None
        current_ver = current.version if current else None

        # Update banner
        if not promoter:
            self.current_label.setText(f"No latest target set   ({source.name})")
            self.current_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #ff8888;")
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
                self.current_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #90ee90;")
            else:
                # Determine if higher versions are new (appeared after promotion)
                # or if the user deliberately chose a lower version
                has_new = self._has_newer_versions_since(current, versions)
                if has_new:
                    # New versions appeared after promotion — dark orange
                    self.current_label.setText(f"Current: {current.version} \u25bc!   ({source.name})")
                    self.current_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #cc8833;")
                else:
                    # User deliberately promoted a lower version — muted green
                    self.current_label.setText(f"Current: {current.version}*   ({source.name})")
                    self.current_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #7abbe0;")
            integrity = promoter.verify()
            if integrity["valid"]:
                self.integrity_label.setText("\u2713 Verified")
                self.integrity_label.setStyleSheet("font-size: 11px; color: #90ee90;")
                self.current_banner.setStyleSheet(
                    "QFrame { background-color: #1a3a1a; border: 1px solid #2d5a2d; "
                    "border-radius: 4px; padding: 8px; }"
                )
            else:
                self.integrity_label.setText(f"\u26a0 {integrity['message']}")
                self.integrity_label.setStyleSheet("font-size: 11px; color: #ffaa00;")
                self.current_banner.setStyleSheet(
                    "QFrame { background-color: #3a3a1a; border: 1px solid #5a5a2d; "
                    "border-radius: 4px; padding: 8px; }"
                )
        else:
            self.current_label.setText(f"No version loaded   ({source.name})")
            self.current_label.setStyleSheet("font-size: 14px; font-weight: bold; color: #888;")
            self.integrity_label.setText("")
            self.current_banner.setStyleSheet(
                "QFrame { background-color: #2a2a2a; border: 1px solid #444; "
                "border-radius: 4px; padding: 8px; }"
            )

        # Determine highest version number and whether new versions appeared
        highest_ver = versions[-1].version_number if versions else 0
        has_new = (
            current is not None
            and current_ver != (versions[-1].version_string if versions else None)
            and self._has_newer_versions_since(current, versions)
        )

        # Populate version tree
        current_tc = current.start_timecode if current else None

        for v in reversed(versions):  # Newest first
            is_manual = v.source_path in manual_paths
            version_label = f"{v.version_string} [manual]" if is_manual else v.version_string

            item = QTreeWidgetItem([
                version_label,
                str(v.file_count),
                v.total_size_human,
                v.frame_range or "\u2014",
                v.start_timecode or "\u2014",
                v.source_path,
            ])
            item.setData(0, Qt.UserRole, v)

            if is_manual:
                # Cyan tint for manually imported versions
                manual_color = QColor("#66cccc")
                for col in range(6):
                    item.setForeground(col, manual_color)

            if v.version_string == current_ver:
                is_highest = (v.version_number == highest_ver)
                if is_highest:
                    # Promoted version IS the highest — bright green
                    suffix = " [manual]" if is_manual else ""
                    item.setText(0, f"{v.version_string}{suffix} \u25c0")
                    color = QColor("#90ee90")
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
                for col in range(6):
                    item.setForeground(col, color)

            # Highlight timecode changes vs current promoted version
            if (current_tc and v.start_timecode
                    and v.start_timecode != current_tc
                    and v.version_string != current_ver):
                item.setForeground(4, QColor("#ff9944"))

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
                        item.setForeground(col, QColor("#90ee90"))
                self.history_tree.addTopLevelItem(item)

        self.history_tree.itemSelectionChanged.connect(self._on_history_selected)

    @staticmethod
    def _has_newer_versions_since(current: HistoryEntry, versions: list) -> bool:
        """Check if any version higher than the current one appeared after promotion.

        Compares the promotion timestamp against the modification time of
        higher-version source paths. Returns True if at least one higher
        version was created/modified *after* the promotion — meaning the
        user didn't deliberately skip it.

        A 2-second tolerance is applied because set_at is stored with
        second-level precision while filesystem timestamps have sub-second
        resolution.
        """
        from datetime import datetime, timedelta

        if not current or not current.set_at or not versions:
            return False

        try:
            promoted_at = datetime.fromisoformat(current.set_at)
        except (ValueError, TypeError):
            return False

        # Add tolerance for timestamp rounding (set_at truncates to seconds)
        threshold = promoted_at + timedelta(seconds=2)

        current_num = None
        for v in versions:
            if v.version_string == current.version:
                current_num = v.version_number
                break
        if current_num is None:
            return False

        for v in versions:
            if v.version_number <= current_num:
                continue
            # Check when this higher version's source path was last modified
            try:
                source_path = Path(v.source_path)
                mtime = datetime.fromtimestamp(source_path.stat().st_mtime)
                if mtime > threshold:
                    return True
            except (OSError, ValueError):
                continue

        return False

    _PROMOTE_STYLE = (
        "QPushButton { background-color: #2d5a2d; color: white; padding: 8px 16px; "
        "border-radius: 4px; font-weight: bold; font-size: 13px; }"
        "QPushButton:hover { background-color: #3a7a3a; }"
        "QPushButton:disabled { background-color: #444; color: #888; }"
    )
    _KEEP_STYLE = (
        "QPushButton { background-color: #2d4a5a; color: white; padding: 8px 16px; "
        "border-radius: 4px; font-weight: bold; font-size: 13px; }"
        "QPushButton:hover { background-color: #3a6a7a; }"
        "QPushButton:disabled { background-color: #444; color: #888; }"
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
            # Refresh the version display
            row = self.source_list.currentRow()
            if row >= 0:
                self._on_source_selected(row)
            self.statusBar().showMessage(
                f"Imported {added} manual version{'s' if added != 1 else ''}"
            )

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

        version: VersionInfo = items[0].data(0, Qt.UserRole)
        source = self._current_source

        # Ensure latest path is set
        if not self._ensure_latest_path(source):
            return

        # Re-fetch promoter after possible reload
        promoter = self._promoters.get(source.name)
        if not promoter:
            return

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

        self._start_promotion(promoter, version)

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

    def _start_promotion(self, promoter: Promoter, version: VersionInfo):
        """Start the promotion in a background thread, checking link mode availability first."""
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

        self._worker = PromoteWorker(promoter, version, self)
        self._worker.progress.connect(self._on_promote_progress)
        self._worker.finished.connect(self._on_promote_finished)
        self._worker.error.connect(self._on_promote_error)
        self._worker.start()

    def _on_promote_progress(self, current, total, filename):
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)
        self.progress_bar.setFormat(f"{current}/{total} \u2014 {filename}")

    def _on_promote_finished(self, entry):
        self._worker = None
        self.progress_bar.setVisible(False)

        # Check if this is part of a batch promotion
        if hasattr(self, '_batch_promote_list') and self._batch_promote_list:
            self._versions_cache.pop(self._current_source.name, None)
            self._batch_promote_index += 1
            self._batch_promote_next()
            return

        promoted_name = self._current_source.name
        self.statusBar().showMessage(
            f"Promoted {promoted_name} \u2192 {entry.version}"
        )
        # Refresh the current source view
        self._versions_cache.pop(promoted_name, None)
        self._reload_ui()
        # Reselect the promoted source
        for i in range(self.source_list.count()):
            item = self.source_list.item(i)
            if item.data(Qt.UserRole) == promoted_name:
                self.source_list.setCurrentRow(i)
                break

    def _on_promote_error(self, error_msg):
        self._worker = None
        self.progress_bar.setVisible(False)
        self.btn_promote.setEnabled(True)

        # If batch promotion, ask whether to continue
        if hasattr(self, '_batch_promote_list') and self._batch_promote_list:
            source_name = self._current_source.name if self._current_source else "Unknown"
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
                self._reload_ui()
            return

        QMessageBox.critical(self, "Promotion Failed", error_msg)

    # --- File Watcher ---

    def _toggle_watcher(self):
        if self.watcher.is_running:
            self.watcher.stop()
            self.watch_toggle.setText("Start Watching")
            self.watch_toggle.setChecked(False)
        else:
            if self.config:
                self.watcher.start(self.config.watched_sources)
                self.watch_toggle.setText("Stop Watching")
                self.watch_toggle.setChecked(True)

    def _on_watcher_change(self, source_name: str):
        """A watched source had new files — invalidate cache and refresh."""
        logger.info(f"Watcher detected changes in: {source_name}")
        self._versions_cache.pop(source_name, None)

        # If this source is currently selected, refresh it
        if self._current_source and self._current_source.name == source_name:
            row = self.source_list.currentRow()
            self._reload_ui()
            if row >= 0 and row < self.source_list.count():
                self.source_list.setCurrentRow(row)

        self.statusBar().showMessage(f"New version detected in: {source_name}")

    def _on_watch_status(self, status: str):
        self.statusBar().showMessage(status)

    # --- State persistence ---

    def _restore_state(self):
        last_project = self._settings.value("last_project", None)
        if last_project and os.path.exists(last_project):
            self._load_project(last_project)

    def closeEvent(self, event):
        if self.config_path:
            self._settings.setValue("last_project", self.config_path)
        self.watcher.stop()
        event.accept()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(APP_VERSION)

    if LOGO_PATH.exists():
        app.setWindowIcon(_load_app_icon())

    # Dark palette
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(40, 40, 40))
    palette.setColor(QPalette.WindowText, QColor(210, 210, 210))
    palette.setColor(QPalette.Base, QColor(30, 30, 30))
    palette.setColor(QPalette.AlternateBase, QColor(38, 38, 38))
    palette.setColor(QPalette.Text, QColor(210, 210, 210))
    palette.setColor(QPalette.Button, QColor(50, 50, 50))
    palette.setColor(QPalette.ButtonText, QColor(210, 210, 210))
    palette.setColor(QPalette.Highlight, QColor(45, 90, 45))
    palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
    app.setPalette(palette)

    # Global stylesheet
    app.setStyleSheet("""
        QMainWindow { background-color: #282828; }
        QGroupBox {
            font-weight: bold;
            border: 1px solid #444;
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
            border: 1px solid #444;
            border-radius: 2px;
        }
        QTreeWidget::item:selected {
            background-color: #2d5a2d;
        }
        QListWidget {
            border: 1px solid #444;
            border-radius: 2px;
        }
        QListWidget::item {
            padding: 2px 6px;
        }
        QListWidget::item:selected {
            background-color: #2d5a2d;
        }
        QPushButton {
            padding: 5px 12px;
            border: 1px solid #555;
            border-radius: 3px;
            background-color: #3a3a3a;
        }
        QPushButton:hover {
            background-color: #4a4a4a;
        }
        QPushButton:pressed {
            background-color: #2a2a2a;
        }
        QPushButton:disabled {
            color: #666;
        }
        QProgressBar {
            border: 1px solid #444;
            border-radius: 3px;
            text-align: center;
            background-color: #1e1e1e;
        }
        QProgressBar::chunk {
            background-color: #2d5a2d;
        }
        QToolBar {
            spacing: 4px;
            padding: 4px;
            border-bottom: 1px solid #444;
        }
        QStatusBar {
            border-top: 1px solid #444;
        }
        QSplitter::handle {
            background-color: #444;
        }
    """)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
