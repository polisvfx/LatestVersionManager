"""Main application window."""

import time

from app._common import *  # noqa: F401,F403
from app._common import (
    _STATUS_MARKERS,
    _STATUS_LABELS,
    _STATUS_COLORS,
    _OVERRIDE_COLOR,
    _CONFLICT_COLOR,
    _DEFAULT_GROUP_COLOR_HEX,
    _GROUP_COLOR_CACHE,
    _REVEAL_LABEL,
    _BASE_DIR,
    _PLATFORM,
    _SINGLE_FILE_EXTS,
    _group_qcolor,
    _load_app_icon,
    _preview_sample_suffix,
    _expand_group_token,
    _resolve_group_root,
)
from app.widgets import _GROUP_COLOR_PALETTE
from app.workers import (
    PromoteWorker, ThumbnailWorker, ThumbnailQueueWorker, ScanWorker,
    StatusWorker, SyncNamesWorker, ProjectLoadWorker, AutoPromoteCheckWorker,
)
from app.widgets import VersionTreeWidget, SourceListWidget
from app.controllers import (
    ProjectControllerMixin, ScanControllerMixin, PromoteControllerMixin,
    AutoPromoteControllerMixin, NleSyncControllerMixin,
)
from app.dialogs.about import AboutDialog
from app.dialogs.batch_promote import BatchPromoteReviewDialog, UndoPromoteDialog, DiscoverPromotePreviewDialog
from app.dialogs.bulk_edit import BulkEditSourcesDialog
from app.dialogs.discovery import DiscoveryDialog, DiscoveryWorker, add_discovery_results_to_config
from app.dialogs.dry_run import DryRunDialog
from app.dialogs.history_timeline import HistoryTimelineDialog
from app.dialogs.latest_path import LatestPathDialog
from app.dialogs.manage_groups import ManageGroupsDialog
from app.dialogs.obsolete_layer import ObsoleteLayerDialog
from app.dialogs.project_setup import ProjectSetupDialog
from app.dialogs.settings import ProjectSettingsDialog
from app.dialogs.source import SourceDialog
from app.dialogs.update import UpdateDialog

# Auto-promote: how often to re-check a container file that is still being
# rendered, and how long a file of an unrecognised container format must sit
# unmodified before it counts as finished.
RENDER_WAIT_RETRY_MS = 10_000
RENDER_SETTLE_SECONDS = 10.0

class MainWindow(
    ProjectControllerMixin,
    ScanControllerMixin,
    PromoteControllerMixin,
    AutoPromoteControllerMixin,
    NleSyncControllerMixin,
    QMainWindow,
):
    # Emitted from the _io_executor thread when a background project save
    # finishes: ok, config_path, project_dir (ok) or error message (failed).
    _save_finished = Signal(bool, str, str)
    # Emitted from the _io_executor thread when NLE availability / running
    # probes complete: resolve_available, resolve_alive, premiere_alive, premiere_install_dir.
    _nle_probe_finished = Signal(bool, bool, bool, object)

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
        self._promoting_force: bool = False
        self._promoting_keep_layers: set[str] | None = None
        self._promoting_pinned: bool = False
        self._fallback_original_mode: str = None  # original link_mode before copy fallback
        self._fallback_source: WatchedSource = None  # source whose link_mode was swapped
        self._batch_promote_list: list = []
        self._batch_promote_index: int = 0
        self._batch_keep_layers: dict = {}
        # Sources whose auto-promote is waiting for a container render to finish
        self._render_wait_sources: set[str] = set()
        # Auto-promote checks running off the GUI thread, by source name, and
        # sources to re-check once theirs finishes (value: was waiting on a render)
        self._auto_check_workers: dict[str, AutoPromoteCheckWorker] = {}
        self._auto_check_rerun: dict[str, bool] = {}
        # What the last auto-promote check saw of each source's candidate
        # version (SettleState); a version is promoted only once two checks
        # in a row saw the same files.
        self._auto_settle: dict = {}
        # Source whose current promotion was started by auto-promote (for
        # the promoted / failed notification); None for manual promotions.
        self._auto_promoting_name: str | None = None
        from app.notifier import AutoPromoteNotifier
        self._notifier = AutoPromoteNotifier(self, on_clicked=self._select_source_by_name)
        self._target_conflicts: dict = {}
        self._source_status: dict = {}  # filled by StatusWorker
        self._deferred_refresh_results: dict = None  # scan results deferred due to promotion in progress
        self._scan_worker: ScanWorker = None
        self._status_worker: StatusWorker = None
        self._project_load_worker: ProjectLoadWorker = None
        self._project_generation: int = 0
        self._thumb_request_id: int = 0
        self._active_thumb_workers: set = set()
        self._reload_pending: bool = False
        self._rescan_after_cache: bool = False
        # Sources whose refresh was requested while a scan/status worker was
        # busy; refreshed together once it finishes. _pending_refresh_user is
        # True if any of those requests came from the user (full rescan,
        # buttons disabled) rather than the watcher.
        self._pending_refresh_names: set[str] = set()
        self._pending_refresh_user: bool = False
        self._reload_select_source: str = None  # source to select after async _reload_ui
        self._refresh_select_source: str = None  # source name to re-select after background refresh
        self._thumb_worker: ThumbnailWorker = None
        self._io_executor = ThreadPoolExecutor(max_workers=1)
        self._dirty = False  # True when config has unsaved changes
        self._log_autoshow_cooldown_until = 0.0  # error-burst cooldown
        self._nle_probe_pending = False
        self._closing = False
        self._batch_saved_source_name: str = None
        self._save_finished.connect(self._on_save_finished)
        self._nle_probe_finished.connect(self._apply_sync_names_state)

        # File watcher
        self.watcher = SourceWatcher(self)
        self.watcher.source_changed.connect(self._on_watcher_change)
        self.watcher.watch_status_changed.connect(self._on_watch_status)

        self._settings = QSettings("LatestVersionManager", "LVM")
        self._thumb_queue_worker: ThumbnailQueueWorker = None
        self._version_thumbs_enabled = self._settings.value(
            "version_thumbs_enabled", False, type=bool
        )

        self._build_ui()
        self._build_menu()
        self._build_shortcuts()

        # Log handler setup (Feature #19)
        from lvm.log_handler import QtLogHandler
        self._log_handler = QtLogHandler(max_buffer=1000)
        self._log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"))
        logging.getLogger().addHandler(self._log_handler)
        self._log_handler.log_record.connect(self._append_log_entry)
        # Live records only — buffer replays via _filter_log must not pop
        # the dock for historical errors.
        self._log_handler.log_record.connect(self._maybe_autoshow_log_dock)

        # Console stays at INFO. DEBUG records are only produced while the
        # log dock's filter asks for them: at DEBUG every per-file message of
        # a large promotion is formatted and pushed through a Qt signal.
        for h in logging.getLogger().handlers:
            if isinstance(h, logging.StreamHandler) and h is not self._log_handler:
                h.setLevel(logging.INFO)
        self._apply_log_capture_level()
        self.log_level_filter.currentTextChanged.connect(self._apply_log_capture_level)

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
        # The explicit Refresh button always lists every folder again; all
        # automatic refreshes reuse unchanged version folders.
        self.btn_refresh.clicked.connect(lambda: self._refresh_all(full=True))
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
        self.main_splitter = splitter

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

        # Search box — debounced so a fast typist doesn't trigger one full
        # source-list rebuild per keystroke. 150ms feels instant but coalesces
        # bursts.
        self.source_search = QLineEdit()
        self.source_search.setPlaceholderText("Search name, group, or path...")
        self.source_search.setToolTip(
            "Filter sources by name, sample filename, group, or any part "
            "of the source directory path"
        )
        self.source_search.setClearButtonEnabled(True)
        self._search_debounce_timer = QTimer(self)
        self._search_debounce_timer.setSingleShot(True)
        self._search_debounce_timer.setInterval(150)
        self._search_debounce_timer.timeout.connect(self._apply_source_filter)
        self.source_search.textChanged.connect(lambda _t: self._search_debounce_timer.start())
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

        self.source_list = SourceListWidget()
        self.source_list.paths_dropped.connect(self._handle_source_drop)
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
        # Ctrl+Z = undo with confirmation; Ctrl+Shift+Z = skip the dialog
        from PySide6.QtGui import QShortcut
        undo_shortcut = QShortcut(QKeySequence.Undo, self.source_list)
        undo_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        undo_shortcut.activated.connect(self._undo_selected_source)
        undo_skip_shortcut = QShortcut(QKeySequence("Ctrl+Shift+Z"), self.source_list)
        undo_skip_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        undo_skip_shortcut.activated.connect(
            lambda: self._undo_selected_source(skip_confirm=True)
        )
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
            "Promotes sources that are not on their highest version (F9).\n"
            "Hold Shift to force re-promote all sources (Shift+F9)."
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

        # Discover & Promote — one-click roundtrip: rediscover the last
        # discovery root, auto-add any new sources, then batch-promote
        # everything that isn't on its latest version. Hidden unless opted in
        # from Project Settings.
        discover_promote_style = (
            "QPushButton { background-color: #2f6a4a; color: white; padding: 8px 16px; "
            "border-radius: 4px; font-weight: bold; }"
            "QPushButton:hover { background-color: #3d8a60; }"
            "QPushButton:disabled { background-color: #2a2a2a; color: #8c8c8c; }"
        )
        self.btn_discover_promote = QPushButton("Discover && Promote")
        self.btn_discover_promote.setStyleSheet(discover_promote_style)
        self.btn_discover_promote.setEnabled(False)
        self.btn_discover_promote.setVisible(False)
        self.btn_discover_promote.setToolTip(
            "Rediscover the last-scanned directory, auto-add any new\n"
            "versioned sources, and promote everything that isn't on its\n"
            "latest version — all in one click.\n"
            "Hold Shift to also rescan every existing source for new\n"
            "versions before promoting."
        )
        self.btn_discover_promote.clicked.connect(self._discover_and_promote)
        left_layout.addWidget(self.btn_discover_promote)

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
        if self._version_thumbs_enabled:
            self.version_tree.setIconSize(QSize(48, 27))

        # Thumbnails toggle — opt-in per-row icons, generated lazily in the
        # background (one ffmpeg/oiiotool at a time via ThumbnailQueueWorker)
        thumbs_row = QHBoxLayout()
        thumbs_row.addStretch(1)
        self._version_thumbs_toggle = QToolButton()
        self._version_thumbs_toggle.setText("Thumbnails")
        self._version_thumbs_toggle.setCheckable(True)
        self._version_thumbs_toggle.setChecked(self._version_thumbs_enabled)
        self._version_thumbs_toggle.setToolTip(
            "Show a first-frame thumbnail next to each version "
            "(generated in the background, cached on disk)"
        )
        self._version_thumbs_toggle.toggled.connect(self._toggle_version_thumbnails)
        thumbs_row.addWidget(self._version_thumbs_toggle)
        ver_layout.addLayout(thumbs_row)

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
        # Connected once here — connecting per source selection stacked up
        # duplicate slots, so each click ran the handler once per selection.
        self.version_tree.itemSelectionChanged.connect(self._on_version_selected)

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
        self.btn_cancel_promote.setMinimumWidth(70)
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
        self.history_tree.itemSelectionChanged.connect(self._on_history_selected)

        # Revert + Export buttons
        revert_row = QHBoxLayout()
        self.btn_timeline = QPushButton("Timeline View...")
        self.btn_timeline.setToolTip("Open the Gantt-style promotion history timeline")
        self.btn_timeline.clicked.connect(self._open_history_timeline)
        self.btn_timeline.setEnabled(False)
        revert_row.addWidget(self.btn_timeline)
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

        # NLE sync indicators — flat buttons so the user can click to sync
        # without hunting through the Tools menu. Greyed out when the
        # corresponding NLE isn't detected.
        nle_btn_style = (
            "QPushButton { font-size: 11pt; padding: 2px 8px; margin-right: 4px; "
            "border: 1px solid #555; border-radius: 3px; }"
            "QPushButton:hover:!disabled { background-color: #3a3a3a; }"
            "QPushButton:disabled { color: #6c6c6c; border-color: #3a3a3a; }"
        )
        self._nle_sync_btn = QPushButton("")
        self._nle_sync_btn.setFlat(True)
        self._nle_sync_btn.setCursor(Qt.PointingHandCursor)
        self._nle_sync_btn.setStyleSheet(nle_btn_style)
        self._nle_sync_btn.clicked.connect(self._sync_names_resolve)
        self.statusBar().addPermanentWidget(self._nle_sync_btn)

        self._nle_premiere_btn = QPushButton("")
        self._nle_premiere_btn.setFlat(True)
        self._nle_premiere_btn.setCursor(Qt.PointingHandCursor)
        self._nle_premiere_btn.setStyleSheet(nle_btn_style)
        self._nle_premiere_btn.clicked.connect(self._sync_names_premiere)
        self.statusBar().addPermanentWidget(self._nle_premiere_btn)

        # Premiere panel heartbeat goes stale ~60 s after the panel stops
        # writing it. Polling at 30 s flips the button between "Sync
        # Premiere" and "Premiere: not detected" within one window.
        self._nle_state_timer = QTimer(self)
        self._nle_state_timer.setInterval(30000)
        self._nle_state_timer.timeout.connect(self._refresh_sync_names_state)
        self._nle_state_timer.start()

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
        self._recent_menu = recent_menu
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

        tools_menu.addSeparator()

        sync_names_menu = tools_menu.addMenu("Sync &Names in NLE")
        self._sync_names_resolve_action = QAction("DaVinci &Resolve", self)
        self._sync_names_resolve_action.triggered.connect(self._sync_names_resolve)
        sync_names_menu.addAction(self._sync_names_resolve_action)
        self._sync_names_premiere_action = QAction("Adobe &Premiere", self)
        self._sync_names_premiere_action.triggered.connect(self._sync_names_premiere)
        sync_names_menu.addAction(self._sync_names_premiere_action)
        self._refresh_sync_names_state()

        view_menu = menubar.addMenu("&View")
        self.log_dock_action = self.log_dock.toggleViewAction()
        self.log_dock_action.setShortcut(QKeySequence("Ctrl+L"))
        view_menu.addAction(self.log_dock_action)

        view_menu.addSeparator()
        self._history_timeline_action = QAction("History &Timeline...", self)
        self._history_timeline_action.triggered.connect(self._open_history_timeline)
        self._history_timeline_action.setEnabled(False)
        view_menu.addAction(self._history_timeline_action)

        source_menu = menubar.addMenu("&Sources")

        add_source_action = QAction("&Add Source...", self)
        add_source_action.triggered.connect(self._add_source)
        source_menu.addAction(add_source_action)

        refresh_action = QAction("&Refresh All", self)
        refresh_action.setShortcut(QKeySequence("Ctrl+R"))
        refresh_action.triggered.connect(lambda _=False: self._refresh_all(full=True))
        source_menu.addAction(refresh_action)

        source_menu.addSeparator()

        promote_all_action = QAction("&Promote All to Latest", self)
        # F9 is shown in the menu; Ctrl+Alt+Up still works. The Shift
        # variants force re-promotion (_promote_all_or_selected reads Shift).
        promote_all_action.setShortcuts([
            QKeySequence(s) for s in PROMOTE_ALL_SHORTCUTS + PROMOTE_ALL_FORCE_SHORTCUTS
        ])
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
        # F5: Refresh all (full rescan, matching toolbar Refresh All and Ctrl+R)
        f5 = QAction(self)
        f5.setShortcut(QKeySequence("F5"))
        f5.triggered.connect(lambda _=False: self._refresh_all(full=True))
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

        # Delete: Remove selected sources — scoped to source_list so it never
        # swallows Delete keystrokes inside source_search or other inputs.
        delete_sc = QShortcut(QKeySequence(Qt.Key_Delete), self.source_list)
        delete_sc.setContext(Qt.WidgetWithChildrenShortcut)
        delete_sc.activated.connect(self._delete_selected_sources)
        self._delete_shortcut = delete_sc

        # Ctrl+D: Deselect all sources
        deselect_sc = QAction(self)
        deselect_sc.setShortcut(QKeySequence("Ctrl+D"))
        deselect_sc.triggered.connect(self.source_list.clearSelection)
        self.addAction(deselect_sc)



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
        if getattr(self, "watcher", None) and self.watcher.is_running:
            self._start_watcher()
        if self.config_path:
            self._save_project()
        self._reload_ui()

    def _check_for_updates(self):
        dlg = UpdateDialog(self)
        dlg.exec()

    def _show_about(self):
        dlg = AboutDialog(self)
        dlg.exec()



    # --- Project management ---













    # --- Hook trust management ---







    # --- Source management ---

    def _add_source(self):
        if not self.config:
            return
        source = self._prompt_new_source(None)
        if source is None:
            return
        self._finish_adding_sources()
        self.statusBar().showMessage(f"Added source: {source.name}")

    def _prompt_new_source(self, draft):
        """Run the Add Source dialog (optionally prefilled with ``draft``).

        On OK the source is appended to the project and returned; on Cancel
        returns None. Callers finish with :meth:`_finish_adding_sources`.
        """
        while True:
            dlg = SourceDialog(source=draft, project_config=self.config,
                               parent=self, is_new=True)
            if dlg.exec() != QDialog.Accepted:
                return None
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
        return draft

    def _finish_adding_sources(self):
        """Refill inherited fields (e.g. latest target), save and refresh."""
        apply_project_defaults(self.config)
        # Sources sharing a latest_target must not share a history sidecar,
        # or their promotions overwrite each other's records. New sources
        # are appended last, so they're the ones that get namespaced and
        # existing sources keep their on-disk history files.
        from lvm.config import _migrate_shared_history_filenames
        _migrate_shared_history_filenames(self.config)
        self._mark_dirty()
        if getattr(self, "watcher", None) and self.watcher.is_running:
            self._start_watcher()
        if self.config_path:
            self._save_project()
        self._reload_ui()

    def _handle_source_drop(self, paths: list):
        """Turn folders dropped onto the sources list into new sources.

        Each dropped path gets a quick depth-0 discovery scan and opens a
        prefilled Add Source dialog; Cancel skips just that one.
        """
        if not self.config:
            self.statusBar().showMessage("Open or create a project before adding sources")
            return

        from lvm.discovery import resolve_drop_dir, source_drafts_for_path

        def _key(p) -> str:
            try:
                return os.path.normcase(str(Path(p).resolve()))
            except (OSError, ValueError):
                return os.path.normcase(str(p))

        existing = {_key(s.source_dir) for s in self.config.watched_sources if s.source_dir}
        seen = set()
        drafts = []
        skipped = []
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            for p in paths:
                root = resolve_drop_dir(p)
                k = _key(root)
                if k in seen:
                    continue
                seen.add(k)
                if k in existing:
                    skipped.append(root.name)
                    continue
                try:
                    drafts.extend(source_drafts_for_path(root, self.config))
                except Exception as e:  # scan failure shouldn't block the add
                    logger.warning(f"Drop scan failed for {root}: {e}")
                    drafts.append(WatchedSource(name=root.name, source_dir=str(root)))
        finally:
            QApplication.restoreOverrideCursor()

        added = [s for s in (self._prompt_new_source(d) for d in drafts) if s]
        if added:
            self._finish_adding_sources()
        msg = f"Added {len(added)} source(s) from drop"
        if skipped:
            msg += f" — skipped (already a source): {', '.join(skipped)}"
        self.statusBar().showMessage(msg)

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
        apply_project_defaults(self.config)
        from lvm.config import _migrate_shared_history_filenames
        _migrate_shared_history_filenames(self.config)
        # Trust hooks explicitly saved by the user on this source
        self._trust_hook_commands(draft.pre_promote_cmd, draft.post_promote_cmd)
        self._mark_dirty()
        if getattr(self, "watcher", None) and self.watcher.is_running:
            self._start_watcher()
        if self.config_path:
            self._save_project()
        self._reload_ui()

    def _bulk_edit_sources(self, indices: list):
        """Apply selected field changes to several sources at once."""
        if not self.config:
            return
        sources = [
            self.config.watched_sources[i] for i in indices
            if 0 <= i < len(self.config.watched_sources)
        ]
        if not sources:
            return
        dlg = BulkEditSourcesDialog(sources, project_config=self.config, parent=self)
        if dlg.exec() != QDialog.Accepted:
            return
        edits = dlg.get_edits()
        if not edits:
            return

        # A typed-in group that doesn't exist yet is created with the next
        # free palette color (same behaviour as "Assign to New Group...").
        group_edit = edits.get("group")
        new_group = group_edit.get("value") if group_edit else ""
        if new_group and new_group not in self.config.groups:
            used = {v.get("color", "") for v in self.config.groups.values()}
            color = "#8c8c8c"
            for c in _GROUP_COLOR_PALETTE:
                if c not in used:
                    color = c
                    break
            self.config.groups[new_group] = {"color": color}

        from lvm.config import apply_bulk_edits
        n = apply_bulk_edits(self.config, [s.name for s in sources], edits)
        if not n:
            return
        # Refill inherited fields from project defaults
        apply_project_defaults(self.config)
        self._mark_dirty()
        if self.config_path:
            self._save_project()
        self.statusBar().showMessage(f"Updated {n} source{'s' if n != 1 else ''}")
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
            if self.watcher.is_running:
                self._start_watcher()
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
            menu.addAction(f"Edit {len(selected_indices)} Sources...", lambda: self._bulk_edit_sources(selected_indices))
            menu.addAction(f"Remove {len(selected_indices)} Sources", lambda: self._remove_sources(selected_indices))

        # Refresh selected
        menu.addSeparator()
        if len(selected_indices) == 1:
            menu.addAction("Refresh Source", lambda: self._refresh_selected_sources(selected_indices))
        else:
            menu.addAction(f"Refresh {len(selected_indices)} Sources", lambda: self._refresh_selected_sources(selected_indices))

        # Undo last promote (single source only)
        if len(selected_indices) == 1:
            menu.addSeparator()
            idx = selected_indices[0]
            source = self.config.watched_sources[idx]
            promoter = self._promoters.get(source.name)
            try:
                can_undo = bool(promoter and len(promoter.get_history()) >= 2)
            except OSError:
                can_undo = False
            undo_action = menu.addAction("Undo Last Promote", lambda: self._undo_source(idx))
            undo_action.setShortcut(QKeySequence.Undo)
            undo_action.setEnabled(can_undo and not self._is_promotion_active)

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
        try:
            current = promoter.get_current_version() if promoter else None
        except OSError:
            current = None
        is_promoted = current and version_strings_match(version.version_string, current.version, version.version_number)

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
        promote_action.setEnabled(not self._is_promotion_active)

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
                from lvm.task_tokens import format_date_display
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

    def _open_history_timeline(self):
        """Open the Gantt-style promotion history viewer."""
        if not self.config:
            QMessageBox.information(self, "No Project", "Open or create a project first.")
            return
        dlg = HistoryTimelineDialog(self.config, parent=self)
        dlg.source_activated.connect(self._select_source_by_name)
        dlg.exec()

    def _select_source_by_name(self, name: str):
        """Select the named source in the main source list (no-op if missing)."""
        for i in range(self.source_list.topLevelItemCount()):
            item = self.source_list.topLevelItem(i)
            if item.data(0, Qt.UserRole) == name:
                self.source_list.setCurrentItem(item)
                self.source_list.scrollToItem(item)
                return

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






    # ----- Discover & Promote (one-click roundtrip) -----

    _discover_promote_worker = None








    # --- UI Updates ---







    # Keep old name as alias for the cache-first load path





































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
            populate_timecodes_parallel(versions, max_workers=8)
        # "always" — already populated during scan (see _reload_ui)
        # "never"  — leave as None

        # Use cached status from StatusWorker to avoid redundant I/O
        status_info = self._source_status.get(source.name, {})
        if status_info:
            current = status_info.get("current")
        elif promoter:
            try:
                current = promoter.get_current_version()
            except OSError as e:
                logger.warning("Could not read history for '%s': %s", source.name, e)
                current = None
        else:
            current = None
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
            highest_num = versions[-1].version_number if versions else None
            is_highest = bool(versions) and version_strings_match(highest_ver, current.version, highest_num)
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
                try:
                    integrity = promoter.verify()
                except OSError as e:
                    integrity = {"valid": False, "message": f"History unreadable: {e}"}
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
        elif status_info.get("status") == "error":
            message = (status_info.get("integrity") or {}).get("message", "")
            self.current_label.setText(f"Status unavailable   ({source.name})")
            self.current_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #ff4444;")
            self.integrity_label.setText(f"⚠ {message}  — press Refresh to retry")
            self.integrity_label.setStyleSheet("font-size: 11pt; color: #ffaa00;")
            self.current_banner.setStyleSheet(
                "QFrame { background-color: #3a2020; border: 1px solid #5a3030; "
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
            and versions
            and not version_strings_match(versions[-1].version_string, current_ver, versions[-1].version_number)
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
                from lvm.task_tokens import format_date_display
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

            if version_strings_match(v.version_string, current_ver, v.version_number):
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
                    and not version_strings_match(v.version_string, current_ver, v.version_number)):
                item.setForeground(5, QColor("#ff9944"))

            self.version_tree.addTopLevelItem(item)

        self._queue_version_thumbnails()

        # Populate history
        if promoter:
            try:
                history = promoter.get_history()
            except OSError as e:
                logger.warning("Could not read history for '%s': %s", source.name, e)
                history = []
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
            try:
                current = promoter.get_current_version() if promoter else None
            except OSError:
                current = None

            if current and version_strings_match(version.version_string, current.version, version.version_number):
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







    # --- Promotion ---
















    # --- File Watcher ---













    # --- Log viewer helpers (Feature #19) ---

    _LOG_COLORS = {
        "DEBUG": "#8c8c8c",
        "INFO": "#cccccc",
        "WARNING": "#ffaa00",
        "ERROR": "#ff4444",
        "CRITICAL": "#ff0000",
    }

    def _maybe_autoshow_log_dock(self, level: str, _message: str = ""):
        """Pop the log dock when an error is logged, so failures aren't
        only visible in dismissable message boxes.

        The 30s cooldown keeps it from fighting the user: closing the dock
        during an error burst keeps it closed for the rest of the burst.
        """
        if level not in ("ERROR", "CRITICAL"):
            return
        if self.log_dock.isVisible():
            return
        now = time.monotonic()
        if now < self._log_autoshow_cooldown_until:
            return
        self._log_autoshow_cooldown_until = now + 30.0
        self.log_dock.setVisible(True)

    def _append_log_entry(self, level: str, message: str):
        import html as _html
        color = self._LOG_COLORS.get(level, "#cccccc")
        min_level = self.log_level_filter.currentText()
        level_order = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if min_level != "ALL":
            if level_order.index(level) < level_order.index(min_level):
                return
        self.log_text.appendHtml(f'<span style="color:{color}">{_html.escape(message)}</span>')

    def _apply_log_capture_level(self, *_):
        """Capture DEBUG only when the log filter is ALL or DEBUG. Records
        dropped while it was higher are not in the buffer, so switching to
        DEBUG shows debug output from that point on."""
        wants_debug = self.log_level_filter.currentText() in ("ALL", "DEBUG")
        level = logging.DEBUG if wants_debug else logging.INFO
        self._log_handler.setLevel(level)
        logging.getLogger().setLevel(level)

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
        # Bump the selected version to the front of the row-thumbnail queue
        # (LIFO), regardless of the preview panel's visibility.
        if (self._version_thumbs_enabled and current is not None
                and self._current_source and self._thumb_queue_worker
                and self.config_path):
            v = current.data(0, Qt.UserRole)
            if v is not None:
                self._thumb_queue_worker.request(
                    v.source_path, v.version_string,
                    self._current_source.file_extensions,
                    str(Path(self.config_path).parent / ".lvm_cache"),
                )

        # Only load the large preview when the panel is visible
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

        self._thumb_request_id += 1
        req_id = self._thumb_request_id
        w = ThumbnailWorker(
            version.source_path, version.version_string,
            self._current_source.file_extensions, cache_dir,
            request_id=req_id, parent=self,
        )
        self._thumb_worker = w
        self._active_thumb_workers.add(w)
        w.completed.connect(self._on_thumbnail_ready)
        w.finished.connect(lambda worker=w: self._on_thumb_worker_done(worker))
        w.start()

    def _on_thumb_worker_done(self, worker):
        self._active_thumb_workers.discard(worker)
        if self._thumb_worker is worker:
            self._thumb_worker = None
        worker.deleteLater()

    def _on_thumbnail_ready(self, req_id: int, thumb_path: str):
        if req_id != self._thumb_request_id:
            return
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

    # --- Version-tree thumbnails (opt-in row icons) ---

    def _toggle_version_thumbnails(self, checked: bool):
        """Enable/disable per-version thumbnails in the version tree."""
        self._version_thumbs_enabled = checked
        self._settings.setValue("version_thumbs_enabled", checked)
        if checked:
            self.version_tree.setIconSize(QSize(48, 27))
            self._queue_version_thumbnails()
        else:
            self.version_tree.setIconSize(QSize())
            if self._thumb_queue_worker:
                self._thumb_queue_worker.clear_pending()
            for i in range(self.version_tree.topLevelItemCount()):
                self.version_tree.topLevelItem(i).setIcon(0, QIcon())

    def _ensure_thumb_queue_worker(self) -> ThumbnailQueueWorker:
        if self._thumb_queue_worker is None:
            self._thumb_queue_worker = ThumbnailQueueWorker(self)
            self._thumb_queue_worker.ready.connect(self._on_version_thumb_ready)
            self._thumb_queue_worker.start()
        return self._thumb_queue_worker

    def _queue_version_thumbnails(self):
        """Queue background thumbnail generation for all version rows."""
        if not (self._version_thumbs_enabled and self.config_path
                and self._current_source):
            return
        cache_dir = str(Path(self.config_path).parent / ".lvm_cache")
        worker = self._ensure_thumb_queue_worker()
        worker.clear_pending()
        exts = self._current_source.file_extensions
        # Bottom-up so the topmost (newest) rows land at the front of the
        # LIFO queue and get their thumbnails first.
        for i in range(self.version_tree.topLevelItemCount() - 1, -1, -1):
            item = self.version_tree.topLevelItem(i)
            v = item.data(0, Qt.UserRole)
            if v is not None:
                worker.request(v.source_path, v.version_string, exts, cache_dir)

    def _on_version_thumb_ready(self, key: str, thumb_path: str):
        """Set the row icon when its thumbnail lands (rows looked up by
        source_path — item pointers may have been invalidated by a repopulate)."""
        if not thumb_path or not self._version_thumbs_enabled:
            return
        for i in range(self.version_tree.topLevelItemCount()):
            item = self.version_tree.topLevelItem(i)
            v = item.data(0, Qt.UserRole)
            if v is not None and v.source_path == key:
                item.setIcon(0, QIcon(thumb_path))
                return

    # --- State persistence ---

    def _restore_state(self):
        geometry = self._settings.value("window_geometry", None)
        if geometry is not None:
            self.restoreGeometry(geometry)
        main_sizes = self._settings.value("main_splitter_sizes", None)
        if main_sizes and hasattr(self, "main_splitter"):
            try:
                self.main_splitter.setSizes([int(x) for x in main_sizes])
            except (TypeError, ValueError):
                pass
        vh_sizes = self._settings.value("ver_hist_splitter_sizes", None)
        if vh_sizes and hasattr(self, "ver_hist_splitter"):
            try:
                self.ver_hist_splitter.setSizes([int(x) for x in vh_sizes])
            except (TypeError, ValueError):
                pass
        last_project = self._settings.value("last_project", None)
        if last_project and os.path.exists(last_project):
            self._load_project(last_project)

    def closeEvent(self, event):
        # A promotion killed mid-copy or mid-swap leaves the target broken
        # until the next promotion recovers it — cancel it cleanly first.
        if self._worker is not None and self._worker.isRunning():
            reply = QMessageBox.question(
                self, "Promotion In Progress",
                "A promotion is still running.\n\n"
                "Cancel it and quit? LVM waits for it to stop safely, so the "
                "target keeps a complete version (the previous one, or the new "
                "one if the promotion was already in its final step).",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if reply != QMessageBox.Yes:
                event.ignore()
                return
            worker = self._worker
            worker.cancel()
            # No "Promotion cancelled" dialogs or batch prompts while closing.
            try:
                worker.disconnect()
            except RuntimeError:
                pass
            self.statusBar().showMessage("Cancelling promotion before quitting…")
            deadline = time.monotonic() + 60.0
            while worker.isRunning():
                if time.monotonic() > deadline:
                    QMessageBox.warning(
                        self, "Still Finishing",
                        "The promotion is still finishing its last step. "
                        "Try closing again in a moment.",
                    )
                    event.ignore()
                    return
                QApplication.processEvents()
                worker.wait(100)

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
                if not self._save_project(blocking=True):
                    event.ignore()
                    return

        self._closing = True
        self._settings.setValue("window_geometry", self.saveGeometry())
        if hasattr(self, "main_splitter"):
            self._settings.setValue("main_splitter_sizes", self.main_splitter.sizes())
        if hasattr(self, "ver_hist_splitter"):
            self._settings.setValue("ver_hist_splitter_sizes", self.ver_hist_splitter.sizes())
        if self.config_path:
            self._settings.setValue("last_project", self.config_path)

        # The queue worker blocks on a condition variable — wake it so its
        # run() can return before the generic quit/wait below.
        if self._thumb_queue_worker is not None:
            self._thumb_queue_worker.stop()

        # Disconnect signals and stop all background workers to avoid
        # callbacks firing into a half-destroyed window.
        workers_to_stop = [
            self._scan_worker,
            self._status_worker,
            *self._auto_check_workers.values(),
            self._worker,
            self._thumb_worker,
            *list(self._active_thumb_workers),
            self._thumb_queue_worker,
            self._project_load_worker,
            getattr(self, "_sync_names_worker", None),
            getattr(self, "_discover_promote_worker", None),
        ]
        seen_ids = set()
        for worker in workers_to_stop:
            if worker is None or id(worker) in seen_ids:
                continue
            seen_ids.add(id(worker))
            try:
                worker.disconnect()
            except RuntimeError:
                pass
            if worker.isRunning():
                worker.quit()
                worker.wait(3000)

        # Flush in-flight project config / scan cache writes before exiting
        self._io_executor.shutdown(wait=True)
        self.watcher.stop()
        self._notifier.set_level("off")  # remove the tray icon
        # Detach QtLogHandler from root logger so post-close log records
        # never emit onto a destroyed MainWindow.
        if hasattr(self, "_log_handler") and self._log_handler is not None:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler.close()
        event.accept()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

