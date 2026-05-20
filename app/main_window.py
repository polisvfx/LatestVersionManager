"""Main application window."""

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
    PromoteWorker, ThumbnailWorker, ScanWorker, StatusWorker,
    SyncNamesWorker, ProjectLoadWorker,
)
from app.widgets import VersionTreeWidget, SourceItemDelegate
from app.dialogs.about import AboutDialog
from app.dialogs.batch_promote import BatchPromoteReviewDialog
from app.dialogs.discovery import DiscoveryDialog
from app.dialogs.dry_run import DryRunDialog
from app.dialogs.latest_path import LatestPathDialog
from app.dialogs.manage_groups import ManageGroupsDialog
from app.dialogs.obsolete_layer import ObsoleteLayerDialog
from app.dialogs.project_setup import ProjectSetupDialog
from app.dialogs.settings import ProjectSettingsDialog
from app.dialogs.source import SourceDialog
from app.dialogs.update import UpdateDialog


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
        self._project_load_worker: ProjectLoadWorker = None
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

        # Search box — debounced so a fast typist doesn't trigger one full
        # source-list rebuild per keystroke. 150ms feels instant but coalesces
        # bursts.
        self.source_search = QLineEdit()
        self.source_search.setPlaceholderText("Search sources...")
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
        # Reentrancy guard — ignore if a load is already in flight
        if getattr(self, "_project_load_worker", None) is not None and self._project_load_worker.isRunning():
            return
        self.statusBar().showMessage(f"Loading: {path}…")
        self._project_load_worker = ProjectLoadWorker(path, self)
        self._project_load_worker.finished.connect(self._on_project_loaded)
        self._project_load_worker.error.connect(self._on_project_load_error)
        self._project_load_worker.start()

    def _on_project_loaded(self, config, cached, path: str):
        """Called on the main thread when ProjectLoadWorker finishes."""
        try:
            self.config = config
            self.config_path = path
            self._add_to_recent(path)

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
            self._refresh_sync_names_state()
            self.statusBar().showMessage(f"Loaded: {path}")
        finally:
            self._project_load_worker = None

    def _on_project_load_error(self, message: str, path: str):
        """Called on the main thread when ProjectLoadWorker raises."""
        try:
            self.statusBar().clearMessage()
            QMessageBox.critical(self, "Error", f"Failed to load config:\n{message}")
        finally:
            self._project_load_worker = None

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
            self._refresh_sync_names_state()
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
                promoter = Promoter(source, self.config.task_tokens, self.config.project_name,
                                    nle_rename_options=self.config.nle_rename_options())
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
            self._maybe_auto_sync_nle()
            return

        source, version = self._batch_promote_list[self._batch_promote_index]
        promoter = self._promoters.get(source.name)
        if not promoter:
            # Create promoter if needed
            if source.latest_target:
                promoter = Promoter(source, self.config.task_tokens, self.config.project_name,
                                    nle_rename_options=self.config.nle_rename_options())
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
        """Check if a source matches the search query (name, filename, task).

        Uses ``WatchedSource.search_text`` which pre-lowercases and combines
        name, sample_filename, and source_dir basename into one cached string.
        """
        if not query:
            return True
        return query.lower() in source.search_text

    def _make_source_item(self, source: WatchedSource) -> QTreeWidgetItem:
        """Create a QTreeWidgetItem for a source with status coloring and multi-column data."""
        info = self._source_status.get(source.name, {})
        status = info.get("status", "no_target")
        current = info.get("current")
        has_overrides = info.get("has_overrides", False)

        ver_tag = current.version if current else ""

        marker = _STATUS_MARKERS.get(status, "")
        name_text = f"{marker}{source.name}"
        if source.name in self._target_conflicts:
            name_text += " [!]"
        group_text = source.group if source.group else ""

        status_text = _STATUS_LABELS.get(status, status)

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
                added_on = datetime.fromisoformat(source.added_at).strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                added_on = source.added_at

        # Last promoted timestamp
        last_promoted = ""
        if current and current.set_at:
            try:
                last_promoted = datetime.fromisoformat(current.set_at).strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                last_promoted = current.set_at

        item = QTreeWidgetItem([name_text, group_text, ver_tag, layers, frames, filetype, added_on, last_promoted, status_text])
        item.setData(0, Qt.UserRole, source.name)

        # Colour comes from a shared cache; tooltip is per-instance.
        color = _STATUS_COLORS.get(status, _STATUS_COLORS["no_target"])
        if status == "newer":
            tooltip = f"Newer versions available since {ver_tag} was promoted"
        elif status == "stale":
            tooltip = f"Source files for {ver_tag} modified since promotion — may have been re-rendered"
        elif status == "deliberate":
            tooltip = f"Pinned on {ver_tag} (Keep) — batch promote skips until a new version arrives"
        elif status == "highest":
            tooltip = f"On latest version: {ver_tag}"
        elif status == "integrity_fail":
            tooltip = f"Integrity issue with {ver_tag}"
        elif status == "no_version":
            tooltip = "No version promoted yet"
        else:  # no_target
            tooltip = "No latest target path configured"

        # Override marker (blue tint on top)
        if has_overrides:
            color = _OVERRIDE_COLOR
            tooltip += " | Custom settings"

        # Conflict warning
        if source.name in self._target_conflicts:
            conflict_names = ", ".join(self._target_conflicts[source.name])
            color = _CONFLICT_COLOR
            tooltip += f" | TARGET CONFLICT with: {conflict_names}"

        if source.group:
            tooltip += f" | Group: {source.group}"

        # Apply color to all columns
        group_col_idx = self._source_col_keys.index("group")
        for col in range(len(self._source_col_keys)):
            if col == group_col_idx:
                continue  # Group column gets its own color
            item.setForeground(col, color)

        # Color the group column with the group's own color (cached by hex)
        if source.group and self.config and source.group in self.config.groups:
            grp_hex = self.config.groups[source.group].get("color", _DEFAULT_GROUP_COLOR_HEX)
            item.setForeground(group_col_idx, _group_qcolor(grp_hex))

        item.setToolTip(0, tooltip)

        return item

    def _populate_source_list(self):
        """Build source list items based on computed status, active filter, search query, and grouping."""
        # Suppress repaints during rebuild — saves dozens of intermediate
        # paints on a 100+-source list.
        self.source_list.setUpdatesEnabled(False)
        try:
            self._populate_source_list_inner()
        finally:
            self.source_list.setUpdatesEnabled(True)

    def _populate_source_list_inner(self):
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
        conflicts = detect_target_conflicts(self.config, self.config.task_tokens)
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

    def _refresh_sync_names_state(self):
        """Refresh the sync menu and status-bar buttons based on what's installed."""
        try:
            from src.lvm.nle_bridge import (
                is_resolve_external_available,
                is_resolve_running,
                is_premiere_panel_alive,
                premiere_panel_install_dir,
            )
            resolve_available = is_resolve_external_available()
            resolve_alive = resolve_available and is_resolve_running()
            premiere_alive = is_premiere_panel_alive()
            premiere_install_dir = premiere_panel_install_dir()
        except ImportError:
            resolve_available = False
            resolve_alive = False
            premiere_alive = False
            premiere_install_dir = None

        # ----- Resolve menu + button -----
        running_resolve = (getattr(self, "_sync_names_worker", None) is not None and
                           self._sync_names_worker.isRunning())

        if not resolve_available:
            r_tip = ("Requires DaVinci Resolve Studio (free version supports "
                     "the in-NLE script only: Workspace -> Scripts -> Edit -> "
                     "lvm_restore_versions).")
        elif not resolve_alive:
            r_tip = ("DaVinci Resolve isn't running. Open it first, then "
                     "click Sync Resolve.")
        else:
            r_tip = ("Run the LVM rename script against the open "
                     "DaVinci Resolve Studio project.")
        self._sync_names_resolve_action.setEnabled(
            resolve_available and resolve_alive and not running_resolve)
        self._sync_names_resolve_action.setToolTip(r_tip)

        if hasattr(self, "_nle_sync_btn"):
            if running_resolve:
                self._nle_sync_btn.setText("Syncing Resolve…")
                self._nle_sync_btn.setEnabled(False)
                self._nle_sync_btn.setToolTip("Sync in progress — see log dock.")
            elif not resolve_available:
                self._nle_sync_btn.setText("Resolve: not detected")
                self._nle_sync_btn.setEnabled(False)
                self._nle_sync_btn.setToolTip(r_tip)
            elif not resolve_alive:
                self._nle_sync_btn.setText("Resolve: not running")
                self._nle_sync_btn.setEnabled(False)
                self._nle_sync_btn.setToolTip(r_tip)
            else:
                self._nle_sync_btn.setText("Sync Resolve")
                self._nle_sync_btn.setEnabled(True)
                auto = (self.config and
                        getattr(self.config, "nle_auto_sync_resolve", False))
                hint = " (auto-sync after promote enabled)" if auto else ""
                self._nle_sync_btn.setToolTip(
                    "Click to run the Resolve rename script now." + hint)

        # ----- Premiere menu + button -----
        # The Premiere bridge is fire-and-forget — LVM writes a trigger
        # file and the panel handles it asynchronously. There's no
        # "running" state to gate against.
        if premiere_alive:
            p_tip = ("Write a sync trigger to the LVM Premiere panel "
                     "(panel processes it within ~1s).")
        elif premiere_install_dir is None:
            p_tip = "Adobe Premiere isn't supported on this OS."
        else:
            p_tip = (f"LVM Premiere panel not detected. Install the panel from "
                     f"companions/premiere/lvm_panel/ into\n{premiere_install_dir}\n"
                     f"and start Premiere. See docs/companions.md.")
        self._sync_names_premiere_action.setEnabled(premiere_alive)
        self._sync_names_premiere_action.setToolTip(p_tip)

        if hasattr(self, "_nle_premiere_btn"):
            if premiere_alive:
                self._nle_premiere_btn.setText("Sync Premiere")
                self._nle_premiere_btn.setEnabled(True)
                auto = (self.config and
                        getattr(self.config, "nle_auto_sync_premiere", False))
                hint = " (auto-sync after promote enabled)" if auto else ""
                self._nle_premiere_btn.setToolTip(
                    "Click to write a Premiere sync trigger now." + hint)
            else:
                self._nle_premiere_btn.setText("Premiere: not detected")
                self._nle_premiere_btn.setEnabled(False)
                self._nle_premiere_btn.setToolTip(p_tip)

    def _sync_names_resolve(self, *, automatic: bool = False):
        """Spawn the Resolve companion script and stream results to the log dock.

        ``automatic=True`` is set when this is fired from the post-promote
        auto-sync hook — it suppresses the modal "already running" dialog
        (just logs and exits) and tags the launch line for log clarity.
        """
        if getattr(self, "_sync_names_worker", None) is not None and \
                self._sync_names_worker.isRunning():
            if automatic:
                logger.info("Sync Names: skipping auto-sync — a previous sync "
                            "is still running.")
                return
            QMessageBox.information(
                self, "Sync Names",
                "A sync is already in progress. Wait for it to finish.",
            )
            return

        # Fast pre-flight: even if the cached "Resolve running" state was
        # stale and the button got clicked, re-check now so we surface a
        # clear message instead of a 4-second timeout from the worker.
        try:
            from src.lvm.nle_bridge import (
                is_resolve_external_available, is_resolve_running,
                invalidate_resolve_running_cache,
            )
        except ImportError:
            is_resolve_external_available = lambda: False
            is_resolve_running = lambda force=False: False
            invalidate_resolve_running_cache = lambda: None

        invalidate_resolve_running_cache()
        if not is_resolve_external_available():
            self._refresh_sync_names_state()
            if not automatic:
                QMessageBox.information(
                    self, "Sync Resolve",
                    "DaVinci Resolve Studio isn't installed on this machine. "
                    "Free Resolve users can run Workspace → Scripts → Edit → "
                    "lvm_restore_versions from inside Resolve instead.",
                )
            else:
                logger.info("Auto-sync Resolve: skipped (Resolve not detected).")
            return

        if not is_resolve_running(force=True):
            self._refresh_sync_names_state()
            if not automatic:
                QMessageBox.information(
                    self, "Sync Resolve",
                    "DaVinci Resolve isn't running.\n\n"
                    "Open Resolve and try again.",
                )
            else:
                logger.info("Auto-sync Resolve: skipped (Resolve not running).")
            return

        self._refresh_sync_names_state()
        self.log_dock.setVisible(True)
        prefix = "auto-sync after promote" if automatic else "manual sync"
        logger.info("Sync Names (%s): launching DaVinci Resolve companion "
                    "script (this can take a while on large projects)...", prefix)

        worker = SyncNamesWorker(self)
        worker.line.connect(self._on_sync_names_line)
        worker.done.connect(self._on_sync_names_done)
        self._sync_names_worker = worker
        worker.start()
        self._refresh_sync_names_state()

    def _sync_names_premiere(self, *, automatic: bool = False):
        """Write a Premiere sync trigger file. The installed CEP panel picks it up.

        Fire-and-forget — the panel runs the rename inside Premiere and
        deletes the trigger when done. No subprocess, no port, no waiting.
        """
        try:
            from src.lvm.nle_bridge import (
                write_premiere_trigger,
                is_premiere_panel_alive,
            )
        except ImportError:
            logger.error("Sync Names → Premiere: bridge unavailable in this build.")
            return

        if not is_premiere_panel_alive():
            if automatic:
                logger.info("Auto-sync Premiere: skipped (panel not running).")
                return
            QMessageBox.information(
                self, "Sync Names → Premiere",
                "The LVM Premiere panel isn't running.\n\n"
                "Install the panel from companions/premiere/lvm_panel/ "
                "into your Adobe CEP extensions folder and open Premiere "
                "to load it. See docs/companions.md.",
            )
            return

        try:
            path = write_premiere_trigger({
                "automatic": bool(automatic),
            })
        except OSError as e:
            logger.error("Sync Names → Premiere: could not write trigger: %s", e)
            return

        prefix = "auto-sync after promote" if automatic else "manual sync"
        logger.info("Sync Names → Premiere (%s): trigger %s written; panel "
                    "should process it shortly.", prefix, path.name)

    def _maybe_auto_sync_nle(self):
        """Trigger NLE renames if the project opted in.

        Called at the end of a successful promote (single or batch) so
        editors don't need to think about the rename themselves. Both
        Resolve (if detected) and Premiere (if panel is alive) can fire,
        independently, based on their respective project settings.
        """
        if not self.config:
            return
        try:
            from src.lvm.nle_bridge import (
                is_resolve_external_available,
                is_premiere_panel_alive,
            )
        except ImportError:
            return

        if getattr(self.config, "nle_auto_sync_resolve", False):
            # Let _sync_names_resolve do its own pre-flight (force-checks
            # the running state and surfaces a clear log line on skip).
            self._sync_names_resolve(automatic=True)

        if getattr(self.config, "nle_auto_sync_premiere", False):
            if is_premiere_panel_alive():
                self._sync_names_premiere(automatic=True)
            else:
                logger.info("Auto-sync Premiere: skipped (panel not running).")

    def _on_sync_names_line(self, level: str, text: str):
        if level == "error":
            logger.error("[resolve] %s", text)
        elif level == "warning":
            logger.warning("[resolve] %s", text)
        else:
            logger.info("[resolve] %s", text)

    def _on_sync_names_done(self, ok: bool, error: str):
        if error:
            logger.error("Sync Names: %s", error)
        if ok:
            logger.info("Sync Names: completed successfully.")
        else:
            logger.warning("Sync Names: finished with errors — see log lines above.")

        self._sync_names_worker = None
        self._refresh_sync_names_state()

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
            populate_timecodes_parallel(versions, max_workers=8)
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

    def _get_next_manual_version_number(self, source_name: str) -> int:
        """Determine the next version number for a manual import."""
        scanned = self._versions_cache.get(source_name, [])
        manual = self._manual_versions.get(source_name, [])
        all_versions = scanned + manual
        if all_versions:
            return max(v.version_number for v in all_versions) + 1
        return 1

    def _infer_manual_padding(self, source_name: str) -> int:
        """Pick a padding width for a manually added version.

        Matches the padding used by the highest-numbered existing version in
        the source (scanned or manual) so manual versions visually match the
        rest of the list. Falls back to 3 when the source has no versions.
        """
        scanned = self._versions_cache.get(source_name, [])
        manual = self._manual_versions.get(source_name, [])
        all_versions = scanned + manual
        if not all_versions:
            return 3
        highest = max(all_versions, key=lambda v: v.version_number)
        digits = "".join(ch for ch in (highest.version_string or "") if ch.isdigit())
        return len(digits) if digits else 3

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
                    padding=self._infer_manual_padding(source.name),
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
                    padding=self._infer_manual_padding(source.name),
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
        is_keep = current and version_strings_match(version.version_string, current.version, version.version_number)

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
            if version_strings_match(v.version_string, entry.version, v.version_number):
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
        self._maybe_auto_sync_nle()

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

        if version_strings_match(highest.version_string, current_entry.version, highest.version_number):
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
                        self._worker, self._thumb_worker,
                        self._project_load_worker):
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

