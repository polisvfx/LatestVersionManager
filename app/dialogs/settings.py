"""settings dialog module."""

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
from app.widgets import CollapsibleSection, TagInputWidget, TokenChipBar


# Tokens accepted by the Latest Path Template (path-shaped output).
# {source_filename} is intentionally excluded — a full filename inside a path
# template rarely makes sense; that token belongs to File Rename instead.
_PATH_TOKENS: dict[str, str] = {
    "source_dir":      "Absolute directory of the watched source",
    "source_title":    "In-project display name of the source",
    "source_name":     "Filename without version / frame / extension",
    "source_basename": "source_name with task tokens stripped",
    "source_fullname": "Filename without frame / extension (keeps version)",
    "project_root":    "The project's root directory",
    "group":           "Group name (omitted with its divider when empty)",
    "group_root":      "Root directory of the group (falls back to project_root)",
}

# Tokens accepted by the File Rename Template (filename-shaped; no path tokens).
_RENAME_TOKENS: dict[str, str] = {
    "source_title":    "In-project display name of the source",
    "source_name":     "Filename without version / frame / extension",
    "source_basename": "source_name with task tokens stripped",
    "source_fullname": "Filename without frame / extension (keeps version)",
    "source_filename": "Raw original filename (including extension)",
    "group":           "Group name (omitted with its divider when empty)",
}

# Tokens accepted by Promotion Hook commands. Mirror src.lvm.hooks._build_hook_tokens.
_HOOK_TOKENS: dict[str, str] = {
    "source_name":  "Name of the source being promoted",
    "version":      "Version string being promoted (e.g. v003)",
    "source_dir":   "Absolute path to the version's source files",
    "target_dir":   "Absolute path of the latest target directory",
    "link_mode":    "Materialisation mode used: copy, hardlink or symlink",
    "user":         "Local username running the promotion",
    "project_name": "Name of the LVM project",
    "frame_range":  "Frame range of the sequence, e.g. 1001-1240 (empty for single files)",
    "file_count":   "Number of files being promoted",
}


def _tokens_tooltip(tokens: dict[str, str]) -> str:
    """Build a multi-line tooltip listing each token with its description."""
    return "Available tokens:\n" + "\n".join(
        f"  {{{name}}} — {desc}" for name, desc in tokens.items()
    )


def _stack_with_chips(edit: QLineEdit, tokens: dict[str, str]) -> QWidget:
    """Return a field-column widget holding *edit* stacked above a chip bar.

    Using a single form-row field (rather than two rows: one with the edit
    and a second with an empty label and the chips) keeps the chips
    naturally aligned with the input column and lets us control the gap
    directly via the inner QVBoxLayout.
    """
    container = QWidget()
    v = QVBoxLayout(container)
    v.setContentsMargins(0, 0, 0, 0)
    v.setSpacing(2)
    v.addWidget(edit)
    v.addWidget(TokenChipBar(edit, tokens))
    return container


def _unknown_tokens(text: str, allowed: set[str]) -> list[str]:
    """Return any {token} names referenced in *text* that are not in *allowed*."""
    return [m for m in re.findall(r"\{(\w+)\}", text or "") if m not in allowed]


class ProjectSettingsDialog(QDialog):
    """Dialog for editing project-wide settings."""

    _last_geometry = None  # remember size/position within session
    _collapsed_states: dict = {}  # title -> bool, remembered within session

    def __init__(self, config: ProjectConfig, selected_source: WatchedSource = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Project Settings")
        self.setMinimumWidth(900)
        self.setMinimumHeight(500)
        if ProjectSettingsDialog._last_geometry:
            self.restoreGeometry(ProjectSettingsDialog._last_geometry)
        else:
            # 25 % larger than the previous 700×600 default so all the
            # help text and the new NLE section breathe.
            self.resize(875, 750)
        self._config = config
        self._selected_source = selected_source
        self._naming_reset = False
        self._sections: list = []  # (title, CollapsibleSection) for state save

        # Outer layout: left nav bar + right scroll area, with bottom bar.
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # Left nav + right scroll, separated by a draggable splitter.
        body_splitter = QSplitter(Qt.Horizontal)
        body_splitter.setChildrenCollapsible(False)
        body_splitter.setHandleWidth(4)
        self._body_splitter = body_splitter

        # Left navigation list — clicking a row jumps to the matching section
        # and collapses all other sections (accordion behavior).
        self._nav_list = QListWidget()
        self._nav_list.setMinimumWidth(140)
        self._nav_list.setFrameShape(QFrame.NoFrame)
        self._nav_list.setStyleSheet(
            "QListWidget { background: #1c1c1c; border-right: 1px solid #2a2a2a;"
            " padding: 8px 0; outline: 0; }"
            "QListWidget::item { padding: 8px 12px; color: #c0c0c0; }"
            "QListWidget::item:hover { background: #242424; color: #fff; }"
            "QListWidget::item:selected { background: #2d4a63; color: #fff; }"
        )
        body_splitter.addWidget(self._nav_list)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll_widget = QWidget()
        top_layout = QVBoxLayout(scroll_widget)
        top_layout.setContentsMargins(12, 12, 12, 12)
        top_layout.setSpacing(8)
        self._scroll = scroll

        # ==================================================================
        # GENERAL
        # ==================================================================
        general_section = self._make_section("General")
        general = general_section.content_layout()

        self.name_edit = QLineEdit(config.project_name)
        self.name_edit.setToolTip("Display name for this project — shown in the title bar and recent-projects list.")
        general.addRow("Project Name:", self.name_edit)

        self.root_edit = QLineEdit(config.effective_project_root)
        self.root_edit.setToolTip("The root directory of the project. Expanded by the {project_root} token in templates.")
        self.root_edit.textChanged.connect(self._update_path_preview)
        self.root_browse_btn = QPushButton("Browse...")
        self.root_browse_btn.clicked.connect(self._browse_root)
        root_row = QHBoxLayout()
        root_row.addWidget(self.root_edit, 1)
        root_row.addWidget(self.root_browse_btn)
        general.addRow("Project Root:", root_row)

        top_layout.addWidget(general_section)

        # ==================================================================
        # OUTPUT PATHS
        # ==================================================================
        paths_section = self._make_section("Output Paths")
        paths = paths_section.content_layout()

        template_help = QLabel(
            "Relative paths resolve from each source's directory. "
            "If {group} is empty, the token and its trailing divider are omitted.\n"
            "Examples:  {group_root}/online/{source_name}  |  latest/{group}/{source_basename}_latest"
        )
        template_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        template_help.setWordWrap(True)
        paths.addRow("", template_help)

        # --- Latest Path Template ---
        self.latest_template_edit = QLineEdit(config.latest_path_template)
        self.latest_template_edit.setPlaceholderText("latest/{source_basename}_latest")
        self._latest_base_tooltip = _tokens_tooltip(_PATH_TOKENS)
        self.latest_template_edit.setToolTip(self._latest_base_tooltip)
        self.latest_template_edit.textChanged.connect(self._update_path_preview)
        self.latest_template_edit.textChanged.connect(
            lambda _t: self._validate_template(
                self.latest_template_edit, set(_PATH_TOKENS), self._latest_base_tooltip))
        paths.addRow("Latest Path Template:",
                     _stack_with_chips(self.latest_template_edit, _PATH_TOKENS))

        # --- File Rename Template ---
        rename_help = QLabel("Controls the output filename (without frame / extension).")
        rename_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        paths.addRow("", rename_help)

        self.rename_template_edit = QLineEdit(config.default_file_rename_template)
        self.rename_template_edit.setPlaceholderText("{source_basename}_latest")
        self._rename_base_tooltip = _tokens_tooltip(_RENAME_TOKENS)
        self.rename_template_edit.setToolTip(self._rename_base_tooltip)
        self.rename_template_edit.textChanged.connect(self._update_path_preview)
        self.rename_template_edit.textChanged.connect(
            lambda _t: self._validate_template(
                self.rename_template_edit, set(_RENAME_TOKENS), self._rename_base_tooltip))
        paths.addRow("File Rename Template:",
                     _stack_with_chips(self.rename_template_edit, _RENAME_TOKENS))

        # Run initial validation pass.
        self._validate_template(
            self.latest_template_edit, set(_PATH_TOKENS), self._latest_base_tooltip)
        self._validate_template(
            self.rename_template_edit, set(_RENAME_TOKENS), self._rename_base_tooltip)

        self.path_preview_label = QLabel("")
        self.path_preview_label.setStyleSheet("color: #3aaa88; font-size: 11pt;")
        self.path_preview_label.setWordWrap(True)
        self.path_preview_label.setMinimumWidth(50)
        self.path_preview_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        paths.addRow("Resolved Preview:", self.path_preview_label)

        top_layout.addWidget(paths_section)

        # ==================================================================
        # DETECTION (controls how versioned content is found on disk)
        # ==================================================================
        detection_section = self._make_section("Detection")
        detection = detection_section.content_layout()

        # --- Version pattern ---
        self.pattern_edit = QLineEdit(config.default_version_pattern)
        self.pattern_edit.setToolTip(
            "Pattern that identifies the version in filenames. "
            "Use {version} as the placeholder, e.g. _v{version} matches _v001, _v042."
        )
        detection.addRow("Version Pattern:", self.pattern_edit)

        # --- File extensions ---
        self.extensions_edit = QLineEdit(" ".join(config.default_file_extensions))
        self.extensions_edit.setToolTip(
            "Space-separated list of file extensions recognised when scanning "
            "for versioned content (e.g. exr dpx mov mp4)."
        )
        detection.addRow("File Extensions:", self.extensions_edit)

        # --- Date format (multi-select; 0 boxes checked == "(none)") ---
        from src.lvm.task_tokens import parse_date_formats as _parse_dfmts_proj
        self.date_format_checks: dict[str, QCheckBox] = {}
        _date_tooltips = {
            "DDMMYY":   "Day-month-year, two digits each — matches 210526",
            "YYMMDD":   "Year-month-day, two digits each — matches 260521",
            "DDMMYYYY": "Day-month-year, four-digit year — matches 21052026",
            "YYYYMMDD": "Year-month-day, four-digit year — matches 20260521",
        }
        date_row = QHBoxLayout()
        for fmt in ("DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD"):
            cb = QCheckBox(fmt)
            cb.setToolTip(_date_tooltips[fmt])
            self.date_format_checks[fmt] = cb
            date_row.addWidget(cb)
        date_row.addStretch(1)
        for fmt in _parse_dfmts_proj(config.default_date_format):
            if fmt in self.date_format_checks:
                self.date_format_checks[fmt].setChecked(True)
        detection.addRow("Date Format:", date_row)

        top_layout.addWidget(detection_section)

        # ==================================================================
        # SOURCE NAMING (controls how detected sources are named)
        # ==================================================================
        naming_section = self._make_section("Source Naming")
        naming = naming_section.content_layout()

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

        task_help = QLabel(
            "Task names stripped from filenames to produce cleaner source names. "
            "Each % matches one character (e.g. comp_%% matches comp_mp). "
            "Bounded by: _ - ."
        )
        task_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        task_help.setWordWrap(True)
        naming.addRow("", task_help)

        self.tasks_edit = QLineEdit(", ".join(config.task_tokens))
        self.tasks_edit.setPlaceholderText("comp, grade, dmp, fx, roto, paint")
        self.tasks_edit.setToolTip(
            "Comma-separated task tokens stripped from filenames to derive "
            "{source_basename}. Use % as a single-character wildcard "
            "(e.g. comp_%% matches comp_mp)."
        )
        self.tasks_edit.textChanged.connect(self._update_path_preview)
        naming.addRow("Task Names:", self.tasks_edit)

        top_layout.addWidget(naming_section)

        # ==================================================================
        # DISCOVERY FILTERS
        # ==================================================================
        filters_section = self._make_section("Discovery Filters")
        filters = filters_section.content_layout()

        self.whitelist_edit = TagInputWidget(config.name_whitelist, placeholder="Type and press comma to add...")
        filters.addRow("Whitelist:", self.whitelist_edit)

        self.blacklist_edit = TagInputWidget(config.name_blacklist, placeholder="Type and press comma to add...")
        filters.addRow("Blacklist:", self.blacklist_edit)

        top_layout.addWidget(filters_section)

        # ==================================================================
        # COPY OPERATIONS (collapsed by default)
        # ==================================================================
        advanced_section = self._make_section("Copy Operations", collapsed=True)
        adv = advanced_section.content_layout()

        # Link mode
        self.link_mode_combo = QComboBox()
        self.link_mode_combo.addItems(["copy", "hardlink", "symlink"])
        self.link_mode_combo.setCurrentText(config.default_link_mode)
        self.link_mode_combo.setToolTip(
            "How promoted files are materialised at the latest path:\n"
            "  copy      — full duplicate (safest, uses disk space)\n"
            "  hardlink  — same inode (instant, same volume only)\n"
            "  symlink   — pointer (instant; needs admin on Windows)"
        )
        adv.addRow("Default Link Mode:", self.link_mode_combo)

        # Timecode mode
        self.timecode_combo = QComboBox()
        self.timecode_combo.addItems(["always", "lazy", "never"])
        self.timecode_combo.setCurrentText(config.timecode_mode)
        self.timecode_combo.setToolTip(
            "When to read media timecodes during discovery:\n"
            "  always — read during scan (slower, all TCs visible immediately)\n"
            "  lazy   — read on demand when a source is viewed (fast scan)\n"
            "  never  — skip timecode extraction entirely (fastest)"
        )
        adv.addRow("Timecode Mode:", self.timecode_combo)

        # Promotion hooks
        hooks_header = QLabel("Promotion Hooks")
        hooks_header.setStyleSheet("font-weight: bold; margin-top: 6px;")
        adv.addRow("", hooks_header)

        hooks_help = QLabel(
            "Shell commands run before / after each promotion. "
            "Click a chip to insert a token. Leave empty to disable. "
            "Same tokens are also exported as LVM_* environment variables."
        )
        hooks_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        hooks_help.setWordWrap(True)
        adv.addRow("", hooks_help)

        hook_tokens_tip = _tokens_tooltip(_HOOK_TOKENS)

        self.pre_promote_edit = QLineEdit(getattr(config, 'pre_promote_cmd', '') or '')
        self.pre_promote_edit.setPlaceholderText("e.g. echo 'Promoting {source_name} {version}'")
        self.pre_promote_edit.setToolTip(hook_tokens_tip)
        adv.addRow("Pre-Promote Command:",
                   _stack_with_chips(self.pre_promote_edit, _HOOK_TOKENS))

        self.post_promote_edit = QLineEdit(getattr(config, 'post_promote_cmd', '') or '')
        self.post_promote_edit.setPlaceholderText("e.g. python notify.py --source {source_name} --version {version}")
        self.post_promote_edit.setToolTip(hook_tokens_tip)
        adv.addRow("Post-Promote Command:",
                   _stack_with_chips(self.post_promote_edit, _HOOK_TOKENS))

        # Sequence validation
        seq_header = QLabel("Sequence Validation")
        seq_header.setStyleSheet("font-weight: bold; margin-top: 6px;")
        adv.addRow("", seq_header)

        self.block_incomplete_cb = QCheckBox("Block promotion of incomplete sequences (warn on frame gaps)")
        self.block_incomplete_cb.setChecked(getattr(config, 'block_incomplete_sequences', False))
        self.block_incomplete_cb.setToolTip(
            "When enabled, promotion is blocked if the selected version has missing frames."
        )
        adv.addRow("", self.block_incomplete_cb)

        # Network / SMB performance
        net_header = QLabel("Network Performance")
        net_header.setStyleSheet("font-weight: bold; margin-top: 6px;")
        adv.addRow("", net_header)

        self.skip_resolve_cb = QCheckBox("Skip symlink resolution during discovery (faster over SMB)")
        self.skip_resolve_cb.setChecked(getattr(config, 'skip_resolve', True))
        self.skip_resolve_cb.setToolTip(
            "Skips Path.resolve() per directory, eliminating extra network "
            "round-trips on SMB/NFS shares. Safe when source directories do "
            "not contain symlink loops."
        )
        adv.addRow("", self.skip_resolve_cb)

        top_layout.addWidget(advanced_section)

        # ==================================================================
        # NLE COMPANION SCRIPTS
        # ==================================================================
        nle_section = self._make_section("NLE Companion Scripts")
        nle = nle_section.content_layout()

        from src.lvm.nle_bridge import (
            is_resolve_external_available,
            is_resolve_running,
            invalidate_resolve_running_cache,
            is_premiere_panel_alive,
            is_premiere_panel_installed,
            premiere_panel_install_dir,
        )

        nle_help = QLabel(
            "Renames clip display names in Resolve / Premiere to the source "
            "filename recorded in the LVM sidecar — independent of the rename "
            "template, so this works whether your output is named *_latest.*, "
            "*_v999.*, *_final.*, or anything else. The on-disk file is "
            "untouched.\n\n"
            "Note: LVM-driven sync (this section) requires DaVinci Resolve "
            "Studio. Free Resolve users get the same renaming via Workspace → "
            "Scripts → Edit → lvm_restore_versions inside Resolve."
        )
        nle_help.setWordWrap(True)
        nle_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        nle.addRow("", nle_help)

        # ==================================================================
        # Clip rename composition — applies to BOTH Resolve and Premiere, so
        # it lives above the NLE-specific blocks below.
        # ==================================================================
        # The three toggles control what gets appended to the precomputed
        # stem at sync time. Custom-template mode swaps the default
        # {source_name} for a user template — frame/extension still come
        # from the checkboxes because there are no tokens for them.
        self.nle_rename_version_cb = QCheckBox("Version")
        self.nle_rename_version_cb.setChecked(
            getattr(config, "nle_rename_include_version", True))
        self.nle_rename_frame_cb = QCheckBox("Frame number (.1001)")
        self.nle_rename_frame_cb.setChecked(
            getattr(config, "nle_rename_include_frame", False))
        self.nle_rename_ext_cb = QCheckBox("File extension (.mov/.exr)")
        self.nle_rename_ext_cb.setChecked(
            getattr(config, "nle_rename_include_extension", False))

        include_row = QHBoxLayout()
        include_row.addWidget(self.nle_rename_version_cb)
        include_row.addWidget(self.nle_rename_frame_cb)
        include_row.addWidget(self.nle_rename_ext_cb)
        include_row.addStretch()
        nle.addRow("Clip rename:", include_row)

        self.nle_rename_custom_cb = QCheckBox("Use custom template")
        self.nle_rename_custom_cb.setChecked(
            getattr(config, "nle_rename_custom_enabled", False))
        self.nle_rename_custom_edit = QLineEdit(
            getattr(config, "nle_rename_custom_template", "") or "{source_name}")
        self.nle_rename_custom_edit.setEnabled(self.nle_rename_custom_cb.isChecked())
        self.nle_rename_custom_edit.setPlaceholderText("{source_name}")
        self.nle_rename_custom_edit.setToolTip(
            "Tokens: {source_name}, {source_basename}, {source_fullname}, "
            "{source_title}, {group}. Version / frame / extension are "
            "appended based on the checkboxes above — they have no token."
        )

        custom_row = QHBoxLayout()
        custom_row.addWidget(self.nle_rename_custom_cb, 0)
        custom_row.addWidget(self.nle_rename_custom_edit, 1)
        nle.addRow("", custom_row)

        # Live preview — always shown, no paths.
        self._nle_rename_preview = QLabel()
        self._nle_rename_preview.setWordWrap(True)
        self._nle_rename_preview.setStyleSheet(
            "color: #c8c8c8; font-size: 11pt; font-family: Consolas, monospace;"
        )
        nle.addRow("Preview:", self._nle_rename_preview)

        # Wire every input that affects the preview.
        self.nle_rename_custom_cb.toggled.connect(
            self.nle_rename_custom_edit.setEnabled)
        for w in (self.nle_rename_version_cb, self.nle_rename_frame_cb,
                  self.nle_rename_ext_cb, self.nle_rename_custom_cb):
            w.toggled.connect(self._update_nle_rename_preview)
        self.nle_rename_custom_edit.textChanged.connect(
            self._update_nle_rename_preview)
        self._update_nle_rename_preview()

        resolve_available = is_resolve_external_available()

        # ----- Resolve scripting library row (install detection) -----
        install_text = ("Scripting library: found — LVM-driven sync available "
                        "when Resolve Studio is running."
                        if resolve_available
                        else "Scripting library: not found — LVM-driven sync "
                             "needs DaVinci Resolve Studio. Free Resolve users "
                             "can still run Workspace → Scripts → Edit → "
                             "lvm_restore_versions from inside Resolve.")
        resolve_install_label = QLabel(install_text)
        resolve_install_label.setWordWrap(True)
        resolve_install_label.setStyleSheet(
            "color: #3aaa88; font-size: 11pt;" if resolve_available
            else "color: #ffaa00; font-size: 11pt;"
        )
        nle.addRow("Status:", resolve_install_label)

        # ----- Resolve running-process row -----
        # Surfaced here so the user can answer "is the LVM-driven sync going
        # to find a Resolve to talk to right now?" without leaving Settings.
        # Running state is cached in nle_bridge — the Refresh button calls
        # invalidate_resolve_running_cache() so the next probe is fresh.
        self._resolve_running_label = QLabel()
        self._resolve_running_label.setWordWrap(True)
        resolve_refresh_btn = QPushButton("Refresh")
        resolve_refresh_btn.setMaximumWidth(90)
        resolve_refresh_btn.setToolTip(
            "Re-check whether DaVinci Resolve is currently running. "
            "The running state is otherwise cached for 10 seconds."
        )

        def _refresh_resolve_running(force: bool = True) -> None:
            if force:
                invalidate_resolve_running_cache()
            running = is_resolve_running(force=True)
            if running:
                self._resolve_running_label.setText("DaVinci Resolve: running")
                self._resolve_running_label.setStyleSheet(
                    "color: #3aaa88; font-size: 11pt;"
                )
            else:
                self._resolve_running_label.setText("DaVinci Resolve: not running")
                self._resolve_running_label.setStyleSheet(
                    "color: #ffaa00; font-size: 11pt;"
                )

        resolve_refresh_btn.clicked.connect(_refresh_resolve_running)
        _refresh_resolve_running(force=False)  # initial fill without busting cache

        running_row = QHBoxLayout()
        running_row.addWidget(self._resolve_running_label, 1)
        running_row.addWidget(resolve_refresh_btn, 0)
        nle.addRow("Running:", running_row)

        self.nle_auto_sync_resolve_cb = QCheckBox(
            "Run the Resolve script automatically after every successful promote"
        )
        self.nle_auto_sync_resolve_cb.setChecked(
            getattr(config, "nle_auto_sync_resolve", False)
        )
        self.nle_auto_sync_resolve_cb.setEnabled(resolve_available)
        if not resolve_available:
            self.nle_auto_sync_resolve_cb.setToolTip(
                "Requires DaVinci Resolve Studio. Free Resolve users can run "
                "the script manually from inside Resolve."
            )
        nle.addRow("Auto-sync:", self.nle_auto_sync_resolve_cb)

        auto_help = QLabel(
            "When enabled, every successful promote (or batch promote) "
            "immediately runs the rename script against a running Resolve, "
            "so editors never see the stale, template-named clips. Output "
            "goes to the log dock. Off by default — promote stays decoupled "
            "from the NLE."
        )
        auto_help.setWordWrap(True)
        auto_help.setStyleSheet("color: #8c8c8c; font-size: 11pt;")
        nle.addRow("", auto_help)

        # ----- Premiere row -----
        premiere_alive = is_premiere_panel_alive()
        premiere_installed = is_premiere_panel_installed()
        premiere_install_dir = premiere_panel_install_dir()

        if premiere_install_dir is None:
            p_status_text = "Adobe Premiere isn't supported on this OS."
        elif premiere_alive:
            p_status_text = ("LVM Premiere panel: connected (Premiere is "
                             "running with the panel loaded).")
        elif premiere_installed:
            p_status_text = ("LVM Premiere panel: installed but not running. "
                             "Open Premiere and dock it via "
                             "Window → Extensions → LVM Sync Versions.")
        else:
            p_status_text = ("LVM Premiere panel: not installed. Click "
                             "“Install panel” below to set it up "
                             "in one step.")
        self._premiere_status_label = QLabel(p_status_text)
        self._premiere_status_label.setWordWrap(True)
        self._premiere_status_label.setStyleSheet(
            "color: #3aaa88; font-size: 11pt;" if premiere_alive
            else "color: #ffaa00; font-size: 11pt;"
        )
        nle.addRow("Premiere:", self._premiere_status_label)

        # Install / Uninstall row — only shown when Premiere is supported.
        if premiere_install_dir is not None:
            install_row = QHBoxLayout()
            self._premiere_install_btn = QPushButton(
                "Reinstall panel..." if premiere_installed else "Install panel..."
            )
            self._premiere_install_btn.setToolTip(
                "Copies the bundled panel to "
                f"{premiere_install_dir} and enables unsigned CEP extensions "
                "for the current user. No admin rights or registry tools "
                "needed."
            )
            self._premiere_install_btn.clicked.connect(self._install_premiere_panel)
            install_row.addWidget(self._premiere_install_btn)

            self._premiere_uninstall_btn = QPushButton("Uninstall panel")
            self._premiere_uninstall_btn.setEnabled(premiere_installed)
            self._premiere_uninstall_btn.setToolTip(
                "Removes the panel folder. The PlayerDebugMode flag stays "
                "set so any other unsigned CEP panels you've installed "
                "keep working."
            )
            self._premiere_uninstall_btn.clicked.connect(self._uninstall_premiere_panel)
            install_row.addWidget(self._premiere_uninstall_btn)
            install_row.addStretch()
            nle.addRow("", install_row)

        self.nle_auto_sync_premiere_cb = QCheckBox(
            "Write a Premiere sync trigger automatically after every "
            "successful promote"
        )
        self.nle_auto_sync_premiere_cb.setChecked(
            getattr(config, "nle_auto_sync_premiere", False)
        )
        if not premiere_alive:
            # Allow toggling the setting even when the panel isn't currently
            # alive — the user may be configuring before installing or
            # before opening Premiere. The trigger writer is silent on
            # missing panels at runtime (logs and skips).
            self.nle_auto_sync_premiere_cb.setToolTip(
                "Setting persists even when the panel isn't running; the "
                "trigger writer skips gracefully if Premiere isn't open."
            )
        nle.addRow("", self.nle_auto_sync_premiere_cb)

        top_layout.addWidget(nle_section)

        # ==================================================================
        # Footer (Save as Template + OK/Cancel)
        # ==================================================================
        top_layout.addStretch()
        scroll.setWidget(scroll_widget)
        body_splitter.addWidget(scroll)
        body_splitter.setStretchFactor(0, 0)
        body_splitter.setStretchFactor(1, 1)
        body_splitter.setSizes([170, 730])
        outer.addWidget(body_splitter, 1)

        # Populate the left nav now that all sections are registered.
        # QListWidget treats text literally — strip Qt's '&&' mnemonic escape
        # so labels read naturally ("Source Naming & Detection", not "&&").
        for title, _section in self._sections:
            self._nav_list.addItem(title.replace("&&", "&"))
        self._nav_list.currentRowChanged.connect(self._on_nav_changed)
        if self._sections:
            self._nav_list.setCurrentRow(0)

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

    def _update_nle_rename_preview(self):
        """Recompute the NLE clip-rename preview from the current dialog state.

        Shows two example renderings — one for a sequence frame and one for
        a single-file clip — so the user can see how the toggles interact
        without needing a real promote first.
        """
        try:
            from src.lvm.task_tokens import (
                compose_nle_display_stem, derive_source_tokens,
            )
        except ImportError:
            self._nle_rename_preview.setText("(preview unavailable)")
            return

        include_version = self.nle_rename_version_cb.isChecked()
        include_frame = self.nle_rename_frame_cb.isChecked()
        include_ext = self.nle_rename_ext_cb.isChecked()
        custom_enabled = self.nle_rename_custom_cb.isChecked()
        custom_template = (
            self.nle_rename_custom_edit.text().strip() or "{source_name}"
        )

        # Two representative inputs — one sequence-shaped, one container-shaped.
        # If a source is selected, prefer its sample filename so the preview
        # reflects the user's actual project; otherwise fall back to canned
        # examples that exercise both shapes.
        frame_ext_re = re.compile(r"([._])(\d+)\.(\w+)$")
        if self._selected_source and self._selected_source.sample_filename:
            samples = [("Source", self._selected_source.sample_filename)]
        else:
            samples = [
                ("Sequence", "SH0010_comp_v003.1001.exr"),
                ("Movie",    "SH0010_comp_v003.mov"),
            ]
        lines = []
        for label, sample in samples:
            try:
                tokens = derive_source_tokens(
                    sample,
                    self._config.task_tokens,
                    self._config.default_date_format,
                    source_title=(self._selected_source.name
                                  if self._selected_source else ""),
                )
            except Exception:
                tokens = {"source_name": "", "source_basename": "",
                          "source_fullname": "", "source_filename": sample,
                          "source_title": ""}
            stem = compose_nle_display_stem(
                tokens,
                include_version=include_version,
                custom_enabled=custom_enabled,
                custom_template=custom_template,
                group_token_expander=_expand_group_token,
                group="",
            )
            frame_match = frame_ext_re.search(sample)
            name = stem
            if frame_match and include_frame:
                name = f"{name}{frame_match.group(1)}{frame_match.group(2)}"
            if include_ext:
                if frame_match:
                    ext = frame_match.group(3)
                else:
                    _, _, ext = sample.rpartition(".")
                if ext:
                    name = f"{name}.{ext}"
            lines.append(f"{label:9s} {sample}  →  {name or '(empty)'}")
        self._nle_rename_preview.setText("\n".join(lines))

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
                sample_file = f"{rename_resolved}{_preview_sample_suffix(source)}"
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

    def _install_premiere_panel(self):
        """One-click install: copy panel + enable PlayerDebugMode."""
        from src.lvm.nle_bridge import install_premiere_panel

        log_lines = []
        result = install_premiere_panel(
            log=lambda lvl, msg: log_lines.append((lvl, msg)))

        if not result["ok"]:
            QMessageBox.critical(
                self, "Install failed",
                result.get("error") or "Install failed (see app log)."
            )
            return

        body = (
            f"Panel installed.\n\n"
            f"Files copied: {result['files_copied']}\n"
            f"PlayerDebugMode set for {result['csxs_flags_set']} CSXS "
            f"version(s).\n\n"
            f"Install path:\n{result['install_dir']}\n\n"
        )
        if result.get("needs_premiere_restart"):
            body += ("Restart Premiere if it's currently open, then dock the "
                     "panel via Window → Extensions → LVM Sync Versions.")
        QMessageBox.information(self, "Premiere panel installed", body)

        # Refresh the dialog's local labels and the main window's
        # status-bar buttons so state is immediately accurate.
        self._refresh_premiere_install_row()
        from app.main_window import MainWindow as _MW
        if isinstance(self.parent(), _MW):
            self.parent()._refresh_sync_names_state()

    def _uninstall_premiere_panel(self):
        from src.lvm.nle_bridge import uninstall_premiere_panel

        reply = QMessageBox.question(
            self, "Uninstall Premiere panel",
            "Remove the LVM Premiere panel from your Adobe CEP "
            "extensions folder?\n\n"
            "PlayerDebugMode stays enabled so any other unsigned CEP "
            "panels you've installed keep working.",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        log_lines = []
        result = uninstall_premiere_panel(
            log=lambda lvl, msg: log_lines.append((lvl, msg)))

        if not result["ok"]:
            QMessageBox.critical(
                self, "Uninstall failed",
                result.get("error") or "Uninstall failed (see app log)."
            )
            return

        QMessageBox.information(
            self, "Premiere panel uninstalled",
            ("Panel removed." if result["files_removed"]
             else "Panel was not installed; nothing to remove.")
        )
        self._refresh_premiere_install_row()
        from app.main_window import MainWindow as _MW
        if isinstance(self.parent(), _MW):
            self.parent()._refresh_sync_names_state()

    def _refresh_premiere_install_row(self):
        """Re-read installer state and update the dialog's Premiere widgets."""
        from src.lvm.nle_bridge import (
            is_premiere_panel_installed,
            is_premiere_panel_alive,
            premiere_panel_install_dir,
        )
        if premiere_panel_install_dir() is None:
            return
        installed = is_premiere_panel_installed()
        alive = is_premiere_panel_alive()

        if alive:
            text = ("LVM Premiere panel: connected (Premiere is running with "
                    "the panel loaded).")
            color = "#3aaa88"
        elif installed:
            text = ("LVM Premiere panel: installed but not running. Open "
                    "Premiere and dock it via Window → Extensions → "
                    "LVM Sync Versions.")
            color = "#ffaa00"
        else:
            text = ("LVM Premiere panel: not installed. Click “Install "
                    "panel” below to set it up in one step.")
            color = "#ffaa00"
        self._premiere_status_label.setText(text)
        self._premiere_status_label.setStyleSheet(
            f"color: {color}; font-size: 11pt;")

        if hasattr(self, "_premiere_install_btn"):
            self._premiere_install_btn.setText(
                "Reinstall panel..." if installed else "Install panel...")
        if hasattr(self, "_premiere_uninstall_btn"):
            self._premiere_uninstall_btn.setEnabled(installed)

    def _validate_template(self, edit: QLineEdit, allowed: set, base_tooltip: str) -> None:
        """Highlight *edit* red if its text references tokens outside *allowed*.

        Restores the empty stylesheet and base tooltip on clean input.
        """
        bad = _unknown_tokens(edit.text(), allowed)
        if bad:
            edit.setStyleSheet("QLineEdit { border: 1px solid #c0504d; }")
            edit.setToolTip(
                base_tooltip + "\n\nUnknown token"
                + ("s: " if len(bad) > 1 else ": ")
                + ", ".join("{" + b + "}" for b in bad)
            )
        else:
            edit.setStyleSheet("")
            edit.setToolTip(base_tooltip)

    def _on_nav_changed(self, row: int) -> None:
        """Accordion: expand only the section matching the selected nav row."""
        if row < 0 or row >= len(self._sections):
            return
        target_title, target_section = self._sections[row]
        for title, section in self._sections:
            should_expand = (section is target_section)
            if section._collapsed == (not should_expand):
                continue  # already in desired state
            section._collapsed = not should_expand
            section._toggle_btn.setChecked(should_expand)
            section._toggle_btn.setArrowType(Qt.DownArrow if should_expand else Qt.RightArrow)
            section._content.setVisible(should_expand)
        # Scroll target into view (deferred so the layout has time to settle).
        QTimer.singleShot(0, lambda: self._scroll.ensureWidgetVisible(target_section, 0, 12))

    def _make_section(self, title: str, collapsed: bool = False) -> CollapsibleSection:
        """Create a CollapsibleSection that remembers its collapsed state.

        Per-title state is stored on the class and survives across dialog
        opens within the session. The *collapsed* arg is the first-time
        default — used only when this title hasn't been seen before.
        """
        remembered = ProjectSettingsDialog._collapsed_states.get(title)
        if remembered is not None:
            collapsed = remembered
        section = CollapsibleSection(title, collapsed=collapsed)
        self._sections.append((title, section))
        return section

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

        config.default_date_format = ",".join(
            fmt for fmt, cb in self.date_format_checks.items() if cb.isChecked()
        )

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

        # NLE companion scripts
        config.nle_auto_sync_resolve = self.nle_auto_sync_resolve_cb.isChecked()
        config.nle_auto_sync_premiere = self.nle_auto_sync_premiere_cb.isChecked()
        config.nle_rename_include_version = self.nle_rename_version_cb.isChecked()
        config.nle_rename_include_frame = self.nle_rename_frame_cb.isChecked()
        config.nle_rename_include_extension = self.nle_rename_ext_cb.isChecked()
        config.nle_rename_custom_enabled = self.nle_rename_custom_cb.isChecked()
        config.nle_rename_custom_template = (
            self.nle_rename_custom_edit.text().strip() or "{source_name}"
        )

        # Re-apply defaults to non-overridden sources
        apply_project_defaults(config)

    def done(self, result):
        ProjectSettingsDialog._last_geometry = self.saveGeometry()
        for title, section in self._sections:
            ProjectSettingsDialog._collapsed_states[title] = section._collapsed
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

