"""discovery dialog module."""

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
from app.widgets import CollapsibleSection, TagInputWidget
from app.dialogs.latest_path import LatestPathDialog
from app.dialogs.naming_rule import NamingRuleDialog


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
        # Path-only fallback for legacy callers that don't pass a name. Must
        # NOT fire when result_name is set: with multi-shot flat folders,
        # several sources legitimately share one source_dir under different
        # names, so a path-only match would flag every sibling shot as
        # already-added.
        return not result_name and any(d == resolved for d, _ in existing_sources)

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

        # Populate timecodes once on new results rather than on every tree rebuild.
        # Flatten across all results into a single parallel pool so a slow
        # ffprobe in one result doesn't stall others.
        tc_mode = self._config.timecode_mode if self._config else "lazy"
        if tc_mode != "never":
            all_versions = [v for r in self._results for v in r.versions_found]
            populate_timecodes_parallel(all_versions, max_workers=8)
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

        # Add sources using the naming rule.
        # Names must be unique across the project, so detect collisions and
        # auto-disambiguate. This matters when several discovery results share
        # the same source_dir (multi-shot flat folder) and the user's naming
        # rule (e.g. parent:0) yields the same name for all of them.
        existing_names = {s.name for s in self._config.watched_sources}

        # Pre-build an index of existing latest_target -> history filenames so
        # we can disambiguate a new source's history file when it'd land in a
        # directory that another source is already writing to. Without this,
        # several sources sharing a target dir all read/write the same
        # .latest_history.json and stomp each other's promotion records.
        def _resolved(p: str) -> str:
            try:
                return str(Path(p).resolve())
            except (OSError, ValueError):
                return p

        target_history_map: dict[str, set[str]] = {}
        for s in self._config.watched_sources:
            if s.latest_target:
                target_history_map.setdefault(_resolved(s.latest_target), set()).add(
                    s.history_filename or ".latest_history.json"
                )

        renamed_count = 0
        history_disambiguated = 0
        added = 0
        for result in selected_results:
            source_name = compute_source_name(
                result,
                self._config.default_naming_rule,
                self._config.task_tokens,
            )
            if source_name in existing_names:
                # Fall back to the per-cluster source_name rule, which is
                # naturally unique because discovery clustered by basename.
                fallback_name = compute_source_name(
                    result, "source_name", self._config.task_tokens)
                if fallback_name and fallback_name not in existing_names:
                    source_name = fallback_name
                    renamed_count += 1
                else:
                    # Still colliding (or empty) — append a numeric suffix.
                    base = fallback_name or source_name
                    n = 2
                    while f"{base}_{n}" in existing_names:
                        n += 1
                    source_name = f"{base}_{n}"
                    renamed_count += 1
            existing_names.add(source_name)

            suggested_date_fmt = getattr(result, "suggested_date_format", "")
            from datetime import datetime as _dt
            # Discovery never sets an override flag. The values it found
            # (suggested_pattern, suggested_extensions, suggested_date_format)
            # are seeded into the source as initial values, but
            # apply_project_defaults below normalises everything against the
            # project defaults. Per-source overrides are reserved for fields
            # the user *explicitly* changes via the per-source settings
            # dialog — that's the only place the flag gets set today.
            source = WatchedSource(
                name=source_name,
                source_dir=result.path,
                version_pattern=result.suggested_pattern or self._config.default_version_pattern,
                file_extensions=result.suggested_extensions or list(self._config.default_file_extensions),
                sample_filename=result.sample_filename or "",
                date_format=suggested_date_fmt or self._config.default_date_format,
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

            # Disambiguate history filename when this source's target dir is
            # already claimed by another source (existing or earlier in this
            # batch). The first source in a fresh dir keeps the default
            # ".latest_history.json"; subsequent sources get a name-derived
            # filename so their promotion records don't overwrite each other.
            if source.latest_target:
                resolved_dir = _resolved(source.latest_target)
                taken = target_history_map.setdefault(resolved_dir, set())
                if taken:
                    safe = re.sub(r"[^A-Za-z0-9_-]", "_", source.name) or "source"
                    desired = f".latest_history_{safe}.json"
                    n = 2
                    while desired in taken:
                        desired = f".latest_history_{safe}_{n}.json"
                        n += 1
                    source.history_filename = desired
                    history_disambiguated += 1
                taken.add(source.history_filename or ".latest_history.json")

            self._config.watched_sources.append(source)
            added += 1

        if added:
            # Populate every inheritable field on the new sources from the
            # current project defaults — file_rename_template, link_mode,
            # block_incomplete_sequences, pre/post-promote hooks, etc. The
            # WatchedSource constructor only sets dataclass defaults, so
            # without this call a freshly added source would promote with
            # an empty file_rename_template and the no-template fallback in
            # _remap_filename would strip "_latest" (or whatever suffix the
            # user configured) until the project is reloaded.
            apply_project_defaults(self._config)
            self.sources_added.emit(added)
            msg = f"Added {added} source(s) to the project."
            if renamed_count:
                msg += (
                    f"\n\n{renamed_count} source(s) were auto-renamed to avoid "
                    f"name collisions (fell back to source_name rule)."
                )
            if history_disambiguated:
                msg += (
                    f"\n\n{history_disambiguated} source(s) share a latest target "
                    f"directory; their history files were auto-namespaced so "
                    f"promotions don't overwrite each other."
                )
            QMessageBox.information(self, "Sources Added", msg)
            # Rebuild tree so newly-added sources get marked/hidden
            self._rebuild_tree()


# ---------------------------------------------------------------------------
# Source list delegate — renders group tags as colored pills
# ---------------------------------------------------------------------------

