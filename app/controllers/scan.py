"""Scanning, status computation, caching, and source-list population controller mixin."""

from app._common import *  # noqa: F401,F403
from app._common import (
    _STATUS_MARKERS, _STATUS_LABELS, _STATUS_COLORS,
    _OVERRIDE_COLOR, _CONFLICT_COLOR, _DEFAULT_GROUP_COLOR_HEX, _group_qcolor,
)
from app.workers import ScanWorker, StatusWorker


class ScanControllerMixin:
    """Handles async scanning, status refresh, scan caching, and source-list rendering."""

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
        self._history_timeline_action.setEnabled(enabled)
        self.btn_timeline.setEnabled(enabled)
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
            gen = self._project_generation
            w = ScanWorker(self.config, parent=self)
            self._scan_worker = w
            w.progress.connect(self._on_refresh_progress)
            w.completed.connect(lambda res, g=gen: self._on_reload_scan_complete(res, g))
            w.error.connect(lambda msg, g=gen: self._on_reload_error(msg, g))
            w.finished.connect(w.deleteLater)
            w.start()

    def _on_reload_scan_complete(self, scan_results: dict, generation: int = None):
        """Phase 1 done — scan results ready, start status computation."""
        if generation is not None and generation != self._project_generation:
            return
        self._scan_worker = None
        self._versions_cache = dict(scan_results)
        self._start_status_worker(scan_results)

    def _on_reload_error(self, msg: str, generation: int = None):
        """Handle errors during reload scan phase."""
        if generation is not None and generation != self._project_generation:
            return
        self._scan_worker = None
        self.btn_refresh.setEnabled(True)
        self._scan_indicator.setText("")
        self.statusBar().showMessage(f"Scan error: {msg}")
        logger.error(f"Reload scan error: {msg}")
        self._check_reload_pending()

    def _start_status_worker(self, versions_cache: dict):
        """Phase 2: compute statuses in background thread."""
        gen = self._project_generation
        w = StatusWorker(self.config, versions_cache, parent=self)
        self._status_worker = w
        w.completed.connect(
            lambda st, cf, pr, sc, g=gen: self._on_reload_status_complete(st, cf, pr, sc, g)
        )
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_reload_status_complete(self, source_status: dict, target_conflicts: dict,
                                   promoters: dict, scanners: dict, generation: int = None):
        """Phase 2 done — populate caches and rebuild UI."""
        if generation is not None and generation != self._project_generation:
            return
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
            self._select_first_source_item()

        self._save_scan_cache()
        self._scan_indicator.setText("")
        self._update_discover_promote_button()

        # If this was a cache-first load, kick off a background rescan now
        if getattr(self, '_rescan_after_cache', False):
            self._rescan_after_cache = False
            self._trigger_background_rescan()
        else:
            self._check_reload_pending()

    def _check_reload_pending(self):
        """After a scan/status cycle: start a reload, or the per-source
        refreshes, that were requested while it was running."""
        if self._reload_pending:
            self._reload_pending = False
            self._pending_refresh_names.clear()  # a full reload covers them
            self._pending_refresh_user = False
            self._reload_ui()
            return
        if (self._pending_refresh_names and self._scan_worker is None
                and self._status_worker is None and self.config):
            names = sorted(self._pending_refresh_names)
            background = not self._pending_refresh_user
            self._pending_refresh_names.clear()
            self._pending_refresh_user = False
            self._refresh_sources_by_name(names, background=background)

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
        gen = self._project_generation
        w = ScanWorker(self.config, previous_cache=dict(self._versions_cache), parent=self)
        self._scan_worker = w
        w.progress.connect(self._on_refresh_progress)
        w.completed.connect(lambda res, g=gen: self._on_refresh_complete(res, g))
        w.error.connect(lambda msg, g=gen: self._on_refresh_error(msg, g))
        w.finished.connect(w.deleteLater)
        w.start()

    def _save_scan_cache(self):
        """Save current _versions_cache to disk (background I/O)."""
        if not self.config_path or not self.config:
            return
        from lvm.scan_cache import save_cache
        # Snapshot refs for the background thread
        config_path = self.config_path
        sources = list(self.config.watched_sources)
        cache = dict(self._versions_cache)
        task_tokens = list(self.config.task_tokens)

        def _write():
            try:
                save_cache(config_path, sources, cache, task_tokens=task_tokens)
            except Exception as e:
                logging.getLogger(__name__).warning("Failed to save scan cache: %s", e)

        self._io_executor.submit(_write)

    def _source_matches_search(self, source: WatchedSource, query: str) -> bool:
        """Check if a source matches the search query (name, filename, path, group).

        Uses ``WatchedSource.search_text`` which pre-lowercases and combines
        name, sample_filename, full source_dir path, and group into one
        cached string. The query's backslashes are normalized so a pasted
        Windows path matches the normalized haystack.
        """
        if not query:
            return True
        return query.lower().replace("\\", "/") in source.search_text

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
        elif status == "error":
            integrity = info.get("integrity") or {}
            tooltip = integrity.get("message") or "Status could not be determined"
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

        group_by = self.group_by_check.isChecked() if hasattr(self, 'group_by_check') else False

        # Build filtered source list
        filtered = [s for s in self.config.watched_sources if self._source_visible(s)]

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
        # Keep sorting disabled while "Group by groups" is active so flat group
        # header rows stay anchored above their member sources.
        self.source_list.setSortingEnabled(not (group_by and bool(self.config.groups)))
        self._apply_source_column_visibility()

    def _select_first_source_item(self) -> None:
        """Select the first real source row in source_list, skipping group headers."""
        for i in range(self.source_list.topLevelItemCount()):
            item = self.source_list.topLevelItem(i)
            if item is not None and item.data(0, Qt.UserRole):
                self.source_list.setCurrentItem(item)
                return

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
            self._select_first_source_item()

    def _refresh_sources_by_name(self, source_names: list[str], select_source: str = None,
                                 background: bool = False):
        """Re-scan only the given sources (by name) using the background worker path.

        Delegates to _refresh_selected_sources which uses ScanWorker + StatusWorker
        so the UI stays responsive even on slow network paths. *background*
        marks a refresh the user didn't ask for (watcher): it is incremental
        and leaves the buttons usable.
        """
        if not self.config:
            return
        if self._scan_worker is not None or self._status_worker is not None:
            # Queue it; _check_reload_pending runs the queue when the worker
            # is done. (This used to set _rescan_after_cache, which only the
            # project-load path reads, so the request was silently dropped.)
            logger.debug("Refresh worker busy — queueing refresh of %s", ", ".join(source_names))
            self._pending_refresh_names.update(source_names)
            if not background:
                self._pending_refresh_user = True
            if select_source:
                self._refresh_select_source = select_source
            return

        # Resolve source names to indices
        indices = []
        for i, s in enumerate(self.config.watched_sources):
            if s.name in source_names:
                indices.append(i)
        if not indices:
            return

        self._refresh_selected_sources(indices, select_source=select_source,
                                       background=background)

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

    def _refresh_selected_sources(self, indices: list[int], select_source: str = None,
                                  background: bool = False):
        """Re-scan only the specified sources in background thread.

        *select_source* is selected afterwards if given; otherwise the
        current selection is kept. A *background* refresh (watcher) reuses
        unchanged version folders and doesn't disable any buttons; a user
        refresh lists everything again.
        """
        if not self.config or self._scan_worker is not None or self._status_worker is not None:
            return
        sources = [self.config.watched_sources[i] for i in indices if i < len(self.config.watched_sources)]
        if not sources:
            return

        if select_source:
            self._refresh_select_source = select_source

        if not background:
            self.btn_refresh.setEnabled(False)
            self.btn_refresh_versions.setEnabled(False)
            self.btn_promote.setEnabled(False)
            self.btn_promote_all.setEnabled(False)
            self.btn_promote_split_all.setEnabled(False)
        names = ", ".join(s.name for s in sources)
        self._scan_indicator.setText("Updating...")
        self._scan_indicator.setStyleSheet("color: #d4a849; font-size: 11pt; margin-right: 8px;")
        self.statusBar().showMessage(f"Scanning: {names}")

        previous = (
            {s.name: self._versions_cache.get(s.name, []) for s in sources}
            if background else {}
        )
        gen = self._project_generation
        w = ScanWorker(self.config, sources=sources,
                       previous_cache=previous, parent=self)
        self._scan_worker = w
        w.progress.connect(self._on_refresh_progress)
        w.completed.connect(lambda res, g=gen: self._on_partial_refresh_complete(res, g))
        w.error.connect(lambda msg, g=gen: self._on_refresh_error(msg, g))
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_partial_refresh_complete(self, scan_results: dict, generation: int = None):
        """Called when a partial (selected sources) scan finishes. Delegate to StatusWorker."""
        if generation is not None and generation != self._project_generation:
            return
        self._scan_worker = None
        if self._is_promotion_active:
            self._pending_refresh_names.update(scan_results.keys())
            return
        self._partial_scan_count = len(scan_results)

        # Merge new scan results into the existing versions cache
        for source_name, versions in scan_results.items():
            self._versions_cache[source_name] = versions

        # Only recompute status for the sources that were actually re-scanned
        changed_sources = [
            s for s in self.config.watched_sources if s.name in scan_results
        ]
        if not changed_sources:
            self._scan_indicator.setText("")
            self.btn_refresh.setEnabled(True)
            self._check_reload_pending()
            return
        gen = self._project_generation
        w = StatusWorker(
            self.config, dict(self._versions_cache),
            sources=changed_sources, parent=self,
        )
        self._status_worker = w
        w.completed.connect(
            lambda st, cf, pr, sc, g=gen: self._on_partial_status_complete(st, cf, pr, sc, g)
        )
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_partial_status_complete(self, source_status: dict, target_conflicts: dict,
                                    promoters: dict, scanners: dict, generation: int = None):
        """Status computation done after partial refresh — rebuild UI."""
        if generation is not None and generation != self._project_generation:
            return
        self._status_worker = None
        # Merge partial results into existing caches (not replace)
        self._source_status.update(source_status)
        self._promoters.update(promoters)
        self._scanners.update(scanners)
        # Recompute conflicts for all sources (cheap — just path comparison)
        from lvm.conflicts import detect_target_conflicts
        conflicts = detect_target_conflicts(self.config, self.config.task_tokens)
        self._target_conflicts = {}
        for target, name_a, name_b in conflicts:
            self._target_conflicts.setdefault(name_a, []).append(name_b)
            self._target_conflicts.setdefault(name_b, []).append(name_a)

        # Update the refreshed rows in place — rebuilding the whole list
        # (and both trees) on every watcher event flickered, lost the scroll
        # position and the selected version. Fall back to a rebuild only when
        # a row has to appear or disappear under the current filter.
        refreshed = set(source_status)
        if not self._update_source_items(refreshed):
            self._apply_source_filter()

        self.btn_refresh.setEnabled(True)
        self._scan_indicator.setText("")
        count = getattr(self, '_partial_scan_count', 0)
        self.statusBar().showMessage(f"Refreshed {count} source{'s' if count != 1 else ''}", 3000)

        target = self._refresh_select_source
        self._refresh_select_source = None
        current = self._current_source.name if self._current_source else None
        if target and target != current:
            self._select_source_by_name(target)       # re-renders via selection change
        elif current in refreshed:
            self._rerender_current_source()
        elif current is None and self.source_list.topLevelItemCount() > 0:
            self._select_first_source_item()

        # Re-derive button states from the (unchanged or restored) selection.
        self.btn_refresh_versions.setEnabled(self._current_source is not None)
        self._on_version_selected()
        self._on_history_selected()
        self._on_source_selection_changed()
        self._save_scan_cache()

        self._check_reload_pending()

    def _source_visible(self, source: WatchedSource) -> bool:
        """Whether *source* passes the status filter and search box."""
        filter_mode = self.source_filter.currentText()
        status = self._source_status.get(source.name, {}).get("status", "no_target")
        if filter_mode == "Newer Available" and status != "newer":
            return False
        if filter_mode == "Stale" and status != "stale":
            return False
        if filter_mode == "Not on Highest" and status not in ("newer", "deliberate", "stale", "no_version"):
            return False
        search_query = self.source_search.text().strip() if hasattr(self, 'source_search') else ""
        return self._source_matches_search(source, search_query)

    def _update_source_items(self, names: set) -> bool:
        """Refresh the source-list rows for *names* in place.

        Returns False (nothing or only part updated) when the list needs a
        full rebuild instead: a source that should now appear or disappear
        under the active filter, or one that isn't in the config any more.
        """
        items = {}
        for i in range(self.source_list.topLevelItemCount()):
            item = self.source_list.topLevelItem(i)
            key = item.data(0, Qt.UserRole)
            if key:
                items[key] = item
        updates = []
        for name in names:
            source = self._source_by_name(name)
            if source is None:
                return False
            item = items.get(name)
            if (item is not None) != self._source_visible(source):
                return False
            if item is not None:
                updates.append((item, source))
        self.source_list.setUpdatesEnabled(False)
        try:
            for item, source in updates:
                fresh = self._make_source_item(source)
                for col in range(fresh.columnCount()):
                    item.setText(col, fresh.text(col))
                    item.setForeground(col, fresh.foreground(col))
                    item.setToolTip(col, fresh.toolTip(col))
        finally:
            self.source_list.setUpdatesEnabled(True)
        return True

    def _rerender_current_source(self):
        """Rebuild the version/history trees of the selected source after its
        data changed, keeping the selected versions and the scroll position."""
        if not self._current_source:
            return
        selected = {
            getattr(item.data(0, Qt.UserRole), "source_path", None)
            for item in self.version_tree.selectedItems()
        }
        current_item = self.version_tree.currentItem()
        current_key = (getattr(current_item.data(0, Qt.UserRole), "source_path", None)
                       if current_item else None)
        v_scroll = self.version_tree.verticalScrollBar().value()
        h_scroll = self.history_tree.verticalScrollBar().value()

        self._on_source_selected_by_name(self._current_source.name)

        for i in range(self.version_tree.topLevelItemCount()):
            item = self.version_tree.topLevelItem(i)
            key = getattr(item.data(0, Qt.UserRole), "source_path", None)
            if key is not None and key == current_key:
                self.version_tree.setCurrentItem(item)
            if key is not None and key in selected:
                item.setSelected(True)
        self.version_tree.verticalScrollBar().setValue(v_scroll)
        self.history_tree.verticalScrollBar().setValue(h_scroll)

    def _refresh_all(self, full: bool = False):
        """Re-scan all sources in background thread.

        Incremental by default (unchanged version folders are reused);
        ``full=True`` lists every folder again.
        """
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

        previous = {} if full else dict(self._versions_cache)
        gen = self._project_generation
        w = ScanWorker(self.config, previous_cache=previous, parent=self)
        self._scan_worker = w
        w.progress.connect(self._on_refresh_progress)
        w.completed.connect(lambda res, g=gen: self._on_refresh_complete(res, g))
        w.error.connect(lambda msg, g=gen: self._on_refresh_error(msg, g))
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_refresh_progress(self, current: int, total: int, source_name: str):
        self.statusBar().showMessage(f"Scanning source {current}/{total}: {source_name}")

    def _on_refresh_error(self, msg: str, generation: int = None):
        if generation is not None and generation != self._project_generation:
            return
        self._scan_worker = None
        self.btn_refresh.setEnabled(True)
        self.btn_refresh_versions.setEnabled(self._current_source is not None)
        self._on_version_selected()
        self._on_source_selection_changed()
        self._scan_indicator.setText("")
        self.statusBar().showMessage(f"Scan error: {msg}")
        logger.error(f"Scan error: {msg}")

        self._check_reload_pending()

    def _on_refresh_complete(self, scan_results: dict, generation: int = None):
        """Called when background scan finishes. Delegate to StatusWorker."""
        if generation is not None and generation != self._project_generation:
            return
        self._scan_worker = None

        if self._is_promotion_active:
            logger.debug("Deferring refresh results — promotion in progress")
            self._deferred_refresh_results = scan_results
            return

        self._apply_refresh_results(scan_results)

    def _apply_refresh_results(self, scan_results: dict):
        """Apply full refresh scan results: update caches and start StatusWorker."""
        self._versions_cache = dict(scan_results)
        self._scanners.clear()
        self._promoters.clear()
        self._current_source = None
        self._source_status = {}

        gen = self._project_generation
        w = StatusWorker(self.config, scan_results, parent=self)
        self._status_worker = w
        w.completed.connect(
            lambda st, cf, pr, sc, g=gen: self._on_refresh_status_complete(st, cf, pr, sc, g)
        )
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_refresh_status_complete(self, source_status: dict, target_conflicts: dict,
                                    promoters: dict, scanners: dict, generation: int = None):
        """Status computation done after _refresh_all — rebuild UI."""
        if generation is not None and generation != self._project_generation:
            return
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
            self._select_first_source_item()

        # Save scan results to cache and clear indicator
        self._save_scan_cache()
        self._scan_indicator.setText("")
        self._update_discover_promote_button()
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

