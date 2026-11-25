"""Manual, batch, undo/revert, and Discover and Promote controller mixin."""

import sys

from app._common import *  # noqa: F401,F403
from app.workers import PromoteWorker
from app.dialogs.batch_promote import (
    BatchPromoteReviewDialog, UndoPromoteDialog, DiscoverPromotePreviewDialog,
)
from app.dialogs.discovery import DiscoveryWorker, add_discovery_results_to_config
from app.dialogs.dry_run import DryRunDialog
from app.dialogs.latest_path import LatestPathDialog
from app.dialogs.obsolete_layer import ObsoleteLayerDialog


def _mw_attr(name: str, default):
    mw = sys.modules.get("app.main_window")
    return getattr(mw, name, default) if mw is not None else default


class PromoteControllerMixin:
    """Handles single/batch promotion, Discover and Promote, undo/revert, and manual versions."""

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
            self.btn_promote_all.setToolTip(
                "Promotes selected sources that are not on their highest version.\n"
                "Hold Shift to force re-promote selected sources."
            )
            self.btn_promote_all.setEnabled(self._worker is None)
            self.btn_promote_split_all.setVisible(True)
            self.btn_promote_split_all.setToolTip(
                "Promote all sources to latest (F9).\n"
                "Hold Shift to force re-promote all sources (Shift+F9)."
            )
            self.btn_promote_split_all.setEnabled(has_sources and self._worker is None)
        else:
            self.btn_promote_all.setText("Promote All to Latest")
            self.btn_promote_all.setToolTip(
                "Promotes sources that are not on their highest version (F9).\n"
                "Hold Shift to force re-promote all sources (Shift+F9)."
            )
            self.btn_promote_all.setEnabled(has_sources and self._worker is None)
            self.btn_promote_split_all.setVisible(False)
        self._update_discover_promote_button()

    def _update_discover_promote_button(self):
        """Sync visibility/enabled state of the Discover & Promote button."""
        cfg = self.config
        if not cfg or not getattr(cfg, "discover_and_promote_enabled", False):
            self.btn_discover_promote.setVisible(False)
            return
        self.btn_discover_promote.setVisible(True)
        has_discovery = bool(cfg.discovery_search_history)
        idle = (self._worker is None and self._scan_worker is None
                and self._status_worker is None
                and not getattr(self, "_discover_promote_worker", None))
        if not has_discovery:
            self.btn_discover_promote.setEnabled(False)
            self.btn_discover_promote.setToolTip(
                "Run a Discovery scan first — the one-click roundtrip "
                "rediscovers whichever directory you last scanned."
            )
        else:
            self.btn_discover_promote.setEnabled(idle)
            last_dir = cfg.discovery_search_history[0]
            self.btn_discover_promote.setToolTip(
                f"Rediscover {last_dir}, auto-add new sources, and promote "
                "everything that isn't on its latest version."
            )

    def _promote_all(self, _checked: bool = False):
        """Promote highest version of all sources (ignoring current selection)."""
        self._promote_all_or_selected(all_sources=True)

    def _promote_all_or_selected(self, _checked: bool = False, *, all_sources: bool = False):
        """Promote highest version of all or selected sources.

        By default, only promotes sources that are not already on their
        highest version (status "newer" or "deliberate").  This avoids
        rewriting identical data, which would trigger unnecessary resyncs
        with file-sync solutions.

        Hold Shift while clicking (or press Shift+F9) to force-promote every
        source, including those already on the highest version.
        """
        if not self.config or self._is_promotion_active:
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
            self._rebuild_promoters_in_place()
            if self.config_path:
                self._save_project()

        selected_items = [] if all_sources else self.source_list.selectedItems()
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
                    "Hold Shift and click (or press Shift+F9) to force re-promote."
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

    def _promote_all_forced(self, _checked: bool = False):
        """Promote all sources regardless of selection (used by split 'All' button)."""
        self._promote_all()

    def _discover_and_promote(self):
        """Run discovery + auto-add new sources + batch-promote in one click.

        Phase 1 (this method): kick off a background DiscoveryWorker against
        whichever directory the user last scanned. Phase 2
        (_on_discover_promote_complete): filter to brand-new paths, append
        them as WatchedSources, then refresh and trigger the existing batch
        promotion path (which already covers obsolete-layer prompts).
        """
        if not self.config:
            return
        if not self.config.discovery_search_history:
            QMessageBox.information(
                self, "Discover & Promote",
                "Run a Discovery scan first — Discover & Promote rediscovers "
                "whichever directory you last scanned.",
            )
            return
        if self._discover_promote_worker is not None:
            return

        root_dir = self.config.discovery_search_history[0]
        if not os.path.isdir(root_dir):
            QMessageBox.warning(
                self, "Discover & Promote",
                f"Last-used discovery directory no longer exists:\n{root_dir}\n\n"
                "Open Discover Versions and pick a new directory.",
            )
            return

        # Shift+click also rescans existing sources for new versions before
        # determining what to promote — so a source that picked up a new
        # version since the last scan is included even when there's nothing
        # new to discover.
        force_refresh = bool(QApplication.keyboardModifiers() & Qt.ShiftModifier)
        self._discover_promote_root_dir = root_dir

        if force_refresh and self.config.watched_sources:
            self._scan_indicator.setText("Refreshing...")
            self._scan_indicator.setStyleSheet("color: #d4a849; font-size: 11pt; margin-right: 8px;")
            self.statusBar().showMessage(
                "Discover & Promote: rescanning existing sources…"
            )
            self._discover_promote_pending_refresh = True
            self._refresh_all()
            QTimer.singleShot(100, self._discover_promote_after_pre_refresh)
            self._update_discover_promote_button()
        else:
            self._start_discover_promote_scan(root_dir)

    def _discover_promote_after_pre_refresh(self):
        """Wait for the Shift+click pre-refresh to finish, then start discovery."""
        if self._scan_worker is not None or self._status_worker is not None:
            QTimer.singleShot(100, self._discover_promote_after_pre_refresh)
            return
        if not getattr(self, "_discover_promote_pending_refresh", False):
            return
        self._discover_promote_pending_refresh = False
        root_dir = getattr(self, "_discover_promote_root_dir", "")
        if not root_dir:
            self._update_discover_promote_button()
            return
        self._start_discover_promote_scan(root_dir)

    def _start_discover_promote_scan(self, root_dir: str):
        """Kick off the DiscoveryWorker phase of the roundtrip."""
        self._scan_indicator.setText("Discovering...")
        self._scan_indicator.setStyleSheet("color: #d4a849; font-size: 11pt; margin-right: 8px;")
        self.statusBar().showMessage(f"Discover & Promote: scanning {root_dir}…")

        gen = self._project_generation
        worker = DiscoveryWorker(
            root_dir=root_dir,
            max_depth=4,
            extensions=self.config.default_file_extensions,
            whitelist=self.config.name_whitelist,
            blacklist=self.config.name_blacklist,
            skip_resolve=self.config.skip_resolve,
            parent=self,
        )
        worker.completed.connect(lambda res, g=gen: self._on_discover_promote_complete(res, g))
        worker.error.connect(lambda msg, g=gen: self._on_discover_promote_error(msg, g))
        worker.finished.connect(worker.deleteLater)
        self._discover_promote_worker = worker
        self._update_discover_promote_button()
        worker.start()

    def _on_discover_promote_error(self, msg: str, generation: int = None):
        if generation is not None and generation != self._project_generation:
            return
        self._discover_promote_worker = None
        self._scan_indicator.setText("")
        self.statusBar().showMessage(f"Discover & Promote failed: {msg}")
        logger.error("Discover & Promote: %s", msg)
        self._update_discover_promote_button()

    def _on_discover_promote_complete(self, results: list, generation: int = None):
        if generation is not None and generation != self._project_generation:
            return
        self._discover_promote_worker = None
        self._scan_indicator.setText("")

        existing_paths = {
            str(Path(s.source_dir).resolve()) if s.source_dir else ""
            for s in self.config.watched_sources
        }
        new_results = []
        for r in results:
            try:
                p = str(Path(r.path).resolve())
            except (OSError, ValueError):
                p = r.path
            if p not in existing_paths:
                new_results.append(r)

        # Build the batch promote list from currently watched sources that
        # aren't on their highest version. New sources (added below) join
        # the list by definition — they have no current promotion yet.
        non_latest_existing = []
        for source in self.config.watched_sources:
            if not source.latest_target:
                continue
            status = self._source_status.get(source.name, {}).get("status", "")
            if status in ("newer", "stale", "no_version"):
                non_latest_existing.append(source.name)

        new_count = len(new_results)
        promote_existing_count = len(non_latest_existing)

        if new_count == 0 and promote_existing_count == 0:
            QMessageBox.information(
                self, "Discover & Promote",
                "Nothing to do — no new sources discovered and every existing "
                "source is already on its latest version.",
            )
            self._update_discover_promote_button()
            return

        # First-run preview, unless the user previously opted out.
        if not self.config.discover_and_promote_skip_preview:
            dlg = DiscoverPromotePreviewDialog(
                root_dir=self.config.discovery_search_history[0],
                new_results=new_results,
                non_latest_names=non_latest_existing,
                source_status=self._source_status,
                parent=self,
            )
            if dlg.exec() != QDialog.Accepted:
                self._update_discover_promote_button()
                return
            if dlg.skip_future_previews():
                self.config.discover_and_promote_skip_preview = True
                self._mark_dirty()
                if self.config_path:
                    self._save_project()

        # Append the new sources, then run the existing batch promote flow.
        added = 0
        if new_results:
            added, renamed, history_disambig = add_discovery_results_to_config(
                self.config, new_results
            )
            if added:
                self._mark_dirty()
                if self.config_path:
                    self._save_project()
                logger.info(
                    "Discover & Promote: added %d source(s) "
                    "(%d renamed, %d history-disambiguated).",
                    added, renamed, history_disambig,
                )

        # Select all sources we want to promote so _promote_all_or_selected
        # uses the selected-path. New sources land in the source list after
        # the next reload, so kick that off first and chain the promotion
        # onto its completion.
        self._pending_discover_promote_names = (
            set(non_latest_existing) | {s.name for s in self.config.watched_sources[-added:]}
            if added else set(non_latest_existing)
        )
        # Reload (rescans new + existing sources, recomputes statuses) and
        # then fire the batch promote.
        self._reload_ui()
        # _on_reload_status_complete runs after the rescan; chain via a
        # single-shot timer so it fires on the next event loop turn once
        # _reload_ui has wired its workers.
        QTimer.singleShot(0, self._maybe_run_pending_discover_promote)

    def _maybe_run_pending_discover_promote(self):
        """Wait for the post-reload scan to finish, then trigger batch promote."""
        if self._scan_worker is not None or self._status_worker is not None:
            QTimer.singleShot(100, self._maybe_run_pending_discover_promote)
            return
        names = getattr(self, "_pending_discover_promote_names", None)
        if not names:
            self._update_discover_promote_button()
            return
        self._pending_discover_promote_names = None

        # Select the target sources, then call the existing promote path.
        self.source_list.blockSignals(True)
        self.source_list.clearSelection()
        for i in range(self.source_list.topLevelItemCount()):
            item = self.source_list.topLevelItem(i)
            if item.data(0, Qt.UserRole) in names:
                item.setSelected(True)
        self.source_list.blockSignals(False)
        self._on_source_selection_changed()

        if self.source_list.selectedItems():
            self._promote_all_or_selected()
        else:
            self.statusBar().showMessage("Discover & Promote: nothing to promote.")
            self._update_discover_promote_button()

    def _batch_promote_next(self):
        """Promote the next source in the batch list."""
        if self._batch_promote_index == 0 and self._batch_promote_list:
            self._batch_saved_source_name = (
                self._current_source.name if self._current_source else None
            )
        if self._batch_promote_index >= len(self._batch_promote_list):
            # All done — rescan only the sources that were promoted
            batch = self._batch_promote_list
            promoted_names = [s.name for s, _v in batch]
            count = len(batch)
            saved_name = self._batch_saved_source_name
            self._batch_saved_source_name = None
            self._batch_promote_list = []
            self._batch_keep_layers = {}
            for name in promoted_names:
                self._versions_cache.pop(name, None)
            self._process_deferred_or_refresh(promoted_names, select_source=saved_name)
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
        # Same ordered, atomic, off-thread path as every other save, so it
        # can't race a pending background write of an older snapshot.
        self._save_project()

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

    def _rebuild_promoters_in_place(self) -> None:
        """Rebuild Promoter/VersionScanner instances in place without clearing
        the UI or cached version scans (used when setting latest_path_template
        mid-promote so the active promote flow can continue immediately)."""
        if not self.config:
            return
        nle_opts = self.config.nle_rename_options()
        for src in self.config.watched_sources:
            self._promoters[src.name] = Promoter(
                src, self.config.task_tokens, self.config.project_name,
                nle_rename_options=nle_opts,
            )
            if src.name not in self._scanners:
                self._scanners[src.name] = VersionScanner(src, self.config.task_tokens)

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

        # Rebuild promoters in place so the caller's promote can proceed immediately
        self._rebuild_promoters_in_place()
        if self.config_path:
            self._save_project()
        return bool(source.latest_target)

    def _promote_selected(self):
        if self._is_promotion_active:
            return
        items = self.version_tree.selectedItems()
        if not items or not self._current_source:
            return

        force_promote = False
        version: VersionInfo = items[0].data(0, Qt.UserRole)
        source = self._current_source

        # Ensure latest path is set
        if not self._ensure_latest_path(source):
            return

        # Re-fetch promoter after possible template setup
        promoter = self._promoters.get(source.name)
        if not promoter:
            return

        # Check for incomplete sequences (Feature #11)
        from lvm.promoter import has_frame_gaps
        block_incomplete = getattr(source, 'block_incomplete_sequences', False) or getattr(self.config, 'block_incomplete_sequences', False)
        if block_incomplete and has_frame_gaps(version):
            reply = QMessageBox.warning(
                self, "Incomplete Sequence",
                f"Sequence has frame gaps: {version.frame_range}\n\nPromote anyway?",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply != QMessageBox.Yes:
                return
            force_promote = True

        # Container files (MOV/MXF/...) that are still being rendered
        unfinished = promoter.find_unfinished_renders(version)
        if unfinished:
            details = "\n".join(f"  {p.name}: {why}" for p, why in unfinished[:10])
            reply = QMessageBox.warning(
                self, "Still Rendering",
                f"{version.version_string} doesn't look finished yet:\n\n{details}\n\n"
                f"Promoting now may publish a truncated clip. Promote anyway?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
            )
            if reply != QMessageBox.Yes:
                return
            force_promote = True

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

        self._start_promotion(
            promoter, version, keep_layers=keep_layers,
            force=force_promote, pinned=bool(is_keep),
        )

    def _revert_selected(self):
        """Revert to a version from history by re-scanning and promoting."""
        if self._is_promotion_active:
            return
        items = self.history_tree.selectedItems()
        if not items or not self._current_source:
            return

        entry: HistoryEntry = items[0].data(0, Qt.UserRole)
        source = self._current_source
        scanner = self._scanners.get(source.name)
        promoter = self._promoters.get(source.name)
        if not scanner or not promoter:
            return

        # Find the version in current scan results (avoid eager scanner.scan()
        # when the cache already holds this source's versions).
        versions = self._versions_cache.get(source.name)
        if versions is None:
            versions = scanner.scan()
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

    def _undo_selected_source(self, skip_confirm: bool = False):
        """Ctrl+Z handler — undoes the currently-selected source's last promote.

        When *skip_confirm* is True (Ctrl+Shift+Z), bypass the confirmation
        dialog and immediately undo a single step.
        """
        items = self.source_list.selectedItems()
        if not items:
            return
        idx = self._resolve_source_index_from_item(items[0])
        if idx < 0:
            return
        self._undo_source(idx, skip_confirm=skip_confirm)

    def _undo_source(self, index: int, skip_confirm: bool = False):
        """Open the undo confirmation dialog for source at *index* and dispatch.

        When *skip_confirm* is True, undo one step immediately without a dialog.
        """
        if not self.config or self._is_promotion_active:
            return
        if index < 0 or index >= len(self.config.watched_sources):
            return
        source = self.config.watched_sources[index]
        promoter = self._promoters.get(source.name)
        scanner = self._scanners.get(source.name)
        if not promoter or not scanner:
            QMessageBox.warning(self, "Undo Unavailable",
                                f"No scanner/promoter loaded for {source.name}.")
            return
        try:
            history_len = len(promoter.get_history())
        except OSError as e:
            QMessageBox.warning(self, "Undo Unavailable",
                                f"Could not read history for {source.name}:\n{e}")
            return
        if history_len < 2:
            QMessageBox.information(self, "Nothing to Undo",
                                    f"{source.name} has fewer than two history entries.")
            return

        if skip_confirm:
            # Resolve previous version using the same logic the dialog uses,
            # so we can surface a useful error instead of silently failing.
            from lvm.promoter import PromotionError
            try:
                versions = scanner.scan()
                prev = promoter.history.get_previous(1)
                target_version = None
                if prev is not None:
                    for v in versions:
                        if version_strings_match(v.version_string, prev.version, v.version_number):
                            target_version = v
                            break
                if target_version is None:
                    raise PromotionError(
                        f"Source files for {prev.version if prev else 'previous version'} "
                        f"no longer exist on disk."
                    )
            except PromotionError as e:
                QMessageBox.warning(self, "Cannot Undo", str(e))
                return
            self._start_promotion(promoter, target_version)
            return

        dlg = UndoPromoteDialog(source, promoter, scanner, parent=self)
        if dlg.exec() != QDialog.Accepted:
            return
        target_version = dlg.get_target_version()
        if not target_version:
            return
        self._start_promotion(promoter, target_version)

    def _start_promotion(self, promoter: Promoter, version: VersionInfo,
                          keep_layers: set[str] | None = None,
                          *, force: bool = False, pinned: bool = False,
                          is_auto: bool = False):
        """Start the promotion in a background thread, checking hooks and link mode first."""
        if not self._confirm_hooks_trusted(promoter.source, is_auto=is_auto):
            self._restore_fallback_link_mode()
            self._auto_promoting_name = None
            return
        self._promoting_source_name = promoter.source.name
        self._promoting_version = version
        self._promoting_force = force
        self._promoting_keep_layers = keep_layers
        self._promoting_pinned = pinned
        mode = promoter.source.link_mode
        available, reason = _mw_attr("check_link_mode_available", check_link_mode_available)(mode)
        if not available and mode == "symlink":
            self._restore_fallback_link_mode()
            reply = QMessageBox.question(
                self, "Elevation Required",
                f"{reason}\n\nRestart with Administrator privileges?",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply == QMessageBox.Yes:
                if restart_elevated():
                    self.close()
                    return
                else:
                    QMessageBox.warning(self, "Elevation Failed",
                                        "Could not restart with elevated privileges.\n"
                                        "The UAC prompt may have been declined.")
            return
        elif not available:
            self._restore_fallback_link_mode()
            QMessageBox.warning(self, "Link Mode Unavailable", reason)
            return

        self.btn_promote.setEnabled(False)
        self.btn_revert.setEnabled(False)
        self._progress_container.setVisible(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.btn_cancel_promote.setVisible(True)
        self.btn_cancel_promote.setEnabled(True)
        self.btn_cancel_promote.setText("Cancel")

        w = _mw_attr("PromoteWorker", PromoteWorker)(promoter, version, self, force=force,
                          pinned=pinned, keep_layers=keep_layers)
        self._worker = w
        w.progress.connect(self._on_promote_progress)
        w.byte_progress.connect(self._on_promote_byte_progress)
        w.completed.connect(self._on_promote_finished)
        w.error.connect(self._on_promote_error)
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_promote_progress(self, current, total, filename):
        if total == 1 and current == 0:
            # A single file (MOV/MXF/...) is starting: show a busy bar until
            # byte progress arrives — on macOS the copy reports none.
            self.progress_bar.setMaximum(0)
            self.progress_bar.setFormat(filename)
            return
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)
        self.progress_bar.setFormat(f"{current}/{total} \u2014 {filename}")

    @staticmethod
    def _format_bytes(n: float) -> str:
        for unit in ("B", "KB", "MB", "GB"):
            if n < 1024:
                return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
            n /= 1024
        return f"{n:.1f} TB"

    def _on_promote_byte_progress(self, done: float, total: float, filename: str):
        """Byte progress of a single large file being copied."""
        if total <= 0:
            return
        self.progress_bar.setMaximum(1000)
        self.progress_bar.setValue(int(min(done, total) / total * 1000))
        self.progress_bar.setFormat(
            f"{filename} \u2014 {self._format_bytes(done)} of "
            f"{self._format_bytes(total)} ({done / total:.0%})"
        )

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
        self._progress_container.setVisible(False)
        self.btn_cancel_promote.setVisible(False)

        self._restore_fallback_link_mode()

        # Check if this is part of a batch promotion
        if hasattr(self, '_batch_promote_list') and self._batch_promote_list:
            # The promoted source, not whatever is selected now (the user may
            # have clicked another source, or a refresh cleared it).
            source_name = (
                self._promoting_source_name
                or (self._current_source.name if self._current_source else "unknown")
            )
            self._versions_cache.pop(source_name, None)
            self._batch_promote_index += 1
            self._batch_promote_next()
            return

        promoted_name = self._promoting_source_name or (
            self._current_source.name if self._current_source else "unknown"
        )
        self._promoting_source_name = None
        if self._auto_promoting_name == promoted_name:
            self._notifier.notify(promoted_name, "promoted",
                                  f"{promoted_name} \u2192 {entry.version}")
        self._auto_promoting_name = None
        self.statusBar().showMessage(
            f"Promoted {promoted_name} \u2192 {entry.version}"
        )
        # Rescan only the promoted source instead of all sources
        self._versions_cache.pop(promoted_name, None)
        self._process_deferred_or_refresh([promoted_name], select_source=promoted_name)
        self._maybe_auto_sync_nle()

    def _source_by_name(self, name: str):
        if not name or not self.config:
            return None
        for s in self.config.watched_sources:
            if s.name == name:
                return s
        return None

    def _restore_fallback_link_mode(self):
        """Undo a temporary copy/hardlink fallback on the source it was
        applied to — not on whichever source happens to be selected now."""
        if self._fallback_original_mode and self._fallback_source is not None:
            self._fallback_source.link_mode = self._fallback_original_mode
        self._fallback_original_mode = None
        self._fallback_source = None

    def _on_promote_error(self, error_msg):
        # Message boxes are dismissable — make sure the failure also lands
        # in the log dock (which auto-opens on ERROR).
        logging.getLogger(__name__).error("Promotion failed: %s", error_msg)
        self._worker = None
        error_source_name = self._promoting_source_name
        self._promoting_source_name = None
        if error_source_name and self._auto_promoting_name == error_source_name:
            # Before any message box below: it blocks until dismissed.
            first_line = (error_msg or "").strip().splitlines()[:1]
            self._notifier.notify(error_source_name, "failed",
                                  f"{error_source_name}: {first_line[0] if first_line else 'error'}")
        self._auto_promoting_name = None
        self.progress_bar.setVisible(False)
        self._progress_container.setVisible(False)
        self.btn_cancel_promote.setVisible(False)
        self.btn_promote.setEnabled(True)

        self._restore_fallback_link_mode()

        # Check for symlink/hardlink failure — offer fallback options
        symlink_failed = "Symlink creation failed" in error_msg
        hardlink_failed = "Hardlink creation failed" in error_msg
        if symlink_failed or hardlink_failed:
            source = self._source_by_name(error_source_name) or self._current_source
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
                        self._fallback_source = source
                        source.link_mode = fallback_mode
                        self._start_promotion(
                            promoter, version,
                            keep_layers=self._promoting_keep_layers,
                            force=self._promoting_force,
                            pinned=self._promoting_pinned,
                        )
                        return

        # If batch promotion, ask whether to continue
        if hasattr(self, '_batch_promote_list') and self._batch_promote_list:
            source_name = error_source_name or (
                self._current_source.name if self._current_source else "Unknown"
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

