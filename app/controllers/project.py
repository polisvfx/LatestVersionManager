"""Project lifecycle, config persistence, hook trust, and settings controller mixin."""

import sys

from app._common import *  # noqa: F401,F403
from app.workers import ProjectLoadWorker
from app.dialogs.discovery import DiscoveryDialog
from app.dialogs.project_setup import ProjectSetupDialog
from app.dialogs.settings import ProjectSettingsDialog


def _mw_attr(name: str, default):
    mw = sys.modules.get("app.main_window")
    return getattr(mw, name, default) if mw is not None else default


class ProjectControllerMixin:
    """Handles project open/new/save, recent projects, hook trust, settings, and validation."""

    def _get_recent_projects(self) -> list[str]:
        """Read recent_projects from QSettings, handling 1-item str coercion."""
        recents = self._settings.value("recent_projects", [])
        if isinstance(recents, str):
            return [recents] if recents else []
        if isinstance(recents, list):
            return [str(p) for p in recents if p]
        return []

    def _populate_recent_menu(self, menu: QMenu):
        menu.clear()
        recents = self._get_recent_projects()
        if not recents:
            action = menu.addAction("(No recent projects)")
            action.setEnabled(False)
            return
        for path in recents[:10]:
            action = menu.addAction(path)
            action.triggered.connect(lambda checked=False, p=path: self._load_project(p))

    def _new_project(self):
        try:
            dlg = _mw_attr("ProjectSetupDialog", ProjectSetupDialog)(parent=self)
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

            # Apply template to the newly created project on disk BEFORE
            # loading asynchronously, so an already-open project is never
            # mutated or overwritten while ProjectLoadWorker is in flight.
            template_path = info.get("template_path", "")
            if template_path:
                from lvm.templates import load_template, apply_template
                template_data = load_template(template_path)
                new_cfg = load_config(output_path)
                apply_template(new_cfg, template_data)
                save_config(new_cfg, output_path)

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
        # Refuse switching projects mid-promotion so active PromoteWorker state
        # cannot be torn down or attributed to a different project.
        if self._is_promotion_active:
            QMessageBox.warning(
                self, "Promotion In Progress",
                "Cannot switch projects while a promotion is running. "
                "Wait for it to finish or cancel it first.",
            )
            return
        # Reentrancy guard — ignore if a load is already in flight
        if getattr(self, "_project_load_worker", None) is not None and self._project_load_worker.isRunning():
            return
        # Prompt to save unsaved changes before replacing an open project
        if self.config is not None and self._dirty and self.config_path:
            reply = QMessageBox.question(
                self, "Unsaved Changes",
                "You have unsaved changes in the current project. Save before opening another?",
                QMessageBox.Save | QMessageBox.Discard | QMessageBox.Cancel,
                QMessageBox.Save,
            )
            if reply == QMessageBox.Cancel:
                return
            if reply == QMessageBox.Save:
                if not self._save_project(blocking=True):
                    return

        self._project_generation += 1
        gen = self._project_generation
        # Stop watching the outgoing project so its events never fire into the new one
        if self.watcher.is_running:
            self.watcher.stop()
            if hasattr(self, "watch_toggle"):
                self.watch_toggle.blockSignals(True)
                self.watch_toggle.setChecked(False)
                self.watch_toggle.setText("Start Watching")
                self.watch_toggle.blockSignals(False)
            if hasattr(self, "auto_promote_cb"):
                self.auto_promote_cb.setEnabled(False)
        self._pending_refresh_names.clear()
        self._reload_pending = False
        self._auto_settle.clear()
        self._render_wait_sources.clear()
        self._auto_check_rerun.clear()

        self.statusBar().showMessage(f"Loading: {path}…")
        w = ProjectLoadWorker(path, self)
        self._project_load_worker = w
        w.completed.connect(lambda cfg, cached, p, g=gen: self._on_project_loaded(cfg, cached, p, g))
        w.error.connect(lambda msg, p, g=gen: self._on_project_load_error(msg, p, g))
        w.finished.connect(w.deleteLater)
        w.start()

    def _on_project_loaded(self, config, cached, path: str, generation: int = None):
        """Called on the main thread when ProjectLoadWorker finishes."""
        try:
            if generation is not None and generation != self._project_generation:
                return
            # Reset in-flight scan/status guards from the previous project so
            # _reload_ui starts cleanly for the newly loaded project.
            self._scan_worker = None
            self._status_worker = None
            self.config = config
            self.config_path = path
            self._add_to_recent(path)

            if cached:
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
            self._notifier.set_level(config.auto_promote_notifications)
            self._warn_if_newer_config(config)
        finally:
            self._project_load_worker = None

    def _warn_if_newer_config(self, config):
        """A newer LVM saved this project: unknown settings would be lost on
        save, so say so once, right after loading."""
        if config.loaded_config_version <= CONFIG_VERSION:
            return
        QMessageBox.warning(
            self, "Project saved by a newer LVM",
            f"This project was saved by a newer version of Latest Version "
            f"Manager (config version {config.loaded_config_version}; this "
            f"version understands up to {CONFIG_VERSION}).\n\n"
            f"Settings this version doesn't know are ignored, and saving the "
            f"project here will remove them. Update LVM before changing it.",
        )

    def _on_project_load_error(self, message: str, path: str, generation: int = None):
        """Called on the main thread when ProjectLoadWorker raises."""
        if generation is not None and generation != self._project_generation:
            return
        logging.getLogger(__name__).error(
            "Failed to load project %s: %s", path, message
        )
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

    def _save_project(self, blocking: bool = False) -> bool:
        """Save the project config.

        The snapshot is taken on the main thread; the write (atomic, see
        ``config.write_config_snapshot``) runs on ``_io_executor`` so a slow
        share doesn't freeze the UI. Writes stay ordered because the executor
        has a single thread. The dirty flag is cleared optimistically and set
        again if the write fails.

        ``blocking=True`` waits for the write (used when the window closes)
        and returns whether it succeeded.
        """
        if not self.config:
            return True
        if not self.config_path:
            self._save_project_as()
            return not self._dirty
        from lvm.config import write_config_snapshot

        config_snapshot = self.config.to_dict()
        config_path = self.config_path
        self._dirty = False
        self._update_title()

        if blocking:
            future = self._io_executor.submit(write_config_snapshot, config_snapshot, config_path)
            try:
                project_dir = future.result()
            except Exception as e:
                self._on_save_finished(False, config_path, str(e))
                return False
            self._on_save_finished(True, config_path, project_dir)
            return True

        def _write():
            try:
                project_dir = write_config_snapshot(config_snapshot, config_path)
            except Exception as e:
                self._save_finished.emit(False, config_path, str(e))
            else:
                self._save_finished.emit(True, config_path, project_dir)

        self.statusBar().showMessage(f"Saving: {config_path}…")
        self._io_executor.submit(_write)
        return True

    def _on_save_finished(self, ok: bool, config_path: str, detail: str):
        """Main-thread completion of a project save (*detail* is the project
        dir on success, the error message on failure)."""
        if ok:
            if self.config and config_path == self.config_path:
                self.config.project_dir = detail
            self.statusBar().showMessage(f"Saved: {config_path}", 5000)
            return
        logger.error("Failed to save project %s: %s", config_path, detail)
        if config_path == self.config_path:
            self._dirty = True
            self._update_title()
        self.statusBar().showMessage(f"Save FAILED: {config_path}")
        QMessageBox.warning(
            self, "Save Failed",
            f"Could not save the project to:\n{config_path}\n\n{detail}\n\n"
            f"The previous file on disk is unchanged. Your changes are still "
            f"in memory — fix the problem and save again.",
        )

    def _save_project_as(self):
        if not self.config:
            return
        from lvm.patterns import sanitize_filename as _sanitize_filename
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
        recents = self._get_recent_projects()
        if path in recents:
            recents.remove(path)
        recents.insert(0, path)
        self._settings.setValue("recent_projects", recents[:10])
        if hasattr(self, "_recent_menu") and self._recent_menu is not None:
            self._populate_recent_menu(self._recent_menu)

    def _get_trusted_hooks(self) -> list[str]:
        val = self._settings.value("trusted_hooks", [])
        if isinstance(val, str):
            return [val] if val else []
        if isinstance(val, list):
            return [str(x) for x in val if x]
        return []

    def _trust_hook_commands(self, pre_cmd: str, post_cmd: str) -> None:
        from lvm.hook_trust import add_trusted_hook
        updated = add_trusted_hook(
            self.config_path or "", pre_cmd, post_cmd, self._get_trusted_hooks()
        )
        self._settings.setValue("trusted_hooks", updated)

    def _confirm_hooks_trusted(self, source: WatchedSource, *, is_auto: bool = False) -> bool:
        """Ensure any configured pre/post-promote shell hooks are approved."""
        from lvm.hook_trust import is_hook_trusted
        pre = (getattr(source, "pre_promote_cmd", "") or "").strip()
        post = (getattr(source, "post_promote_cmd", "") or "").strip()
        if not pre and not post:
            return True
        if is_hook_trusted(self.config_path or "", pre, post, self._get_trusted_hooks()):
            return True
        if is_auto:
            msg = (
                f"Auto-promote skipped for {source.name}: shell hook commands "
                f"have not been approved for this project yet. Promote manually once to review."
            )
            logger.warning(msg)
            self.statusBar().showMessage(msg)
            self._notifier.notify(source.name, "skipped", f"{source.name}: unapproved shell hooks")
            return False

        lines = [
            f"Promotion for '{source.name}' will execute the following shell command(s):",
            "",
        ]
        if pre:
            lines.append(f"Pre-promote:\n  {pre}")
        if post:
            lines.append(f"Post-promote:\n  {post}")
        lines.append("\nOnly approve hooks from projects and teammates you trust.")

        box = QMessageBox(self)
        box.setWindowTitle("Approve Promotion Shell Hooks")
        box.setIcon(QMessageBox.Warning)
        box.setText("\n".join(lines))
        trust_btn = box.addButton("Trust && Run for This Project", QMessageBox.AcceptRole)
        once_btn = box.addButton("Run Once", QMessageBox.ActionRole)
        cancel_btn = box.addButton(QMessageBox.Cancel)
        box.setDefaultButton(cancel_btn)
        box.exec()
        clicked = box.clickedButton()
        if clicked == trust_btn:
            self._trust_hook_commands(pre, post)
            return True
        return clicked == once_btn

    def _open_project_settings(self):
        if not self.config:
            QMessageBox.information(self, "No Project", "Open or create a project first.")
            return
        dlg = ProjectSettingsDialog(self.config, selected_source=self._current_source, parent=self)
        dlg.nle_state_changed.connect(self._refresh_sync_names_state)
        if dlg.exec() == QDialog.Accepted:
            dlg.apply_to_config(self.config)
            # Hooks explicitly saved by the user in Settings are trusted automatically
            self._trust_hook_commands(self.config.pre_promote_cmd, self.config.post_promote_cmd)
            self._mark_dirty()
            self._notifier.set_level(self.config.auto_promote_notifications)
            if self.watcher.is_running:
                self._start_watcher()  # pick up a changed watch mode / poll interval
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
        if self.watcher.is_running:
            self._start_watcher()
        if self.config_path:
            self._save_project()
        self._reload_ui()
        self.statusBar().showMessage(f"Added {count} source(s) from discovery")

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

