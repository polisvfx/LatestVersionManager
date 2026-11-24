"""DaVinci Resolve and Adobe Premiere NLE clip-sync controller mixin."""

from app._common import *  # noqa: F401,F403
from app.workers import SyncNamesWorker


class NleSyncControllerMixin:
    """Handles NLE companion availability probes, manual sync, and post-promote auto-sync."""

    def _refresh_sync_names_state(self):
        """Probe NLE availability off the GUI thread and update buttons on completion."""
        if self._closing or self._nle_probe_pending:
            return
        self._nle_probe_pending = True

        def _probe():
            try:
                from lvm.nle_bridge import (
                    is_resolve_external_available,
                    is_resolve_running,
                    is_premiere_panel_alive,
                    premiere_panel_install_dir,
                )
                resolve_available = is_resolve_external_available()
                resolve_alive = resolve_available and is_resolve_running()
                premiere_alive = is_premiere_panel_alive()
                premiere_install_dir = premiere_panel_install_dir()
            except Exception:
                resolve_available = False
                resolve_alive = False
                premiere_alive = False
                premiere_install_dir = None
            if not self._closing:
                try:
                    self._nle_probe_finished.emit(
                        resolve_available, resolve_alive,
                        premiere_alive, premiere_install_dir,
                    )
                except RuntimeError:
                    pass

        try:
            self._io_executor.submit(_probe)
        except RuntimeError:
            self._nle_probe_pending = False

    def _apply_sync_names_state(
        self,
        resolve_available: bool,
        resolve_alive: bool,
        premiere_alive: bool,
        premiere_install_dir,
    ):
        """Apply NLE probe results to the sync menu and status-bar buttons."""
        self._nle_probe_pending = False
        if self._closing:
            return

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
            from lvm.nle_bridge import (
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
            from lvm.nle_bridge import (
                write_premiere_trigger,
                is_premiere_panel_alive,
                collect_premiere_renames,
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

        # Precompute the rename list so the panel can apply the whole
        # batch in one ExtendScript call instead of polling N triggers.
        renames: list = []
        if self.config:
            latest_dirs = []
            seen = set()
            for src in self.config.watched_sources:
                d = src.latest_target
                if d and d not in seen:
                    seen.add(d)
                    latest_dirs.append(d)
            try:
                renames = collect_premiere_renames(latest_dirs)
            except Exception as e:
                logger.warning("Sync Names → Premiere: rename precompute "
                               "failed (%s); falling back to scan trigger.", e)
                renames = []

        payload = {"automatic": bool(automatic)}
        if renames:
            payload["renames"] = renames

        try:
            path = write_premiere_trigger(payload)
        except OSError as e:
            logger.error("Sync Names → Premiere: could not write trigger: %s", e)
            return

        prefix = "auto-sync after promote" if automatic else "manual sync"
        mode = f"batched x{len(renames)}" if renames else "scan"
        logger.info("Sync Names → Premiere (%s, %s): trigger %s written; "
                    "panel should process it shortly.",
                    prefix, mode, path.name)

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
            from lvm.nle_bridge import (
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

