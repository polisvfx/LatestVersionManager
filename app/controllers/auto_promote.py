"""File watcher and background auto-promotion controller mixin."""

import sys

from app._common import *  # noqa: F401,F403
from app.workers import AutoPromoteCheckWorker

RENDER_WAIT_RETRY_MS = 10_000
RENDER_SETTLE_SECONDS = 10.0


def _mw_attr(name: str, default):
    mw = sys.modules.get("app.main_window")
    return getattr(mw, name, default) if mw is not None else default


class AutoPromoteControllerMixin:
    """Handles filesystem watching and off-thread auto-promotion decisions."""

    def _toggle_watcher(self):
        if self.watcher.is_running:
            self.watcher.stop()
            self._auto_settle.clear()
            self.watch_toggle.setText("Start Watching")
            self.watch_toggle.setChecked(False)
            self.auto_promote_cb.setEnabled(False)
        else:
            if self.config:
                self._start_watcher()
                if self.watcher.is_running:
                    self.watch_toggle.setText("Stop Watching")
                    self.watch_toggle.setChecked(True)
                    self.auto_promote_cb.setEnabled(True)
                else:
                    self.watch_toggle.setText("Start Watching")
                    self.watch_toggle.setChecked(False)
                    self.auto_promote_cb.setEnabled(False)
                    self.statusBar().showMessage("No watched sources to watch")
            else:
                self.watch_toggle.setChecked(False)

    def _start_watcher(self):
        """(Re)start the watcher with the project's sources and watch settings."""
        self.watcher.start(
            self.config.watched_sources,
            mode=self.config.watch_mode,
            poll_interval=self.config.watch_poll_interval,
        )

    def _on_watcher_change(self, source_name: str):
        """A watched source had new files — refresh only that source."""
        logger.info(f"Watcher detected changes in: {source_name}")

        # Refresh only the changed source, incrementally. The cached versions
        # are kept: the scan reuses them, and selecting the source meanwhile
        # shows them instead of scanning on the GUI thread.
        if self.config:
            self._refresh_sources_by_name([source_name], background=True)

        self.statusBar().showMessage(f"New version detected in: {source_name}")

        # Attempt auto-promotion if enabled
        self._try_auto_promote(source_name)

    def _on_watch_status(self, status: str):
        self.statusBar().showMessage(status)

    @staticmethod
    def _normalize_frame_range(frame_range):
        """Extract core frame range, stripping gap annotations (see
        :func:`lvm.auto_promote.normalize_frame_range`)."""
        from lvm.auto_promote import normalize_frame_range
        return normalize_frame_range(frame_range)

    def _auto_promote_enabled(self) -> bool:
        return self.auto_promote_cb.isChecked() and self.watcher.is_running

    def _try_auto_promote(self, source_name: str):
        """Attempt auto-promotion for a source after the watcher saw changes.

        Cheap guards run here; the checks that touch the filesystem run on an
        :class:`AutoPromoteCheckWorker` and come back through
        :meth:`_on_auto_promote_decided`. Auto-promotes only when:

        - The Auto-Promote checkbox is checked and the watcher is running
        - No promotion is already in progress
        - The source has a previous promotion (history entry)
        - A newer highest version exists
        - That version is complete: unchanged since the previous check, no
          frame gaps, not a partial range of the last promoted version, last
          frames not open for writing, container files (MOV/MXF/...)
          finalised. While it isn't, it re-checks every RENDER_WAIT_RETRY_MS
          — the watcher doesn't see frames landing inside a version folder
        - The new version's frame range matches the last promoted version
        - There are no obsolete layers in the target
        - The source's link mode is available

        At most one check runs per source; a request while one is running
        re-runs the check once the current one has finished.
        """
        waiting_on_render = source_name in self._render_wait_sources
        self._render_wait_sources.discard(source_name)

        if not self._auto_promote_enabled():
            return

        if source_name in self._auto_check_workers:
            # Newer files may have landed since that check started.
            self._auto_check_rerun[source_name] = (
                self._auto_check_rerun.get(source_name, False) or waiting_on_render
            )
            return

        if self._worker is not None:
            self._auto_promote_blocked_by_promotion(source_name, waiting_on_render)
            return

        # Only sources with a latest target have a promoter; the scanner
        # exists once the source's status has been computed.
        if source_name not in self._promoters or source_name not in self._scanners:
            return
        source = self._source_by_name(source_name)
        if source is None:
            return

        gen = self._project_generation
        worker = AutoPromoteCheckWorker(
            self.config, source, waiting_on_render, RENDER_SETTLE_SECONDS,
            previous_settle=self._auto_settle.get(source_name), parent=self,
        )
        self._auto_check_workers[source_name] = worker
        worker.decided.connect(lambda d, g=gen: self._on_auto_promote_decided(d, g))
        worker.finished.connect(lambda: self._on_auto_check_thread_done(source_name, worker))
        worker.start()

    def _auto_promote_blocked_by_promotion(self, source_name: str, waiting_on_render: bool):
        if waiting_on_render:
            # Keep waiting rather than dropping the pending auto-promote.
            self._schedule_render_wait_retry(source_name)
            return
        msg = f"Auto-promote skipped for {source_name}: promotion already in progress"
        logger.info(msg)
        self.statusBar().showMessage(msg)

    def _on_auto_promote_decided(self, decision, generation: int = None):
        """Main-thread half of auto-promote: act on a worker's decision."""
        if generation is not None and generation != self._project_generation:
            return
        from lvm import auto_promote as ap

        name = decision.source_name
        if decision.versions is not None:
            self._versions_cache[name] = decision.versions
        if decision.settle is not None:
            self._auto_settle[name] = decision.settle

        # The world may have moved on while the check ran.
        if name in self._auto_check_rerun or not self._auto_promote_enabled():
            return  # superseded by a newer request / auto-promote switched off

        if decision.action == ap.NOOP:
            return

        if decision.action in (ap.SKIP, ap.ERROR):
            msg = f"Auto-promote skipped for {name}: {decision.reason}"
            detail = f" ({decision.log_detail})" if decision.log_detail else ""
            logger.log(decision.log_level, msg + detail)
            self.statusBar().showMessage(msg)
            self._notifier.notify(
                name, "failed" if decision.action == ap.ERROR else "skipped",
                f"{name}: {decision.reason}",
            )
            return

        if self._worker is not None:
            # A manual or batch promotion started while the check ran.
            self._auto_promote_blocked_by_promotion(
                name, decision.waiting_on_render or decision.action == ap.WAIT_RENDER,
            )
            return

        version = decision.version
        if decision.action == ap.WAIT_RENDER:
            detail = decision.reason
            if decision.waiting_file and decision.waiting_file not in detail:
                detail = f"{decision.waiting_file}: {detail}"
            if not decision.waiting_on_render:
                logger.info(
                    f"Auto-promote of {name} {version.version_string} "
                    f"waiting for render to finish ({detail})"
                )
                self._notifier.notify(
                    name, "waiting",
                    f"{name} {version.version_string} is still rendering; "
                    f"it will be promoted when it's complete.",
                )
            self.statusBar().showMessage(
                f"Auto-promote waiting for {name} {version.version_string} "
                f"to finish rendering ({detail})"
            )
            self._schedule_render_wait_retry(name)
            return

        if decision.action == ap.PROMOTE:
            promoter = self._promoters.get(name)
            if promoter is None:
                return  # latest target removed while the check ran
            if not self._confirm_hooks_trusted(promoter.source, is_auto=True):
                return
            self._auto_settle.pop(name, None)
            logger.info(f"Auto-promoting {name}: {version.version_string}")
            self.statusBar().showMessage(
                f"Auto-promoting {name} to {version.version_string}..."
            )
            self._start_promotion(promoter, version)
            if self._worker is not None:
                self._auto_promoting_name = name

    def _on_auto_check_thread_done(self, source_name: str, worker):
        """The check thread has exited: release it and run a queued re-check."""
        if self._auto_check_workers.get(source_name) is worker:
            del self._auto_check_workers[source_name]
        worker.deleteLater()
        if source_name in self._auto_check_rerun:
            waiting = self._auto_check_rerun.pop(source_name)
            if waiting:
                # Consumed again immediately by _try_auto_promote.
                self._render_wait_sources.add(source_name)
            self._try_auto_promote(source_name)

    def _schedule_render_wait_retry(self, source_name: str):
        """Re-run auto-promote for *source_name* after _mw_attr("RENDER_WAIT_RETRY_MS", RENDER_WAIT_RETRY_MS).

        At most one retry is pending per source; a watcher event in the
        meantime simply runs the check sooner.
        """
        if source_name in self._render_wait_sources:
            return
        self._render_wait_sources.add(source_name)
        QTimer.singleShot(
            _mw_attr("RENDER_WAIT_RETRY_MS", RENDER_WAIT_RETRY_MS),
            lambda: self._on_render_wait_timeout(source_name),
        )

    def _on_render_wait_timeout(self, source_name: str):
        # A watcher-triggered attempt may already have consumed this wait.
        if source_name in self._render_wait_sources:
            self._try_auto_promote(source_name)

