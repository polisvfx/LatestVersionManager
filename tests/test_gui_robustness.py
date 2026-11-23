"""
GUI robustness tests (local only — needs PySide6; runs offscreen).

- StatusWorker always emits ``finished`` and reports per-source failures as
  an "error" status instead of dying (which used to block every refresh).
- MainWindow connects the version/history selection slots once, not once
  per source selection.
- A failed project save keeps the project dirty and returns False.

MainWindow is built with a throwaway INI-backed QSettings so the user's real
settings (and last opened project) are never touched.
"""

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

try:
    from PySide6.QtCore import QSettings, SIGNAL
    from PySide6.QtWidgets import QApplication
    HAVE_QT = True
except ImportError:  # pragma: no cover - headless CI without Qt
    HAVE_QT = False


def _app():
    return QApplication.instance() or QApplication([])


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestStatusWorkerErrors(unittest.TestCase):

    def setUp(self):
        _app()
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_gui_"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _config(self):
        from src.lvm.models import ProjectConfig, WatchedSource
        sources = []
        for name in ("good", "broken"):
            (self.tmp / name / "online").mkdir(parents=True)
            sources.append(WatchedSource(
                name=name, source_dir=str(self.tmp / name),
                latest_target=str(self.tmp / name / "online"),
                history_filename=f".latest_history_{name}.json",
            ))
        return ProjectConfig(project_name="P", watched_sources=sources)

    def _run(self, worker):
        results = []
        worker.finished.connect(lambda *a: results.append(a))
        worker.run()  # synchronous: emits on this thread
        self.assertEqual(len(results), 1, "finished must be emitted exactly once")
        return results[0]

    def test_unreadable_history_reports_error_status(self):
        from app.workers import StatusWorker
        from src.lvm.history import HistoryManager

        real_load = HistoryManager.load

        def flaky_load(hm):
            if "broken" in str(hm.path):
                raise PermissionError("share dropped")
            return real_load(hm)

        with patch.object(HistoryManager, "load", flaky_load):
            status, conflicts, promoters, scanners = self._run(
                StatusWorker(self._config(), {}))

        self.assertEqual(status["good"]["status"], "no_version")
        self.assertEqual(status["broken"]["status"], "error")
        self.assertIn("share dropped", status["broken"]["integrity"]["message"])
        self.assertIn("broken", promoters)
        self.assertEqual(set(scanners), {"good", "broken"})

    def test_unexpected_failure_still_emits_finished(self):
        from app.workers import StatusWorker
        with patch("src.lvm.conflicts.detect_target_conflicts",
                   side_effect=RuntimeError("boom")):
            status, conflicts, _, _ = self._run(StatusWorker(self._config(), {}))
        self.assertEqual(conflicts, {})


class _MainWindowTestBase(unittest.TestCase):
    """MainWindow on a throwaway INI QSettings, last-project restore disabled."""

    def setUp(self):
        _app()
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_gui_"))
        ini = str(self.tmp / "settings.ini")
        self._patches = [
            patch("app.main_window.QSettings",
                  lambda *a, **k: QSettings(ini, QSettings.IniFormat)),
            patch("app.main_window.MainWindow._restore_state", lambda self: None),
        ]
        for p in self._patches:
            p.start()
        from app.main_window import MainWindow
        self.win = MainWindow()

    def tearDown(self):
        self.win._dirty = False
        self.win.close()
        self.win.deleteLater()
        for p in self._patches:
            p.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestMainWindowWiring(_MainWindowTestBase):

    def test_selection_slots_connected_once(self):
        from src.lvm.models import ProjectConfig, WatchedSource
        from src.lvm.scanner import VersionScanner
        src = WatchedSource(name="S", source_dir=str(self.tmp))
        self.win.config = ProjectConfig(project_name="P", watched_sources=[src])
        self.win._scanners["S"] = VersionScanner(src)
        self.win._versions_cache["S"] = []

        sig = SIGNAL("itemSelectionChanged()")
        before_v = self.win.version_tree.receivers(sig)
        before_h = self.win.history_tree.receivers(sig)
        for _ in range(5):
            self.win._on_source_selected_by_name("S")
        self.assertEqual(self.win.version_tree.receivers(sig), before_v)
        self.assertEqual(self.win.history_tree.receivers(sig), before_h)
        self.assertGreaterEqual(before_v, 1)

    def test_failed_save_keeps_project_dirty(self):
        from src.lvm.models import ProjectConfig
        self.win.config = ProjectConfig(project_name="P")
        self.win.config_path = str(self.tmp / "p.json")
        self.win._dirty = True
        with patch("src.lvm.config.atomic_write_json", side_effect=OSError("disk full")), \
                patch("app.main_window.QMessageBox.warning") as warn:
            ok = self.win._save_project(blocking=True)
        self.assertFalse(ok)
        self.assertTrue(self.win._dirty)
        warn.assert_called_once()
        self.assertFalse(Path(self.win.config_path).exists())

    def test_successful_blocking_save(self):
        from src.lvm.models import ProjectConfig
        self.win.config = ProjectConfig(project_name="P")
        self.win.config_path = str(self.tmp / "p.json")
        self.win._dirty = True
        self.assertTrue(self.win._save_project(blocking=True))
        self.assertFalse(self.win._dirty)
        self.assertTrue(Path(self.win.config_path).exists())
        self.assertEqual(Path(self.win.config.project_dir), self.tmp.resolve())

    def test_newer_config_version_warns(self):
        from src.lvm.models import CONFIG_VERSION, ProjectConfig
        cfg = ProjectConfig(project_name="P")
        cfg.loaded_config_version = CONFIG_VERSION + 1
        with patch("app.main_window.QMessageBox.warning") as warn:
            self.win._warn_if_newer_config(cfg)
        warn.assert_called_once()
        self.assertIn("newer version", warn.call_args.args[2])

    def test_current_config_version_does_not_warn(self):
        from src.lvm.models import ProjectConfig
        for v in (0, None):
            cfg = ProjectConfig(project_name="P")
            if v is not None:
                cfg.loaded_config_version = v
            with patch("app.main_window.QMessageBox.warning") as warn:
                self.win._warn_if_newer_config(cfg)
            warn.assert_not_called()


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestAutoPromoteOffGuiThread(_MainWindowTestBase):
    """Auto-promote's filesystem checks run on AutoPromoteCheckWorker."""

    def setUp(self):
        super().setUp()
        import threading
        from unittest.mock import PropertyMock
        from src.lvm.models import ProjectConfig, WatchedSource
        from src.lvm.promoter import Promoter
        from src.lvm.scanner import VersionScanner
        from src.lvm.watcher import SourceWatcher

        renders = self.tmp / "renders"
        self.src = WatchedSource(
            name="shot", source_dir=str(renders), latest_target=str(self.tmp / "online"),
            version_pattern="_v{version}", file_extensions=[".exr"],
        )
        for version in ("v001", "v002"):
            vdir = renders / f"shot_{version}"
            vdir.mkdir(parents=True)
            for f in range(1001, 1004):
                (vdir / f"shot_{version}.{f}.exr").write_bytes(b"x")
        Promoter(self.src).promote(VersionScanner(self.src).scan()[0], user="t")  # v001 live

        self.win.config = ProjectConfig(project_name="P", watched_sources=[self.src])
        self.win._promoters["shot"] = Promoter(self.src)
        self.win._scanners["shot"] = VersionScanner(self.src)
        self.win.auto_promote_cb.setEnabled(True)
        self.win.auto_promote_cb.setChecked(True)

        self.scan_threads = []
        real_scan = VersionScanner.scan

        def spy_scan(scanner):
            self.scan_threads.append(threading.current_thread())
            return real_scan(scanner)

        self.started = []
        extra = [
            patch.object(SourceWatcher, "is_running", new_callable=PropertyMock, return_value=True),
            patch.object(VersionScanner, "scan", spy_scan),
            patch.object(type(self.win), "_start_promotion",
                         lambda win, promoter, version, keep_layers=None:
                         self.started.append(version.version_string)),
            # The first check of a new version waits for a second look.
            patch("app.main_window.RENDER_WAIT_RETRY_MS", 30),
        ]
        for p in extra:
            p.start()
        self._patches.extend(extra)

    def _drain(self, timeout=10.0):
        """Run the event loop until no check is running or pending a retry."""
        import time as _time
        deadline = _time.monotonic() + timeout
        idle_since = None
        while _time.monotonic() < deadline:
            QApplication.processEvents()
            busy = self.win._auto_check_workers or self.win._render_wait_sources
            if busy:
                idle_since = None
            elif idle_since is None:
                idle_since = _time.monotonic()
            elif _time.monotonic() - idle_since > 0.2:
                break
            _time.sleep(0.01)
        self.assertEqual(self.win._auto_check_workers, {}, "check worker did not finish")
        self.assertEqual(self.win._render_wait_sources, set(), "retry still pending")

    def test_checks_run_on_worker_thread(self):
        import threading
        self.win._try_auto_promote("shot")
        self.assertEqual(self.started, [], "promotion decided synchronously")
        self._drain()
        self.assertEqual(self.started, ["v002"])
        self.assertTrue(self.scan_threads)
        self.assertNotIn(threading.main_thread(), self.scan_threads,
                         "scan ran on the GUI thread")
        self.assertEqual(self.win._versions_cache["shot"][-1].version_string, "v002")

    def test_requests_during_a_check_are_coalesced(self):
        self.win._try_auto_promote("shot")
        self.win._try_auto_promote("shot")
        self.win._try_auto_promote("shot")
        self._drain()
        self.assertEqual(self.started, ["v002"], "expected exactly one promotion")

    def test_render_inside_version_folder_promoted_when_complete(self):
        """Folder-per-version: one watcher event when v002/ appears, frames
        keep landing inside it afterwards. The check keeps re-trying and
        promotes once the range is complete."""
        import time as _time
        last = self.tmp / "renders" / "shot_v002" / "shot_v002.1003.exr"
        last.unlink()  # v002 is mid-render: 1001-1002 of 1001-1003
        self.win._try_auto_promote("shot")
        deadline = _time.monotonic() + 0.5
        while _time.monotonic() < deadline:
            QApplication.processEvents()
            _time.sleep(0.01)
        self.assertEqual(self.started, [], "partial render was promoted")
        self.assertTrue(self.win._render_wait_sources or self.win._auto_check_workers,
                        "stopped re-checking a partial render")
        last.write_bytes(b"x")  # render finishes, no watcher event
        self._drain()
        self.assertEqual(self.started, ["v002"])

    def test_switching_auto_promote_off_drops_pending_decision(self):
        self.win._try_auto_promote("shot")
        self.win.auto_promote_cb.setChecked(False)
        self._drain()
        self.assertEqual(self.started, [])


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestIncrementalRefreshUI(_MainWindowTestBase):
    """Refresh queueing and in-place source-list updates (plan item #10)."""

    def setUp(self):
        super().setUp()
        from src.lvm.models import ProjectConfig, VersionInfo, WatchedSource
        from src.lvm.scanner import VersionScanner
        self.sources = []
        for name in ("alpha", "beta"):
            d = self.tmp / name
            d.mkdir()
            self.sources.append(WatchedSource(name=name, source_dir=str(d)))
        self.win.config = ProjectConfig(project_name="P", watched_sources=self.sources)
        for s in self.sources:
            self.win._scanners[s.name] = VersionScanner(s)
            self.win._versions_cache[s.name] = [
                VersionInfo("v001", 1, str(self.tmp / s.name / "v001"), file_count=1),
                VersionInfo("v002", 2, str(self.tmp / s.name / "v002"), file_count=1),
            ]
        self.win._source_status = {s.name: {"status": "no_version"} for s in self.sources}
        self.win._populate_source_list()
        self.win._select_source_by_name("alpha")

    def _item(self, name):
        for i in range(self.win.source_list.topLevelItemCount()):
            item = self.win.source_list.topLevelItem(i)
            if item.data(0, 0x0100) == name:  # Qt.UserRole
                return item
        return None

    def _status_col(self):
        return self.win._source_col_keys.index("status")

    def test_busy_refresh_is_queued_not_dropped(self):
        self.win._scan_worker = object()          # a scan is running
        self.win._refresh_sources_by_name(["beta"], background=True)
        self.win._refresh_sources_by_name(["alpha"], background=True)
        self.assertEqual(self.win._pending_refresh_names, {"alpha", "beta"})
        self.assertFalse(self.win._rescan_after_cache)

        calls = []
        with patch.object(type(self.win), "_refresh_selected_sources",
                          lambda win, idx, select_source=None, background=False:
                          calls.append((sorted(idx), background))):
            self.win._scan_worker = None          # the scan finished
            self.win._check_reload_pending()
        self.assertEqual(calls, [([0, 1], True)])
        self.assertEqual(self.win._pending_refresh_names, set())

    def test_user_request_in_queue_makes_it_a_user_refresh(self):
        self.win._scan_worker = object()
        self.win._refresh_sources_by_name(["beta"], background=True)
        self.win._refresh_sources_by_name(["alpha"])  # user-initiated
        calls = []
        with patch.object(type(self.win), "_refresh_selected_sources",
                          lambda win, idx, select_source=None, background=False:
                          calls.append(background)):
            self.win._scan_worker = None
            self.win._check_reload_pending()
        self.assertEqual(calls, [False])

    def test_partial_refresh_updates_rows_in_place(self):
        alpha_item, beta_item = self._item("alpha"), self._item("beta")
        version_item = self.win.version_tree.topLevelItem(1)   # v001 (newest first)
        self.win.version_tree.setCurrentItem(version_item)
        selected_path = version_item.data(0, 0x0100).source_path

        self.win._on_partial_status_complete(
            {"beta": {"status": "error", "integrity": {"valid": False, "message": "x"}},
             "alpha": {"status": "no_version"}},
            {}, {}, {},
        )

        self.assertIs(self._item("alpha"), alpha_item, "row was rebuilt")
        self.assertIs(self._item("beta"), beta_item, "row was rebuilt")
        self.assertEqual(beta_item.text(self._status_col()), "Error")
        self.assertIs(self.win.source_list.currentItem(), alpha_item)
        current = self.win.version_tree.currentItem()
        self.assertIsNotNone(current, "version selection lost")
        self.assertEqual(current.data(0, 0x0100).source_path, selected_path)

    def test_filter_membership_change_rebuilds_list(self):
        self.win.source_filter.setCurrentText("Newer Available")
        self.win._apply_source_filter()
        self.assertIsNone(self._item("beta"))
        self.win._on_partial_status_complete({"beta": {"status": "newer"}}, {}, {}, {})
        self.assertIsNotNone(self._item("beta"), "newly matching source not shown")

    def test_background_refresh_keeps_buttons_enabled(self):
        import time as _time
        self.win.btn_promote.setEnabled(True)
        self.win.btn_refresh.setEnabled(True)
        self.win._refresh_selected_sources([1], background=True)
        try:
            self.assertTrue(self.win.btn_promote.isEnabled())
            self.assertTrue(self.win.btn_refresh.isEnabled())
            self.assertEqual(set(self.win._scan_worker.previous_cache), {"beta"})
        finally:
            deadline = _time.monotonic() + 10
            while ((self.win._scan_worker or self.win._status_worker)
                   and _time.monotonic() < deadline):
                QApplication.processEvents()
                _time.sleep(0.01)

    def test_user_refresh_is_full_and_disables_buttons(self):
        import time as _time
        self.win.btn_promote.setEnabled(True)
        self.win._refresh_selected_sources([1])
        try:
            self.assertFalse(self.win.btn_promote.isEnabled())
            self.assertEqual(self.win._scan_worker.previous_cache, {})
        finally:
            deadline = _time.monotonic() + 10
            while ((self.win._scan_worker or self.win._status_worker)
                   and _time.monotonic() < deadline):
                QApplication.processEvents()
                _time.sleep(0.01)


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestPromoteProgressDisplay(_MainWindowTestBase):
    """Byte progress for large single files (plan item #17)."""

    def test_single_file_start_shows_busy_bar(self):
        self.win._on_promote_progress(0, 1, "clip_v002.mov")
        self.assertEqual(self.win.progress_bar.maximum(), 0)

    def test_byte_progress_beyond_two_gigabytes(self):
        gib = 1024 ** 3
        self.win._on_promote_byte_progress(3.0 * gib, 6.0 * gib, "clip_v002.mov")
        self.assertEqual(self.win.progress_bar.maximum(), 1000)
        self.assertEqual(self.win.progress_bar.value(), 500)
        self.assertEqual(self.win.progress_bar.format(),
                         "clip_v002.mov — 3.0 GB of 6.0 GB (50%)")

    def test_worker_signal_carries_large_values(self):
        from app.workers import PromoteWorker
        seen = []
        worker = PromoteWorker(promoter=None, version=None)
        worker.byte_progress.connect(lambda d, t, n: seen.append((d, t, n)))
        worker._on_byte_progress(5 * 1024 ** 3, 40 * 1024 ** 3, "big.mxf")
        self.assertEqual(seen, [(5.0 * 1024 ** 3, 40.0 * 1024 ** 3, "big.mxf")])


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestWatcherEvents(unittest.TestCase):

    def test_created_and_moved_events_reach_callback(self):
        from watchdog.events import (
            DirCreatedEvent, DirMovedEvent, FileModifiedEvent, FileMovedEvent,
        )
        from src.lvm.models import WatchedSource
        from src.lvm.watcher import _FolderEventHandler
        seen = []
        handler = _FolderEventHandler(seen.append, WatchedSource(name="S", source_dir="/r"))
        handler.dispatch(DirCreatedEvent("/r/shot_v002"))
        handler.dispatch(FileMovedEvent("/r/shot_v002.mov.tmp", "/r/shot_v002.mov"))
        handler.dispatch(DirMovedEvent("/r/.tmp_v003", "/r/shot_v003"))
        handler.dispatch(FileModifiedEvent("/r/shot_v002.mov"))  # ignored
        self.assertEqual(seen, ["S", "S", "S"])


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestSourceWatcherBackends(unittest.TestCase):

    def setUp(self):
        _app()
        from src.lvm.models import WatchedSource
        from src.lvm.watcher import SourceWatcher
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_watch_"))
        (self.tmp / "renders").mkdir()
        self.src = WatchedSource(name="shot", source_dir=str(self.tmp / "renders"))
        self.watcher = SourceWatcher()
        self.watcher._debounce_timer.setInterval(10)
        self.changed = []
        self.status = []
        self.watcher.source_changed.connect(self.changed.append)
        self.watcher.watch_status_changed.connect(self.status.append)

    def tearDown(self):
        self.watcher.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _spin_until(self, cond, timeout=5.0):
        import time as _time
        deadline = _time.monotonic() + timeout
        while not cond() and _time.monotonic() < deadline:
            QApplication.processEvents()
            _time.sleep(0.01)
        QApplication.processEvents()
        return cond()

    def test_polling_mode_reports_new_version_folder(self):
        import time as _time
        self.watcher.start([self.src], mode="polling", poll_interval=0.05)
        self.assertEqual(self.watcher.backend_for("shot"), "polling")
        self.assertTrue(self.watcher.is_running)
        self.assertIn("by polling", self.status[-1])
        _time.sleep(0.15)  # first poll records the current state
        (self.tmp / "renders" / "shot_v002").mkdir()
        self.assertTrue(self._spin_until(lambda: self.changed), "no source_changed")
        self.assertEqual(self.changed, ["shot"])

    def test_missing_folder_falls_back_to_polling(self):
        from src.lvm.models import WatchedSource
        missing = WatchedSource(name="later", source_dir=str(self.tmp / "not_mounted"))
        self.watcher.start([self.src, missing], mode="native", poll_interval=0.05)
        self.assertEqual(self.watcher.backend_for("shot"), "native")
        self.assertEqual(self.watcher.backend_for("later"), "polling")
        (self.tmp / "not_mounted").mkdir()  # share comes online
        self.assertTrue(self._spin_until(lambda: "later" in self.changed))

    def test_dead_native_watch_switches_to_polling_with_catch_up(self):
        self.watcher.start([self.src], mode="native", poll_interval=0.05)
        self.assertEqual(self.watcher.backend_for("shot"), "native")
        # Simulate the emitter thread dying (share dropped): its watch is gone.
        self.watcher._observer.unschedule(self.watcher._native_watches["shot"])
        self.watcher._check_health()
        self.assertEqual(self.watcher.backend_for("shot"), "polling")
        self.assertTrue(self.watcher.is_running)
        self.assertTrue(self._spin_until(lambda: self.changed), "no catch-up rescan")

    def test_stop_ends_both_backends(self):
        self.watcher.start([self.src], mode="polling", poll_interval=0.05)
        self.watcher.stop()
        self.assertFalse(self.watcher.is_running)
        self.assertEqual(self.status[-1], "Watcher stopped")


if __name__ == "__main__":
    unittest.main()
