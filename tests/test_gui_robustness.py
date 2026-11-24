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
        from lvm.models import ProjectConfig, WatchedSource
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
        worker.completed.connect(lambda *a: results.append(a))
        worker.run()  # synchronous: emits on this thread
        self.assertEqual(len(results), 1, "completed must be emitted exactly once")
        return results[0]

    def test_unreadable_history_reports_error_status(self):
        from app.workers import StatusWorker
        from lvm.history import HistoryManager

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
        with patch("lvm.conflicts.detect_target_conflicts",
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
            patch("lvm.nle_bridge.is_resolve_external_available", return_value=False),
            patch("lvm.nle_bridge.is_resolve_running", return_value=False),
            patch("lvm.nle_bridge.is_premiere_panel_alive", return_value=False),
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
        from lvm.models import ProjectConfig, WatchedSource
        from lvm.scanner import VersionScanner
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
        from lvm.models import ProjectConfig
        self.win.config = ProjectConfig(project_name="P")
        self.win.config_path = str(self.tmp / "p.json")
        self.win._dirty = True
        with patch("lvm.config.atomic_write_json", side_effect=OSError("disk full")), \
                patch("app.main_window.QMessageBox.warning") as warn:
            ok = self.win._save_project(blocking=True)
        self.assertFalse(ok)
        self.assertTrue(self.win._dirty)
        warn.assert_called_once()
        self.assertFalse(Path(self.win.config_path).exists())

    def test_successful_blocking_save(self):
        from lvm.models import ProjectConfig
        self.win.config = ProjectConfig(project_name="P")
        self.win.config_path = str(self.tmp / "p.json")
        self.win._dirty = True
        self.assertTrue(self.win._save_project(blocking=True))
        self.assertFalse(self.win._dirty)
        self.assertTrue(Path(self.win.config_path).exists())
        self.assertEqual(Path(self.win.config.project_dir), self.tmp.resolve())

    def test_newer_config_version_warns(self):
        from lvm.models import CONFIG_VERSION, ProjectConfig
        cfg = ProjectConfig(project_name="P")
        cfg.loaded_config_version = CONFIG_VERSION + 1
        with patch("app.main_window.QMessageBox.warning") as warn:
            self.win._warn_if_newer_config(cfg)
        warn.assert_called_once()
        self.assertIn("newer version", warn.call_args.args[2])

    def test_current_config_version_does_not_warn(self):
        from lvm.models import ProjectConfig
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
        from lvm.models import ProjectConfig, WatchedSource
        from lvm.promoter import Promoter
        from lvm.scanner import VersionScanner
        from lvm.watcher import SourceWatcher

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
        from lvm.models import ProjectConfig, VersionInfo, WatchedSource
        from lvm.scanner import VersionScanner
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
        from lvm.models import WatchedSource
        from lvm.watcher import _FolderEventHandler
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
        from lvm.models import WatchedSource
        from lvm.watcher import SourceWatcher
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
        from lvm.models import WatchedSource
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


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestPromoteAllShortcuts(_MainWindowTestBase):
    """F9 promotes all; Enter, F9 and Ctrl+Alt+Up confirm the review dialog."""

    def _dialog(self):
        from app.dialogs.batch_promote import BatchPromoteReviewDialog
        from lvm.models import VersionInfo, WatchedSource
        src = WatchedSource(name="S", source_dir=str(self.tmp))
        ver = VersionInfo(version_string="v002", version_number=2,
                          source_path=str(self.tmp / "v002"))
        dlg = BatchPromoteReviewDialog([(src, ver)], {}, {}, [], [], parent=self.win)
        dlg.show()
        dlg.activateWindow()
        from PySide6.QtTest import QTest
        QTest.qWaitForWindowActive(dlg, 1000)
        self.addCleanup(dlg.deleteLater)
        return dlg

    def _press(self, dlg, key, modifiers=None, autorep=False):
        from PySide6.QtCore import QEvent, Qt
        from PySide6.QtGui import QKeyEvent
        mods = modifiers if modifiers is not None else Qt.NoModifier
        target = QApplication.focusWidget() or dlg
        for etype in (QEvent.ShortcutOverride, QEvent.KeyPress):
            ev = QKeyEvent(etype, key, mods, "", autorep)
            QApplication.sendEvent(target, ev)
        QApplication.sendEvent(target, QKeyEvent(QEvent.KeyRelease, key, mods, "", autorep))
        QApplication.processEvents()

    def test_menu_action_has_f9_and_old_shortcut(self):
        from PySide6.QtGui import QKeySequence
        action = next(a for m in self.win.menuBar().actions()
                      for a in m.menu().actions() if a.text() == "&Promote All to Latest")
        seqs = [s.toString() for s in action.shortcuts()]
        self.assertEqual(seqs[0], "F9")
        for want in ("Ctrl+Alt+Up", "Shift+F9", "Ctrl+Alt+Shift+Up"):
            self.assertIn(QKeySequence(want).toString(), seqs)

    def test_promote_is_default_button(self):
        dlg = self._dialog()
        self.assertTrue(dlg.btn_promote.isDefault())

    def test_return_and_enter_confirm(self):
        from PySide6.QtCore import Qt
        from PySide6.QtWidgets import QDialog
        for key in (Qt.Key_Return, Qt.Key_Enter):
            dlg = self._dialog()
            dlg.tree.setFocus()
            self._press(dlg, key)
            self.assertEqual(dlg.result(), QDialog.Accepted, key)

    def test_promote_all_shortcuts_confirm(self):
        from PySide6.QtCore import Qt
        from PySide6.QtWidgets import QDialog
        mods_ctrl_alt = Qt.ControlModifier | Qt.AltModifier
        for key, mods in ((Qt.Key_F9, Qt.NoModifier), (Qt.Key_Up, mods_ctrl_alt),
                          (Qt.Key_F9, Qt.ShiftModifier),
                          (Qt.Key_Up, mods_ctrl_alt | Qt.ShiftModifier)):
            dlg = self._dialog()
            self._press(dlg, key, mods)
            self.assertEqual(dlg.result(), QDialog.Accepted, (key, mods))

    def test_held_f9_does_not_confirm(self):
        from PySide6.QtCore import Qt
        dlg = self._dialog()
        self._press(dlg, Qt.Key_F9, autorep=True)
        self.assertTrue(dlg.isVisible())
        self.assertEqual(dlg.result(), 0)


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestResolveMetadataSetting(unittest.TestCase):
    """Settings -> NLE -> Resolve metadata combo round-trips the option."""

    def test_combo_reads_and_writes_option(self):
        _app()
        from app.dialogs.settings import ProjectSettingsDialog
        from lvm.models import ProjectConfig
        cfg = ProjectConfig(project_name="P", nle_resolve_metadata_field="Description")
        dlg = ProjectSettingsDialog(cfg)
        self.addCleanup(dlg.deleteLater)
        self.assertEqual(dlg.nle_metadata_combo.currentData(), "Description")
        dlg.nle_metadata_combo.setCurrentIndex(dlg.nle_metadata_combo.findData(""))
        dlg.apply_to_config(cfg)
        self.assertEqual(cfg.nle_resolve_metadata_field, "")
        dlg.nle_metadata_combo.setCurrentIndex(dlg.nle_metadata_combo.findData("Comments"))
        dlg.apply_to_config(cfg)
        self.assertEqual(cfg.nle_resolve_metadata_field, "Comments")


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestAutoPromoteNotifier(unittest.TestCase):
    """Level filtering, de-duplication and the active-window rule."""

    def setUp(self):
        _app()
        from unittest.mock import MagicMock
        from PySide6.QtWidgets import QWidget
        from app.notifier import AutoPromoteNotifier
        self.window = QWidget()
        self.addCleanup(self.window.deleteLater)
        self.n = AutoPromoteNotifier(self.window)
        self.tray = MagicMock()

    def _level(self, level):
        with patch("app.notifier.AutoPromoteNotifier.available", return_value=False):
            self.n.set_level(level)
        self.n._tray = None if level == "off" else self.tray

    def test_off_shows_nothing(self):
        self._level("off")
        self.assertFalse(self.n.notify("a", "failed", "a: boom"))

    def test_problems_level_filters(self):
        self._level("problems")
        self.assertFalse(self.n.notify("a", "promoted", "a -> v002"))
        self.assertFalse(self.n.notify("a", "waiting", "a waiting"))
        self.assertTrue(self.n.notify("a", "skipped", "a: range"))
        self.assertTrue(self.n.notify("b", "failed", "b: boom"))
        self.assertEqual(self.tray.showMessage.call_count, 2)

    def test_all_level_and_dedupe(self):
        self._level("all")
        self.assertTrue(self.n.notify("a", "skipped", "a: range"))
        self.assertFalse(self.n.notify("a", "skipped", "a: range"))  # same again
        self.assertTrue(self.n.notify("a", "promoted", "a -> v002"))
        self.assertTrue(self.n.notify("a", "skipped", "a: range"))  # after something else

    def test_not_shown_while_window_active(self):
        self._level("all")
        with patch.object(self.window, "isActiveWindow", return_value=True):
            self.assertFalse(self.n.notify("a", "failed", "a: boom"))
        self.tray.showMessage.assert_not_called()

    def test_unknown_level_is_off(self):
        self._level("loud")
        self.assertEqual(self.n.level, "off")

    def test_click_selects_source(self):
        from unittest.mock import MagicMock
        clicked = MagicMock()
        self.n._on_clicked = clicked
        self._level("all")
        self.n.notify("shot", "promoted", "shot -> v003")
        self.n._message_clicked()
        clicked.assert_called_once_with("shot")


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestAutoPromoteNotifications(_MainWindowTestBase):
    """MainWindow reports auto-promote outcomes to the notifier."""

    def setUp(self):
        super().setUp()
        from unittest.mock import MagicMock
        self.notify = MagicMock()
        self.win._notifier.notify = self.notify
        self._enabled = patch.object(type(self.win), "_auto_promote_enabled", return_value=True)
        self._enabled.start()
        self.addCleanup(self._enabled.stop)

    def _decision(self, action, **kw):
        from lvm.auto_promote import AutoPromoteDecision
        return AutoPromoteDecision(source_name="shot", action=action, **kw)

    def test_skip_and_error_notify(self):
        from lvm import auto_promote as ap
        self.win._on_auto_promote_decided(self._decision(ap.SKIP, reason="frame range changed"))
        self.notify.assert_called_with("shot", "skipped", "shot: frame range changed")
        self.win._on_auto_promote_decided(self._decision(ap.ERROR, reason="scan failed"))
        self.notify.assert_called_with("shot", "failed", "shot: scan failed")

    def test_waiting_notifies_first_time_only(self):
        from lvm import auto_promote as ap
        from lvm.models import VersionInfo
        v = VersionInfo("v002", 2, str(self.tmp / "v002"))
        with patch.object(self.win, "_schedule_render_wait_retry"):
            self.win._on_auto_promote_decided(self._decision(ap.WAIT_RENDER, version=v,
                                                             reason="open"))
            self.win._on_auto_promote_decided(self._decision(ap.WAIT_RENDER, version=v,
                                                             reason="open",
                                                             waiting_on_render=True))
        self.assertEqual(self.notify.call_count, 1)
        self.assertEqual(self.notify.call_args.args[1], "waiting")

    def test_auto_promotion_finish_and_failure_notify(self):
        from lvm.models import HistoryEntry
        entry = HistoryEntry(version="v002", source="", set_by="t", set_at="")
        with patch.object(self.win, "_process_deferred_or_refresh"), \
             patch.object(self.win, "_maybe_auto_sync_nle"):
            self.win._promoting_source_name = "shot"
            self.win._auto_promoting_name = "shot"
            self.win._on_promote_finished(entry)
        self.notify.assert_called_once_with("shot", "promoted", "shot → v002")
        self.assertIsNone(self.win._auto_promoting_name)

        self.notify.reset_mock()
        with patch("app.main_window.QMessageBox.critical"):
            self.win._promoting_source_name = "shot"
            self.win._auto_promoting_name = "shot"
            self.win._on_promote_error("Disk full\nmore detail")
        self.notify.assert_called_once_with("shot", "failed", "shot: Disk full")

    def test_manual_promotion_does_not_notify(self):
        from lvm.models import HistoryEntry
        entry = HistoryEntry(version="v002", source="", set_by="t", set_at="")
        with patch.object(self.win, "_process_deferred_or_refresh"), \
             patch.object(self.win, "_maybe_auto_sync_nle"):
            self.win._promoting_source_name = "shot"
            self.win._on_promote_finished(entry)
        self.notify.assert_not_called()

    def test_option_round_trip(self):
        from lvm.models import ProjectConfig
        cfg = ProjectConfig(project_name="P", auto_promote_notifications="problems")
        d = cfg.to_dict()
        self.assertEqual(ProjectConfig.from_dict(d).auto_promote_notifications, "problems")
        d["auto_promote_notifications"] = "loud"
        self.assertEqual(ProjectConfig.from_dict(d).auto_promote_notifications, "off")
        self.assertNotIn("auto_promote_notifications",
                         ProjectConfig(project_name="P").to_dict())


@unittest.skipUnless(HAVE_QT, "PySide6 not available")
class TestP0GuiRegressions(_MainWindowTestBase):
    """Regression tests for P0 GUI bugs (templates, groups, extensions, force flag)."""

    def test_normalize_extensions_adds_leading_dots(self):
        from app._common import normalize_extensions
        self.assertEqual(normalize_extensions("exr .DPX, mov"), [".exr", ".dpx", ".mov"])
        self.assertEqual(normalize_extensions(""), list(normalize_extensions("")))

    def test_manage_groups_cancel_does_not_mutate_sources(self):
        from app.dialogs.manage_groups import ManageGroupsDialog
        from lvm.models import ProjectConfig, WatchedSource
        cfg = ProjectConfig(
            project_name="G",
            groups={"Comp": {"color": "#ff0000"}},
            watched_sources=[WatchedSource(name="s1", source_dir="/a", group="Comp")],
        )
        dlg = ManageGroupsDialog(cfg)
        self.addCleanup(dlg.deleteLater)
        dlg.group_list.setCurrentRow(0)
        with patch("app.dialogs.manage_groups.QInputDialog.getText", return_value=("Renamed", True)):
            dlg._rename_group()
        # Cancel — live source must still belong to "Comp"
        dlg.reject()
        self.assertEqual(cfg.watched_sources[0].group, "Comp")
        self.assertIn("Comp", cfg.groups)

        # Now verify OK applies both groups and source assignments
        dlg2 = ManageGroupsDialog(cfg)
        self.addCleanup(dlg2.deleteLater)
        dlg2.group_list.setCurrentRow(0)
        with patch("app.dialogs.manage_groups.QInputDialog.getText", return_value=("Renamed", True)):
            dlg2._rename_group()
        dlg2.apply_to_config(cfg)
        self.assertEqual(cfg.watched_sources[0].group, "Renamed")
        self.assertIn("Renamed", cfg.groups)

    def test_group_by_keeps_sorting_disabled_and_skips_header_selection(self):
        from PySide6.QtCore import Qt
        from lvm.models import ProjectConfig, WatchedSource
        cfg = ProjectConfig(
            project_name="G",
            groups={"Alpha": {"color": "#ff0000"}, "Beta": {"color": "#00ff00"}},
            watched_sources=[
                WatchedSource(name="z_shot", source_dir=str(self.tmp / "z"), group="Alpha"),
                WatchedSource(name="a_shot", source_dir=str(self.tmp / "a"), group="Beta"),
            ],
        )
        self.win.config = cfg
        self.win.group_by_check.setChecked(True)
        self.win._populate_source_list()
        self.assertFalse(self.win.source_list.isSortingEnabled())
        # Item 0 is the Alpha group header; item 1 is z_shot
        self.assertIn("Alpha", self.win.source_list.topLevelItem(0).text(0))
        self.assertEqual(self.win.source_list.topLevelItem(1).data(0, Qt.UserRole), "z_shot")
        self.win._select_first_source_item()
        cur = self.win.source_list.currentItem()
        self.assertIsNotNone(cur)
        self.assertEqual(cur.data(0, Qt.UserRole), "z_shot")

    def test_new_project_with_template_does_not_clobber_open_project(self):
        from lvm.config import save_config, load_config
        from lvm.models import ProjectConfig
        from lvm.templates import save_template

        open_path = str(self.tmp / "open_proj.json")
        open_cfg = ProjectConfig(project_name="OpenProj", default_link_mode="copy")
        save_config(open_cfg, open_path)
        self.win.config = open_cfg
        self.win.config_path = open_path

        tpl_cfg = ProjectConfig(project_name="Tpl", default_link_mode="hardlink")
        tpl_path = save_template(tpl_cfg, "HardlinkTpl", location="project", project_dir=str(self.tmp))

        new_dir = self.tmp / "new_proj"
        new_dir.mkdir()
        fake_info = {
            "project_name": "NewProj",
            "project_root": str(new_dir),
            "save_dir": str(new_dir),
            "name_whitelist": [],
            "name_blacklist": [],
            "task_tokens": [],
            "template_path": str(tpl_path),
        }
        fake_dlg = type("D", (), {
            "exec": lambda self_: 1,
            "get_project_info": lambda self_: fake_info,
        })()
        with patch("app.main_window.ProjectSetupDialog", return_value=fake_dlg), \
             patch.object(self.win, "_load_project") as mock_load:
            self.win._new_project()

        # Previously open project must remain untouched on disk and in memory
        self.assertEqual(self.win.config.default_link_mode, "copy")
        self.assertEqual(load_config(open_path).default_link_mode, "copy")
        # Newly created project on disk must have the template applied
        created_path = mock_load.call_args.args[0]
        self.assertEqual(load_config(created_path).default_link_mode, "hardlink")

    def test_force_promote_does_not_leak_to_next_promotion(self):
        from lvm.models import VersionInfo, WatchedSource
        from lvm.promoter import Promoter
        src = WatchedSource(name="shot", source_dir=str(self.tmp), latest_target=str(self.tmp))
        p = Promoter(src)
        v = VersionInfo("v001", 1, str(self.tmp))
        captured_force = []

        class FakeWorker:
            def __init__(self_, promoter, version, parent, force=False, pinned=False, keep_layers=None):
                captured_force.append(force)
                self_.progress = type("S", (), {"connect": lambda *a: None})()
                self_.byte_progress = type("S", (), {"connect": lambda *a: None})()
                self_.completed = type("S", (), {"connect": lambda *a: None})()
                self_.finished = type("S", (), {"connect": lambda *a: None})()
                self_.error = type("S", (), {"connect": lambda *a: None})()
            def start(self_):
                pass
            def isRunning(self_):
                return False
            def deleteLater(self_):
                pass
            def disconnect(self_):
                pass

        with patch("app.main_window.check_link_mode_available", return_value=(True, "")), \
             patch("app.main_window.PromoteWorker", FakeWorker):
            self.win._start_promotion(p, v, force=True)
            self.win._start_promotion(p, v)
        self.win._worker = None
        self.assertEqual(captured_force, [True, False])


if __name__ == "__main__":
    unittest.main()
