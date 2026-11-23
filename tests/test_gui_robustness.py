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
        ]
        for p in extra:
            p.start()
        self._patches.extend(extra)

    def _drain(self, timeout=10.0):
        import time as _time
        deadline = _time.monotonic() + timeout
        while self.win._auto_check_workers and _time.monotonic() < deadline:
            QApplication.processEvents()
            _time.sleep(0.01)
        QApplication.processEvents()
        self.assertEqual(self.win._auto_check_workers, {}, "check worker did not finish")

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
        self._drain()  # the queued re-check
        self.assertEqual(self.started, ["v002"], "expected exactly one promotion")

    def test_switching_auto_promote_off_drops_pending_decision(self):
        self.win._try_auto_promote("shot")
        self.win.auto_promote_cb.setChecked(False)
        self._drain()
        self.assertEqual(self.started, [])


if __name__ == "__main__":
    unittest.main()
