"""
Tests for MainWindow's add-source finishing step (shared by the Add Source
button and drag-and-drop): new sources get project defaults applied and a
history sidecar of their own when they share a latest folder.

GUI test (imports PySide6) — run locally, not part of the portable CI suite.
"""

import sys
import tempfile
import unittest
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "src"))

from PySide6.QtWidgets import QApplication

_app = QApplication.instance() or QApplication(sys.argv)

from app.main_window import MainWindow
from lvm.models import ProjectConfig, WatchedSource


class _FakeWindow:
    """Just the attributes _finish_adding_sources touches."""

    def __init__(self, config):
        self.config = config
        self.config_path = None
        self.dirty = False
        self.reloaded = False

    def _mark_dirty(self):
        self.dirty = True

    def _reload_ui(self):
        self.reloaded = True


class TestFinishAddingSources(unittest.TestCase):

    def test_added_source_gets_latest_target_and_own_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = ProjectConfig(project_name="Show")
            cfg.project_dir = tmp
            cfg.latest_path_template = str(Path(tmp) / "COMP_LATEST")
            existing = WatchedSource(
                name="SH0030_faceswap", source_dir=tmp,
                sample_filename="SH0030_faceswap_v003.mov",
                latest_target=str(Path(tmp) / "COMP_LATEST"),
            )
            added = WatchedSource(  # as it comes out of the Add Source dialog
                name="DC2070_faceswap", source_dir=tmp,
                sample_filename="DC2070_faceswap_v002.mov",
            )
            cfg.watched_sources = [existing, added]

            win = _FakeWindow(cfg)
            MainWindow._finish_adding_sources(win)

            self.assertTrue(win.dirty)
            self.assertTrue(win.reloaded)
            self.assertEqual(Path(added.latest_target), Path(tmp, "COMP_LATEST").resolve())
            self.assertEqual(existing.history_filename, ".latest_history.json")
            self.assertEqual(added.history_filename, ".latest_history_DC2070_faceswap.json")


if __name__ == "__main__":
    unittest.main()
