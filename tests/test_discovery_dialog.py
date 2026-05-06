"""
Tests for DiscoveryDialog logic — currently focused on the existing-source
filter, which has to handle multi-shot flat folders where many sources
legitimately share one source_dir.
"""

import sys
import tempfile
import unittest
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "src"))

from PySide6.QtWidgets import QApplication

# A QApplication is required before any widget can be instantiated.
_app = QApplication.instance() or QApplication(sys.argv)

from app import DiscoveryDialog


class TestIsExistingMultiShot(unittest.TestCase):
    """Regression tests for _is_existing.

    Multi-shot flat folders (one folder, many WatchedSources differing by
    name/sample_filename) require the (path, name) match to win over a raw
    path-only match. Otherwise every new sibling shot dropped into a folder
    that already has *any* registered source is flagged as already-added.
    """

    def setUp(self):
        # One DiscoveryDialog per class is enough — _is_existing is pure.
        self._dialog = DiscoveryDialog()
        self._tmpdir = tempfile.mkdtemp()
        self._folder = str(Path(self._tmpdir).resolve())
        self._existing = {(self._folder.lower(), "a001c007_r1wc_comp")}

    def tearDown(self):
        self._dialog.deleteLater()
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_sibling_shot_in_same_folder_is_not_existing(self):
        """A002C009 in a folder where A001C007 is registered must NOT be
        flagged as already-added — they're different shots."""
        self.assertFalse(
            self._dialog._is_existing(
                self._folder, self._existing, "a002c009_r1wc_comp"))

    def test_same_shot_same_folder_is_existing(self):
        """The exact (path, name) pair still matches."""
        self.assertTrue(
            self._dialog._is_existing(
                self._folder, self._existing, "a001c007_r1wc_comp"))

    def test_path_only_fallback_when_no_name(self):
        """Legacy callers that don't supply a result_name still hit the
        path-only fallback — preserves backward compatibility."""
        self.assertTrue(
            self._dialog._is_existing(self._folder, self._existing, ""))

    def test_different_folder_is_not_existing(self):
        """A folder that doesn't appear in existing_sources at all is new."""
        with tempfile.TemporaryDirectory() as other:
            self.assertFalse(
                self._dialog._is_existing(
                    other, self._existing, "a002c009_r1wc_comp"))


if __name__ == "__main__":
    unittest.main()
