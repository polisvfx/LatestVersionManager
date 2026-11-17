"""
Tests for the drag-and-drop "add source" helpers in ``src.lvm.discovery``:
``resolve_drop_dir`` and ``source_drafts_for_path``.

Portable (no Qt) — part of the CI suite.
"""

import sys
import tempfile
import unittest
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "src"))

from src.lvm.discovery import resolve_drop_dir, source_drafts_for_path
from src.lvm.models import ProjectConfig


def _touch(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"\0")


def _make_version_dirs(parent: Path, base: str, versions=(1, 2), sep="_v", ext=".exr"):
    for v in versions:
        vdir = parent / f"{base}{sep}{v:03d}"
        for f in (1001, 1002):
            _touch(vdir / f"{base}{sep}{v:03d}.{f}{ext}")


class TestResolveDropDir(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_plain_folder_is_itself(self):
        d = self.root / "sh010" / "comp"
        d.mkdir(parents=True)
        self.assertEqual(resolve_drop_dir(d), d)

    def test_version_folder_maps_to_parent(self):
        parent = self.root / "sh010" / "comp"
        _make_version_dirs(parent, "sh010_comp")
        self.assertEqual(resolve_drop_dir(parent / "sh010_comp_v002"), parent)

    def test_date_folder_maps_to_parent(self):
        parent = self.root / "plates"
        d = parent / "plate_240115"
        d.mkdir(parents=True)
        self.assertEqual(resolve_drop_dir(d), parent)

    def test_file_maps_to_parent(self):
        f = self.root / "renders" / "shot_v001.mov"
        _touch(f)
        self.assertEqual(resolve_drop_dir(f), f.parent)


class TestSourceDraftsForPath(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve()
        self.config = ProjectConfig(project_name="Show")

    def tearDown(self):
        self._tmp.cleanup()

    def test_version_dirs_give_one_draft(self):
        parent = self.root / "sh010_comp"
        _make_version_dirs(parent, "sh010_comp")
        drafts = source_drafts_for_path(parent, self.config)
        self.assertEqual(len(drafts), 1)
        d = drafts[0]
        self.assertEqual(Path(d.source_dir), parent)
        self.assertEqual(d.name, "sh010_comp")
        # Discovery scans version dirs in parallel, so the sample may come
        # from any version — only its shape is deterministic.
        self.assertRegex(d.sample_filename, r"^sh010_comp_v00[12]\.\d{4}\.exr$")
        # Matches project defaults -> nothing overridden
        self.assertFalse(d.override_version_pattern)
        self.assertFalse(d.override_file_extensions)
        self.assertFalse(d.override_date_format)

    def test_dropping_version_folder_equals_dropping_parent(self):
        parent = self.root / "sh010_comp"
        _make_version_dirs(parent, "sh010_comp")
        a = source_drafts_for_path(parent, self.config)
        b = source_drafts_for_path(parent / "sh010_comp_v001", self.config)
        self.assertEqual([(d.name, d.source_dir) for d in a],
                         [(d.name, d.source_dir) for d in b])

    def test_two_shots_in_one_folder_give_two_drafts(self):
        parent = self.root / "renders"
        _make_version_dirs(parent, "sh010_comp")
        _make_version_dirs(parent, "sh020_comp")
        drafts = source_drafts_for_path(parent, self.config)
        self.assertEqual(sorted(d.name for d in drafts), ["sh010_comp", "sh020_comp"])

    def test_unrecognised_folder_gives_bare_draft(self):
        d = self.root / "weird_shot_name"
        _touch(d / "readme.txt")
        drafts = source_drafts_for_path(d, self.config)
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0].name, "weird_shot_name")
        self.assertEqual(Path(drafts[0].source_dir), d)
        self.assertFalse(drafts[0].override_version_pattern)

    def test_non_default_pattern_is_overridden(self):
        parent = self.root / "sh030_comp"
        _make_version_dirs(parent, "sh030_comp", sep=".v")
        drafts = source_drafts_for_path(parent, self.config)
        self.assertEqual(len(drafts), 1)
        self.assertTrue(drafts[0].override_version_pattern)
        self.assertEqual(drafts[0].version_pattern, ".v{version}")

    def test_extension_outside_defaults_is_overridden(self):
        self.config.default_file_extensions = [".exr"]
        parent = self.root / "sh040_comp"
        _make_version_dirs(parent, "sh040_comp", ext=".png")
        drafts = source_drafts_for_path(parent, self.config)
        self.assertEqual(len(drafts), 1)
        self.assertTrue(drafts[0].override_file_extensions)
        self.assertEqual(drafts[0].file_extensions, [".exr", ".png"])

    def test_naming_rule_applied_when_configured(self):
        parent = self.root / "sh050" / "comp"
        _make_version_dirs(parent, "sh050_comp")
        self.config.naming_configured = True
        self.config.default_naming_rule = "parent:1"
        drafts = source_drafts_for_path(parent, self.config)
        self.assertEqual(drafts[0].name, "sh050")


class TestAddedSourceGetsOwnHistory(unittest.TestCase):
    """Regression: a source added via Add Source / drag-and-drop into a
    shared latest folder must not reuse the first source's
    .latest_history.json (they'd overwrite each other's promotion records
    and be flagged as a target conflict)."""

    def test_new_source_is_namespaced_existing_keeps_file(self):
        from src.lvm.config import _migrate_shared_history_filenames
        from src.lvm.conflicts import detect_target_conflicts
        from src.lvm.models import WatchedSource

        with tempfile.TemporaryDirectory() as tmp:
            latest = str(Path(tmp) / "COMP_LATEST")
            cfg = ProjectConfig(project_name="Show")
            cfg.watched_sources = [
                WatchedSource(name="SH0030_faceswap", source_dir=tmp,
                              latest_target=latest,
                              sample_filename="SH0030_faceswap_v003.mov"),
                WatchedSource(name="SH0040_faceswap", source_dir=tmp,
                              latest_target=latest,
                              history_filename=".latest_history_SH0040_faceswap.json",
                              sample_filename="SH0040_faceswap_v003.mov"),
                # Newly added, appended last, default history filename
                WatchedSource(name="DC2070_faceswap", source_dir=tmp,
                              latest_target=latest,
                              sample_filename="DC2070_C_0006C001_261029_p1EES_faceswap_v002.mov"),
            ]
            self.assertTrue(detect_target_conflicts(cfg))

            _migrate_shared_history_filenames(cfg)

            by_name = {s.name: s for s in cfg.watched_sources}
            self.assertEqual(by_name["SH0030_faceswap"].history_filename, ".latest_history.json")
            self.assertEqual(by_name["SH0040_faceswap"].history_filename,
                             ".latest_history_SH0040_faceswap.json")
            self.assertEqual(by_name["DC2070_faceswap"].history_filename,
                             ".latest_history_DC2070_faceswap.json")
            self.assertEqual(detect_target_conflicts(cfg), [])


if __name__ == "__main__":
    unittest.main()
