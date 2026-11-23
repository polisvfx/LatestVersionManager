"""Regression tests for P0 data-loss, flat-layout, and NLE-probe fixes."""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lvm.models import VersionInfo, WatchedSource
from lvm.promoter import Promoter, PromotionError
from lvm import nle_bridge


class TestCaseInsensitiveSwap(unittest.TestCase):
    """Changing filename casing between versions must never delete the new files."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="lvm_p0_swap_")
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_case_change_between_versions_preserves_new_frames(self):
        renders = self.root / "renders"
        target = self.root / "latest"
        v1 = renders / "shot_v001"
        v2 = renders / "shot_v002"
        v1.mkdir(parents=True)
        v2.mkdir(parents=True)
        target.mkdir(parents=True)

        # v001 has UpperCase filenames; v002 has lowercase filenames.
        # Disable file_rename_template so the case difference survives into target.
        for f in (1001, 1002, 1003):
            (v1 / f"Hero_Comp_v001.{f}.exr").write_bytes(b"v1-" + str(f).encode())
            (v2 / f"hero_comp_v002.{f}.exr").write_bytes(b"v2-" + str(f).encode())

        src = WatchedSource(
            name="shot",
            source_dir=str(renders),
            latest_target=str(target),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            file_rename_template="",
            link_mode="copy",
        )
        p = Promoter(src)

        p.promote(VersionInfo("v001", 1, str(v1), file_count=3, total_size_bytes=18), user="t")
        p.promote(VersionInfo("v002", 2, str(v2), file_count=3, total_size_bytes=18), user="t")

        live = sorted(e.name for e in os.scandir(target) if not e.name.startswith("."))
        self.assertEqual(len(live), 3)
        for name in live:
            self.assertTrue((target / name).read_bytes().startswith(b"v2-"),
                            f"Expected v2 content in {name}")

    def test_case_collision_within_single_promote_rejected_on_case_insensitive_os(self):
        renders = self.root / "renders"
        target = self.root / "latest"
        v1 = renders / "shot_v001"
        v1.mkdir(parents=True)
        target.mkdir(parents=True)

        (v1 / "Hero_v001.1001.exr").write_bytes(b"a")
        (v1 / "Hero_v001.1002.exr").write_bytes(b"b")

        src = WatchedSource(
            name="shot",
            source_dir=str(renders),
            latest_target=str(target),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            file_rename_template="constant",
            link_mode="copy",
        )
        p = Promoter(src)
        with patch.object(os.path, "normcase", side_effect=lambda s: s.lower()):
            with patch.object(p, "_remap_filename", side_effect=["Out.1001.exr", "out.1001.exr"]):
                with self.assertRaises(PromotionError):
                    p.promote(VersionInfo("v001", 1, str(v1), file_count=2), user="t")


class TestFlatLayoutFiltering(unittest.TestCase):
    """Flat-layout _filter_version_files must never fall back to all files or mix shots."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="lvm_p0_flat_")
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_zero_match_raises_instead_of_promoting_all_versions(self):
        renders = self.root / "renders"
        target = self.root / "latest"
        renders.mkdir()
        target.mkdir()

        (renders / "shot010_v001.1001.exr").write_bytes(b"1")
        (renders / "shot010_v002.1001.exr").write_bytes(b"2")

        src = WatchedSource(
            name="shot010",
            source_dir=str(renders),
            latest_target=str(target),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            sample_filename="shot010_v001.1001.exr",
            link_mode="copy",
        )
        p = Promoter(src)
        missing_v = VersionInfo("v099", 99, str(renders), file_count=1)
        with self.assertRaises(PromotionError):
            p.promote(missing_v, user="t")

        live = [e.name for e in os.scandir(target) if not e.name.startswith(".")]
        self.assertEqual(live, [])

    def test_multi_shot_flat_dir_only_promotes_own_shot(self):
        renders = self.root / "renders"
        target = self.root / "latest"
        renders.mkdir()
        target.mkdir()

        for f in (1001, 1002):
            (renders / f"shot010_v001.{f}.exr").write_bytes(b"s10")
            (renders / f"shot020_v001.{f}.exr").write_bytes(b"s20")

        src = WatchedSource(
            name="shot010",
            source_dir=str(renders),
            latest_target=str(target),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            sample_filename="shot010_v001.1001.exr",
            file_rename_template="{source_basename}_latest",
            link_mode="copy",
        )
        p = Promoter(src)
        v = VersionInfo("v001", 1, str(renders), file_count=2)
        p.promote(v, user="t")

        live = sorted(e.name for e in os.scandir(target) if not e.name.startswith("."))
        self.assertEqual(live, ["shot010_latest.1001.exr", "shot010_latest.1002.exr"])


class TestNleBridgePermissionErrors(unittest.TestCase):
    """Restricted OS directories must return False/None instead of crashing."""

    def test_premiere_installed_handles_permission_error(self):
        with patch.object(Path, "is_file", side_effect=PermissionError("denied")):
            self.assertFalse(nle_bridge.is_premiere_panel_installed())

    def test_resolve_probes_handle_permission_error(self):
        with patch.object(Path, "is_dir", side_effect=PermissionError("denied")):
            self.assertIsNone(nle_bridge.resolve_modules_path())
        with patch.object(Path, "is_file", side_effect=PermissionError("denied")):
            self.assertIsNone(nle_bridge.resolve_script_lib_path())


if __name__ == "__main__":
    unittest.main()
