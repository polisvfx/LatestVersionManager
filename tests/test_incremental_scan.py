"""
Tests for incremental scanning (VersionScanner.scan(previous=...)).

Self-contained: temp directories. Portable (no Qt).
"""

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.models import VersionInfo, WatchedSource
from lvm.scanner import VersionScanner, source_scan_fingerprint


class TestIncrementalScan(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_incr_"))
        self.renders = self.tmp / "renders"
        self.source = WatchedSource(
            name="shot", source_dir=str(self.renders),
            version_pattern="_v{version}", file_extensions=[".exr"],
        )
        for v in ("v001", "v002", "v003"):
            self._frames(v, 1001, 1004)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _frames(self, version, first, last):
        vdir = self.renders / f"shot_{version}"
        vdir.mkdir(parents=True, exist_ok=True)
        for f in range(first, last + 1):
            (vdir / f"shot_{version}.{f}.exr").write_bytes(b"x" * 10)
        return vdir

    def _touch_dir(self, vdir):
        """Move the folder's mtime forward explicitly — filesystems with
        coarse timestamps could otherwise leave it unchanged within a test."""
        st = vdir.stat()
        os.utime(vdir, ns=(st.st_atime_ns, st.st_mtime_ns + 2_000_000_000))

    def _summary(self, versions):
        return [(v.version_string, v.file_count, v.frame_range, v.total_size_bytes)
                for v in versions]

    def test_full_scan_sets_tokens(self):
        versions = VersionScanner(self.source).scan()
        self.assertEqual(len(versions), 3)
        self.assertTrue(all(v.scan_token for v in versions))

    def test_unchanged_folders_are_reused_except_newest(self):
        first = VersionScanner(self.source).scan()
        scanner = VersionScanner(self.source)
        with patch.object(VersionScanner, "_scan_version_folder",
                          autospec=True, wraps=VersionScanner._scan_version_folder) as spy:
            second = scanner.scan(previous=first)
        listed = [Path(c.args[1]).name for c in spy.call_args_list]
        self.assertEqual(listed, ["shot_v003"], "only the newest folder should be listed")
        self.assertEqual((scanner.last_reused, scanner.last_scanned), (2, 1))
        self.assertEqual(self._summary(second), self._summary(first))

    def test_changed_old_folder_is_rescanned(self):
        first = VersionScanner(self.source).scan()
        vdir = self._frames("v001", 1005, 1006)        # late frames land in v001
        self._touch_dir(vdir)
        scanner = VersionScanner(self.source)
        second = scanner.scan(previous=first)
        v001 = next(v for v in second if v.version_string == "v001")
        self.assertEqual(v001.file_count, 6)
        self.assertEqual(v001.frame_range, "1001-1006")
        self.assertEqual(scanner.last_scanned, 2)      # v001 + newest

    def test_new_and_removed_folders(self):
        first = VersionScanner(self.source).scan()
        shutil.rmtree(self.renders / "shot_v001")
        self._frames("v004", 1001, 1002)
        second = VersionScanner(self.source).scan(previous=first)
        self.assertEqual([v.version_string for v in second], ["v002", "v003", "v004"])

    def test_newest_folder_always_relisted(self):
        """A frame growing in the newest folder doesn't change its mtime."""
        first = VersionScanner(self.source).scan()
        (self.renders / "shot_v003" / "shot_v003.1004.exr").write_bytes(b"x" * 500)
        second = VersionScanner(self.source).scan(previous=first)
        self.assertEqual(second[-1].total_size_bytes, 3 * 10 + 500)

    def test_changed_settings_disable_reuse(self):
        first = VersionScanner(self.source).scan()
        self.source.file_extensions = [".exr", ".dpx"]
        scanner = VersionScanner(self.source)
        scanner.scan(previous=first)
        self.assertEqual(scanner.last_reused, 0)
        self.assertNotEqual(source_scan_fingerprint(self.source),
                            first[0].scan_token.split(":")[0])

    def test_reuse_keeps_timecode_and_copies_objects(self):
        first = VersionScanner(self.source).scan()
        first[0].start_timecode = "01:00:00:00"
        first[1].timecode_checked = True
        second = VersionScanner(self.source).scan(previous=first)
        self.assertEqual(second[0].start_timecode, "01:00:00:00")
        self.assertTrue(second[1].timecode_checked)
        self.assertIsNot(second[0], first[0])

    def test_previous_without_tokens_is_ignored(self):
        """Results from an older LVM (or the scan cache before this
        change) carry no scan_token and are never reused."""
        first = VersionScanner(self.source).scan()
        for v in first:
            v.scan_token = ""
        scanner = VersionScanner(self.source)
        scanner.scan(previous=first)
        self.assertEqual(scanner.last_reused, 0)

    def test_scan_token_round_trips_through_cache_format(self):
        v = VersionScanner(self.source).scan()[0]
        self.assertEqual(VersionInfo.from_dict(v.to_dict()).scan_token, v.scan_token)
        self.assertNotIn("scan_token", VersionInfo("v001", 1, "/x").to_dict())

    def test_flat_layout_unaffected(self):
        flat = self.tmp / "flat"
        flat.mkdir()
        for v in ("v001", "v002"):
            for f in (1001, 1002):
                (flat / f"plate_{v}.{f}.exr").write_bytes(b"x")
        source = WatchedSource(name="plate", source_dir=str(flat),
                               version_pattern="_v{version}", file_extensions=[".exr"])
        first = VersionScanner(source).scan()
        (flat / "plate_v002.1003.exr").write_bytes(b"x")
        second = VersionScanner(source).scan(previous=first)
        self.assertEqual(second[-1].file_count, 3)


if __name__ == "__main__":
    unittest.main()
