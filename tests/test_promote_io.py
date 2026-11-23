"""
Filesystem-access tests for the promote path (plan items #12 and #13).

Over SMB every stat/rename is a network round-trip, so these tests count
calls rather than time them: the number of per-file Path checks must not
grow with the number of frames, and the swap must keep the target complete
(old or new) at every step.

Self-contained: temp directories. Portable (no Qt).
"""

import os
import pathlib
import shutil
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.models import VersionInfo, WatchedSource
from lvm.promoter import BACKUP_DIRNAME, STAGING_DIRNAME, Promoter, PromotionError


class _Base(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_pio_"))
        self.renders = self.tmp / "renders"
        self.target = self.tmp / "online"

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _source(self, **kw):
        defaults = dict(name="shot", source_dir=str(self.renders),
                        latest_target=str(self.target),
                        version_pattern="_v{version}", file_extensions=[".exr"],
                        link_mode="copy")
        defaults.update(kw)
        return WatchedSource(**defaults)

    def _render(self, version, frames):
        vdir = self.renders / f"shot_{version}"
        vdir.mkdir(parents=True, exist_ok=True)
        for f in frames:
            (vdir / f"shot_{version}.{f}.exr").write_bytes(version.encode() * 8)
        return VersionInfo(version, int(version[1:]), str(vdir), file_count=len(frames),
                           total_size_bytes=len(frames) * 32)

    def _target_files(self):
        return sorted(p.name for p in self.target.iterdir()
                      if p.is_file() and not p.name.startswith("."))


class TestPerFileCalls(_Base):

    def _count_path_calls(self, frames):
        promoter = Promoter(self._source())
        promoter.promote(self._render("v001", frames), user="t")
        v2 = self._render("v002", frames)
        counts = {"n": 0}
        lock = threading.Lock()
        originals = {name: getattr(pathlib.Path, name)
                     for name in ("is_file", "exists", "is_symlink", "stat")}

        def counting(name):
            real = originals[name]

            def wrapper(self, *a, **k):
                with lock:
                    counts["n"] += 1
                return real(self, *a, **k)
            return wrapper

        with patch.multiple(pathlib.Path, **{n: counting(n) for n in originals}):
            promoter.promote(v2, user="t")
        return counts["n"]

    def test_path_checks_do_not_scale_with_frame_count(self):
        """Path.is_file/exists/is_symlink/stat used to run once or twice per
        frame (listing, staging existence checks, target mtime). Listings
        now come from os.scandir, so the count is the same for 10 and 60
        frames."""
        small = self._count_path_calls(range(1001, 1011))
        shutil.rmtree(self.tmp)
        self.tmp.mkdir()
        large = self._count_path_calls(range(1001, 1061))
        self.assertEqual(small, large, f"{small} Path calls for 10 frames, {large} for 60")

    def test_dry_run_sizes_from_listing(self):
        promoter = Promoter(self._source())
        v1 = self._render("v001", range(1001, 1004))
        result = promoter.dry_run(v1)
        self.assertEqual(result["total_files"], 3)
        self.assertEqual(result["total_size_bytes"], 3 * 32)
        self.assertEqual([m["target_name"] for m in result["file_map"]],
                         ["shot.1001.exr", "shot.1002.exr", "shot.1003.exr"])


if __name__ == "__main__":
    unittest.main()
