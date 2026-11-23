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


class TestSwap(_Base):
    """The swap phase (plan item #13)."""

    def _expected_names(self, frames):
        return {f"shot.{f}.exr" for f in frames}

    def _observe_swap(self, promoter, version, frames):
        """Promote *version*, recording after every rename into or out of
        the target how many expected frame names were missing."""
        import lvm.promoter as pm
        expected = self._expected_names(frames)
        real_replace = os.replace
        missing_seen = []
        threads = set()
        lock = threading.Lock()

        def watching_replace(src, dst, *a, **k):
            result = real_replace(src, dst, *a, **k)
            parent = Path(dst).parent
            if parent in (self.target, self.target / BACKUP_DIRNAME):
                present = {p.name for p in self.target.iterdir()}
                with lock:
                    missing_seen.append(len(expected - present))
                    threads.add(threading.get_ident())
            return result

        with patch.object(pm.os, "replace", watching_replace):
            promoter.promote(version, user="t")
        return missing_seen, threads

    def test_each_frame_missing_for_at_most_one_rename(self):
        """The old swap moved *all* old frames out before placing any new
        one, so the whole sequence was missing for the duration of N
        renames. Now each name is swapped as a unit."""
        import lvm.promoter as pm
        frames = range(1001, 1021)
        promoter = Promoter(self._source())
        promoter.promote(self._render("v001", frames), user="t")
        with patch.object(pm, "_SWAP_PARALLEL_MIN", 10**6):   # sequential, deterministic
            missing, _ = self._observe_swap(promoter, self._render("v002", frames), frames)
        self.assertTrue(missing)
        self.assertLessEqual(max(missing), 1, f"up to {max(missing)} frames missing at once")
        self.assertEqual(self._target_files(), sorted(self._expected_names(frames)))

    def test_large_swaps_run_in_parallel(self):
        frames = range(1001, 1041)
        promoter = Promoter(self._source())
        promoter.promote(self._render("v001", frames), user="t")
        missing, threads = self._observe_swap(promoter, self._render("v002", frames), frames)
        self.assertGreater(len(threads), 1, "swap renames ran on a single thread")
        self.assertLessEqual(max(missing), 8, "more frames missing than rename workers")
        for name in self._target_files():
            self.assertIn(b"v002", (self.target / name).read_bytes())

    def test_old_frames_outside_new_range_are_retired(self):
        promoter = Promoter(self._source())
        promoter.promote(self._render("v001", range(1001, 1011)), user="t")
        promoter.promote(self._render("v002", range(1001, 1006)), user="t")
        self.assertEqual(self._target_files(), sorted(self._expected_names(range(1001, 1006))))
        self.assertFalse((self.target / BACKUP_DIRNAME).exists())
        self.assertFalse((self.target / STAGING_DIRNAME).exists())

    def test_parallel_failure_rolls_back_completely(self):
        import lvm.promoter as pm
        frames = range(1001, 1041)
        promoter = Promoter(self._source())
        promoter.promote(self._render("v001", frames), user="t")
        before = self._target_files()
        real_replace = os.replace
        calls = {"n": 0}
        lock = threading.Lock()

        def flaky_replace(src, dst, *a, **k):
            with lock:
                calls["n"] += 1
                n = calls["n"]
            if n == 30:
                raise OSError("simulated rename failure")
            return real_replace(src, dst, *a, **k)

        with patch.object(pm.os, "replace", flaky_replace):
            with self.assertRaises(PromotionError) as cm:
                promoter.promote(self._render("v002", frames), user="t")
        self.assertIn("previous version restored", str(cm.exception))
        self.assertEqual(self._target_files(), before)
        for name in before:
            self.assertIn(b"v001", (self.target / name).read_bytes())
        self.assertFalse((self.target / BACKUP_DIRNAME).exists())
        self.assertFalse((self.target / STAGING_DIRNAME).exists())

    def test_overwritten_kept_layer_is_restored_on_rollback(self):
        """A staged file can replace an existing file that isn't in the
        removal set (here: a kept layer the new version also provides).
        The old swap overwrote it without a backup, so a rollback lost it."""
        import lvm.promoter as pm
        frames = range(1001, 1004)
        promoter = Promoter(self._source())
        v1 = self._render("v001", frames)
        for f in frames:
            (Path(v1.source_path) / f"shot_alpha_v001.{f}.exr").write_bytes(b"alpha-v001")
        promoter.promote(v1, user="t")
        v2 = self._render("v002", frames)
        for f in frames:
            (Path(v2.source_path) / f"shot_alpha_v002.{f}.exr").write_bytes(b"alpha-v002")

        real_replace = os.replace

        def fail_last_placement(src, dst, *a, **k):
            if Path(dst).name == "shot_alpha.1003.exr" and Path(src).parent.name == STAGING_DIRNAME:
                raise OSError("simulated rename failure")
            return real_replace(src, dst, *a, **k)

        with patch.object(pm, "_SWAP_PARALLEL_MIN", 10**6), \
                patch.object(pm.os, "replace", fail_last_placement):
            with self.assertRaises(PromotionError):
                promoter.promote(v2, user="t", keep_layers={"shot_alpha."})
        for f in frames:
            self.assertEqual((self.target / f"shot_alpha.{f}.exr").read_bytes(), b"alpha-v001")
            self.assertIn(b"v001", (self.target / f"shot.{f}.exr").read_bytes())

    def test_swap_time_is_logged(self):
        promoter = Promoter(self._source())
        with self.assertLogs("lvm.promoter", level="INFO") as logs:
            promoter.promote(self._render("v001", range(1001, 1006)), user="t")
        self.assertIsNotNone(promoter.last_swap_seconds)
        text = "\n".join(logs.output)
        self.assertIn("Swapped 5 file(s)", text)
        self.assertIn(", swap ", text)


if __name__ == "__main__":
    unittest.main()
