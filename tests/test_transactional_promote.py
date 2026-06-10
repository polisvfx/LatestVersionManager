"""
Tests for transactional (staged) promotion, the promote lock, and the
disk-space preflight.

The contract under test: a failed or cancelled promotion must NEVER leave
the latest_target directory broken — the previously promoted version stays
live until the new one is fully staged and swapped in. Concurrent
promotions to the same target are serialized by a lease-style lock file.

Self-contained: temp directories, no external data. Portable (no Qt).
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.models import VersionInfo, WatchedSource
from lvm.promoter import (
    Promoter, PromotionError, STAGING_DIRNAME, BACKUP_DIRNAME,
)
from lvm.lockfile import PromoteLock, LockHeldError, LOCK_FILENAME
from lvm.scanner import _group_files_by_sequence
from lvm.fast_copy import smart_copy as real_smart_copy


def _make_exr_sequence(folder, basename, version, start=1001, end=1010):
    """Create a fake EXR frame sequence in folder.

    File contents are stamped with the version so tests can verify WHICH
    version's bytes ended up in the target — target filenames are
    version-stripped by default, so names alone can't tell versions apart.
    """
    Path(folder).mkdir(parents=True, exist_ok=True)
    files = []
    for frame in range(start, end + 1):
        f = Path(folder) / f"{basename}_{version}.{frame:04d}.exr"
        f.write_bytes(f"{basename}_{version}.{frame:04d}".encode())
        files.append(f)
    return files


def _can_symlink(tmpdir) -> bool:
    """Probe whether this process may create symlinks (Windows: admin/dev mode)."""
    target = Path(tmpdir) / "_probe_target.txt"
    link = Path(tmpdir) / "_probe_link"
    try:
        target.write_text("x")
        link.symlink_to(target)
        link.unlink()
        target.unlink()
        return True
    except OSError:
        return False


def _spawn_dead_pid() -> int:
    """Return the PID of a process that has already exited."""
    proc = subprocess.run(
        [sys.executable, "-c", "import os; print(os.getpid())"],
        capture_output=True, text=True, check=True,
    )
    return int(proc.stdout.strip())


class _FlakySmartCopy:
    """smart_copy stand-in that raises on the Nth call (thread-safe)."""

    def __init__(self, fail_on_call: int):
        self.fail_on_call = fail_on_call
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self, src, dst, *args, **kwargs):
        with self._lock:
            self.calls += 1
            n = self.calls
        if n == self.fail_on_call:
            raise OSError("simulated copy failure (network drop)")
        return real_smart_copy(src, dst, *args, **kwargs)


class _PromoterTestBase(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_txn_")
        self.source_dir = str(Path(self.tmpdir) / "renders")
        self.target_dir = str(Path(self.tmpdir) / "online")
        Path(self.source_dir).mkdir()
        Path(self.target_dir).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_source(self, **kwargs):
        defaults = {
            "name": "TestSource",
            "source_dir": self.source_dir,
            "latest_target": self.target_dir,
            "version_pattern": "_v{version}",
            "file_extensions": [".exr"],
            "link_mode": "copy",
            "override_latest_target": True,
        }
        defaults.update(kwargs)
        return WatchedSource(**defaults)

    def _make_version(self, version="v001", num=1, count=5, basename="shot"):
        vdir = Path(self.source_dir) / f"{basename}_{version}"
        _make_exr_sequence(str(vdir), basename, version, 1001, 1000 + count)
        return VersionInfo(version, num, str(vdir), file_count=count)

    def _target_media_names(self):
        return sorted(
            p.name for p in Path(self.target_dir).iterdir()
            if p.is_file() and not p.name.startswith(".")
        )

    def assert_target_content_is(self, version: str):
        """Every media file in the target carries *version*'s bytes."""
        names = self._target_media_names()
        self.assertTrue(names, "target has no media files")
        for n in names:
            content = (Path(self.target_dir) / n).read_bytes()
            self.assertIn(version.encode(), content,
                          f"{n} does not contain {version} content")

    def assert_no_promotion_internals(self):
        t = Path(self.target_dir)
        self.assertFalse((t / STAGING_DIRNAME).exists(), "staging dir left behind")
        self.assertFalse((t / BACKUP_DIRNAME).exists(), "backup dir left behind")
        self.assertFalse((t / LOCK_FILENAME).exists(), "lock file left behind")


# ============================================================================
# Failure / cancellation safety
# ============================================================================

class TestStagedPromotionSafety(_PromoterTestBase):

    def test_mid_copy_failure_preserves_previous_version(self):
        """Sequential copy path (<=10 files): failure mid-copy leaves target intact."""
        vi1 = self._make_version("v001", 1, count=5)
        vi2 = self._make_version("v002", 2, count=5)
        promoter = Promoter(self._make_source())
        promoter.promote(vi1, user="x")
        before = self._target_media_names()

        with patch("lvm.promoter.smart_copy", new=_FlakySmartCopy(fail_on_call=3)):
            with self.assertRaises(PromotionError):
                promoter.promote(vi2, user="x")

        self.assertEqual(self._target_media_names(), before)
        self.assert_target_content_is("v001")
        self.assert_no_promotion_internals()
        self.assertEqual(promoter.get_current_version().version, "v001")

    def test_parallel_copy_failure_preserves_target(self):
        """Parallel copy path (>10 files): failure mid-copy leaves target intact."""
        vi1 = self._make_version("v001", 1, count=12)
        vi2 = self._make_version("v002", 2, count=12)
        promoter = Promoter(self._make_source())
        promoter.promote(vi1, user="x")
        before = self._target_media_names()

        with patch("lvm.promoter.smart_copy", new=_FlakySmartCopy(fail_on_call=6)):
            with self.assertRaises(PromotionError):
                promoter.promote(vi2, user="x")

        self.assertEqual(self._target_media_names(), before)
        self.assert_target_content_is("v001")
        self.assert_no_promotion_internals()
        self.assertEqual(promoter.get_current_version().version, "v001")

    def test_failure_on_empty_target_leaves_target_empty(self):
        """First-ever promote failing must not leave partial files."""
        vi1 = self._make_version("v001", 1, count=5)
        promoter = Promoter(self._make_source())

        with patch("lvm.promoter.smart_copy", new=_FlakySmartCopy(fail_on_call=3)):
            with self.assertRaises(PromotionError):
                promoter.promote(vi1, user="x")

        self.assertEqual(self._target_media_names(), [])
        self.assert_no_promotion_internals()

    def test_cancel_during_copy_leaves_target_unchanged(self):
        vi1 = self._make_version("v001", 1, count=5)
        vi2 = self._make_version("v002", 2, count=5)
        promoter = Promoter(self._make_source())
        promoter.promote(vi1, user="x")
        before = self._target_media_names()

        def cancel_after_two(current, total, name):
            if current >= 2:
                promoter.cancel()

        with self.assertRaises(PromotionError) as cm:
            promoter.promote(vi2, user="x", progress_callback=cancel_after_two)

        self.assertIn("cancelled", str(cm.exception).lower())
        self.assertEqual(self._target_media_names(), before)
        self.assert_target_content_is("v001")
        self.assert_no_promotion_internals()
        self.assertEqual(promoter.get_current_version().version, "v001")

    def test_single_file_failure_keeps_old_file(self):
        for v in ("v001", "v002"):
            (Path(self.source_dir) / f"clip_{v}.mov").write_bytes(v.encode())
        source = self._make_source(file_extensions=[".mov"])
        promoter = Promoter(source)
        vi1 = VersionInfo("v001", 1, str(Path(self.source_dir) / "clip_v001.mov"), file_count=1)
        vi2 = VersionInfo("v002", 2, str(Path(self.source_dir) / "clip_v002.mov"), file_count=1)
        promoter.promote(vi1, user="x")
        before = self._target_media_names()

        with patch("lvm.promoter.smart_copy", side_effect=OSError("boom")):
            with self.assertRaises(PromotionError):
                promoter.promote(vi2, user="x")

        self.assertEqual(self._target_media_names(), before)
        # Old content untouched
        old = Path(self.target_dir) / before[0]
        self.assertEqual(old.read_bytes(), b"v001")
        self.assert_no_promotion_internals()

    def test_swap_failure_restores_backup(self):
        """A rename failure during the swap phase restores the old version."""
        vi1 = self._make_version("v001", 1, count=5)
        vi2 = self._make_version("v002", 2, count=5)
        promoter = Promoter(self._make_source())
        promoter.promote(vi1, user="x")
        before = self._target_media_names()

        real_replace = os.replace
        calls = {"n": 0}

        def flaky_replace(src, dst, *args, **kwargs):
            calls["n"] += 1
            # Calls 1-5 park the old files in the backup dir; call 6 is the
            # first staged file moving into the target — fail there.
            if calls["n"] == 6:
                raise OSError("simulated rename failure")
            return real_replace(src, dst, *args, **kwargs)

        with patch("os.replace", new=flaky_replace):
            with self.assertRaises(PromotionError) as cm:
                promoter.promote(vi2, user="x")

        self.assertIn("previous version restored", str(cm.exception))
        self.assertEqual(self._target_media_names(), before)
        self.assert_target_content_is("v001")
        self.assert_no_promotion_internals()
        self.assertEqual(promoter.get_current_version().version, "v001")

    def test_promote_success_leaves_no_internals(self):
        vi1 = self._make_version("v001", 1, count=5)
        vi2 = self._make_version("v002", 2, count=5)
        promoter = Promoter(self._make_source())
        promoter.promote(vi1, user="x")
        promoter.promote(vi2, user="x")

        self.assertEqual(len(self._target_media_names()), 5)
        self.assert_target_content_is("v002")
        self.assert_no_promotion_internals()
        self.assertEqual(promoter.get_current_version().version, "v002")

    def test_undo_inherits_transaction(self):
        """undo() funnels through promote() and gets the same safety."""
        vi1 = self._make_version("v001", 1, count=5)
        vi2 = self._make_version("v002", 2, count=5)
        promoter = Promoter(self._make_source())
        promoter.promote(vi1, user="x")
        promoter.promote(vi2, user="x")
        before = self._target_media_names()

        with patch("lvm.promoter.smart_copy", new=_FlakySmartCopy(fail_on_call=3)):
            with self.assertRaises(PromotionError):
                promoter.undo(1, user="x")

        self.assertEqual(self._target_media_names(), before)
        self.assert_target_content_is("v002")
        self.assert_no_promotion_internals()
        self.assertEqual(promoter.get_current_version().version, "v002")


# ============================================================================
# keep_layers interplay
# ============================================================================

class TestKeepLayersStaged(_PromoterTestBase):

    def _promote_two_layer_v001(self, promoter):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "beauty", "v001", 1001, 1003)
        _make_exr_sequence(str(vdir), "matte", "v001", 1001, 1003)
        vi1 = VersionInfo("v001", 1, str(vdir), file_count=6)
        promoter.promote(vi1, user="x")

    def _matte_prefix(self):
        """Sequence-group prefix of the matte layer currently in the target."""
        media = sorted(
            f for f in Path(self.target_dir).iterdir()
            if f.is_file() and f.suffix == ".exr"
        )
        groups = _group_files_by_sequence(media)
        for prefix in groups:
            if "matte" in prefix:
                return prefix
        raise AssertionError("matte layer not found in target")

    def test_keep_layers_survive_success(self):
        promoter = Promoter(self._make_source())
        self._promote_two_layer_v001(promoter)
        keep = {self._matte_prefix()}

        vdir2 = Path(self.source_dir) / "shot_v002"
        _make_exr_sequence(str(vdir2), "beauty", "v002", 1001, 1003)
        vi2 = VersionInfo("v002", 2, str(vdir2), file_count=3)
        promoter.promote(vi2, user="x", keep_layers=keep)

        names = self._target_media_names()
        self.assertEqual(len(names), 6)
        beauty = [n for n in names if n.startswith("beauty")]
        matte = [n for n in names if n.startswith("matte")]
        self.assertEqual(len(beauty), 3)
        self.assertEqual(len(matte), 3)
        for n in beauty:  # replaced by the new version
            self.assertIn(b"v002", (Path(self.target_dir) / n).read_bytes())
        for n in matte:  # kept layer retains the old version's bytes
            self.assertIn(b"v001", (Path(self.target_dir) / n).read_bytes())
        self.assert_no_promotion_internals()

    def test_keep_layers_survive_failure(self):
        promoter = Promoter(self._make_source())
        self._promote_two_layer_v001(promoter)
        before = self._target_media_names()
        keep = {self._matte_prefix()}

        vdir2 = Path(self.source_dir) / "shot_v002"
        _make_exr_sequence(str(vdir2), "beauty", "v002", 1001, 1003)
        vi2 = VersionInfo("v002", 2, str(vdir2), file_count=3)

        with patch("lvm.promoter.smart_copy", new=_FlakySmartCopy(fail_on_call=2)):
            with self.assertRaises(PromotionError):
                promoter.promote(vi2, user="x", keep_layers=keep)

        self.assertEqual(self._target_media_names(), before)
        self.assert_no_promotion_internals()


# ============================================================================
# Shared target directory isolation (single-file sources)
# ============================================================================

class TestSharedTargetIsolation(_PromoterTestBase):

    def _make_shared_sources(self):
        for shot in ("ShotA", "ShotB"):
            (Path(self.source_dir) / f"{shot}_v001.mov").write_bytes(shot.encode())
        srcA = self._make_source(
            name="ShotA", file_extensions=[".mov"],
            sample_filename="ShotA_v001.mov",
            file_rename_template="{source_name}_latest",
            history_filename=".latest_history_ShotA.json",
        )
        srcB = self._make_source(
            name="ShotB", file_extensions=[".mov"],
            sample_filename="ShotB_v001.mov",
            file_rename_template="{source_name}_latest",
            history_filename=".latest_history_ShotB.json",
        )
        return srcA, srcB

    def test_shared_target_single_file_sources_isolated(self):
        srcA, srcB = self._make_shared_sources()
        promA, promB = Promoter(srcA), Promoter(srcB)
        viA = VersionInfo("v001", 1, str(Path(self.source_dir) / "ShotA_v001.mov"), file_count=1)
        viB = VersionInfo("v001", 1, str(Path(self.source_dir) / "ShotB_v001.mov"), file_count=1)
        promA.promote(viA, user="x")
        promB.promote(viB, user="x")
        self.assertEqual(len(self._target_media_names()), 2)

        # Failing re-promote of A must not touch B's file
        (Path(self.source_dir) / "ShotA_v002.mov").write_bytes(b"ShotA2")
        viA2 = VersionInfo("v002", 2, str(Path(self.source_dir) / "ShotA_v002.mov"), file_count=1)
        before = self._target_media_names()
        with patch("lvm.promoter.smart_copy", side_effect=OSError("boom")):
            with self.assertRaises(PromotionError):
                promA.promote(viA2, user="x")
        self.assertEqual(self._target_media_names(), before)

        # Successful re-promote of A replaces only A's file
        promA.promote(viA2, user="x")
        names = self._target_media_names()
        self.assertEqual(len(names), 2)
        b_files = [n for n in names if "ShotB" in n]
        self.assertEqual(len(b_files), 1)
        self.assertEqual(
            (Path(self.target_dir) / b_files[0]).read_bytes(), b"ShotB"
        )
        self.assert_no_promotion_internals()


# ============================================================================
# Link modes through the staged path
# ============================================================================

class TestLinkModesStaged(_PromoterTestBase):

    def test_hardlink_mode_staged(self):
        vi1 = self._make_version("v001", 1, count=3)
        promoter = Promoter(self._make_source(link_mode="hardlink"))
        try:
            promoter.promote(vi1, user="x")
        except PromotionError as e:
            if "Hardlink creation failed" in str(e):
                self.skipTest(f"hardlinks not supported here: {e}")
            raise
        self.assertEqual(len(self._target_media_names()), 3)
        self.assert_no_promotion_internals()

    def test_symlink_mode_staged(self):
        if not _can_symlink(self.tmpdir):
            self.skipTest("symlinks not permitted for this process")
        vi1 = self._make_version("v001", 1, count=3)
        promoter = Promoter(self._make_source(link_mode="symlink"))
        promoter.promote(vi1, user="x")
        targets = [
            Path(self.target_dir) / n for n in self._target_media_names()
        ]
        self.assertEqual(len(targets), 3)
        self.assertTrue(all(p.is_symlink() for p in targets))
        self.assert_no_promotion_internals()


# ============================================================================
# Orphan recovery
# ============================================================================

class TestOrphanRecovery(_PromoterTestBase):

    def test_orphaned_backup_restored_directly(self):
        """A torn swap's backup files are moved back when missing from target."""
        vi1 = self._make_version("v001", 1, count=3)
        promoter = Promoter(self._make_source())
        promoter.promote(vi1, user="x")

        # Simulate a crash mid-swap: one file parked in backup, gone from target
        backup = Path(self.target_dir) / BACKUP_DIRNAME
        backup.mkdir()
        victim = Path(self.target_dir) / self._target_media_names()[0]
        os.replace(victim, backup / victim.name)
        # And a half-written staging dir
        staging = Path(self.target_dir) / STAGING_DIRNAME
        staging.mkdir()
        (staging / "partial.exr").write_bytes(b"\x00")

        promoter._cleanup_orphaned_promotion_dirs(Path(self.target_dir))

        self.assertTrue(victim.exists(), "backup file was not restored")
        self.assertFalse(backup.exists())
        self.assertFalse(staging.exists())

    def test_orphaned_dirs_cleaned_on_next_promote(self):
        vi1 = self._make_version("v001", 1, count=3)
        vi2 = self._make_version("v002", 2, count=3)
        promoter = Promoter(self._make_source())
        promoter.promote(vi1, user="x")

        # Manufacture leftovers from a hypothetical crash
        backup = Path(self.target_dir) / BACKUP_DIRNAME
        backup.mkdir()
        (backup / "stray_v000.exr").write_bytes(b"\x00")
        staging = Path(self.target_dir) / STAGING_DIRNAME
        staging.mkdir()
        (staging / "partial.exr").write_bytes(b"\x00")

        promoter.promote(vi2, user="x")

        names = self._target_media_names()
        self.assertEqual(len(names), 3)
        self.assertNotIn("stray_v000.exr", names)
        self.assert_target_content_is("v002")
        self.assert_no_promotion_internals()


# ============================================================================
# Disk-space preflight
# ============================================================================

class TestDiskPreflight(_PromoterTestBase):

    def test_disk_preflight_blocks_copy_mode(self):
        vi1 = self._make_version("v001", 1, count=3)
        vi1.total_size_bytes = 10 * 1024 ** 3  # pretend 10 GB
        promoter = Promoter(self._make_source(link_mode="copy"))

        with patch("shutil.disk_usage",
                   return_value=SimpleNamespace(free=1 * 1024 ** 3)):
            with self.assertRaises(PromotionError) as cm:
                promoter.promote(vi1, user="x")

        self.assertIn("free space", str(cm.exception))
        self.assertEqual(self._target_media_names(), [])
        self.assert_no_promotion_internals()

    def test_preflight_skipped_for_link_modes(self):
        vi1 = self._make_version("v001", 1, count=3)
        vi1.total_size_bytes = 10 * 1024 ** 3
        promoter = Promoter(self._make_source(link_mode="hardlink"))
        try:
            with patch("shutil.disk_usage",
                       return_value=SimpleNamespace(free=1 * 1024 ** 3)):
                promoter.promote(vi1, user="x")
        except PromotionError as e:
            if "Hardlink creation failed" in str(e):
                self.skipTest(f"hardlinks not supported here: {e}")
            raise
        self.assertEqual(len(self._target_media_names()), 3)

    def test_preflight_skipped_for_unknown_size(self):
        vi1 = self._make_version("v001", 1, count=3)  # total_size_bytes defaults to 0
        promoter = Promoter(self._make_source(link_mode="copy"))
        with patch("shutil.disk_usage",
                   return_value=SimpleNamespace(free=1)):
            promoter.promote(vi1, user="x")
        self.assertEqual(len(self._target_media_names()), 3)


# ============================================================================
# Promote lock
# ============================================================================

class TestPromoteLockUnit(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_lock_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_acquire_release(self):
        lock = PromoteLock(self.tmpdir)
        lock.acquire()
        self.assertTrue(lock.path.exists())
        info = json.loads(lock.path.read_text())
        self.assertEqual(info["pid"], os.getpid())
        self.assertEqual(info["host"], socket.gethostname())
        lock.release()
        self.assertFalse(lock.path.exists())
        lock.release()  # double release is safe

    def test_second_acquire_blocked(self):
        lock1 = PromoteLock(self.tmpdir)
        lock1.acquire()
        try:
            lock2 = PromoteLock(self.tmpdir)
            with self.assertRaises(LockHeldError) as cm:
                lock2.acquire()
            self.assertEqual(cm.exception.info.get("pid"), os.getpid())
        finally:
            lock1.release()

    def test_context_manager(self):
        with PromoteLock(self.tmpdir) as lock:
            self.assertTrue(lock.path.exists())
        self.assertFalse(lock.path.exists())

    def test_heartbeat_refreshes_mtime(self):
        lock = PromoteLock(self.tmpdir, heartbeat_interval=0.05)
        lock.acquire()
        try:
            # Backdate, then wait for the heartbeat to refresh it
            past = time.time() - 100
            os.utime(lock.path, (past, past))
            deadline = time.time() + 2.0
            while time.time() < deadline:
                if lock.path.stat().st_mtime > past + 50:
                    break
                time.sleep(0.05)
            self.assertGreater(lock.path.stat().st_mtime, past + 50)
        finally:
            lock.release()

    def test_stale_mtime_taken_over(self):
        lock_path = Path(self.tmpdir) / LOCK_FILENAME
        lock_path.write_text(json.dumps(
            {"pid": 999999, "host": "some-other-host", "user": "ghost",
             "started_at": "2026-01-01T00:00:00"}
        ))
        past = time.time() - 3600
        os.utime(lock_path, (past, past))

        lock = PromoteLock(self.tmpdir, stale_after=300)
        lock.acquire()  # must take over, not raise
        try:
            info = json.loads(lock.path.read_text())
            self.assertEqual(info["pid"], os.getpid())
        finally:
            lock.release()

    def test_fresh_foreign_lock_not_taken_over(self):
        lock_path = Path(self.tmpdir) / LOCK_FILENAME
        lock_path.write_text(json.dumps(
            {"pid": 999999, "host": "some-other-host", "user": "artist2",
             "started_at": "2026-01-01T00:00:00"}
        ))
        lock = PromoteLock(self.tmpdir, stale_after=300)
        with self.assertRaises(LockHeldError):
            lock.acquire()

    def test_dead_pid_same_host_taken_over(self):
        dead_pid = _spawn_dead_pid()
        lock_path = Path(self.tmpdir) / LOCK_FILENAME
        lock_path.write_text(json.dumps(
            {"pid": dead_pid, "host": socket.gethostname(), "user": "crashed",
             "started_at": "2026-01-01T00:00:00"}
        ))
        lock = PromoteLock(self.tmpdir, stale_after=3600)
        lock.acquire()  # dead PID -> immediate takeover despite fresh mtime
        lock.release()

    def test_corrupt_lock_file_respects_mtime(self):
        lock_path = Path(self.tmpdir) / LOCK_FILENAME
        lock_path.write_text("not json{{{")
        # Fresh corrupt lock: held (can't prove staleness)
        lock = PromoteLock(self.tmpdir, stale_after=300)
        with self.assertRaises(LockHeldError):
            lock.acquire()
        # Old corrupt lock: stale via mtime
        past = time.time() - 3600
        os.utime(lock_path, (past, past))
        lock.acquire()
        lock.release()


class TestPromoteLockIntegration(_PromoterTestBase):

    def test_lock_blocks_concurrent_promotion(self):
        vi1 = self._make_version("v001", 1, count=3)
        promoter = Promoter(self._make_source())

        holder = PromoteLock(self.target_dir)
        holder.acquire()
        try:
            with self.assertRaises(PromotionError) as cm:
                promoter.promote(vi1, user="x")
            self.assertIn("Another promotion", str(cm.exception))
        finally:
            holder.release()

        # Lock released — promotion now succeeds
        promoter.promote(vi1, user="x")
        self.assertEqual(len(self._target_media_names()), 3)
        self.assert_no_promotion_internals()

    def test_dead_pid_lock_taken_over_by_promote(self):
        vi1 = self._make_version("v001", 1, count=3)
        promoter = Promoter(self._make_source())

        dead_pid = _spawn_dead_pid()
        lock_path = Path(self.target_dir) / LOCK_FILENAME
        lock_path.write_text(json.dumps(
            {"pid": dead_pid, "host": socket.gethostname(), "user": "crashed",
             "started_at": "2026-01-01T00:00:00"}
        ))

        promoter.promote(vi1, user="x")
        self.assertEqual(len(self._target_media_names()), 3)
        self.assert_no_promotion_internals()

    def test_lock_released_after_failed_promotion(self):
        vi1 = self._make_version("v001", 1, count=5)
        promoter = Promoter(self._make_source())
        with patch("lvm.promoter.smart_copy", new=_FlakySmartCopy(fail_on_call=2)):
            with self.assertRaises(PromotionError):
                promoter.promote(vi1, user="x")
        self.assertFalse((Path(self.target_dir) / LOCK_FILENAME).exists())


if __name__ == "__main__":
    unittest.main()
