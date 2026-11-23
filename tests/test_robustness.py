"""
Robustness tests: atomic config writes, history sidecar failure modes,
bounded timecode probing, and parallel-copy progress.

Self-contained: temp directories, no external data. Portable (no Qt).
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm import timecode
from lvm.config import load_config, save_config, write_config_snapshot
from lvm.fileutil import atomic_write_json
from lvm.history import HistoryManager
from lvm.models import HistoryEntry, ProjectConfig, VersionInfo, WatchedSource
from lvm.promoter import Promoter, PromotionError


class _TmpDirTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_robust_"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


# ============================================================================
# Atomic JSON / config writes
# ============================================================================

class TestAtomicWrites(_TmpDirTest):

    def test_failed_write_keeps_previous_file(self):
        path = self.tmp / "project.json"
        atomic_write_json(path, {"v": 1})
        with self.assertRaises(TypeError):
            atomic_write_json(path, {"v": object()})  # not serialisable
        self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"v": 1})
        leftovers = [p.name for p in self.tmp.iterdir() if p.name != "project.json"]
        self.assertEqual(leftovers, [], "temp file left behind")

    def test_save_config_does_not_mutate_live_manual_versions(self):
        """save_config relativises paths in the snapshot only — the live
        config must keep absolute manual-version paths."""
        media = self.tmp / "media" / "plate_v001.mov"
        media.parent.mkdir()
        media.write_bytes(b"x")
        src = WatchedSource(name="S", source_dir=str(self.tmp / "media"))
        src.manual_versions = [{"version_string": "v001", "version_number": 1,
                                "source_path": str(media)}]
        cfg = ProjectConfig(project_name="P", watched_sources=[src])
        cfg_path = self.tmp / "proj" / "p.json"

        save_config(cfg, str(cfg_path))

        self.assertEqual(src.manual_versions[0]["source_path"], str(media))
        on_disk = json.loads(cfg_path.read_text(encoding="utf-8"))
        stored = on_disk["watched_sources"][0]["manual_versions"][0]["source_path"]
        self.assertFalse(Path(stored).is_absolute(), stored)
        reloaded = load_config(str(cfg_path))
        self.assertEqual(
            Path(reloaded.watched_sources[0].manual_versions[0]["source_path"]),
            media.resolve(),
        )

    def test_write_config_snapshot_returns_project_dir(self):
        cfg = ProjectConfig(project_name="P")
        cfg_path = self.tmp / "p.json"
        project_dir = write_config_snapshot(cfg.to_dict(), str(cfg_path))
        self.assertEqual(Path(project_dir), cfg_path.resolve().parent)
        self.assertTrue(cfg_path.exists())


# ============================================================================
# History sidecar failure modes
# ============================================================================

def _entry(version="v001"):
    return HistoryEntry(version=version, source="/src", set_by="t",
                        set_at="2026-01-01T00:00:00", file_count=1)


class TestHistoryRobustness(_TmpDirTest):

    def test_second_corruption_does_not_crash(self):
        """A leftover .json.bak used to make Path.rename raise on Windows."""
        path = self.tmp / ".latest_history.json"
        hm = HistoryManager(str(path))
        for _ in range(2):
            path.write_text("{ not json", encoding="utf-8")
            data = hm.load()
            self.assertIsNone(data["current"])
            self.assertFalse(path.exists())
            self.assertTrue(path.with_suffix(".json.bak").exists())

    def test_non_object_json_treated_as_corrupt(self):
        path = self.tmp / ".latest_history.json"
        path.write_text("[1, 2, 3]", encoding="utf-8")
        data = HistoryManager(str(path)).load()
        self.assertEqual(data["history"], [])
        self.assertTrue(path.with_suffix(".json.bak").exists())

    def test_unreadable_history_raises_instead_of_reporting_empty(self):
        path = self.tmp / ".latest_history.json"
        hm = HistoryManager(str(path))
        hm.record_promotion(_entry())
        hm._cache = None
        with patch("builtins.open", side_effect=PermissionError("denied")):
            with self.assertRaises(OSError):
                hm.load()
        self.assertTrue(path.exists(), "unreadable history must not be moved away")

    def test_save_leaves_no_temp_files(self):
        path = self.tmp / ".latest_history.json"
        hm = HistoryManager(str(path))
        hm.record_promotion(_entry("v001"))
        hm.record_promotion(_entry("v002"))
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir()),
                         [".latest_history.json"])
        self.assertEqual(hm.get_current().version, "v002")


class TestPromoteWithUnreadableHistory(_TmpDirTest):

    def test_promote_aborts_before_touching_target(self):
        src_dir = self.tmp / "renders"
        target = self.tmp / "online"
        vdir = src_dir / "shot_v001"
        vdir.mkdir(parents=True)
        for f in range(1001, 1004):
            (vdir / f"shot_v001.{f}.exr").write_bytes(b"x")
        target.mkdir()
        existing = target / "shot.1001.exr"
        existing.write_bytes(b"old")

        source = WatchedSource(name="S", source_dir=str(src_dir),
                               latest_target=str(target),
                               file_extensions=[".exr"])
        promoter = Promoter(source)
        version = VersionInfo("v001", 1, str(vdir), file_count=3)
        with patch.object(promoter.history, "load",
                          side_effect=PermissionError("share dropped")):
            with self.assertRaises(PromotionError) as ctx:
                promoter.promote(version, user="t")
        self.assertIn("Cannot read promotion history", str(ctx.exception))
        self.assertEqual(existing.read_bytes(), b"old")
        self.assertEqual(sorted(p.name for p in target.iterdir()), ["shot.1001.exr"])


# ============================================================================
# Bounded timecode probing
# ============================================================================

class TestTimecodeProbing(_TmpDirTest):

    def _make_frames(self, n=50):
        vdir = self.tmp / "shot_v001"
        vdir.mkdir()
        (vdir / "notes.txt").write_text("not media")
        for f in range(1001, 1001 + n):
            (vdir / f"shot_v001.{f}.exr").write_bytes(b"no header")
        return vdir

    def test_folder_without_timecode_probes_only_a_few_files(self):
        vdir = self._make_frames()
        with patch.object(timecode, "extract_timecode", return_value=None) as probe:
            self.assertIsNone(timecode.extract_timecode_for_version(vdir))
        self.assertLessEqual(probe.call_count, timecode.MAX_TIMECODE_PROBES)
        probed = [Path(c.args[0]).suffix for c in probe.call_args_list]
        self.assertNotIn(".txt", probed, "media files should be probed first")

    def test_missing_timecode_is_cached(self):
        vdir = self._make_frames(5)
        versions = [VersionInfo("v001", 1, str(vdir)), VersionInfo("v002", 2, str(vdir))]
        with patch.object(timecode, "extract_timecode", return_value=None) as probe:
            timecode.populate_timecodes_parallel(versions)
            first = probe.call_count
            timecode.populate_timecodes_parallel(versions)
            timecode.populate_timecodes(versions)
        self.assertGreater(first, 0)
        self.assertEqual(probe.call_count, first, "versions without TC were re-probed")
        self.assertTrue(all(v.timecode_checked for v in versions))

    def test_timecode_checked_round_trips(self):
        v = VersionInfo("v001", 1, "/x", timecode_checked=True)
        self.assertTrue(VersionInfo.from_dict(v.to_dict()).timecode_checked)
        self.assertNotIn("timecode_checked", VersionInfo("v001", 1, "/x").to_dict())


# ============================================================================
# Parallel copy progress
# ============================================================================

class TestParallelCopyProgress(_TmpDirTest):

    def test_progress_reaches_total(self):
        src_dir = self.tmp / "renders"
        vdir = src_dir / "shot_v001"
        vdir.mkdir(parents=True)
        n = 40
        for f in range(1001, 1001 + n):
            (vdir / f"shot_v001.{f}.exr").write_bytes(b"x" * 64)
        source = WatchedSource(name="S", source_dir=str(src_dir),
                               latest_target=str(self.tmp / "online"),
                               file_extensions=[".exr"], link_mode="copy")
        seen = []
        Promoter(source).promote(
            VersionInfo("v001", 1, str(vdir), file_count=n), user="t",
            progress_callback=lambda cur, total, name: seen.append(cur),
        )
        self.assertEqual(len(seen), n)
        self.assertEqual(sorted(seen), list(range(1, n + 1)))


if __name__ == "__main__":
    unittest.main()
