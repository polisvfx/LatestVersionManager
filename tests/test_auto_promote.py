"""
Tests for the auto-promote decision logic (src/lvm/auto_promote.py).

Self-contained: fakes for each decision branch, plus end-to-end runs
against a real Promoter/VersionScanner on temp folders. Portable (no Qt).
"""

import logging
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm import auto_promote as ap
from lvm.models import HistoryEntry, VersionInfo, WatchedSource
from lvm.promoter import Promoter
from lvm.scanner import VersionScanner


def _v(num, frame_range="1001-1010", file_count=10, size=1000):
    return VersionInfo(f"v{num:03d}", num, f"/renders/shot_v{num:03d}",
                       frame_range=frame_range, file_count=file_count,
                       total_size_bytes=size)


def _current(version="v001", frame_range="1001-1010"):
    return HistoryEntry(version=version, source="/renders/x", set_by="t",
                        set_at="2026-01-01T00:00:00", frame_range=frame_range)


def _seen(version, first_seen=0.0):
    """SettleState as if a previous check had seen exactly *version*."""
    return ap.SettleState(version.version_string, version.file_count,
                          version.total_size_bytes, version.frame_range, first_seen)


class _FakePromoter:
    def __init__(self, current=None, obsolete=None, unfinished=None,
                 link_mode="copy", raises=None, open_frame=None, block_gaps=True):
        self.source = SimpleNamespace(name="S", link_mode=link_mode,
                                      block_incomplete_sequences=block_gaps)
        self._current = current
        self._obsolete = obsolete or []
        self._unfinished = unfinished or []
        self._raises = raises
        self._open_frame = open_frame
        self.settle_seen = None

    def get_current_version(self):
        if self._raises:
            raise self._raises
        return self._current

    def detect_obsolete_layers(self, version):
        return self._obsolete

    def find_unfinished_renders(self, version, settle_seconds=0.0):
        self.settle_seen = settle_seconds
        return self._unfinished

    def newest_frames_open_for_writing(self, version):
        return self._open_frame


class _FakeScanner:
    def __init__(self, versions):
        self._versions = versions

    def scan(self):
        return list(self._versions)


class _Base(unittest.TestCase):
    def decide(self, promoter, versions, settle=10.0, previous="settled", now=100.0,
               max_wait=ap.DEFAULT_MAX_WAIT_SECONDS):
        if previous == "settled":
            previous = _seen(versions[-1]) if versions else None
        return ap.evaluate_auto_promote(promoter, _FakeScanner(versions), settle,
                                        previous=previous, now=now,
                                        max_wait_seconds=max_wait)


class TestDecisionBranches(_Base):

    def test_no_versions_is_noop(self):
        d = self.decide(_FakePromoter(current=_current()), [])
        self.assertEqual(d.action, ap.NOOP)
        self.assertEqual(d.versions, [])

    def test_no_previous_promotion_is_skipped(self):
        d = self.decide(_FakePromoter(current=None), [_v(1), _v(2)])
        self.assertEqual(d.action, ap.SKIP)
        self.assertIn("no previous promotion", d.reason)

    def test_already_on_highest_is_noop(self):
        d = self.decide(_FakePromoter(current=_current("v002")), [_v(1), _v(2)])
        self.assertEqual(d.action, ap.NOOP)

    def test_frame_range_change_is_skipped(self):
        d = self.decide(_FakePromoter(current=_current()), [_v(1), _v(2, "1001-1020")])
        self.assertEqual(d.action, ap.SKIP)
        self.assertIn("frame range changed (1001-1010 → 1001-1020)", d.reason)

    def test_gap_annotation_ignored_when_gaps_allowed(self):
        gappy = "1001-1010 (9/10 frames, gaps detected)"
        d = self.decide(_FakePromoter(current=_current(), block_gaps=False),
                        [_v(1), _v(2, gappy)])
        self.assertEqual(d.action, ap.PROMOTE)

    def test_obsolete_layers_are_skipped(self):
        p = _FakePromoter(current=_current(), obsolete=[{"name": "alpha"}, {"name": "zdepth"}])
        d = self.decide(p, [_v(1), _v(2)])
        self.assertEqual(d.action, ap.SKIP)
        self.assertIn("obsolete layers in target (alpha, zdepth)", d.reason)

    def test_unfinished_container_waits(self):
        p = _FakePromoter(current=_current(),
                          unfinished=[(Path("/r/shot_v002.mov"), "no moov atom")])
        d = self.decide(p, [_v(1), _v(2)], settle=7.5)
        self.assertEqual(d.action, ap.WAIT_RENDER)
        self.assertEqual(d.waiting_file, "shot_v002.mov")
        self.assertEqual(d.reason, "no moov atom")
        self.assertEqual(p.settle_seen, 7.5)

    def test_unavailable_link_mode_is_skipped_with_warning(self):
        with patch.object(ap, "check_link_mode_available",
                          return_value=(False, "Symlinks need admin")):
            d = self.decide(_FakePromoter(current=_current(), link_mode="symlink"),
                            [_v(1), _v(2)])
        self.assertEqual(d.action, ap.SKIP)
        self.assertEqual(d.reason, "Symlinks need admin")
        self.assertEqual(d.log_level, logging.WARNING)

    def test_all_checks_pass_promotes_highest(self):
        versions = [_v(1), _v(2), _v(3)]
        d = self.decide(_FakePromoter(current=_current()), versions)
        self.assertEqual(d.action, ap.PROMOTE)
        self.assertEqual(d.version.version_string, "v003")
        self.assertEqual([v.version_string for v in d.versions], ["v001", "v002", "v003"])

    def test_errors_become_error_decision(self):
        p = _FakePromoter(raises=PermissionError("share dropped"))
        d = self.decide(p, [_v(1), _v(2)])
        self.assertEqual(d.action, ap.ERROR)
        self.assertIn("share dropped", d.reason)
        self.assertEqual(d.log_level, logging.WARNING)

    def test_normalize_frame_range(self):
        self.assertIsNone(ap.normalize_frame_range(None))
        self.assertEqual(ap.normalize_frame_range("1001-1120"), "1001-1120")
        self.assertEqual(
            ap.normalize_frame_range("1001-1120 (95/120 frames, gaps detected)"),
            "1001-1120",
        )


class TestCompleteness(_Base):
    """Item 4 of the checks: wait while the version is still arriving."""

    def test_first_sight_waits_for_a_second_look(self):
        d = self.decide(_FakePromoter(current=_current()), [_v(1), _v(2)], previous=None)
        self.assertEqual(d.action, ap.WAIT_RENDER)
        self.assertIn("settle", d.reason)
        self.assertEqual(d.settle.version_string, "v002")

    def test_changed_files_wait_again(self):
        grown = _v(2, file_count=10, size=2000)
        previous = _seen(_v(2, file_count=10, size=1000))  # last frame still growing
        d = self.decide(_FakePromoter(current=_current()), [_v(1), grown], previous=previous)
        self.assertEqual(d.action, ap.WAIT_RENDER)

    def test_same_files_twice_then_promote(self):
        p = _FakePromoter(current=_current())
        versions = [_v(1), _v(2)]
        first = self.decide(p, versions, previous=None, now=10.0)
        second = self.decide(p, versions, previous=first.settle, now=20.0)
        self.assertEqual(first.action, ap.WAIT_RENDER)
        self.assertEqual(second.action, ap.PROMOTE)

    def test_partial_range_keeps_waiting_instead_of_skipping(self):
        partial = _v(2, "1001-1004", file_count=4)
        d = self.decide(_FakePromoter(current=_current()), [_v(1), partial])
        self.assertEqual(d.action, ap.WAIT_RENDER)
        self.assertIn("1001-1004 of 1001-1010", d.reason)

    def test_frame_gaps_wait_when_gaps_block_promotion(self):
        gappy = _v(2, "1001-1010 (6/10 frames, gaps detected; missing: 1004-1007)", file_count=6)
        d = self.decide(_FakePromoter(current=_current()), [_v(1), gappy])
        self.assertEqual(d.action, ap.WAIT_RENDER)
        self.assertIn("frames missing", d.reason)

    def test_frame_open_for_writing_waits(self):
        p = _FakePromoter(current=_current(), open_frame="shot_v002.1010.exr")
        d = self.decide(p, [_v(1), _v(2)])
        self.assertEqual(d.action, ap.WAIT_RENDER)
        self.assertEqual(d.waiting_file, "shot_v002.1010.exr")
        self.assertIn("still being written", d.reason)

    def test_first_seen_kept_for_same_version_and_reset_for_new(self):
        p = _FakePromoter(current=_current())
        d1 = self.decide(p, [_v(1), _v(2)], previous=None, now=50.0)
        d2 = self.decide(p, [_v(1), _v(2, file_count=11)], previous=d1.settle, now=60.0)
        d3 = self.decide(p, [_v(1), _v(2), _v(3)], previous=d2.settle, now=70.0)
        self.assertEqual(d2.settle.first_seen, 50.0)
        self.assertEqual(d3.settle.first_seen, 70.0)

    def test_gives_up_after_max_wait(self):
        partial = _v(2, "1001-1004", file_count=4)
        previous = _seen(partial, first_seen=0.0)
        d = self.decide(_FakePromoter(current=_current()), [_v(1), partial],
                        previous=previous, now=3 * 3600.0, max_wait=2 * 3600.0)
        self.assertEqual(d.action, ap.SKIP)
        self.assertIn("stopped waiting after 3.0 h", d.reason)


class TestEndToEnd(unittest.TestCase):
    """Real Promoter + VersionScanner on temp folders."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_auto_"))
        self.renders = self.tmp / "renders"
        self.source = WatchedSource(
            name="shot", source_dir=str(self.renders),
            latest_target=str(self.tmp / "online"),
            version_pattern="_v{version}", file_extensions=[".exr"],
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _render(self, version, first=1001, last=1005):
        vdir = self.renders / f"shot_{version}"
        vdir.mkdir(parents=True, exist_ok=True)
        for f in range(first, last + 1):
            (vdir / f"shot_{version}.{f}.exr").write_bytes(b"x")
        return vdir

    def _decide(self, previous=None, now=0.0):
        return ap.evaluate_auto_promote(
            Promoter(self.source), VersionScanner(self.source), settle_seconds=0.0,
            previous=previous, now=now)

    def _promote_v001(self):
        self._render("v001")
        Promoter(self.source).promote(VersionScanner(self.source).scan()[-1], user="t")

    def test_new_matching_version_is_promoted_on_second_look(self):
        self._promote_v001()
        self._render("v002")
        first = self._decide()
        second = self._decide(previous=first.settle)
        self.assertEqual(first.action, ap.WAIT_RENDER)
        self.assertEqual(second.action, ap.PROMOTE)
        self.assertEqual(second.version.version_string, "v002")

    def test_render_into_version_folder_is_followed_to_completion(self):
        """Folder-per-version: frames land inside v002/ without any watcher
        event, so the check must keep waiting until the range is complete."""
        self._promote_v001()
        self._render("v002", last=1002)
        d = self._decide()
        d = self._decide(previous=d.settle)            # unchanged, but partial
        self.assertEqual(d.action, ap.WAIT_RENDER)
        self.assertIn("1001-1002 of 1001-1005", d.reason)

        self._render("v002", first=1003, last=1005)   # render finishes
        d = self._decide(previous=d.settle)            # files changed
        self.assertEqual(d.action, ap.WAIT_RENDER)
        d = self._decide(previous=d.settle)            # settled
        self.assertEqual(d.action, ap.PROMOTE)

    @unittest.skipUnless(sys.platform == "win32", "write-share check is Windows-only")
    def test_last_frame_open_for_writing_is_detected(self):
        self._promote_v001()
        vdir = self._render("v002")
        promoter = Promoter(self.source)
        version = VersionScanner(self.source).scan()[-1]
        with open(vdir / "shot_v002.1005.exr", "ab"):
            self.assertEqual(promoter.newest_frames_open_for_writing(version),
                             "shot_v002.1005.exr")
        self.assertIsNone(promoter.newest_frames_open_for_writing(version))


if __name__ == "__main__":
    unittest.main()
