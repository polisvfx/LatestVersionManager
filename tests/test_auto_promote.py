"""
Tests for the auto-promote decision logic (src/lvm/auto_promote.py).

Self-contained: fakes for each decision branch, plus an end-to-end run
against a real Promoter/VersionScanner on temp folders. Portable (no Qt).
"""

import logging
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


def _v(num, frame_range="1001-1010"):
    return VersionInfo(f"v{num:03d}", num, f"/renders/shot_v{num:03d}",
                       frame_range=frame_range, file_count=10)


def _current(version="v001", frame_range="1001-1010"):
    return HistoryEntry(version=version, source="/renders/x", set_by="t",
                        set_at="2026-01-01T00:00:00", frame_range=frame_range)


class _FakePromoter:
    def __init__(self, current=None, obsolete=None, unfinished=None,
                 link_mode="copy", raises=None):
        self.source = SimpleNamespace(name="S", link_mode=link_mode)
        self._current = current
        self._obsolete = obsolete or []
        self._unfinished = unfinished or []
        self._raises = raises
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


class _FakeScanner:
    def __init__(self, versions):
        self._versions = versions

    def scan(self):
        return list(self._versions)


class TestDecisionBranches(unittest.TestCase):

    def decide(self, promoter, versions, settle=10.0):
        return ap.evaluate_auto_promote(promoter, _FakeScanner(versions), settle)

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
        d = self.decide(_FakePromoter(current=_current()), [_v(1), _v(2, "1001-1005")])
        self.assertEqual(d.action, ap.SKIP)
        self.assertIn("frame range changed (1001-1010 → 1001-1005)", d.reason)

    def test_gap_annotation_ignored_in_range_compare(self):
        gappy = "1001-1010 (9/10 frames, gaps detected)"
        d = self.decide(_FakePromoter(current=_current()), [_v(1), _v(2, gappy)])
        self.assertEqual(d.action, ap.PROMOTE)

    def test_obsolete_layers_are_skipped(self):
        p = _FakePromoter(current=_current(), obsolete=[{"name": "alpha"}, {"name": "zdepth"}])
        d = self.decide(p, [_v(1), _v(2)])
        self.assertEqual(d.action, ap.SKIP)
        self.assertIn("obsolete layers in target (alpha, zdepth)", d.reason)

    def test_unfinished_render_waits(self):
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

    def _render(self, version, last_frame=1005):
        vdir = self.renders / f"shot_{version}"
        vdir.mkdir(parents=True)
        for f in range(1001, last_frame + 1):
            (vdir / f"shot_{version}.{f}.exr").write_bytes(b"x")

    def _decide(self):
        return ap.evaluate_auto_promote(
            Promoter(self.source), VersionScanner(self.source), settle_seconds=0.0)

    def test_new_matching_version_is_promoted(self):
        self._render("v001")
        scanner = VersionScanner(self.source)
        Promoter(self.source).promote(scanner.scan()[-1], user="t")
        self._render("v002")
        d = self._decide()
        self.assertEqual(d.action, ap.PROMOTE)
        self.assertEqual(d.version.version_string, "v002")

    def test_partial_render_is_skipped_by_range(self):
        self._render("v001")
        Promoter(self.source).promote(VersionScanner(self.source).scan()[-1], user="t")
        self._render("v002", last_frame=1003)   # still rendering
        d = self._decide()
        self.assertEqual(d.action, ap.SKIP)
        self.assertIn("frame range changed", d.reason)


if __name__ == "__main__":
    unittest.main()
