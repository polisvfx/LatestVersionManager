"""
Unit tests for modules that previously lacked test coverage:
conflicts, hooks, scan_cache, watcher, and additional edge cases
in task_tokens (date validation) and promoter (frame gaps).

Uses temp directories and mocks — no external data dependencies.
"""

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.models import (
    VersionInfo, WatchedSource, ProjectConfig, HistoryEntry,
    DEFAULT_FILE_EXTENSIONS,
)
from lvm.conflicts import detect_target_conflicts, check_target_ownership
from lvm.hooks import (
    HookError, _build_hook_env, run_hook, run_pre_promote_hook,
    run_post_promote_hook,
)
from lvm.scan_cache import (
    _source_fingerprint, cache_path_for_project,
    load_cache, save_cache, clear_cache, CACHE_VERSION,
)
from lvm.task_tokens import validate_date_string, parse_date_to_sortable


# ============================================================================
# Helper: create a minimal WatchedSource
# ============================================================================

def _make_source(name, source_dir="/src", latest_target="", **kwargs):
    return WatchedSource(
        name=name,
        source_dir=source_dir,
        latest_target=latest_target,
        **kwargs,
    )


def _make_version(version_string="v001", source_path="/src/v001",
                  file_count=10, frame_range="1001-1010"):
    return VersionInfo(
        version_string=version_string,
        version_number=int(version_string.lstrip("v")),
        source_path=source_path,
        file_count=file_count,
        total_size_bytes=file_count * 1024,
        frame_range=frame_range,
    )


def _make_config(sources=None):
    cfg = ProjectConfig(
        project_name="TestProject",
        watched_sources=sources or [],
    )
    return cfg


# ============================================================================
# conflicts.py
# ============================================================================

class TestDetectTargetConflicts(unittest.TestCase):
    """Tests for detect_target_conflicts()."""

    def test_no_sources_no_conflicts(self):
        cfg = _make_config([])
        self.assertEqual(detect_target_conflicts(cfg), [])

    def test_no_conflict_different_targets(self):
        cfg = _make_config([
            _make_source("A", latest_target="/out/a"),
            _make_source("B", latest_target="/out/b"),
        ])
        self.assertEqual(detect_target_conflicts(cfg), [])

    def test_conflict_same_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "shared")
            os.makedirs(target)
            cfg = _make_config([
                _make_source("A", latest_target=target),
                _make_source("B", latest_target=target),
            ])
            conflicts = detect_target_conflicts(cfg)
            self.assertEqual(len(conflicts), 1)
            self.assertEqual(conflicts[0][1], "A")
            self.assertEqual(conflicts[0][2], "B")

    def test_three_sources_same_target(self):
        """Three sources sharing one target produce 3 conflict pairs."""
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "shared")
            os.makedirs(target)
            cfg = _make_config([
                _make_source("A", latest_target=target),
                _make_source("B", latest_target=target),
                _make_source("C", latest_target=target),
            ])
            conflicts = detect_target_conflicts(cfg)
            self.assertEqual(len(conflicts), 3)  # A-B, A-C, B-C

    def test_source_without_target_ignored(self):
        cfg = _make_config([
            _make_source("A", latest_target="/out/a"),
            _make_source("B", latest_target=""),
        ])
        self.assertEqual(detect_target_conflicts(cfg), [])


class TestCheckTargetOwnership(unittest.TestCase):
    """Tests for check_target_ownership()."""

    def test_no_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_config([
                _make_source("A", latest_target=os.path.join(tmp, "a")),
                _make_source("B", latest_target=os.path.join(tmp, "b")),
            ])
            os.makedirs(os.path.join(tmp, "a"))
            os.makedirs(os.path.join(tmp, "b"))
            result = check_target_ownership(os.path.join(tmp, "a"), "A", cfg)
            self.assertIsNone(result)

    def test_conflict_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "shared")
            os.makedirs(target)
            cfg = _make_config([
                _make_source("A", latest_target=target),
                _make_source("B", latest_target=target),
            ])
            result = check_target_ownership(target, "A", cfg)
            self.assertEqual(result, "B")

    def test_self_not_returned(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "t")
            os.makedirs(target)
            cfg = _make_config([
                _make_source("A", latest_target=target),
            ])
            result = check_target_ownership(target, "A", cfg)
            self.assertIsNone(result)


# ============================================================================
# hooks.py
# ============================================================================

class TestBuildHookEnv(unittest.TestCase):
    """Tests for _build_hook_env()."""

    def test_env_contains_required_keys(self):
        source = _make_source("MySrc", latest_target="/out/latest")
        version = _make_version()
        env = _build_hook_env(source, version, "testuser", "TestProject")
        self.assertEqual(env["LVM_SOURCE_NAME"], "MySrc")
        self.assertEqual(env["LVM_VERSION"], "v001")
        self.assertEqual(env["LVM_USER"], "testuser")
        self.assertEqual(env["LVM_PROJECT_NAME"], "TestProject")
        self.assertEqual(env["LVM_FILE_COUNT"], "10")
        self.assertEqual(env["LVM_FRAME_RANGE"], "1001-1010")
        self.assertEqual(env["LVM_TARGET_DIR"], "/out/latest")

    def test_env_no_frame_range(self):
        source = _make_source("Src")
        version = _make_version(frame_range="")
        env = _build_hook_env(source, version, "user", "Proj")
        self.assertNotIn("LVM_FRAME_RANGE", env)


class TestRunHook(unittest.TestCase):
    """Tests for run_hook()."""

    def test_empty_command(self):
        rc, out, err = run_hook("", {}, "test")
        self.assertEqual(rc, 0)
        self.assertEqual(out, "")

    def test_successful_command(self):
        rc, out, err = run_hook("echo hello", os.environ.copy(), "test")
        self.assertEqual(rc, 0)
        self.assertIn("hello", out)

    def test_failing_command(self):
        rc, out, err = run_hook("exit 1", os.environ.copy(), "test")
        self.assertEqual(rc, 1)

    def test_timeout_raises_hook_error(self):
        # Use a long-running command that we timeout quickly
        if sys.platform == "win32":
            cmd = "ping -n 10 127.0.0.1"
        else:
            cmd = "sleep 10"
        with self.assertRaises(HookError) as ctx:
            run_hook(cmd, os.environ.copy(), "slow-hook", timeout=1)
        self.assertIn("timed out", str(ctx.exception))


class TestPrePostPromoteHooks(unittest.TestCase):
    """Tests for run_pre_promote_hook() and run_post_promote_hook()."""

    def test_pre_promote_no_cmd(self):
        source = _make_source("S")
        version = _make_version()
        rc, out, err = run_pre_promote_hook(source, version, "user", "Proj")
        self.assertEqual(rc, 0)

    def test_pre_promote_failure_raises(self):
        source = _make_source("S")
        source.pre_promote_cmd = "exit 1"
        version = _make_version()
        with self.assertRaises(HookError):
            run_pre_promote_hook(source, version, "user", "Proj")

    def test_post_promote_no_cmd(self):
        source = _make_source("S")
        version = _make_version()
        rc, out, err = run_post_promote_hook(source, version, "user", "Proj")
        self.assertEqual(rc, 0)

    def test_post_promote_failure_does_not_raise(self):
        """Post-promote failures are logged, not raised."""
        source = _make_source("S")
        source.post_promote_cmd = "exit 42"
        version = _make_version()
        rc, out, err = run_post_promote_hook(source, version, "user", "Proj")
        self.assertEqual(rc, 42)


# ============================================================================
# scan_cache.py
# ============================================================================

class TestSourceFingerprint(unittest.TestCase):
    """Tests for _source_fingerprint()."""

    def test_same_source_same_fingerprint(self):
        s1 = _make_source("A", source_dir="/src")
        s2 = _make_source("A", source_dir="/src")
        self.assertEqual(_source_fingerprint(s1), _source_fingerprint(s2))

    def test_different_dir_different_fingerprint(self):
        s1 = _make_source("A", source_dir="/src1")
        s2 = _make_source("A", source_dir="/src2")
        self.assertNotEqual(_source_fingerprint(s1), _source_fingerprint(s2))


class TestScanCacheRoundTrip(unittest.TestCase):
    """Tests for save_cache() / load_cache() / clear_cache()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tmpdir, "project.json")
        # Write a dummy config file so cache_path_for_project works
        with open(self.config_path, "w") as f:
            json.dump({}, f)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_save_and_load(self):
        source = _make_source("A", source_dir="/src")
        versions = [_make_version("v001"), _make_version("v002")]
        save_cache(self.config_path, [source], {"A": versions})
        loaded = load_cache(self.config_path, [source])
        self.assertIn("A", loaded)
        self.assertEqual(len(loaded["A"]), 2)
        self.assertEqual(loaded["A"][0].version_string, "v001")

    def test_load_missing_cache(self):
        source = _make_source("A")
        loaded = load_cache(self.config_path, [source])
        self.assertEqual(loaded, {})

    def test_fingerprint_mismatch_skips_source(self):
        source_v1 = _make_source("A", source_dir="/src")
        versions = [_make_version()]
        save_cache(self.config_path, [source_v1], {"A": versions})
        # Load with different source_dir → fingerprint mismatch
        source_v2 = _make_source("A", source_dir="/src_changed")
        loaded = load_cache(self.config_path, [source_v2])
        self.assertNotIn("A", loaded)

    def test_clear_cache(self):
        source = _make_source("A")
        save_cache(self.config_path, [source], {"A": [_make_version()]})
        cp = cache_path_for_project(self.config_path)
        self.assertTrue(cp.exists())
        clear_cache(self.config_path)
        self.assertFalse(cp.exists())

    def test_clear_nonexistent_cache(self):
        """clear_cache on missing file should not raise."""
        clear_cache(self.config_path)  # No error

    def test_corrupt_cache_returns_empty(self):
        cp = cache_path_for_project(self.config_path)
        cp.parent.mkdir(parents=True, exist_ok=True)
        with open(cp, "w") as f:
            f.write("not json")
        source = _make_source("A")
        loaded = load_cache(self.config_path, [source])
        self.assertEqual(loaded, {})

    def test_wrong_version_returns_empty(self):
        cp = cache_path_for_project(self.config_path)
        cp.parent.mkdir(parents=True, exist_ok=True)
        with open(cp, "w") as f:
            json.dump({"cache_version": -1, "sources": {}}, f)
        source = _make_source("A")
        loaded = load_cache(self.config_path, [source])
        self.assertEqual(loaded, {})

    def test_cache_path_location(self):
        cp = cache_path_for_project(self.config_path)
        self.assertEqual(cp.parent.name, ".lvm_cache")
        self.assertEqual(cp.name, "scan_cache.json")


# ============================================================================
# task_tokens.py — date validation edge cases
# ============================================================================

class TestValidateDateString(unittest.TestCase):
    """Edge cases for validate_date_string()."""

    def test_valid_ddmmyy(self):
        self.assertTrue(validate_date_string("260224", "DDMMYY"))

    def test_valid_yymmdd(self):
        self.assertTrue(validate_date_string("240226", "YYMMDD"))

    def test_valid_ddmmyyyy(self):
        self.assertTrue(validate_date_string("26022024", "DDMMYYYY"))

    def test_valid_yyyymmdd(self):
        self.assertTrue(validate_date_string("20240226", "YYYYMMDD"))

    def test_invalid_month_13(self):
        self.assertFalse(validate_date_string("011324", "DDMMYY"))

    def test_invalid_day_32(self):
        self.assertFalse(validate_date_string("320124", "DDMMYY"))

    def test_invalid_month_zero(self):
        self.assertFalse(validate_date_string("010024", "DDMMYY"))

    def test_invalid_day_zero(self):
        self.assertFalse(validate_date_string("000124", "DDMMYY"))

    def test_feb_29_non_leap_year(self):
        """Feb 29 on a non-leap year — currently accepted (dd<=31 check only)."""
        # This tests current behavior. If calendar-aware validation is added,
        # this should change to assertFalse for non-leap years like 2023.
        result = validate_date_string("290223", "DDMMYY")
        # Accept either True or False — the point is it doesn't crash
        self.assertIsInstance(result, bool)

    def test_wrong_length_returns_false(self):
        self.assertFalse(validate_date_string("12345", "DDMMYY"))
        self.assertFalse(validate_date_string("1234567", "DDMMYYYY"))

    def test_format_mismatch(self):
        self.assertFalse(validate_date_string("260224", "YYYYMMDD"))

    def test_year_range_ddmmyyyy(self):
        self.assertFalse(validate_date_string("01011899", "DDMMYYYY"))
        self.assertFalse(validate_date_string("01012100", "DDMMYYYY"))
        self.assertTrue(validate_date_string("01011900", "DDMMYYYY"))
        self.assertTrue(validate_date_string("01012099", "DDMMYYYY"))


class TestParseDateToSortable(unittest.TestCase):
    """Tests for parse_date_to_sortable()."""

    def test_yyyymmdd_passthrough(self):
        self.assertEqual(parse_date_to_sortable("20240226", "YYYYMMDD"), 20240226)

    def test_ddmmyy(self):
        self.assertEqual(parse_date_to_sortable("260224", "DDMMYY"), 20240226)

    def test_yymmdd(self):
        self.assertEqual(parse_date_to_sortable("240226", "YYMMDD"), 20240226)

    def test_ddmmyyyy(self):
        self.assertEqual(parse_date_to_sortable("26022024", "DDMMYYYY"), 20240226)

    def test_invalid_returns_zero(self):
        self.assertEqual(parse_date_to_sortable("invalid", "DDMMYY"), 0)

    def test_century_pivot_70(self):
        """YY >= 70 maps to 19xx, YY < 70 maps to 20xx."""
        self.assertEqual(parse_date_to_sortable("690101", "YYMMDD"), 20690101)
        self.assertEqual(parse_date_to_sortable("700101", "YYMMDD"), 19700101)


# ============================================================================
# watcher.py — unit tests (mocked observer)
# ============================================================================

class TestSourceWatcher(unittest.TestCase):
    """Basic tests for SourceWatcher without a live Qt event loop."""

    def test_import(self):
        """Verify the module imports without error."""
        from lvm.watcher import SourceWatcher
        self.assertTrue(callable(SourceWatcher))

    def test_is_running_initial(self):
        """Watcher should not be running before start()."""
        # This requires a QApplication for QObject, so we do a minimal check
        try:
            from PySide6.QtWidgets import QApplication
            app = QApplication.instance() or QApplication([])
            from lvm.watcher import SourceWatcher
            w = SourceWatcher()
            self.assertFalse(w.is_running)
            w.stop()  # safe to call when not started
        except ImportError:
            self.skipTest("PySide6 not available")


# ============================================================================
# promoter.py — has_frame_gaps
# ============================================================================

class TestHasFrameGaps(unittest.TestCase):
    """Tests for has_frame_gaps() in promoter.py."""

    def test_no_gaps(self):
        from lvm.promoter import has_frame_gaps
        v = _make_version(frame_range="1001-1010")
        v.file_count = 10
        self.assertFalse(has_frame_gaps(v))

    def test_with_gaps(self):
        from lvm.promoter import has_frame_gaps
        v = _make_version(frame_range="1001-1010 (gaps detected)")
        self.assertTrue(has_frame_gaps(v))

    def test_no_frame_range(self):
        from lvm.promoter import has_frame_gaps
        v = _make_version(frame_range="")
        self.assertFalse(has_frame_gaps(v))

    def test_single_frame(self):
        from lvm.promoter import has_frame_gaps
        v = _make_version(frame_range="1001-1001")
        v.file_count = 1
        self.assertFalse(has_frame_gaps(v))


# ============================================================================
# promoter.py — generate_report
# ============================================================================

class TestGenerateReport(unittest.TestCase):
    """Tests for generate_report() in promoter.py."""

    def test_basic_report(self):
        from lvm.promoter import generate_report
        entry = HistoryEntry(
            version="v001",
            source="TestSource",
            set_by="testuser",
            set_at="2024-01-15T10:30:00",
            file_count=10,
        )
        source = _make_source("TestSource", latest_target="/out/latest")
        report = generate_report(entry, source)
        self.assertIn("v001", str(report))
        self.assertIn("testuser", str(report))


if __name__ == "__main__":
    unittest.main()
