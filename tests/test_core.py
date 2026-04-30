"""
Self-contained unit tests for all LVM core modules.

Uses temp directories and synthetic fixtures — no external data dependencies.
Covers: models, scanner, discovery, config, promoter, history, task_tokens.
"""

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.models import (
    VersionInfo, HistoryEntry, WatchedSource, ProjectConfig,
    DiscoveryResult, resolve_path, make_relative, DEFAULT_FILE_EXTENSIONS,
)
from lvm.scanner import (
    VersionScanner, detect_sequence_from_file,
    scan_directory_as_version, create_manual_version,
    _group_files_by_sequence, _detect_frame_range_for_group,
)
from lvm.discovery import (
    discover, format_discovery_report,
    _detect_date_format, _is_plausible_date, _suggest_pattern,
)
from lvm.config import (
    load_config, save_config, create_project, apply_project_defaults,
    _expand_group_token, _resolve_group_root,
)
from lvm.history import HistoryManager, MAX_HISTORY_ENTRIES, has_newer_versions_since
from lvm.promoter import Promoter, PromotionError, generate_report, has_frame_gaps
from lvm.task_tokens import (
    compile_task_pattern, find_task_tokens, strip_task_tokens,
    strip_version, strip_frame_and_ext, derive_source_tokens,
    compute_source_name, get_naming_options,
)


# ============================================================================
# Helper to create test file structures
# ============================================================================

def _make_exr_sequence(folder, basename, version, start=1001, end=1010):
    """Create a fake EXR frame sequence in folder.

    E.g. _make_exr_sequence(d, "hero_comp", "v001", 1001, 1005) creates:
        hero_comp_v001.1001.exr ... hero_comp_v001.1005.exr
    """
    Path(folder).mkdir(parents=True, exist_ok=True)
    files = []
    for frame in range(start, end + 1):
        f = Path(folder) / f"{basename}_{version}.{frame:04d}.exr"
        f.write_bytes(b"\x00" * 64)
        files.append(f)
    return files


def _make_versioned_dirs(root, basename, versions, start=1001, end=1005):
    """Create versioned subdirectories each containing an EXR sequence.

    E.g. _make_versioned_dirs(root, "hero_comp", ["v001", "v002"])
    Creates: root/hero_comp_v001/*.exr, root/hero_comp_v002/*.exr
    """
    for v in versions:
        vdir = Path(root) / f"{basename}_{v}"
        _make_exr_sequence(vdir, basename, v, start, end)


def _make_versioned_files(root, basename, versions, ext=".mov"):
    """Create versioned single files in root.

    E.g. _make_versioned_files(root, "hero_comp", ["v001", "v002"], ".mov")
    """
    Path(root).mkdir(parents=True, exist_ok=True)
    for v in versions:
        f = Path(root) / f"{basename}_{v}{ext}"
        f.write_bytes(b"\x00" * 128)


# ============================================================================
# Models
# ============================================================================

class TestVersionInfo(unittest.TestCase):

    def test_size_human_various(self):
        sizes = [
            (0, "0.0 B"), (1023, "1023.0 B"), (1024, "1.0 KB"),
            (1048576, "1.0 MB"), (1073741824, "1.0 GB"),
        ]
        for sz, expected in sizes:
            vi = VersionInfo("v001", 1, "/tmp", total_size_bytes=sz)
            self.assertEqual(vi.total_size_human, expected)

    def test_roundtrip(self):
        vi = VersionInfo(
            version_string="v003", version_number=3,
            source_path="/renders/v003",
            frame_range="1001-1010", frame_count=10,
            sub_sequences=[{"name": "alpha", "frame_range": "1001-1010"}],
            file_count=10, total_size_bytes=5000,
            start_timecode="01:00:00:00",
            date_string="260224", date_sortable=20240226,
        )
        d = vi.to_dict()
        restored = VersionInfo.from_dict(d)
        self.assertEqual(restored.version_string, "v003")
        self.assertEqual(restored.version_number, 3)
        self.assertEqual(restored.frame_range, "1001-1010")
        self.assertEqual(restored.frame_count, 10)
        self.assertEqual(restored.date_string, "260224")
        self.assertEqual(restored.date_sortable, 20240226)
        self.assertEqual(restored.start_timecode, "01:00:00:00")
        self.assertEqual(len(restored.sub_sequences), 1)

    def test_compact_serialization(self):
        """Optional fields absent when not set."""
        vi = VersionInfo("v001", 1, "/tmp")
        d = vi.to_dict()
        self.assertNotIn("frame_range", d)
        self.assertNotIn("start_timecode", d)
        self.assertNotIn("date_string", d)
        self.assertNotIn("sub_sequences", d)

    def test_from_dict_defaults(self):
        """Minimal dict loads with sensible defaults."""
        d = {"version_string": "v001", "version_number": 1, "source_path": "/tmp"}
        vi = VersionInfo.from_dict(d)
        self.assertIsNone(vi.frame_range)
        self.assertEqual(vi.frame_count, 0)
        self.assertEqual(vi.date_sortable, 0)
        self.assertIsNone(vi.date_string)


class TestHistoryEntry(unittest.TestCase):

    def test_roundtrip(self):
        he = HistoryEntry(
            version="v003", source="/renders/v003",
            set_by="testuser", set_at="2024-01-15T10:30:00",
            frame_range="1001-1010", frame_count=10, file_count=10,
            start_timecode="01:00:00:00",
            source_mtime=1700000000.0, target_mtime=1700000001.0,
            pinned=True,
        )
        d = he.to_dict()
        restored = HistoryEntry.from_dict(d)
        self.assertEqual(restored.version, "v003")
        self.assertEqual(restored.set_by, "testuser")
        self.assertEqual(restored.source_mtime, 1700000000.0)
        self.assertTrue(restored.pinned)

    def test_from_version_info(self):
        vi = VersionInfo("v005", 5, "/renders/v005",
                         frame_range="1001-1100", frame_count=100, file_count=100)
        he = HistoryEntry.from_version_info(vi, "artist")
        self.assertEqual(he.version, "v005")
        self.assertEqual(he.set_by, "artist")
        self.assertEqual(he.frame_count, 100)
        self.assertTrue(he.set_at)  # timestamp is set

    def test_backward_compat_no_mtime(self):
        d = {"version": "v001", "source": "/tmp", "set_by": "x", "set_at": "2024-01-01"}
        he = HistoryEntry.from_dict(d)
        self.assertIsNone(he.source_mtime)
        self.assertIsNone(he.target_mtime)
        self.assertFalse(he.pinned)

    def test_pinned_not_serialized_when_false(self):
        he = HistoryEntry("v001", "/tmp", "x", "2024-01-01")
        d = he.to_dict()
        self.assertNotIn("pinned", d)


class TestWatchedSource(unittest.TestCase):

    def test_roundtrip(self):
        ws = WatchedSource(
            name="Hero Comp", source_dir="/renders/hero",
            version_pattern="_v{version}",
            file_extensions=[".exr", ".dpx"],
            latest_target="/online/hero",
            link_mode="symlink",
            file_rename_template="{source_name}_latest",
            sample_filename="hero_comp_v001.1001.exr",
            group="INT",
            date_format="DDMMYY",
            override_version_pattern=True,
            override_date_format=True,
            override_file_extensions=True,
            override_latest_target=True,
            override_file_rename=True,
            override_link_mode=True,
            block_incomplete_sequences=False,
            pre_promote_cmd="echo pre",
            post_promote_cmd="echo post",
            manual_versions=[{"version_string": "v099", "version_number": 99, "source_path": "/tmp"}],
        )
        d = ws.to_dict()
        restored = WatchedSource.from_dict(d)
        self.assertEqual(restored.name, "Hero Comp")
        self.assertEqual(restored.link_mode, "symlink")
        self.assertTrue(restored.use_symlinks)
        self.assertEqual(restored.date_format, "DDMMYY")
        self.assertTrue(restored.override_date_format)
        self.assertFalse(restored.block_incomplete_sequences)
        self.assertEqual(len(restored.manual_versions), 1)

    def test_backward_compat_use_symlinks(self):
        """Old configs with use_symlinks bool should map to link_mode."""
        d = {"name": "test", "source_dir": "/tmp", "use_symlinks": True}
        ws = WatchedSource.from_dict(d)
        self.assertEqual(ws.link_mode, "symlink")

    def test_backward_compat_override_use_symlinks(self):
        d = {"name": "test", "source_dir": "/tmp", "override_use_symlinks": True}
        ws = WatchedSource.from_dict(d)
        self.assertTrue(ws.override_link_mode)

    def test_compact_serialization(self):
        """Default values should not appear in serialized dict."""
        ws = WatchedSource(name="test", source_dir="/tmp")
        d = ws.to_dict()
        self.assertNotIn("date_format", d)
        self.assertNotIn("override_date_format", d)
        self.assertNotIn("override_version_pattern", d)
        self.assertNotIn("file_rename_template", d)
        self.assertNotIn("sample_filename", d)
        self.assertNotIn("group", d)
        self.assertNotIn("manual_versions", d)

    def test_has_overrides(self):
        ws = WatchedSource(name="t", source_dir="/tmp")
        self.assertFalse(ws.has_overrides)
        ws.override_latest_target = True
        self.assertTrue(ws.has_overrides)


class TestProjectConfig(unittest.TestCase):

    def test_roundtrip(self):
        config = ProjectConfig(
            project_name="TestShow",
            default_version_pattern="_v{version}",
            default_link_mode="hardlink",
            default_date_format="DDMMYY",
            name_whitelist=["comp", "grade"],
            name_blacklist=["wip"],
            task_tokens=["comp", "grade"],
            groups={"INT": {"color": "#ff0000", "root_dir": "/projects/INT"}},
            default_naming_rule="source_basename",
            naming_configured=True,
            timecode_mode="always",
            block_incomplete_sequences=False,
            pre_promote_cmd="echo pre",
            post_promote_cmd="echo post",
            project_root="/projects/root",
            skip_resolve=False,
            source_list_columns=["version", "status", "layer_count"],
        )
        d = config.to_dict()
        restored = ProjectConfig.from_dict(d)
        self.assertEqual(restored.project_name, "TestShow")
        self.assertEqual(restored.default_link_mode, "hardlink")
        self.assertEqual(restored.default_date_format, "DDMMYY")
        self.assertEqual(restored.name_whitelist, ["comp", "grade"])
        self.assertEqual(restored.task_tokens, ["comp", "grade"])
        self.assertTrue(restored.naming_configured)
        self.assertFalse(restored.block_incomplete_sequences)
        self.assertEqual(restored.project_root, "/projects/root")
        self.assertFalse(restored.skip_resolve)
        self.assertEqual(restored.source_list_columns, ["version", "status", "layers"])

    def test_compact_serialization(self):
        """Default values omitted from serialized dict."""
        config = ProjectConfig(project_name="Test")
        d = config.to_dict()
        self.assertNotIn("default_version_pattern", d)
        self.assertNotIn("default_date_format", d)
        self.assertNotIn("default_link_mode", d)
        self.assertNotIn("skip_resolve", d)
        self.assertNotIn("source_list_columns", d)

    def test_effective_project_root(self):
        config = ProjectConfig(project_name="Test", project_dir="/fallback")
        self.assertEqual(config.effective_project_root, "/fallback")
        config.project_root = "/explicit"
        self.assertEqual(config.effective_project_root, "/explicit")

    def test_backward_compat_default_use_symlinks(self):
        d = {"project_name": "Old", "default_use_symlinks": True}
        config = ProjectConfig.from_dict(d)
        self.assertEqual(config.default_link_mode, "symlink")


class TestResolvePath(unittest.TestCase):

    def test_token_expansion(self):
        result = resolve_path("{project_root}/shots/{shot}/renders",
                              {"shot": "hero"}, "/projects")
        self.assertIn("hero", result)
        self.assertIn("renders", result)

    def test_relative_resolved(self):
        base = "C:/projects" if os.name == "nt" else "/projects"
        result = resolve_path("shots/hero", {}, base)
        self.assertTrue(Path(result).is_absolute())


class TestMakeRelative(unittest.TestCase):

    def test_same_drive(self):
        result = make_relative("/projects/shots/hero", "/projects")
        self.assertEqual(result, "shots/hero")

    @unittest.skipUnless(os.name == "nt", "backslash paths are Windows-only")
    def test_forward_slashes(self):
        result = make_relative("C:\\projects\\shots\\hero", "C:\\projects")
        self.assertNotIn("\\", result)


class TestDiscoveryResult(unittest.TestCase):

    def test_fields(self):
        dr = DiscoveryResult(
            path="/renders", name="hero",
            versions_found=[], suggested_pattern="_v{version}",
            suggested_extensions=[".exr"],
            sample_filename="hero_v001.1001.exr",
            suggested_date_format="DDMMYY",
        )
        self.assertEqual(dr.name, "hero")
        self.assertEqual(dr.suggested_date_format, "DDMMYY")


# ============================================================================
# Task Tokens
# ============================================================================

class TestStripVersion(unittest.TestCase):

    def test_basic(self):
        self.assertEqual(strip_version("hero_comp_v003"), "hero_comp")

    def test_dotted(self):
        self.assertEqual(strip_version("hero_comp.v003"), "hero_comp")

    def test_capital_v(self):
        self.assertEqual(strip_version("hero_comp_V003"), "hero_comp")

    def test_no_version(self):
        self.assertEqual(strip_version("hero_comp"), "hero_comp")

    def test_version_at_start(self):
        """Version at very start without divider prefix is not stripped (requires divider)."""
        # strip_version uses regex [._-]v\d+ which requires a divider before v
        self.assertEqual(strip_version("v003_hero_comp"), "v003_hero_comp")

    def test_multidigit(self):
        self.assertEqual(strip_version("shot_v0051"), "shot")

    def test_double_divider_cleanup(self):
        self.assertEqual(strip_version("shot__v003_comp"), "shot_comp")


class TestStripFrameAndExt(unittest.TestCase):

    def test_frame_exr(self):
        self.assertEqual(strip_frame_and_ext("hero_comp_v003.1001.exr"), "hero_comp_v003")

    def test_underscore_frame(self):
        self.assertEqual(strip_frame_and_ext("hero_comp_v003_1001.exr"), "hero_comp_v003")

    def test_no_frame(self):
        self.assertEqual(strip_frame_and_ext("hero_comp_v003.mov"), "hero_comp_v003")

    def test_no_ext(self):
        self.assertEqual(strip_frame_and_ext("hero_comp_v003"), "hero_comp_v003")


class TestCompileTaskPattern(unittest.TestCase):

    def test_exact_match(self):
        pat = compile_task_pattern("comp")
        self.assertIsNotNone(pat.search("hero_comp_v003"))
        self.assertIsNone(pat.search("hero_compositor_v003"))

    def test_wildcard_two(self):
        pat = compile_task_pattern("comp_%%")
        self.assertIsNotNone(pat.search("hero_comp_mp_v003"))
        self.assertIsNone(pat.search("hero_comp_mpo_v003"))

    def test_wildcard_three(self):
        pat = compile_task_pattern("comp_%%%")
        self.assertIsNotNone(pat.search("hero_comp_mpo_v003"))
        self.assertIsNone(pat.search("hero_comp_mp_v003"))

    def test_bounded(self):
        """Task token must be bounded by dividers."""
        pat = compile_task_pattern("comp")
        self.assertIsNone(pat.search("recomp_v003"))

    def test_at_start(self):
        pat = compile_task_pattern("comp")
        self.assertIsNotNone(pat.search("comp_hero_v003"))

    def test_at_end(self):
        pat = compile_task_pattern("comp")
        self.assertIsNotNone(pat.search("hero_comp"))


class TestFindTaskTokens(unittest.TestCase):

    def test_finds_matches(self):
        results = find_task_tokens("hero_comp_mp", ["comp", "comp_%%"])
        self.assertEqual(len(results), 2)
        tokens = [r["token"] for r in results]
        self.assertIn("comp", tokens)
        self.assertIn("comp_%%", tokens)

    def test_no_match(self):
        results = find_task_tokens("hero_grade", ["comp"])
        self.assertEqual(len(results), 0)


class TestStripTaskTokens(unittest.TestCase):

    def test_strip_single(self):
        self.assertEqual(strip_task_tokens("hero_comp", ["comp"]), "hero")

    def test_strip_wildcard(self):
        self.assertEqual(strip_task_tokens("hero_comp_mp", ["comp_%%"]), "hero")

    def test_strip_multiple(self):
        result = strip_task_tokens("hero_comp_mp", ["comp", "comp_%%"])
        # Both "comp" and "comp_mp" match — stripping both should leave "hero"
        self.assertEqual(result, "hero")

    def test_empty_patterns(self):
        self.assertEqual(strip_task_tokens("hero_comp", []), "hero_comp")

    def test_no_match(self):
        self.assertEqual(strip_task_tokens("hero_grade", ["comp"]), "hero_grade")


class TestDeriveSourceTokens(unittest.TestCase):

    def test_full_derivation(self):
        tokens = derive_source_tokens(
            "hero_comp_v003.1001.exr", ["comp"], source_title="Hero Comp")
        self.assertEqual(tokens["source_title"], "Hero Comp")
        self.assertEqual(tokens["source_filename"], "hero_comp_v003.1001.exr")
        self.assertEqual(tokens["source_fullname"], "hero_comp_v003")
        self.assertEqual(tokens["source_name"], "hero_comp")
        self.assertEqual(tokens["source_basename"], "hero")

    def test_no_task_patterns(self):
        tokens = derive_source_tokens("hero_comp_v003.1001.exr", [])
        self.assertEqual(tokens["source_basename"], "hero_comp")

    def test_path_input(self):
        tokens = derive_source_tokens("/renders/v003/hero_comp_v003.1001.exr", ["comp"])
        self.assertEqual(tokens["source_filename"], "hero_comp_v003.1001.exr")
        self.assertEqual(tokens["source_basename"], "hero")

    def test_single_file(self):
        tokens = derive_source_tokens("hero_comp_v003.mov", ["comp"])
        self.assertEqual(tokens["source_fullname"], "hero_comp_v003")
        self.assertEqual(tokens["source_name"], "hero_comp")
        self.assertEqual(tokens["source_basename"], "hero")

    def test_empty_basename_fallback(self):
        """If stripping everything produces empty, fall back to source_name."""
        tokens = derive_source_tokens("comp_v001.exr", ["comp"])
        self.assertNotEqual(tokens["source_basename"], "")


class TestComputeSourceName(unittest.TestCase):

    def test_parent_rule(self):
        dr = DiscoveryResult(path="/projects/shots/hero/renders", name="renders")
        self.assertEqual(compute_source_name(dr, "parent:0"), "renders")
        self.assertEqual(compute_source_name(dr, "parent:1"), "hero")

    def test_source_basename_rule(self):
        dr = DiscoveryResult(
            path="/renders", name="renders",
            sample_filename="hero_comp_v001.1001.exr")
        self.assertEqual(compute_source_name(dr, "source_basename", ["comp"]), "hero")

    def test_source_name_rule(self):
        dr = DiscoveryResult(
            path="/renders", name="renders",
            sample_filename="hero_comp_v001.1001.exr")
        self.assertEqual(compute_source_name(dr, "source_name"), "hero_comp")


class TestGetNamingOptions(unittest.TestCase):

    def test_returns_options(self):
        dr = DiscoveryResult(
            path="/projects/shots/hero/renders", name="renders",
            sample_filename="hero_comp_v001.1001.exr")
        options = get_naming_options(dr, ["comp"])
        rules = [o["rule"] for o in options]
        self.assertIn("parent:0", rules)
        self.assertIn("source_name", rules)
        self.assertIn("source_basename", rules)


# ============================================================================
# Scanner
# ============================================================================

class TestVersionScanner(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_scanner_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_scan_versioned_dirs(self):
        """Scan subdirectories named shot_comp_v001, shot_comp_v002."""
        _make_versioned_dirs(self.tmpdir, "shot_comp", ["v001", "v002", "v003"])
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(source)
        versions = scanner.scan()
        self.assertEqual(len(versions), 3)
        self.assertEqual(versions[0].version_number, 1)
        self.assertEqual(versions[1].version_number, 2)
        self.assertEqual(versions[2].version_number, 3)
        # Check frame range detected
        self.assertIsNotNone(versions[0].frame_range)
        self.assertIn("1001", versions[0].frame_range)
        self.assertEqual(versions[0].file_count, 5)

    def test_scan_versioned_files(self):
        """Scan single versioned files (e.g. .mov)."""
        _make_versioned_files(self.tmpdir, "shot_comp", ["v001", "v002"], ".mov")
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}",
            file_extensions=[".mov"],
        )
        scanner = VersionScanner(source)
        versions = scanner.scan()
        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[0].version_number, 1)
        self.assertEqual(versions[1].version_number, 2)
        self.assertIsNone(versions[0].frame_range)
        self.assertEqual(versions[0].file_count, 1)

    def test_scan_flat_frame_sequences(self):
        """Scan flat versioned EXR sequences in the same directory."""
        for v in ["v001", "v002"]:
            for frame in range(1001, 1004):
                f = Path(self.tmpdir) / f"shot_comp_{v}.{frame:04d}.exr"
                f.write_bytes(b"\x00" * 64)
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(source)
        versions = scanner.scan()
        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[0].frame_count, 3)
        self.assertEqual(versions[1].frame_count, 3)

    def test_scan_empty_dir(self):
        source = WatchedSource(name="test", source_dir=self.tmpdir,
                               file_extensions=[".exr"])
        scanner = VersionScanner(source)
        self.assertEqual(scanner.scan(), [])

    def test_scan_nonexistent_dir(self):
        source = WatchedSource(name="test", source_dir="/nonexistent/path",
                               file_extensions=[".exr"])
        scanner = VersionScanner(source)
        self.assertEqual(scanner.scan(), [])

    def test_scan_wrong_extensions(self):
        """Files with non-matching extensions should be ignored."""
        _make_versioned_files(self.tmpdir, "shot", ["v001"], ".mov")
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(source)
        self.assertEqual(scanner.scan(), [])

    def test_scan_sorted_by_version(self):
        """Versions should be sorted ascending."""
        _make_versioned_files(self.tmpdir, "shot", ["v005", "v001", "v003"], ".mov")
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}", file_extensions=[".mov"])
        scanner = VersionScanner(source)
        versions = scanner.scan()
        nums = [v.version_number for v in versions]
        self.assertEqual(nums, [1, 3, 5])

    def test_get_latest_version(self):
        _make_versioned_files(self.tmpdir, "shot", ["v001", "v002", "v003"], ".mov")
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}", file_extensions=[".mov"])
        scanner = VersionScanner(source)
        latest = scanner.get_latest_version()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.version_number, 3)

    def test_get_latest_version_empty(self):
        source = WatchedSource(name="test", source_dir=self.tmpdir,
                               file_extensions=[".exr"])
        scanner = VersionScanner(source)
        self.assertIsNone(scanner.get_latest_version())

    def test_scan_with_basename_filter(self):
        """sample_filename filters out non-matching basenames."""
        _make_versioned_dirs(self.tmpdir, "shot_comp", ["v001"])
        _make_versioned_dirs(self.tmpdir, "shot_grade", ["v001"])
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}", file_extensions=[".exr"],
            sample_filename="shot_comp_v001.1001.exr",
        )
        scanner = VersionScanner(source, task_tokens=[])
        versions = scanner.scan()
        self.assertEqual(len(versions), 1)
        self.assertIn("shot_comp", versions[0].source_path)

    def test_version_dir_with_no_matching_files(self):
        """A version dir with no matching extension files should be skipped."""
        vdir = Path(self.tmpdir) / "shot_v001"
        vdir.mkdir()
        (vdir / "readme.txt").write_text("not a media file")
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}", file_extensions=[".exr"])
        scanner = VersionScanner(source)
        self.assertEqual(scanner.scan(), [])

    def test_scan_multi_layer_version_dir(self):
        """Version dir with multiple sequence prefixes -> sub_sequences."""
        vdir = Path(self.tmpdir) / "shot_v001"
        vdir.mkdir()
        for frame in range(1001, 1006):
            (vdir / f"shot_beauty.{frame:04d}.exr").write_bytes(b"\x00" * 64)
            (vdir / f"shot_alpha.{frame:04d}.exr").write_bytes(b"\x00" * 64)
        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}", file_extensions=[".exr"])
        scanner = VersionScanner(source)
        versions = scanner.scan()
        self.assertEqual(len(versions), 1)
        self.assertEqual(versions[0].file_count, 10)
        # Should detect sub_sequences for multiple layers
        self.assertTrue(len(versions[0].sub_sequences) >= 1)


class TestExtractVersion(unittest.TestCase):
    """Test VersionScanner._extract_version with various patterns."""

    def _extract(self, name, pattern="_v{version}", date_format=""):
        source = WatchedSource(name="t", source_dir="/tmp",
                               version_pattern=pattern, date_format=date_format)
        scanner = VersionScanner(source)
        return scanner._extract_version(name)

    def test_standard(self):
        result = self._extract("hero_comp_v003")
        self.assertIsNotNone(result)
        ver_str, ver_num, date_str, date_sortable = result
        self.assertEqual(ver_str, "v003")
        self.assertEqual(ver_num, 3)
        self.assertIsNone(date_str)

    def test_high_version(self):
        result = self._extract("shot_v0051")
        self.assertEqual(result[1], 51)

    def test_no_match(self):
        result = self._extract("no_version_here")
        self.assertIsNone(result)

    def test_date_and_version(self):
        result = self._extract(
            "260224_shot_v03",
            pattern="{date}_shot_v{version}",
            date_format="DDMMYY")
        self.assertIsNotNone(result)
        ver_str, ver_num, date_str, date_sortable = result
        self.assertEqual(ver_num, 3)
        self.assertEqual(date_str, "260224")
        self.assertEqual(date_sortable, 20240226)

    def test_date_only(self):
        result = self._extract(
            "shot_260224",
            pattern="shot_{date}",
            date_format="DDMMYY")
        self.assertIsNotNone(result)
        ver_str, ver_num, date_str, date_sortable = result
        self.assertEqual(ver_num, 0)
        self.assertEqual(date_str, "260224")


class TestDetectSequenceFromFile(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_seq_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_single_mov(self):
        f = Path(self.tmpdir) / "shot_v001.mov"
        f.write_bytes(b"\x00" * 64)
        files, fr, fc = detect_sequence_from_file(f, [".mov"])
        self.assertEqual(len(files), 1)
        self.assertIsNone(fr)
        self.assertEqual(fc, 1)

    def test_exr_sequence(self):
        for frame in range(1001, 1011):
            (Path(self.tmpdir) / f"shot_v001.{frame:04d}.exr").write_bytes(b"\x00" * 64)
        f = Path(self.tmpdir) / "shot_v001.1005.exr"
        files, fr, fc = detect_sequence_from_file(f, [".exr"])
        self.assertEqual(fc, 10)
        self.assertIn("1001", fr)
        self.assertIn("1010", fr)

    def test_nonexistent_file(self):
        f = Path(self.tmpdir) / "nope.exr"
        files, fr, fc = detect_sequence_from_file(f, [".exr"])
        self.assertEqual(fc, 0)


class TestScanDirectoryAsVersion(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_scandir_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_scan_sequence_dir(self):
        for frame in range(1001, 1006):
            (Path(self.tmpdir) / f"shot.{frame:04d}.exr").write_bytes(b"\x00" * 64)
        files, fr, fc = scan_directory_as_version(Path(self.tmpdir), [".exr"])
        self.assertEqual(fc, 5)
        self.assertEqual(len(files), 5)

    def test_empty_dir(self):
        files, fr, fc = scan_directory_as_version(Path(self.tmpdir), [".exr"])
        self.assertEqual(fc, 0)
        self.assertEqual(len(files), 0)


class TestCreateManualVersion(unittest.TestCase):

    def test_basic(self):
        vi = create_manual_version("/tmp/custom", 99, 10, 5000,
                                   frame_range="1001-1010", frame_count=10)
        self.assertEqual(vi.version_string, "v099")
        self.assertEqual(vi.version_number, 99)
        self.assertEqual(vi.file_count, 10)
        self.assertEqual(vi.total_size_bytes, 5000)


class TestGroupFilesBySequence(unittest.TestCase):

    def test_single_sequence(self):
        files = [Path(f"shot.{f:04d}.exr") for f in range(1001, 1006)]
        groups = _group_files_by_sequence(files)
        self.assertEqual(len(groups), 1)

    def test_multiple_sequences(self):
        files = [Path(f"beauty.{f:04d}.exr") for f in range(1001, 1004)]
        files += [Path(f"alpha.{f:04d}.exr") for f in range(1001, 1004)]
        groups = _group_files_by_sequence(files)
        self.assertEqual(len(groups), 2)


class TestDetectFrameRangeForGroup(unittest.TestCase):

    def test_contiguous(self):
        files = [Path(f"shot.{f:04d}.exr") for f in range(1001, 1011)]
        fr, fc = _detect_frame_range_for_group(files)
        self.assertEqual(fc, 10)
        self.assertNotIn("gaps", fr)

    def test_with_gap(self):
        frames = [1001, 1002, 1003, 1005, 1006]  # gap at 1004
        files = [Path(f"shot.{f:04d}.exr") for f in frames]
        fr, fc = _detect_frame_range_for_group(files)
        self.assertEqual(fc, 5)
        self.assertIn("gaps detected", fr)

    def test_single_file(self):
        files = [Path("shot.1001.exr")]
        fr, fc = _detect_frame_range_for_group(files)
        self.assertIsNone(fr)
        self.assertEqual(fc, 1)

    def test_empty(self):
        fr, fc = _detect_frame_range_for_group([])
        self.assertIsNone(fr)
        self.assertEqual(fc, 0)

    def test_padding_preserved(self):
        """Frame numbers with leading zeros should show padded range."""
        files = [Path(f"shot.{f:04d}.exr") for f in range(991, 996)]
        fr, fc = _detect_frame_range_for_group(files)
        self.assertIn("0991", fr)


# ============================================================================
# Discovery
# ============================================================================

class TestDiscover(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_discovery_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_discover_versioned_dirs(self):
        _make_versioned_dirs(self.tmpdir, "hero_comp", ["v001", "v002"])
        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0].versions_found), 2)
        self.assertIn("{version}", results[0].suggested_pattern)

    def test_discover_versioned_files(self):
        _make_versioned_files(self.tmpdir, "hero_comp", ["v001", "v002"], ".mov")
        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0].versions_found), 2)

    def test_discover_nested(self):
        nested = Path(self.tmpdir) / "shots" / "hero" / "renders"
        _make_versioned_dirs(str(nested), "comp", ["v001"])
        results = discover(self.tmpdir, max_depth=4)
        self.assertEqual(len(results), 1)

    def test_discover_depth_limit(self):
        deep = Path(self.tmpdir) / "a" / "b" / "c" / "d" / "e"
        _make_versioned_dirs(str(deep), "comp", ["v001"])
        results = discover(self.tmpdir, max_depth=2)
        self.assertEqual(len(results), 0)

    def test_discover_empty_dir(self):
        results = discover(self.tmpdir, max_depth=4)
        self.assertEqual(len(results), 0)

    def test_discover_nonexistent(self):
        results = discover("/nonexistent/path", max_depth=4)
        self.assertEqual(len(results), 0)

    def test_discover_whitelist(self):
        _make_versioned_dirs(self.tmpdir, "hero_comp", ["v001"])
        sub = Path(self.tmpdir) / "other"
        _make_versioned_dirs(str(sub), "bg_grade", ["v001"])
        results = discover(self.tmpdir, max_depth=2, whitelist=["comp"])
        # Should only have results matching "comp" keyword
        self.assertTrue(len(results) >= 1)
        # The "bg_grade" in "other" subdir should be filtered out
        for r in results:
            search = f"{r.name} {r.path} {r.sample_filename}".lower()
            self.assertIn("comp", search)

    def test_discover_blacklist(self):
        _make_versioned_dirs(self.tmpdir, "hero_comp", ["v001"])
        wip = Path(self.tmpdir) / "wip_output"
        _make_versioned_dirs(str(wip), "wip_comp", ["v001"])
        results = discover(self.tmpdir, max_depth=2, blacklist=["wip"])
        paths = [r.path for r in results]
        self.assertFalse(any("wip" in p for p in paths))

    def test_discover_sample_filename(self):
        _make_versioned_dirs(self.tmpdir, "shot_comp", ["v001"])
        results = discover(self.tmpdir, max_depth=1)
        self.assertTrue(results[0].sample_filename)

    def test_discover_suggested_extensions(self):
        _make_versioned_dirs(self.tmpdir, "shot", ["v001"])
        results = discover(self.tmpdir, max_depth=1)
        self.assertIn(".exr", results[0].suggested_extensions)

    def test_discover_grouped_by_source_name(self):
        """Multiple versions of same source group into one result."""
        _make_versioned_dirs(self.tmpdir, "hero", ["v001", "v002", "v003"])
        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0].versions_found), 3)

    def test_discover_different_sources_same_dir(self):
        """Different basenames in same dir -> separate results."""
        _make_versioned_dirs(self.tmpdir, "hero_comp", ["v001"])
        _make_versioned_dirs(self.tmpdir, "hero_grade", ["v001"])
        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 2)

    def test_discover_multi_shot_flat_files(self):
        """Multiple shots as single .mov files in one folder split into
        one DiscoveryResult per shot, not a fake frame sequence."""
        for shot in ("A001C007", "A001C022", "A001C033"):
            for ver in (1, 2):
                f = (Path(self.tmpdir)
                     / f"{shot}_260401_R1WC_comp_v{ver:02d}.mov")
                f.write_bytes(b"\x00" * 128)

        results = discover(self.tmpdir, max_depth=1)

        # One DiscoveryResult per shot, each with 2 versions.
        self.assertEqual(len(results), 3)
        names = {r.name for r in results}
        # With YYMMDD detected, the date is stripped from the cluster key.
        self.assertEqual(names, {
            "A001C007_R1WC_comp",
            "A001C022_R1WC_comp",
            "A001C033_R1WC_comp",
        })
        for r in results:
            self.assertEqual(len(r.versions_found), 2)
            # sample_filename must belong to *this* cluster — otherwise the
            # scanner's _matches_basename would filter the source's own files out.
            shot_prefix = r.name.split("_", 1)[0]
            self.assertTrue(r.sample_filename.startswith(shot_prefix))
            # Each version is a single file (file_count == 1), not a 3-file
            # "frame sequence" mistakenly aggregated across shots.
            for vi in r.versions_found:
                self.assertEqual(vi.file_count, 1)

    def test_discover_single_shot_flat_files_unchanged(self):
        """Regression: a flat folder with one shot's multi-version files still
        produces a single DiscoveryResult named after the parent directory."""
        _make_versioned_files(self.tmpdir, "hero_comp",
                              ["v001", "v002", "v003"], ".mov")
        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0].versions_found), 3)
        # When only one cluster exists, the result name keeps the legacy
        # behavior of using the parent directory name.
        self.assertEqual(results[0].name, Path(self.tmpdir).name)

    def test_discover_same_shot_multi_date_clusters_together(self):
        """Same shot across different dates should land in one cluster — the
        date is stripped from the cluster key when the format is detected."""
        files = [
            "A001C007_260401_R1WC_comp_v01.mov",
            "A001C007_260402_R1WC_comp_v02.mov",
        ]
        for name in files:
            (Path(self.tmpdir) / name).write_bytes(b"\x00" * 128)

        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0].versions_found), 2)

    def test_progress_callback(self):
        _make_versioned_dirs(self.tmpdir, "shot", ["v001"])
        calls = []
        def cb(path, scanned, total):
            calls.append((path, scanned, total))
        discover(self.tmpdir, max_depth=1, progress_callback=cb)
        self.assertTrue(len(calls) > 0)


class TestFormatDiscoveryReport(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(format_discovery_report([]), "No versioned content found.")

    def test_with_results(self):
        vi = VersionInfo("v001", 1, "/tmp/v001", file_count=5,
                         total_size_bytes=1024)
        dr = DiscoveryResult(
            path="/renders", name="hero",
            versions_found=[vi],
            suggested_pattern="_v{version}",
            suggested_extensions=[".exr"])
        report = format_discovery_report([dr])
        self.assertIn("1 versioned location", report)
        self.assertIn("hero", report)


class TestDetectDateFormat(unittest.TestCase):

    def test_yymmdd(self):
        # 24 <= 31 and 02 <= 12, so heuristic picks DDMMYY for ambiguous 6-digit
        # Use a value where first two > 31 to get YYMMDD
        self.assertEqual(_detect_date_format("990226"), "YYMMDD")

    def test_ddmmyy(self):
        self.assertEqual(_detect_date_format("260224"), "DDMMYY")

    def test_yyyymmdd(self):
        self.assertEqual(_detect_date_format("20240226"), "YYYYMMDD")

    def test_ddmmyyyy(self):
        self.assertEqual(_detect_date_format("26022024"), "DDMMYYYY")


class TestIsPlausibleDate(unittest.TestCase):

    def test_valid_six_digit(self):
        self.assertTrue(_is_plausible_date("260224"))
        self.assertTrue(_is_plausible_date("240226"))

    def test_invalid_six_digit(self):
        self.assertFalse(_is_plausible_date("999999"))
        self.assertFalse(_is_plausible_date("000000"))

    def test_valid_eight_digit(self):
        self.assertTrue(_is_plausible_date("20240226"))

    def test_wrong_length(self):
        self.assertFalse(_is_plausible_date("12345"))


# ============================================================================
# Config
# ============================================================================

class TestConfig(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_config_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_project(self):
        path = create_project(
            "Test Show", self.tmpdir,
            name_whitelist=["comp"], name_blacklist=["wip"],
            task_tokens=["comp", "grade"])
        self.assertTrue(Path(path).exists())
        config = load_config(path)
        self.assertEqual(config.project_name, "Test Show")
        self.assertEqual(config.name_whitelist, ["comp"])
        self.assertEqual(config.task_tokens, ["comp", "grade"])

    def test_create_project_custom_filename(self):
        path = create_project("X", self.tmpdir, output_filename="custom.json")
        self.assertTrue(path.endswith("custom.json"))
        self.assertTrue(Path(path).exists())

    def test_save_and_reload(self):
        config = ProjectConfig(
            project_name="Roundtrip",
            default_link_mode="hardlink",
            name_blacklist=["temp"],
        )
        source = WatchedSource(
            name="Hero", source_dir=str(Path(self.tmpdir) / "renders"),
            latest_target=str(Path(self.tmpdir) / "online"),
            override_latest_target=True,
        )
        config.watched_sources.append(source)
        config_path = str(Path(self.tmpdir) / "test_lvm.json")
        save_config(config, config_path)

        loaded = load_config(config_path)
        self.assertEqual(loaded.project_name, "Roundtrip")
        self.assertEqual(loaded.default_link_mode, "hardlink")
        self.assertEqual(len(loaded.watched_sources), 1)
        self.assertEqual(loaded.watched_sources[0].name, "Hero")
        # Paths should be absolute after loading
        self.assertTrue(Path(loaded.watched_sources[0].source_dir).is_absolute())

    def test_relative_paths_in_json(self):
        """Saved JSON should contain relative paths, not absolute."""
        config = ProjectConfig(project_name="RelTest")
        source = WatchedSource(
            name="S1",
            source_dir=str(Path(self.tmpdir) / "sub" / "renders"),
            latest_target=str(Path(self.tmpdir) / "sub" / "online"),
            override_latest_target=True,
        )
        config.watched_sources.append(source)
        config_path = str(Path(self.tmpdir) / "rel_test.json")
        save_config(config, config_path)

        with open(config_path) as f:
            data = json.load(f)
        sd = data["watched_sources"][0]["source_dir"]
        self.assertFalse(Path(sd).is_absolute())

    def test_load_nonexistent_raises(self):
        with self.assertRaises(FileNotFoundError):
            load_config("/nonexistent/config.json")

    def test_project_root_separate_from_save_dir(self):
        project_root = str(Path(self.tmpdir) / "project_root")
        save_dir = str(Path(self.tmpdir) / "configs")
        path = create_project("X", self.tmpdir,
                              project_root=project_root, save_dir=save_dir)
        config = load_config(path)
        self.assertEqual(str(Path(config.project_root)),
                         str(Path(project_root).resolve()))


class TestApplyProjectDefaults(unittest.TestCase):

    def test_inherits_defaults(self):
        config = ProjectConfig(
            project_name="Test",
            default_version_pattern="custom_v{version}",
            default_file_extensions=[".dpx"],
            default_link_mode="symlink",
            default_date_format="DDMMYY",
            default_file_rename_template="{source_name}_online",
            block_incomplete_sequences=False,
            pre_promote_cmd="echo pre",
            post_promote_cmd="echo post",
        )
        source = WatchedSource(name="s1", source_dir="/tmp")
        config.watched_sources.append(source)
        apply_project_defaults(config)

        self.assertEqual(source.version_pattern, "custom_v{version}")
        self.assertEqual(source.file_extensions, [".dpx"])
        self.assertEqual(source.link_mode, "symlink")
        self.assertEqual(source.date_format, "DDMMYY")
        self.assertEqual(source.file_rename_template, "{source_name}_online")
        self.assertFalse(source.block_incomplete_sequences)
        self.assertEqual(source.pre_promote_cmd, "echo pre")
        self.assertEqual(source.post_promote_cmd, "echo post")

    def test_overrides_preserved(self):
        config = ProjectConfig(
            project_name="Test",
            default_version_pattern="default_v{version}",
            default_link_mode="copy",
            default_date_format="DDMMYY",
        )
        source = WatchedSource(
            name="s1", source_dir="/tmp",
            version_pattern="custom_v{version}",
            override_version_pattern=True,
            link_mode="hardlink",
            override_link_mode=True,
            date_format="YYMMDD",
            override_date_format=True,
        )
        config.watched_sources.append(source)
        apply_project_defaults(config)

        self.assertEqual(source.version_pattern, "custom_v{version}")
        self.assertEqual(source.link_mode, "hardlink")
        self.assertEqual(source.date_format, "YYMMDD")

    def test_latest_target_template(self):
        config = ProjectConfig(
            project_name="Test",
            latest_path_template="{project_root}/online/{source_name}",
            project_dir=str(Path(tempfile.gettempdir()) / "test_project"),
        )
        source = WatchedSource(
            name="Hero Comp", source_dir="/renders",
            sample_filename="hero_comp_v001.1001.exr",
        )
        config.watched_sources.append(source)
        apply_project_defaults(config)
        self.assertIn("hero_comp", source.latest_target)
        self.assertIn("online", source.latest_target)


class TestExpandGroupToken(unittest.TestCase):

    def test_with_group(self):
        self.assertEqual(
            _expand_group_token("{group}/renders", "INT"),
            "INT/renders")

    def test_without_group(self):
        result = _expand_group_token("{group}/renders", "")
        self.assertEqual(result, "renders")

    def test_no_token(self):
        self.assertEqual(
            _expand_group_token("renders/output", "INT"),
            "renders/output")

    def test_trailing_divider_removed(self):
        result = _expand_group_token("{group}_renders", "")
        self.assertEqual(result, "renders")


class TestResolveGroupRoot(unittest.TestCase):

    def test_with_root_dir(self):
        config = ProjectConfig(project_name="T",
                               groups={"INT": {"root_dir": "/groups/INT"}})
        self.assertEqual(_resolve_group_root(config, "INT"), "/groups/INT")

    def test_no_root_dir_fallback(self):
        config = ProjectConfig(project_name="T", project_dir="/projects")
        self.assertEqual(_resolve_group_root(config, "INT"), "/projects")

    def test_no_group(self):
        config = ProjectConfig(project_name="T", project_dir="/projects")
        self.assertEqual(_resolve_group_root(config, ""), "/projects")


# ============================================================================
# History
# ============================================================================

class TestHistoryManager(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_history_")
        self.history_path = str(Path(self.tmpdir) / ".latest_history.json")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_empty_state(self):
        hm = HistoryManager(self.history_path)
        self.assertIsNone(hm.get_current())
        self.assertEqual(hm.get_history(), [])

    def test_record_and_retrieve(self):
        hm = HistoryManager(self.history_path)
        entry = HistoryEntry("v001", "/renders/v001", "artist", "2024-01-15T10:00:00",
                             file_count=10)
        hm.record_promotion(entry)
        current = hm.get_current()
        self.assertEqual(current.version, "v001")
        self.assertEqual(current.set_by, "artist")
        self.assertEqual(len(hm.get_history()), 1)

    def test_multiple_promotions(self):
        hm = HistoryManager(self.history_path)
        for i in range(1, 4):
            entry = HistoryEntry(f"v{i:03d}", f"/renders/v{i:03d}",
                                 "artist", f"2024-01-{i:02d}T10:00:00")
            hm.record_promotion(entry)
        self.assertEqual(hm.get_current().version, "v003")
        history = hm.get_history()
        self.assertEqual(len(history), 3)
        self.assertEqual(history[0].version, "v003")  # newest first

    def test_history_cap(self):
        hm = HistoryManager(self.history_path)
        for i in range(MAX_HISTORY_ENTRIES + 20):
            entry = HistoryEntry(f"v{i:03d}", "/tmp", "x", f"2024-01-01T{i:02d}:00:00")
            hm.record_promotion(entry)
        self.assertEqual(len(hm.get_history()), MAX_HISTORY_ENTRIES)

    def test_corrupt_history_recovery(self):
        """Corrupt JSON should be backed up and fresh state returned."""
        Path(self.history_path).write_text("{corrupt json!!!")
        hm = HistoryManager(self.history_path)
        data = hm.load()
        self.assertIsNone(data["current"])
        backup = Path(self.history_path).with_suffix(".json.bak")
        self.assertTrue(backup.exists())

    def test_verify_integrity_clean(self):
        hm = HistoryManager(self.history_path)
        entry = HistoryEntry("v001", "/tmp", "x", "2024-01-01", file_count=5)
        hm.record_promotion(entry)
        result = hm.verify_integrity(["a.exr", "b.exr", "c.exr", "d.exr", "e.exr"])
        self.assertTrue(result["valid"])

    def test_verify_integrity_count_mismatch(self):
        hm = HistoryManager(self.history_path)
        entry = HistoryEntry("v001", "/tmp", "x", "2024-01-01", file_count=5)
        hm.record_promotion(entry)
        result = hm.verify_integrity(["a.exr", "b.exr"])
        self.assertFalse(result["valid"])
        self.assertIn("5", result["message"])

    def test_verify_integrity_no_history_no_files(self):
        hm = HistoryManager(self.history_path)
        result = hm.verify_integrity([])
        self.assertTrue(result["valid"])

    def test_verify_integrity_no_history_has_files(self):
        hm = HistoryManager(self.history_path)
        result = hm.verify_integrity(["a.exr"])
        self.assertFalse(result["valid"])

    def test_verify_integrity_has_history_no_files(self):
        hm = HistoryManager(self.history_path)
        entry = HistoryEntry("v001", "/tmp", "x", "2024-01-01", file_count=5)
        hm.record_promotion(entry)
        result = hm.verify_integrity([])
        self.assertFalse(result["valid"])

    def test_cache_avoids_reread(self):
        """Second load() call uses cache when file unchanged."""
        hm = HistoryManager(self.history_path)
        entry = HistoryEntry("v001", "/tmp", "x", "2024-01-01")
        hm.record_promotion(entry)
        # First load populates cache
        hm.load()
        # Modify cache directly — if second load re-reads, it would not see this
        hm._cache["current"] = "sentinel"
        data = hm.load()
        self.assertEqual(data["current"], "sentinel")

    def test_pinned_entry(self):
        hm = HistoryManager(self.history_path)
        entry = HistoryEntry("v001", "/tmp", "x", "2024-01-01", pinned=True)
        hm.record_promotion(entry)
        current = hm.get_current()
        self.assertTrue(current.pinned)


class TestHasNewerVersionsSince(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_newer_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_newer(self):
        entry = HistoryEntry("v002", "/tmp", "x", "2099-01-01T00:00:00")
        v1 = VersionInfo("v001", 1, str(Path(self.tmpdir) / "v001"))
        v2 = VersionInfo("v002", 2, str(Path(self.tmpdir) / "v002"))
        # Create source paths
        Path(v1.source_path).mkdir(parents=True, exist_ok=True)
        Path(v2.source_path).mkdir(parents=True, exist_ok=True)
        self.assertFalse(has_newer_versions_since(entry, [v1, v2]))

    def test_none_current(self):
        self.assertFalse(has_newer_versions_since(None, []))

    def test_empty_versions(self):
        entry = HistoryEntry("v001", "/tmp", "x", "2024-01-01T00:00:00")
        self.assertFalse(has_newer_versions_since(entry, []))


# ============================================================================
# Promoter
# ============================================================================

class TestPromoter(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_promoter_")
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

    def test_promote_sequence(self):
        """Promote a versioned directory of EXR frames."""
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1005)

        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir), frame_range="1001-1005",
                         frame_count=5, file_count=5)
        entry = promoter.promote(vi, user="test_user")

        self.assertEqual(entry.version, "v001")
        self.assertEqual(entry.set_by, "test_user")
        # Target should have files
        target_files = list(Path(self.target_dir).glob("*.exr"))
        self.assertEqual(len(target_files), 5)

    def test_promote_single_file(self):
        f = Path(self.source_dir) / "shot_v001.mov"
        f.write_bytes(b"\x00" * 128)

        source = self._make_source(file_extensions=[".mov"])
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(f), file_count=1)
        entry = promoter.promote(vi, user="test_user")

        target_files = list(Path(self.target_dir).glob("*.mov"))
        self.assertEqual(len(target_files), 1)

    def test_promote_overwrites_existing(self):
        """Second promote replaces first promote's files."""
        for v in ["v001", "v002"]:
            vdir = Path(self.source_dir) / f"shot_{v}"
            _make_exr_sequence(str(vdir), "shot", v, 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        vi1 = VersionInfo("v001", 1, str(Path(self.source_dir) / "shot_v001"),
                          file_count=3)
        promoter.promote(vi1, user="x")
        vi2 = VersionInfo("v002", 2, str(Path(self.source_dir) / "shot_v002"),
                          file_count=3)
        promoter.promote(vi2, user="x")

        # History should show v002 as current
        self.assertEqual(promoter.get_current_version().version, "v002")
        self.assertEqual(len(promoter.get_history()), 2)

    def test_promote_nonexistent_source_raises(self):
        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, "/nonexistent/path")
        with self.assertRaises(PromotionError):
            promoter.promote(vi)

    def test_promote_no_target_raises(self):
        with self.assertRaises(PromotionError):
            ws = WatchedSource(name="T", source_dir=self.source_dir, latest_target="")
            Promoter(ws)

    def test_promote_records_mtimes(self):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir), file_count=3)
        entry = promoter.promote(vi, user="x")

        self.assertIsNotNone(entry.source_mtime)
        self.assertIsNotNone(entry.target_mtime)

    def test_verify_clean_after_promote(self):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir), frame_count=3, file_count=3)
        promoter.promote(vi, user="x")

        result = promoter.verify()
        self.assertTrue(result["valid"])

    def test_verify_detects_file_count_mismatch(self):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir), frame_count=3, file_count=3)
        promoter.promote(vi, user="x")

        # Delete one target file
        target_files = sorted(Path(self.target_dir).glob("*.exr"))
        target_files[0].unlink()

        result = promoter.verify()
        self.assertFalse(result["valid"])

    def test_verify_detects_target_tamper(self):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir), frame_count=3, file_count=3)
        promoter.promote(vi, user="x")

        # Tamper with a target file (change mtime significantly)
        time.sleep(0.1)
        target_files = sorted(Path(self.target_dir).glob("*.exr"))
        future_time = time.time() + 3600
        os.utime(target_files[0], (future_time, future_time))

        result = promoter.verify()
        self.assertFalse(result["valid"])

    def test_dry_run(self):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir), file_count=3)
        result = promoter.dry_run(vi)

        self.assertEqual(result["total_files"], 3)
        self.assertEqual(result["link_mode"], "copy")
        self.assertEqual(len(result["file_map"]), 3)
        # Target should NOT have files yet
        self.assertEqual(len(list(Path(self.target_dir).glob("*.exr"))), 0)

    def test_progress_callback(self):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir), file_count=3)

        calls = []
        def on_progress(current, total, filename):
            calls.append((current, total, filename))

        promoter.promote(vi, user="x", progress_callback=on_progress)
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[-1][0], 3)
        self.assertEqual(calls[-1][1], 3)

    def test_block_incomplete_sequence(self):
        source = self._make_source()
        source.block_incomplete_sequences = True
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(Path(self.source_dir) / "shot_v001"),
                         frame_range="1001-1010 (8/10 frames, gaps detected)",
                         file_count=8)
        Path(vi.source_path).mkdir(parents=True, exist_ok=True)
        with self.assertRaises(PromotionError) as ctx:
            promoter.promote(vi)
        self.assertIn("gaps", str(ctx.exception))

    def test_force_overrides_incomplete_block(self):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1003)

        source = self._make_source()
        source.block_incomplete_sequences = True
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir),
                         frame_range="1001-1005 (3/5 frames, gaps detected)",
                         file_count=3)
        # Should not raise with force=True
        entry = promoter.promote(vi, force=True, user="x")
        self.assertEqual(entry.version, "v001")

    def test_pinned_promotion(self):
        vdir = Path(self.source_dir) / "shot_v001"
        _make_exr_sequence(str(vdir), "shot", "v001", 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        vi = VersionInfo("v001", 1, str(vdir), file_count=3)
        entry = promoter.promote(vi, user="x", pinned=True)
        self.assertTrue(entry.pinned)

    def test_verify_flat_layout_no_false_stale(self):
        """New versions rendered into a flat folder must not falsely flag
        the promoted version as stale."""
        # Create flat EXR sequences (all versions in source_dir directly)
        _make_exr_sequence(self.source_dir, "shot", "v001", 1001, 1003)
        _make_exr_sequence(self.source_dir, "shot", "v002", 1001, 1003)

        source = self._make_source()
        promoter = Promoter(source)
        # source_path is the source_dir itself for flat layouts
        vi = VersionInfo("v002", 2, self.source_dir, frame_count=3, file_count=3)
        promoter.promote(vi, user="x")

        # Verify should be clean right after promotion
        result = promoter.verify()
        self.assertTrue(result["valid"], f"Expected clean verify, got: {result}")

        # Simulate rendering v003 into the same flat folder (newer mtime)
        v003_files = _make_exr_sequence(self.source_dir, "shot", "v003", 1001, 1003)
        future_time = time.time() + 3600
        for f in v003_files:
            os.utime(f, (future_time, future_time))

        # Verify should still be clean — v003 files must not affect v002 check
        result = promoter.verify()
        self.assertTrue(result["valid"], f"False stale from new version: {result}")

        # But if v002's own files are re-rendered, verify should detect stale
        v002_files = sorted(
            f for f in Path(self.source_dir).iterdir()
            if f.is_file() and "_v002." in f.name
        )
        for f in v002_files:
            os.utime(f, (future_time, future_time))

        result = promoter.verify()
        self.assertFalse(result["valid"])
        self.assertIn("modified since promotion", result["message"])


class TestRemapFilename(unittest.TestCase):
    """Test Promoter._remap_filename."""

    def _make_promoter(self, template="", sample_filename="shot_comp_v001.1001.exr",
                       task_tokens=None, date_format=""):
        tmpdir = tempfile.mkdtemp(prefix="lvm_remap_")
        self._tmpdir = tmpdir
        target = str(Path(tmpdir) / "target")
        Path(target).mkdir()
        source = WatchedSource(
            name="Test", source_dir=str(Path(tmpdir) / "src"),
            latest_target=target,
            file_rename_template=template,
            sample_filename=sample_filename,
            date_format=date_format,
            override_latest_target=True,
        )
        return Promoter(source, task_tokens=task_tokens or [])

    def tearDown(self):
        if hasattr(self, "_tmpdir"):
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_no_template_strips_version(self):
        p = self._make_promoter()
        result = p._remap_filename("shot_comp_v003.1001.exr")
        self.assertNotIn("v003", result)
        self.assertIn("1001", result)
        self.assertIn(".exr", result)

    def test_template_source_name(self):
        p = self._make_promoter(template="{source_name}")
        result = p._remap_filename("shot_comp_v003.1001.exr")
        self.assertEqual(result, "shot_comp.1001.exr")

    def test_template_source_basename(self):
        p = self._make_promoter(template="{source_basename}_latest",
                                task_tokens=["comp"])
        result = p._remap_filename("shot_comp_v003.1001.exr")
        self.assertEqual(result, "shot_latest.1001.exr")

    def test_template_with_single_file(self):
        p = self._make_promoter(template="{source_name}_latest",
                                sample_filename="shot_comp_v001.mov")
        result = p._remap_filename("shot_comp_v003.mov")
        self.assertEqual(result, "shot_comp_latest.mov")

    def test_layer_suffix_preserved(self):
        p = self._make_promoter(template="{source_name}_latest")
        result = p._remap_filename("shot_comp_alpha_v003.1001.exr")
        self.assertIn("alpha", result)
        self.assertIn("latest", result)

    def test_no_template_strips_date(self):
        p = self._make_promoter(date_format="DDMMYY",
                                sample_filename="260224_shot_v001.mov")
        result = p._remap_filename("260224_shot_v001.mov")
        self.assertNotIn("260224", result)
        self.assertNotIn("v001", result)


class TestHasFrameGaps(unittest.TestCase):

    def test_no_gaps(self):
        vi = VersionInfo("v001", 1, "/tmp", frame_range="1001-1010")
        self.assertFalse(has_frame_gaps(vi))

    def test_with_gaps(self):
        vi = VersionInfo("v001", 1, "/tmp",
                         frame_range="1001-1010 (8/10 frames, gaps detected)")
        self.assertTrue(has_frame_gaps(vi))

    def test_no_frame_range(self):
        vi = VersionInfo("v001", 1, "/tmp")
        self.assertFalse(has_frame_gaps(vi))


class TestGenerateReport(unittest.TestCase):

    def test_basic_report(self):
        entry = HistoryEntry("v003", "/renders/v003", "artist",
                             "2024-01-15T10:30:00",
                             frame_range="1001-1100", frame_count=100,
                             file_count=100)
        source = WatchedSource(name="Hero", source_dir="/renders",
                               latest_target="/online",
                               override_latest_target=True)
        report = generate_report(entry, source)
        self.assertEqual(report["version"], "v003")
        self.assertEqual(report["source_name"], "Hero")
        self.assertEqual(report["set_by"], "artist")
        self.assertEqual(report["link_mode"], "copy")

    def test_report_with_dry_run(self):
        entry = HistoryEntry("v001", "/tmp", "x", "2024-01-01")
        source = WatchedSource(name="T", source_dir="/tmp",
                               latest_target="/online",
                               override_latest_target=True)
        dry_run = {
            "file_map": [{"source": "/a.exr", "target_name": "a.exr", "size_bytes": 100}],
            "total_size_bytes": 100,
        }
        report = generate_report(entry, source, dry_run_data=dry_run)
        self.assertEqual(len(report["file_map"]), 1)
        self.assertEqual(report["total_size_bytes"], 100)

    def test_report_json_serializable(self):
        entry = HistoryEntry("v001", "/tmp", "x", "2024-01-01",
                             source_mtime=1700000000.0, target_mtime=1700000001.0)
        source = WatchedSource(name="T", source_dir="/tmp",
                               latest_target="/online",
                               override_latest_target=True)
        report = generate_report(entry, source)
        json.dumps(report)  # Should not raise


class TestObsoleteLayerDetection(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_obsolete_")
        self.source_dir = str(Path(self.tmpdir) / "renders")
        self.target_dir = str(Path(self.tmpdir) / "online")
        Path(self.source_dir).mkdir()
        Path(self.target_dir).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_obsolete_when_target_empty(self):
        vdir = Path(self.source_dir) / "shot_v002"
        _make_exr_sequence(str(vdir), "shot", "v002", 1001, 1003)

        source = WatchedSource(
            name="T", source_dir=self.source_dir,
            latest_target=self.target_dir,
            file_extensions=[".exr"],
            override_latest_target=True,
        )
        promoter = Promoter(source)
        vi = VersionInfo("v002", 2, str(vdir), file_count=3)
        obsolete = promoter.detect_obsolete_layers(vi)
        self.assertEqual(len(obsolete), 0)

    def test_detect_removed_layer(self):
        """Target has beauty+alpha, new version only has beauty -> alpha is obsolete."""
        # Set up target with two layers
        for frame in range(1001, 1004):
            (Path(self.target_dir) / f"shot_beauty.{frame:04d}.exr").write_bytes(b"\x00" * 64)
            (Path(self.target_dir) / f"shot_alpha.{frame:04d}.exr").write_bytes(b"\x00" * 64)

        # New version has only beauty
        vdir = Path(self.source_dir) / "shot_v002"
        vdir.mkdir(parents=True)
        for frame in range(1001, 1004):
            (vdir / f"shot_beauty.{frame:04d}.exr").write_bytes(b"\x00" * 64)

        source = WatchedSource(
            name="T", source_dir=self.source_dir,
            latest_target=self.target_dir,
            file_extensions=[".exr"],
            override_latest_target=True,
        )
        promoter = Promoter(source)
        vi = VersionInfo("v002", 2, str(vdir), file_count=3)
        obsolete = promoter.detect_obsolete_layers(vi)
        self.assertEqual(len(obsolete), 1)
        self.assertIn("alpha", obsolete[0]["name"])


class TestKeepLayers(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_keep_")
        self.source_dir = str(Path(self.tmpdir) / "renders")
        self.target_dir = str(Path(self.tmpdir) / "online")
        Path(self.source_dir).mkdir()
        Path(self.target_dir).mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_keep_layers_preserves_files(self):
        """Files from kept layers survive the promotion clear."""
        # Seed target with alpha layer
        for frame in range(1001, 1004):
            (Path(self.target_dir) / f"shot_alpha.{frame:04d}.exr").write_bytes(b"\x00" * 64)

        # New version has beauty only
        vdir = Path(self.source_dir) / "shot_v002"
        vdir.mkdir(parents=True)
        for frame in range(1001, 1004):
            (vdir / f"shot_beauty.{frame:04d}.exr").write_bytes(b"\x00" * 64)

        source = WatchedSource(
            name="T", source_dir=self.source_dir,
            latest_target=self.target_dir,
            file_extensions=[".exr"],
            override_latest_target=True,
        )
        promoter = Promoter(source)
        vi = VersionInfo("v002", 2, str(vdir), file_count=3)
        # Keep the alpha layer
        promoter.promote(vi, user="x", keep_layers={"shot_alpha."})

        target_files = sorted(f.name for f in Path(self.target_dir).glob("*.exr"))
        alpha_files = [f for f in target_files if "alpha" in f]
        beauty_files = [f for f in target_files if "beauty" in f]
        self.assertEqual(len(alpha_files), 3)
        self.assertEqual(len(beauty_files), 3)


# ============================================================================
# End-to-end integration (self-contained)
# ============================================================================

class TestEndToEnd(unittest.TestCase):
    """Full workflow: create project -> discover -> promote -> verify."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="lvm_e2e_")
        self.renders = str(Path(self.tmpdir) / "renders")
        self.online = str(Path(self.tmpdir) / "online")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_full_workflow(self):
        # 1. Create version structure
        _make_versioned_dirs(self.renders, "hero_comp", ["v001", "v002", "v003"],
                            start=1001, end=1005)

        # 2. Discover
        results = discover(self.renders, max_depth=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(len(results[0].versions_found), 3)

        # 3. Create project config
        config_path = create_project(
            "E2E Test", self.tmpdir,
            task_tokens=["comp"],
            name_whitelist=["comp"])
        config = load_config(config_path)

        # 4. Add source from discovery
        source = WatchedSource(
            name="Hero Comp",
            source_dir=self.renders,
            version_pattern=results[0].suggested_pattern,
            file_extensions=results[0].suggested_extensions,
            latest_target=self.online,
            sample_filename=results[0].sample_filename,
            override_latest_target=True,
            override_version_pattern=True,
            override_file_extensions=True,
        )
        config.watched_sources.append(source)
        save_config(config, config_path)

        # 5. Reload and apply defaults
        config = load_config(config_path)
        source = config.watched_sources[0]

        # 6. Scan
        scanner = VersionScanner(source, task_tokens=config.task_tokens)
        versions = scanner.scan()
        self.assertEqual(len(versions), 3)

        # 7. Promote latest
        promoter = Promoter(source, task_tokens=config.task_tokens)
        latest = versions[-1]
        entry = promoter.promote(latest, user="e2e_test")
        self.assertEqual(entry.version, "v003")

        # 8. Verify
        result = promoter.verify()
        self.assertTrue(result["valid"])

        # 9. Check target files exist
        target_files = list(Path(self.online).glob("*.exr"))
        self.assertEqual(len(target_files), 5)

    def test_promote_then_upgrade(self):
        """Promote v001, then upgrade to v002 — verify target updated."""
        _make_versioned_dirs(self.renders, "shot", ["v001", "v002"],
                            start=1001, end=1003)

        source = WatchedSource(
            name="Shot", source_dir=self.renders,
            latest_target=self.online,
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            override_latest_target=True,
        )

        scanner = VersionScanner(source)
        versions = scanner.scan()

        promoter = Promoter(source)
        promoter.promote(versions[0], user="x")
        self.assertEqual(promoter.get_current_version().version, "v001")

        promoter.promote(versions[1], user="x")
        self.assertEqual(promoter.get_current_version().version, "v002")
        self.assertEqual(len(promoter.get_history()), 2)

    def test_discover_then_scan_roundtrip(self):
        """Discovery results can be used to configure a scanner."""
        _make_versioned_dirs(self.renders, "hero_comp", ["v001", "v002"])

        results = discover(self.renders, max_depth=1)
        dr = results[0]

        source = WatchedSource(
            name="test",
            source_dir=self.renders,
            version_pattern=dr.suggested_pattern,
            file_extensions=dr.suggested_extensions,
        )
        scanner = VersionScanner(source)
        versions = scanner.scan()
        self.assertEqual(len(versions), 2)


if __name__ == "__main__":
    unittest.main()
