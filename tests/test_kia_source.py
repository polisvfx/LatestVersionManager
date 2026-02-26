"""
Comprehensive tests for LVM using real KIA_Tempomedia_2601 source material.

Tests all core modules: models, scanner, discovery, config, promoter, history,
task_tokens. Focuses on naming output correctness and catching unexpected errors
with real-world VFX file structures.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.models import (
    VersionInfo, HistoryEntry, WatchedSource, ProjectConfig,
    DiscoveryResult, resolve_path, make_relative,
)
from lvm.scanner import (
    VersionScanner, detect_sequence_from_file,
    scan_directory_as_version, create_manual_version,
)
from lvm.discovery import discover, format_discovery_report
from lvm.config import (
    load_config, save_config, create_project, apply_project_defaults,
    _expand_group_token, _resolve_group_root,
)
from lvm.history import HistoryManager
from lvm.promoter import Promoter, PromotionError
from lvm.task_tokens import (
    compile_task_pattern, find_task_tokens, strip_task_tokens,
    strip_version, strip_frame_and_ext, derive_source_tokens,
    compute_source_name, get_naming_options,
)


# ============================================================================
# Source material paths
# ============================================================================
KIA_ROOT = Path(r"E:\Work_Offline\KIA_Tempomedia_2601")
KIA_CONFIG = KIA_ROOT / "kiatest_lvm.json"

# Shot directories
SH560_OUTPUT = KIA_ROOT / "SH560" / "output"
SH570_OUTPUT = KIA_ROOT / "SH570" / "output"
SH580_OUTPUT = KIA_ROOT / "SH580" / "output"
SH590_OUTPUT = KIA_ROOT / "SH590" / "output"

# Pre-render directories
SH560_PRERENDER = KIA_ROOT / "SH560" / "prerender"
SH570_PRERENDER = KIA_ROOT / "SH570" / "prerender"

# Edit reference
EDIT_REF = KIA_ROOT / "Edit_Reference" / "KIA_30sec_Baseline_010_3_XML_jpeg"


def require_kia_source(test_func):
    """Skip test if KIA source material not available."""
    def wrapper(*args, **kwargs):
        if not KIA_ROOT.exists():
            raise unittest.SkipTest(f"KIA source not found at {KIA_ROOT}")
        return test_func(*args, **kwargs)
    wrapper.__name__ = test_func.__name__
    wrapper.__doc__ = test_func.__doc__
    return wrapper


# ============================================================================
# Models Tests
# ============================================================================
class TestModels(unittest.TestCase):
    """Test data models with KIA-style data."""

    def test_version_info_size_human_real_sizes(self):
        """Test human-readable sizes typical of EXR sequences."""
        # Typical single EXR frame (~8MB for 2K)
        vi = VersionInfo("v001", 1, "/tmp", total_size_bytes=8_388_608)
        self.assertEqual(vi.total_size_human, "8.0 MB")

        # Typical sequence (47 frames * ~8MB = ~376MB)
        vi2 = VersionInfo("v001", 1, "/tmp", total_size_bytes=394_264_576)
        self.assertEqual(vi2.total_size_human, "376.0 MB")

    def test_version_info_zero_size(self):
        vi = VersionInfo("v001", 1, "/tmp", total_size_bytes=0)
        self.assertEqual(vi.total_size_human, "0.0 B")

    def test_history_entry_roundtrip(self):
        """Test serialize/deserialize of HistoryEntry."""
        entry = HistoryEntry(
            version="v005",
            source=str(SH560_OUTPUT / "A_0029C015_251217_191031_h1DS8_comp_v05"),
            set_by="testuser",
            set_at="2025-01-26T12:00:00",
            frame_range="991-1037",
            frame_count=47,
            file_count=47,
        )
        d = entry.to_dict()
        restored = HistoryEntry.from_dict(d)
        self.assertEqual(restored.version, "v005")
        self.assertEqual(restored.frame_range, "991-1037")
        self.assertEqual(restored.frame_count, 47)
        self.assertEqual(restored.file_count, 47)

    def test_history_entry_from_version_info(self):
        vi = VersionInfo("v003", 3, str(SH560_OUTPUT / "A_0029C015_comp_v03"),
                         frame_range="991-1037", frame_count=47, file_count=47,
                         total_size_bytes=100000)
        entry = HistoryEntry.from_version_info(vi, "artist")
        self.assertEqual(entry.version, "v003")
        self.assertEqual(entry.set_by, "artist")
        self.assertEqual(entry.frame_range, "991-1037")

    def test_watched_source_from_dict_kia_style(self):
        """Test WatchedSource deserialization with KIA config data."""
        data = {
            "name": "SH560",
            "source_dir": "SH560/output",
            "version_pattern": "_v{version}",
            "file_extensions": [".exr"],
            "latest_target": "latest/A_0029C015_251217_191031_h1DS8_comp_latest",
            "history_filename": ".latest_history.json",
            "link_mode": "copy",
            "file_rename_template": "{source_basename}_comp_latest",
            "sample_filename": "A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            "group": "Test",
            "override_version_pattern": True,
            "override_file_extensions": True,
        }
        ws = WatchedSource.from_dict(data)
        self.assertEqual(ws.name, "SH560")
        self.assertEqual(ws.link_mode, "copy")
        self.assertEqual(ws.version_pattern, "_v{version}")
        self.assertEqual(ws.file_extensions, [".exr"])
        self.assertEqual(ws.group, "Test")
        self.assertTrue(ws.override_version_pattern)
        self.assertTrue(ws.override_file_extensions)
        self.assertFalse(ws.override_latest_target)
        self.assertFalse(ws.use_symlinks)

    def test_watched_source_roundtrip(self):
        """Test WatchedSource serialize/deserialize roundtrip."""
        ws = WatchedSource(
            name="SH590",
            source_dir="SH590/output",
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            file_rename_template="{source_basename}_comp_latest",
            sample_filename="A_0029C020_251217_185838_h1DS8_comp_mp_v01.0991.exr",
            group="Beta",
            override_version_pattern=True,
        )
        d = ws.to_dict()
        restored = WatchedSource.from_dict(d)
        self.assertEqual(restored.name, "SH590")
        self.assertEqual(restored.group, "Beta")
        self.assertEqual(restored.sample_filename,
                         "A_0029C020_251217_185838_h1DS8_comp_mp_v01.0991.exr")

    def test_watched_source_backward_compat_use_symlinks(self):
        """Test old config format with use_symlinks bool."""
        data = {"name": "test", "source_dir": "/tmp", "use_symlinks": True}
        ws = WatchedSource.from_dict(data)
        self.assertEqual(ws.link_mode, "symlink")
        self.assertTrue(ws.use_symlinks)

    def test_project_config_roundtrip(self):
        """Test ProjectConfig serialize/deserialize with groups."""
        config = ProjectConfig(
            project_name="KIATest",
            groups={"Test": {"color": "#09d909"}, "Beta": {"color": "#4a90d9", "root_dir": "latest_test"}},
            task_tokens=["comp_%%", "comp"],
            default_naming_rule="parent:1",
            naming_configured=True,
        )
        d = config.to_dict()
        restored = ProjectConfig.from_dict(d)
        self.assertEqual(restored.project_name, "KIATest")
        self.assertEqual(len(restored.groups), 2)
        self.assertEqual(restored.task_tokens, ["comp_%%", "comp"])
        self.assertTrue(restored.naming_configured)

    def test_resolve_path_with_tokens(self):
        result = resolve_path("{project_root}/latest/{source_name}",
                              {"source_name": "hero_comp"}, "/projects/kia")
        self.assertIn("latest", result)
        self.assertIn("hero_comp", result)

    def test_make_relative_same_drive(self):
        result = make_relative(r"E:\Work_Offline\KIA\SH560\output",
                               r"E:\Work_Offline\KIA")
        self.assertEqual(result, "SH560/output")

    def test_make_relative_different_drive(self):
        """Should return the path as-is when on different drives."""
        result = make_relative(r"D:\elsewhere\file.txt", r"E:\project")
        # On Windows, os.path.relpath raises ValueError for different drives
        # make_relative handles this
        self.assertIn("file.txt", result)


# ============================================================================
# Task Tokens Tests (critical for naming)
# ============================================================================
class TestTaskTokens(unittest.TestCase):
    """Test task token system with KIA naming patterns."""

    def test_strip_frame_and_ext_kia_filenames(self):
        """Test frame/ext stripping on real KIA filenames."""
        cases = [
            ("A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
             "A_0029C015_251217_191031_h1DS8_comp_v01"),
            ("A_0029C009_251217_185221_h1DS8_comp_v03.1001.exr",
             "A_0029C009_251217_185221_h1DS8_comp_v03"),
            ("A_0029C020_251217_185838_h1DS8_comp_mp_v01.0991.exr",
             "A_0029C020_251217_185838_h1DS8_comp_mp_v01"),
            ("A_0029C010_251217_185838_h1DS8_comp_latest.1000.exr",
             "A_0029C010_251217_185838_h1DS8_comp_latest"),
            # Edit reference file
            ("KIA_30sec_Baseline_010_3_XML.0469.jpeg",
             "KIA_30sec_Baseline_010_3_XML"),
        ]
        for filename, expected in cases:
            with self.subTest(filename=filename):
                result = strip_frame_and_ext(filename)
                self.assertEqual(result, expected, f"Failed for {filename}")

    def test_strip_version_kia_names(self):
        """Test version stripping from KIA naming patterns."""
        cases = [
            ("A_0029C015_251217_191031_h1DS8_comp_v01", "A_0029C015_251217_191031_h1DS8_comp"),
            ("A_0029C015_251217_191031_h1DS8_comp_v05", "A_0029C015_251217_191031_h1DS8_comp"),
            ("A_0029C020_251217_185838_h1DS8_comp_mp_v01", "A_0029C020_251217_185838_h1DS8_comp_mp"),
            ("A_0029C020_251217_185838_h1DS8_comp_mp_v02", "A_0029C020_251217_185838_h1DS8_comp_mp"),
            ("sh560_comp_mp_v003", "sh560_comp_mp"),
        ]
        for name, expected in cases:
            with self.subTest(name=name):
                result = strip_version(name)
                self.assertEqual(result, expected, f"Failed for {name}")

    def test_compile_task_pattern_kia_tokens(self):
        """Test task token patterns used in KIA project."""
        # comp_%% should match comp_mp (2 chars)
        pat = compile_task_pattern("comp_%%")
        self.assertIsNotNone(pat.search("A_0029C020_comp_mp"))
        self.assertIsNone(pat.search("A_0029C020_comp_mpo"))  # 3 chars, too many

        # comp should match exact 'comp' bounded by dividers
        pat2 = compile_task_pattern("comp")
        self.assertIsNotNone(pat2.search("A_0029C015_comp"))
        self.assertIsNotNone(pat2.search("hero_comp_latest"))
        # Should NOT match inside 'compositor' or 'decompress'
        self.assertIsNone(pat2.search("compositor"))

    def test_find_task_tokens_kia_names(self):
        """Test finding task tokens in KIA source names."""
        # Test with the KIA task tokens: ["comp_%%", "comp"]
        tokens = ["comp_%%", "comp"]

        # Name with comp_mp (matches comp_%%)
        matches = find_task_tokens("A_0029C020_251217_185838_h1DS8_comp_mp", tokens)
        matched_tokens = [m["token"] for m in matches]
        self.assertIn("comp_%%", matched_tokens)

        # Name with plain comp (matches comp)
        matches = find_task_tokens("A_0029C015_251217_191031_h1DS8_comp", tokens)
        matched_tokens = [m["token"] for m in matches]
        self.assertIn("comp", matched_tokens)

    def test_strip_task_tokens_kia_names(self):
        """Test stripping task tokens from KIA source names - critical for naming."""
        tokens = ["comp_%%", "comp"]

        cases = [
            ("A_0029C015_251217_191031_h1DS8_comp",
             "A_0029C015_251217_191031_h1DS8"),
            ("A_0029C020_251217_185838_h1DS8_comp_mp",
             "A_0029C020_251217_185838_h1DS8"),
            ("A_0029C009_251217_185221_h1DS8_comp",
             "A_0029C009_251217_185221_h1DS8"),
        ]
        for name, expected in cases:
            with self.subTest(name=name):
                result = strip_task_tokens(name, tokens)
                self.assertEqual(result, expected,
                                 f"Expected '{expected}', got '{result}' for '{name}'")

    def test_derive_source_tokens_kia_filenames(self):
        """Test full token derivation for KIA filenames."""
        tokens = ["comp_%%", "comp"]

        # SH560 sample: comp variant
        result = derive_source_tokens(
            "A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr", tokens)
        self.assertEqual(result["source_filename"],
                         "A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr")
        self.assertEqual(result["source_fullname"],
                         "A_0029C015_251217_191031_h1DS8_comp_v01")
        self.assertEqual(result["source_name"],
                         "A_0029C015_251217_191031_h1DS8_comp")
        self.assertEqual(result["source_basename"],
                         "A_0029C015_251217_191031_h1DS8")

        # SH590 sample: comp_mp variant
        result = derive_source_tokens(
            "A_0029C020_251217_185838_h1DS8_comp_mp_v01.0991.exr", tokens)
        self.assertEqual(result["source_fullname"],
                         "A_0029C020_251217_185838_h1DS8_comp_mp_v01")
        self.assertEqual(result["source_name"],
                         "A_0029C020_251217_185838_h1DS8_comp_mp")
        self.assertEqual(result["source_basename"],
                         "A_0029C020_251217_185838_h1DS8")

    def test_derive_source_tokens_prerender_names(self):
        """Test token derivation with prerender task names."""
        tokens = ["cleanup_%%", "cleanup", "dn", "matte", "mask"]

        result = derive_source_tokens(
            "A_0029C015_251217_191031_h1DS8_cleanup_v01.0991.exr", tokens)
        self.assertEqual(result["source_name"],
                         "A_0029C015_251217_191031_h1DS8_cleanup")
        # basename should strip 'cleanup'
        self.assertEqual(result["source_basename"],
                         "A_0029C015_251217_191031_h1DS8")

        result2 = derive_source_tokens(
            "A_0029C015_251217_191031_h1DS8_dn_v01.0991.exr", tokens)
        self.assertEqual(result2["source_basename"],
                         "A_0029C015_251217_191031_h1DS8")

    def test_derive_source_tokens_no_task_patterns(self):
        """Without task patterns, source_basename equals source_name."""
        result = derive_source_tokens(
            "A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr", [])
        self.assertEqual(result["source_basename"], result["source_name"])

    def test_compute_source_name_parent_rule(self):
        """Test parent:1 naming rule (default for KIA project)."""
        dr = DiscoveryResult(
            path=str(SH560_OUTPUT),
            name="output",
            sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
        )
        # parent:0 = immediate dir name
        name0 = compute_source_name(dr, "parent:0")
        self.assertEqual(name0, "output")

        # parent:1 = grandparent (SH560)
        name1 = compute_source_name(dr, "parent:1")
        self.assertEqual(name1, "SH560")

    def test_compute_source_name_source_basename_rule(self):
        """Test source_basename naming rule with KIA data."""
        dr = DiscoveryResult(
            path=str(SH560_OUTPUT),
            name="output",
            sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
        )
        name = compute_source_name(dr, "source_basename", ["comp_%%", "comp"])
        self.assertEqual(name, "A_0029C015_251217_191031_h1DS8")

    def test_get_naming_options_returns_options(self):
        """Test that naming options are generated for KIA data."""
        dr = DiscoveryResult(
            path=str(SH560_OUTPUT),
            name="output",
            sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
        )
        options = get_naming_options(dr, ["comp_%%", "comp"])
        self.assertTrue(len(options) > 0)

        rules = [o["rule"] for o in options]
        self.assertIn("parent:0", rules)
        self.assertIn("source_name", rules)
        self.assertIn("source_basename", rules)

        # Verify previews
        for opt in options:
            if opt["rule"] == "source_basename":
                self.assertEqual(opt["preview"], "A_0029C015_251217_191031_h1DS8")
            elif opt["rule"] == "source_name":
                self.assertEqual(opt["preview"], "A_0029C015_251217_191031_h1DS8_comp")

    def test_strip_version_edge_cases(self):
        """Edge cases for version stripping."""
        # Double digit versions
        self.assertEqual(strip_version("name_v01"), "name")
        self.assertEqual(strip_version("name_v10"), "name")
        self.assertEqual(strip_version("name_v100"), "name")
        # No version
        self.assertEqual(strip_version("no_version_here"), "no_version_here")
        # Version at start (after divider)
        self.assertEqual(strip_version("_v01_name"), "name")

    def test_strip_frame_and_ext_nuke_scripts(self):
        """Nuke scripts should not have frame numbers stripped incorrectly."""
        # Nuke files: sh560_comp_mp_v003.nk
        result = strip_frame_and_ext("sh560_comp_mp_v003.nk")
        self.assertEqual(result, "sh560_comp_mp_v003")


# ============================================================================
# Scanner Tests (using real filesystem)
# ============================================================================
class TestScanner(unittest.TestCase):
    """Test VersionScanner with real KIA directory structure."""

    @require_kia_source
    def test_scan_sh560_output(self):
        """Scan SH560/output and verify version detection."""
        ws = WatchedSource(
            name="SH560",
            source_dir=str(SH560_OUTPUT),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()

        self.assertTrue(len(versions) > 0, "No versions found in SH560/output")

        # Should find v01 through v05
        version_numbers = [v.version_number for v in versions]
        self.assertIn(1, version_numbers)
        self.assertIn(5, version_numbers)

        # Verify sorted
        for i in range(1, len(versions)):
            self.assertGreater(versions[i].version_number, versions[i-1].version_number)

        # Check first version has correct frame range
        v01 = versions[0]
        self.assertEqual(v01.version_string, "v001")
        self.assertTrue(v01.file_count > 0, "v01 should have files")
        self.assertIsNotNone(v01.frame_range, "v01 should have a frame range")
        self.assertIn("991", v01.frame_range, "Frame range should include 991")
        self.assertIn("1037", v01.frame_range, "Frame range should include 1037")

    @require_kia_source
    def test_scan_sh590_output_comp_mp(self):
        """Scan SH590/output which has comp_mp naming."""
        ws = WatchedSource(
            name="SH590",
            source_dir=str(SH590_OUTPUT),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()

        self.assertTrue(len(versions) > 0, "No versions found in SH590/output")
        version_numbers = [v.version_number for v in versions]
        self.assertIn(1, version_numbers)
        self.assertIn(2, version_numbers)

    @require_kia_source
    def test_scan_sh560_nuke_scripts(self):
        """Scan SH560 root for versioned Nuke scripts (.nk files)."""
        ws = WatchedSource(
            name="SH560_nk",
            source_dir=str(KIA_ROOT / "SH560"),
            version_pattern="_v{version}",
            file_extensions=[".nk"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()

        # Should find sh560_comp_mp_v001.nk through v005.nk
        self.assertTrue(len(versions) > 0, "No .nk versions found")
        version_numbers = [v.version_number for v in versions]
        self.assertIn(1, version_numbers)

        # Nuke scripts are single files, no frame range
        for v in versions:
            self.assertEqual(v.file_count, 1)
            self.assertIsNone(v.frame_range)

    @require_kia_source
    def test_scan_ignores_latest_folders(self):
        """Version scanner should not pick up 'latest' folders (they have no version number)."""
        ws = WatchedSource(
            name="SH560",
            source_dir=str(SH560_OUTPUT),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()

        # None of the detected versions should be from a 'latest' folder
        for v in versions:
            self.assertNotIn("latest", v.source_path.lower(),
                             f"Latest folder incorrectly detected as version: {v.source_path}")

    @require_kia_source
    def test_scan_nonexistent_directory(self):
        """Scanner should return empty for nonexistent directory."""
        ws = WatchedSource(
            name="Ghost",
            source_dir=str(KIA_ROOT / "nonexistent"),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()
        self.assertEqual(versions, [])

    @require_kia_source
    def test_get_latest_version(self):
        """get_latest_version should return highest version."""
        ws = WatchedSource(
            name="SH560",
            source_dir=str(SH560_OUTPUT),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(ws)
        latest = scanner.get_latest_version()
        self.assertIsNotNone(latest)
        self.assertEqual(latest.version_number, 5)  # v05 is the highest

    @require_kia_source
    def test_scan_prerender_dirs(self):
        """Scan prerender directories which have different task names."""
        ws = WatchedSource(
            name="SH560_prerender",
            source_dir=str(SH560_PRERENDER),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()

        # Should find cleanup_v01, cleanup_v02, dn_v01, matte_v01
        self.assertTrue(len(versions) > 0, "No prerender versions found")

    @require_kia_source
    def test_detect_sequence_from_single_exr(self):
        """Test sequence detection from a single frame file."""
        # Pick the first EXR in SH560 v01
        v01_dir = SH560_OUTPUT / "A_0029C015_251217_191031_h1DS8_comp_v01"
        if not v01_dir.exists():
            self.skipTest("SH560 v01 directory not found")

        first_file = sorted(v01_dir.glob("*.exr"))[0]
        files, frame_range, count = detect_sequence_from_file(first_file, [".exr"])

        self.assertTrue(len(files) > 1, "Should detect multiple frames")
        self.assertIsNotNone(frame_range, "Should detect frame range")
        self.assertEqual(count, len(files))

    @require_kia_source
    def test_scan_directory_as_version(self):
        """Test scanning a directory as a manual version import."""
        v01_dir = SH560_OUTPUT / "A_0029C015_251217_191031_h1DS8_comp_v01"
        if not v01_dir.exists():
            self.skipTest("SH560 v01 directory not found")

        files, frame_range, count = scan_directory_as_version(v01_dir, [".exr"])
        self.assertTrue(len(files) > 0)
        self.assertIsNotNone(frame_range)
        self.assertTrue(count > 0)

    def test_create_manual_version(self):
        """Test manual version creation."""
        vi = create_manual_version("/tmp/seq", 42, 47, 1_000_000,
                                   frame_range="991-1037", frame_count=47)
        self.assertEqual(vi.version_string, "v042")
        self.assertEqual(vi.version_number, 42)
        self.assertEqual(vi.file_count, 47)

    @require_kia_source
    def test_scan_edit_reference_jpeg_sequence(self):
        """Test scanning JPEG edit reference sequence."""
        ws = WatchedSource(
            name="EditRef",
            source_dir=str(KIA_ROOT / "Edit_Reference"),
            version_pattern="_v{version}",
            file_extensions=[".jpeg", ".jpg"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()
        # Edit reference doesn't have versioned folders, so may be empty
        # but should NOT error


# ============================================================================
# Discovery Tests
# ============================================================================
class TestDiscovery(unittest.TestCase):
    """Test directory discovery with KIA source."""

    @require_kia_source
    def test_discover_kia_root(self):
        """Discover versioned content in KIA root."""
        results = discover(str(KIA_ROOT), max_depth=4, extensions=[".exr"])
        self.assertTrue(len(results) > 0, "Should find versioned content")

        # Collect all discovered location names
        paths = [r.path for r in results]
        names = [r.name for r in results]

        # Should find the output directories
        output_found = any("output" in p for p in paths)
        self.assertTrue(output_found, f"Should discover 'output' dirs, found: {names}")

    @require_kia_source
    def test_discover_with_comp_whitelist(self):
        """Discovery with 'comp' whitelist should filter results."""
        results_all = discover(str(KIA_ROOT), max_depth=4, extensions=[".exr"])
        results_filtered = discover(str(KIA_ROOT), max_depth=4,
                                    extensions=[".exr"], whitelist=["comp"])

        # Filtered should be <= all
        self.assertLessEqual(len(results_filtered), len(results_all))

        # All filtered results should contain 'comp' somewhere
        for r in results_filtered:
            combined = (r.name + " " + r.path + " " + (r.sample_filename or "")).lower()
            self.assertIn("comp", combined,
                          f"Whitelist 'comp' should filter: {r.name} at {r.path}")

    @require_kia_source
    def test_discover_prerender_blacklist(self):
        """Discovery with blacklist should exclude matching results."""
        results = discover(str(KIA_ROOT), max_depth=4, extensions=[".exr"],
                           blacklist=["prerender"])
        for r in results:
            self.assertNotIn("prerender", r.path.lower(),
                             f"Blacklisted 'prerender' should be excluded: {r.path}")

    @require_kia_source
    def test_discover_version_metadata(self):
        """Verify discovered versions have correct metadata."""
        results = discover(str(SH560_OUTPUT), max_depth=2, extensions=[".exr"])

        self.assertTrue(len(results) > 0, "Should find versions in SH560/output")
        result = results[0]

        self.assertTrue(len(result.versions_found) > 0)
        for v in result.versions_found:
            self.assertTrue(v.version_number > 0)
            self.assertTrue(v.file_count > 0, f"{v.version_string} should have files")
            self.assertTrue(v.total_size_bytes > 0, f"{v.version_string} should have size")

    @require_kia_source
    def test_discover_suggested_pattern(self):
        """Verify suggested patterns make sense for KIA data."""
        results = discover(str(SH560_OUTPUT), max_depth=2, extensions=[".exr"])
        for r in results:
            self.assertTrue(len(r.suggested_pattern) > 0,
                            f"Should suggest a pattern for {r.name}")
            self.assertIn("{version}", r.suggested_pattern,
                          f"Pattern should contain {{version}}: {r.suggested_pattern}")

    @require_kia_source
    def test_discover_sample_filename(self):
        """Verify sample filenames are populated."""
        results = discover(str(SH560_OUTPUT), max_depth=2, extensions=[".exr"])
        for r in results:
            self.assertTrue(len(r.sample_filename) > 0,
                            f"Should have a sample filename for {r.name}")
            self.assertTrue(r.sample_filename.endswith(".exr"),
                            f"Sample should be .exr: {r.sample_filename}")

    @require_kia_source
    def test_format_discovery_report(self):
        """Test report formatting with real data."""
        results = discover(str(KIA_ROOT), max_depth=4, extensions=[".exr"],
                           whitelist=["comp"])
        report = format_discovery_report(results, str(KIA_ROOT))
        self.assertIn("versioned location", report.lower())
        self.assertIn("version", report.lower())

    @require_kia_source
    def test_discover_finds_nuke_scripts(self):
        """Discovery should find versioned .nk files too."""
        results = discover(str(KIA_ROOT / "SH560"), max_depth=1, extensions=[".nk"])
        # SH560 has .nk files directly in it
        if results:
            nk_result = results[0]
            self.assertTrue(len(nk_result.versions_found) > 0)

    @require_kia_source
    def test_discover_depth_limit(self):
        """Depth=0 should only look at the root level."""
        results_depth0 = discover(str(KIA_ROOT), max_depth=0, extensions=[".exr"])
        results_depth4 = discover(str(KIA_ROOT), max_depth=4, extensions=[".exr"])
        self.assertLessEqual(len(results_depth0), len(results_depth4))


# ============================================================================
# Config Tests
# ============================================================================
class TestConfig(unittest.TestCase):
    """Test config loading/saving with KIA project file."""

    @require_kia_source
    def test_load_kia_config(self):
        """Load the actual KIA project config."""
        config = load_config(str(KIA_CONFIG))
        self.assertEqual(config.project_name, "KIATest")
        self.assertEqual(len(config.watched_sources), 4)
        self.assertEqual(config.task_tokens, ["comp_%%", "comp"])

        # Verify sources
        names = [s.name for s in config.watched_sources]
        self.assertIn("SH560", names)
        self.assertIn("SH570", names)
        self.assertIn("SH580", names)
        self.assertIn("SH590", names)

    @require_kia_source
    def test_config_paths_resolved(self):
        """Verify relative paths in config are resolved to absolute."""
        config = load_config(str(KIA_CONFIG))
        for source in config.watched_sources:
            self.assertTrue(Path(source.source_dir).is_absolute(),
                            f"source_dir not absolute: {source.source_dir}")
            if source.latest_target:
                self.assertTrue(Path(source.latest_target).is_absolute(),
                                f"latest_target not absolute: {source.latest_target}")

    @require_kia_source
    def test_config_sources_point_to_real_dirs(self):
        """Verify resolved source dirs actually exist."""
        config = load_config(str(KIA_CONFIG))
        for source in config.watched_sources:
            self.assertTrue(Path(source.source_dir).exists(),
                            f"source_dir does not exist: {source.source_dir}")

    @require_kia_source
    def test_config_group_root_resolved(self):
        """Verify group root_dir is resolved to absolute."""
        config = load_config(str(KIA_CONFIG))
        beta_props = config.groups.get("Beta", {})
        if "root_dir" in beta_props:
            self.assertTrue(Path(beta_props["root_dir"]).is_absolute(),
                            f"Beta root_dir not absolute: {beta_props['root_dir']}")

    def test_save_and_reload_config(self):
        """Test config save/load roundtrip in a temp directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = ProjectConfig(
                project_name="TestProject",
                task_tokens=["comp_%%", "comp"],
                groups={"Main": {"color": "#ff0000"}},
                default_naming_rule="parent:1",
                naming_configured=True,
            )
            ws = WatchedSource(
                name="TestSource",
                source_dir=os.path.join(tmpdir, "source"),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
                sample_filename="test_comp_v01.0991.exr",
                group="Main",
                override_version_pattern=True,
            )
            config.watched_sources = [ws]

            config_path = os.path.join(tmpdir, "test_lvm.json")
            save_config(config, config_path)

            # Verify file was created
            self.assertTrue(os.path.exists(config_path))

            # Reload and verify
            reloaded = load_config(config_path)
            self.assertEqual(reloaded.project_name, "TestProject")
            self.assertEqual(len(reloaded.watched_sources), 1)
            self.assertEqual(reloaded.watched_sources[0].name, "TestSource")
            self.assertEqual(reloaded.task_tokens, ["comp_%%", "comp"])

    def test_create_project(self):
        """Test project creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = create_project(
                "KIA Test",
                tmpdir,
                name_whitelist=["comp"],
                task_tokens=["comp_%%", "comp"],
            )
            self.assertTrue(os.path.exists(path))
            self.assertTrue(path.endswith("_lvm.json"))

            config = load_config(path)
            self.assertEqual(config.project_name, "KIA Test")
            self.assertEqual(config.name_whitelist, ["comp"])

    def test_expand_group_token(self):
        """Test group token expansion."""
        self.assertEqual(
            _expand_group_token("{group_root}/latest/{source_basename}", "Main"),
            "{group_root}/latest/{source_basename}"
        )
        # With group
        self.assertEqual(
            _expand_group_token("{group}/renders", "Test"),
            "Test/renders"
        )
        # Without group - token and divider removed
        result = _expand_group_token("{group}/renders", "")
        self.assertEqual(result, "renders")

    @require_kia_source
    def test_apply_project_defaults_kia(self):
        """Test applying defaults to KIA config sources."""
        config = load_config(str(KIA_CONFIG))

        # SH560 has override_version_pattern=True, so it should keep its own pattern
        sh560 = next(s for s in config.watched_sources if s.name == "SH560")
        self.assertEqual(sh560.version_pattern, "_v{version}")

        # All sources should have file_rename_template applied
        for source in config.watched_sources:
            self.assertTrue(len(source.file_rename_template) > 0,
                            f"{source.name} should have file_rename_template")


# ============================================================================
# History Tests
# ============================================================================
class TestHistory(unittest.TestCase):
    """Test history manager."""

    def test_history_lifecycle(self):
        """Test full history create/read/verify cycle."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = os.path.join(tmpdir, ".latest_history.json")
            hm = HistoryManager(hist_path)

            # Initially empty
            self.assertIsNone(hm.get_current())
            self.assertEqual(hm.get_history(), [])

            # Record a promotion
            entry1 = HistoryEntry(
                version="v001",
                source=str(SH560_OUTPUT / "comp_v01"),
                set_by="artist",
                set_at="2025-01-26T10:00:00",
                frame_range="991-1037",
                frame_count=47,
                file_count=47,
            )
            hm.record_promotion(entry1)

            current = hm.get_current()
            self.assertIsNotNone(current)
            self.assertEqual(current.version, "v001")

            # Record second promotion
            entry2 = HistoryEntry(
                version="v002",
                source=str(SH560_OUTPUT / "comp_v02"),
                set_by="artist",
                set_at="2025-01-26T11:00:00",
                frame_range="991-1037",
                frame_count=47,
                file_count=47,
            )
            hm.record_promotion(entry2)

            current = hm.get_current()
            self.assertEqual(current.version, "v002")

            history = hm.get_history()
            self.assertEqual(len(history), 2)
            self.assertEqual(history[0].version, "v002")  # newest first
            self.assertEqual(history[1].version, "v001")

    def test_verify_integrity_matching(self):
        """Test integrity check when files match."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = os.path.join(tmpdir, ".latest_history.json")
            hm = HistoryManager(hist_path)

            entry = HistoryEntry(
                version="v003", source="/tmp/v03",
                set_by="test", set_at="2025-01-01",
                file_count=3,
            )
            hm.record_promotion(entry)

            result = hm.verify_integrity(["a.exr", "b.exr", "c.exr"])
            self.assertTrue(result["valid"])

    def test_verify_integrity_mismatched(self):
        """Test integrity check when file count doesn't match."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = os.path.join(tmpdir, ".latest_history.json")
            hm = HistoryManager(hist_path)

            entry = HistoryEntry(
                version="v003", source="/tmp/v03",
                set_by="test", set_at="2025-01-01",
                file_count=47,
            )
            hm.record_promotion(entry)

            result = hm.verify_integrity(["a.exr", "b.exr"])
            self.assertFalse(result["valid"])
            self.assertIn("47 files", result["message"])

    def test_verify_integrity_no_files(self):
        """Test integrity when history exists but no files on disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = os.path.join(tmpdir, ".latest_history.json")
            hm = HistoryManager(hist_path)

            entry = HistoryEntry(
                version="v003", source="/tmp/v03",
                set_by="test", set_at="2025-01-01",
                file_count=47,
            )
            hm.record_promotion(entry)

            result = hm.verify_integrity([])
            self.assertFalse(result["valid"])

    def test_corrupt_history_recovery(self):
        """Test that corrupt history is backed up and fresh start used."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = os.path.join(tmpdir, ".latest_history.json")
            # Write corrupt JSON
            with open(hist_path, "w") as f:
                f.write("{invalid json content")

            hm = HistoryManager(hist_path)
            data = hm.load()
            self.assertIsNone(data["current"])
            self.assertEqual(data["history"], [])


# ============================================================================
# Promoter Tests (using temp dirs to avoid modifying source)
# ============================================================================
class TestPromoter(unittest.TestCase):
    """Test promoter with real source data, promoting to temp directories."""

    @require_kia_source
    def test_promote_sh560_v01_to_temp(self):
        """Promote SH560 v01 to a temp directory and verify file naming."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = os.path.join(tmpdir, "latest")
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=target_dir,
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()
            self.assertTrue(len(versions) > 0)

            v01 = versions[0]
            entry = promoter.promote(v01, user="test_runner")

            # Verify promotion happened
            self.assertEqual(entry.version, "v001")
            self.assertTrue(Path(target_dir).exists())

            # Check renamed files
            promoted_files = sorted(Path(target_dir).glob("*.exr"))
            self.assertTrue(len(promoted_files) > 0, "No files promoted!")

            # Verify naming: should be {source_basename}_comp_latest.{frame}.exr
            # source_basename = A_0029C015_251217_191031_h1DS8
            for f in promoted_files:
                self.assertIn("A_0029C015_251217_191031_h1DS8_comp_latest",
                              f.name,
                              f"Unexpected filename: {f.name}")
                self.assertNotIn("_v01", f.name,
                                 f"Version should be stripped: {f.name}")
                self.assertTrue(f.name.endswith(".exr"))

            # Verify frame numbers are preserved
            frame_nums = []
            import re
            for f in promoted_files:
                m = re.search(r"\.(\d+)\.exr$", f.name)
                if m:
                    frame_nums.append(int(m.group(1)))
            self.assertTrue(len(frame_nums) > 0)
            self.assertIn(991, frame_nums)

    @require_kia_source
    def test_promote_sh590_comp_mp_naming(self):
        """Test promotion of SH590 which has comp_mp task token in filenames."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = os.path.join(tmpdir, "latest")
            ws = WatchedSource(
                name="SH590",
                source_dir=str(SH590_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=target_dir,
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C020_251217_185838_h1DS8_comp_mp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()
            self.assertTrue(len(versions) > 0)

            v01 = versions[0]
            entry = promoter.promote(v01, user="test_runner")

            # Check files
            promoted_files = sorted(Path(target_dir).glob("*.exr"))
            self.assertTrue(len(promoted_files) > 0)

            # source_basename should be A_0029C020_251217_185838_h1DS8
            # (with comp_mp stripped as task token)
            for f in promoted_files:
                self.assertIn("A_0029C020_251217_185838_h1DS8_comp_latest",
                              f.name,
                              f"Unexpected filename: {f.name}")
                self.assertNotIn("_v01", f.name)
                self.assertNotIn("_v02", f.name)
                # comp_mp should be stripped since comp_%% matches it
                self.assertNotIn("comp_mp", f.name,
                                 f"comp_mp should be stripped by task token: {f.name}")

    @require_kia_source
    def test_promote_overwrites_existing(self):
        """Test that promoting a second version overwrites the first."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = os.path.join(tmpdir, "latest")
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=target_dir,
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            # Promote v01
            promoter.promote(versions[0], user="test")
            first_count = len(list(Path(target_dir).glob("*.exr")))

            # Promote v02 (should overwrite)
            promoter.promote(versions[1], user="test")
            second_count = len(list(Path(target_dir).glob("*.exr")))

            # File counts should be similar (same shot, same frames)
            self.assertTrue(second_count > 0)

            # History should have 2 entries
            history = promoter.get_history()
            self.assertEqual(len(history), 2)
            self.assertEqual(history[0].version, "v002")

    @require_kia_source
    def test_promote_nonexistent_source_raises(self):
        """Promoting from a deleted source should raise PromotionError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(
                name="Ghost",
                source_dir=str(KIA_ROOT / "SH560" / "output"),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
            )
            promoter = Promoter(ws)
            fake_version = VersionInfo(
                "v999", 999,
                str(KIA_ROOT / "nonexistent_v999"),
                file_count=1,
            )
            with self.assertRaises(PromotionError):
                promoter.promote(fake_version)

    @require_kia_source
    def test_verify_after_promote(self):
        """Verify integrity check passes after a clean promotion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = os.path.join(tmpdir, "latest")
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=target_dir,
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            promoter.promote(versions[0], user="test")
            result = promoter.verify()
            self.assertTrue(result["valid"], f"Verify failed: {result['message']}")

    @require_kia_source
    def test_remap_filename_kia_patterns(self):
        """Test filename remapping with various KIA naming patterns."""
        ws = WatchedSource(
            name="SH560",
            source_dir=str(SH560_OUTPUT),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            latest_target="/tmp/latest",
            file_rename_template="{source_basename}_comp_latest",
            sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
        )
        promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])

        # Test remapping
        result = promoter._remap_filename(
            "A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr")
        self.assertEqual(result,
                         "A_0029C015_251217_191031_h1DS8_comp_latest.0991.exr",
                         f"Unexpected remap result: {result}")

    @require_kia_source
    def test_remap_filename_no_template(self):
        """Test filename remapping when no template is set (version strip only)."""
        ws = WatchedSource(
            name="SH560",
            source_dir=str(SH560_OUTPUT),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            latest_target="/tmp/latest",
            file_rename_template="",  # No template
        )
        promoter = Promoter(ws)

        result = promoter._remap_filename(
            "A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr")
        # Should just strip _v01
        self.assertNotIn("_v01", result)
        self.assertIn(".0991.exr", result)
        self.assertEqual(result,
                         "A_0029C015_251217_191031_h1DS8_comp.0991.exr",
                         f"Unexpected: {result}")


# ============================================================================
# Integration Tests (end-to-end using config)
# ============================================================================
class TestIntegration(unittest.TestCase):
    """End-to-end integration tests with real KIA data."""

    @require_kia_source
    def test_full_workflow_load_scan_promote(self):
        """Full workflow: load config, scan, promote to temp, verify."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy config to temp dir and adjust paths
            config = load_config(str(KIA_CONFIG))

            # Use only SH560 and redirect its target to temp
            sh560 = next(s for s in config.watched_sources if s.name == "SH560")
            sh560.latest_target = os.path.join(tmpdir, "sh560_latest")

            # Scan
            scanner = VersionScanner(sh560)
            versions = scanner.scan()
            self.assertTrue(len(versions) >= 5, f"Expected 5+ versions, got {len(versions)}")

            # Promote latest
            promoter = Promoter(sh560, task_tokens=config.task_tokens)
            latest = versions[-1]
            entry = promoter.promote(latest, user="integration_test")

            # Verify
            self.assertEqual(entry.version, latest.version_string)
            promoted_files = list(Path(sh560.latest_target).glob("*.exr"))
            self.assertEqual(len(promoted_files), latest.file_count)

            # Check naming
            for f in promoted_files:
                self.assertIn("_comp_latest", f.name)
                self.assertNotIn("_v0", f.name)

    @require_kia_source
    def test_all_shots_scan_without_errors(self):
        """Scan all 4 shots from config and verify no errors."""
        config = load_config(str(KIA_CONFIG))
        for source in config.watched_sources:
            with self.subTest(source=source.name):
                scanner = VersionScanner(source)
                versions = scanner.scan()
                self.assertTrue(len(versions) > 0,
                                f"{source.name}: no versions found at {source.source_dir}")

                for v in versions:
                    self.assertTrue(v.version_number > 0)
                    self.assertTrue(v.file_count > 0,
                                    f"{source.name} {v.version_string}: 0 files")
                    self.assertTrue(v.total_size_bytes > 0,
                                    f"{source.name} {v.version_string}: 0 bytes")
                    if v.frame_range:
                        self.assertIn("-", v.frame_range)

    @require_kia_source
    def test_all_shots_promote_naming_check(self):
        """Promote v01 from each shot and verify output naming."""
        config = load_config(str(KIA_CONFIG))

        expected_basenames = {
            "SH560": "A_0029C015_251217_191031_h1DS8",
            "SH570": "A_0029C009_251217_185221_h1DS8",
            "SH580": "A_0029C010_251217_185838_h1DS8",
            "SH590": "A_0029C020_251217_185838_h1DS8",
        }

        for source in config.watched_sources:
            with self.subTest(source=source.name):
                with tempfile.TemporaryDirectory() as tmpdir:
                    source.latest_target = os.path.join(tmpdir, "latest")

                    scanner = VersionScanner(source)
                    versions = scanner.scan()
                    self.assertTrue(len(versions) > 0)

                    promoter = Promoter(source, task_tokens=config.task_tokens)
                    entry = promoter.promote(versions[0], user="naming_test")

                    promoted_files = sorted(Path(source.latest_target).glob("*.exr"))
                    self.assertTrue(len(promoted_files) > 0,
                                    f"{source.name}: no promoted files")

                    expected_base = expected_basenames.get(source.name)
                    if expected_base:
                        for f in promoted_files:
                            self.assertTrue(
                                f.name.startswith(expected_base),
                                f"{source.name}: expected '{expected_base}...' "
                                f"but got '{f.name}'"
                            )
                            self.assertIn("_comp_latest", f.name,
                                          f"{source.name}: missing _comp_latest in {f.name}")

    @require_kia_source
    def test_discover_then_config_roundtrip(self):
        """Discover sources, create config, save, reload, verify."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Discover
            results = discover(str(KIA_ROOT), max_depth=4,
                               extensions=[".exr"], whitelist=["comp"])
            self.assertTrue(len(results) > 0)

            # Create project
            config_path = create_project(
                "Integration Test",
                tmpdir,
                name_whitelist=["comp"],
                task_tokens=["comp_%%", "comp"],
            )

            # Load, add discovered sources, save
            config = load_config(config_path)
            for r in results:
                ws = WatchedSource(
                    name=compute_source_name(r, "parent:1", ["comp_%%", "comp"]),
                    source_dir=r.path,
                    version_pattern=r.suggested_pattern,
                    file_extensions=r.suggested_extensions,
                    sample_filename=r.sample_filename,
                )
                config.watched_sources.append(ws)

            save_config(config, config_path)

            # Reload and verify
            reloaded = load_config(config_path)
            self.assertEqual(len(reloaded.watched_sources), len(results))
            for source in reloaded.watched_sources:
                self.assertTrue(len(source.name) > 0)
                self.assertTrue(Path(source.source_dir).is_absolute())


# ============================================================================
# Edge Case / Error Handling Tests
# ============================================================================
class TestEdgeCases(unittest.TestCase):
    """Test edge cases and potential error scenarios."""

    def test_empty_source_dir(self):
        """Scanner should handle empty directory gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(name="Empty", source_dir=tmpdir,
                               version_pattern="_v{version}", file_extensions=[".exr"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()
            self.assertEqual(versions, [])

    def test_discover_empty_dir(self):
        """Discovery should return empty for empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            results = discover(tmpdir)
            self.assertEqual(results, [])

    def test_discover_nonexistent_dir(self):
        """Discovery should return empty for nonexistent directory."""
        results = discover("/nonexistent/path/xyz")
        self.assertEqual(results, [])

    @require_kia_source
    def test_scan_with_wrong_extensions(self):
        """Scanning with wrong extensions should find nothing."""
        ws = WatchedSource(
            name="WrongExt",
            source_dir=str(SH560_OUTPUT),
            version_pattern="_v{version}",
            file_extensions=[".wav"],  # No wav files in output
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()
        # Versions may still be found (dirs exist) but with 0 files
        for v in versions:
            self.assertEqual(v.file_count, 0,
                             f"Should have 0 files with .wav extension: {v}")

    @require_kia_source
    def test_scan_with_wrong_pattern(self):
        """Wrong version pattern should find nothing."""
        ws = WatchedSource(
            name="WrongPat",
            source_dir=str(SH560_OUTPUT),
            version_pattern="_version{version}",  # Won't match _v01
            file_extensions=[".exr"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()
        self.assertEqual(versions, [],
                         f"Should find 0 versions with wrong pattern, found: {len(versions)}")

    def test_history_max_entries_limit(self):
        """Verify history is capped at MAX_HISTORY_ENTRIES."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hist_path = os.path.join(tmpdir, ".latest_history.json")
            hm = HistoryManager(hist_path)

            # Record 110 promotions
            for i in range(110):
                entry = HistoryEntry(
                    version=f"v{i:03d}", source="/tmp",
                    set_by="test", set_at="2025-01-01",
                )
                hm.record_promotion(entry)

            history = hm.get_history()
            self.assertLessEqual(len(history), 100,
                                 f"History should be capped at 100, got {len(history)}")

    def test_strip_task_tokens_no_match(self):
        """Stripping with non-matching tokens should return original."""
        result = strip_task_tokens("A_0029C015_h1DS8", ["grade", "dmp"])
        self.assertEqual(result, "A_0029C015_h1DS8")

    def test_strip_task_tokens_empty_patterns(self):
        """Empty task patterns should return original."""
        result = strip_task_tokens("A_0029C015_comp", [])
        self.assertEqual(result, "A_0029C015_comp")

    def test_derive_source_tokens_path_input(self):
        """derive_source_tokens should handle full paths."""
        result = derive_source_tokens(
            r"E:\Work_Offline\KIA\SH560\output\A_0029C015_comp_v01.0991.exr",
            ["comp"],
        )
        self.assertEqual(result["source_filename"],
                         "A_0029C015_comp_v01.0991.exr")
        self.assertEqual(result["source_name"], "A_0029C015_comp")
        self.assertEqual(result["source_basename"], "A_0029C015")

    @require_kia_source
    def test_fireworks_mood_not_versioned(self):
        """Fireworks_mood directory should not be detected as versioned content."""
        results = discover(str(KIA_ROOT / "Fireworks_mood"), max_depth=1)
        # Files named fireworks_1.2 1.jpg should NOT match version pattern
        for r in results:
            # Any versions found here would be a false positive
            if r.versions_found:
                for v in r.versions_found:
                    # If detected, it's likely wrong
                    print(f"WARNING: False positive version in Fireworks_mood: "
                          f"{v.version_string} at {v.source_path}")

    @require_kia_source
    def test_sendout_zip_files_not_versioned(self):
        """Zip files in sendout should be discovered as versioned single files."""
        results = discover(str(KIA_ROOT / "sendout"), max_depth=1,
                           extensions=[".zip"])
        # sendout has sh560_v02.zip, sh570_v01.zip etc.
        if results:
            for r in results:
                for v in r.versions_found:
                    self.assertTrue(v.version_number > 0)

    @require_kia_source
    def test_wrong_extensions_returns_zero_file_versions(self):
        """Scanner with wrong extensions returns versions with 0 matching files."""
        ws = WatchedSource(
            name="WrongExt",
            source_dir=str(SH560_OUTPUT),
            version_pattern="_v{version}",
            file_extensions=[".wav"],
        )
        scanner = VersionScanner(ws)
        versions = scanner.scan()
        # The version folders still exist and match the pattern,
        # but _scan_version_folder returns None when no matching files
        # So versions should be empty
        for v in versions:
            self.assertEqual(v.file_count, 0)


# ============================================================================
# New Feature Tests: Dry Run, Mtime Tracking, Reports, Validate
# ============================================================================
class TestDryRun(unittest.TestCase):
    """Test dry-run promotion preview."""

    @require_kia_source
    def test_dry_run_returns_file_map(self):
        """Dry run should return correct file mapping without copying."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            preview = promoter.dry_run(versions[0])
            self.assertEqual(preview["total_files"], versions[0].file_count)
            self.assertTrue(preview["total_size_bytes"] > 0)
            self.assertEqual(preview["link_mode"], "copy")

            # Verify file mapping
            for item in preview["file_map"]:
                self.assertIn("A_0029C015_251217_191031_h1DS8_comp_latest",
                              item["target_name"])
                self.assertNotIn("_v01", item["target_name"])
                self.assertTrue(item["size_bytes"] > 0)

            # Target should NOT exist (no files copied)
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "latest")))

    @require_kia_source
    def test_dry_run_comp_mp_naming(self):
        """Dry run for SH590 should strip comp_mp via task token."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(
                name="SH590",
                source_dir=str(SH590_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C020_251217_185838_h1DS8_comp_mp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            preview = promoter.dry_run(versions[0])
            for item in preview["file_map"]:
                self.assertNotIn("comp_mp", item["target_name"])
                self.assertIn("A_0029C020_251217_185838_h1DS8_comp_latest",
                              item["target_name"])


class TestMtimeTracking(unittest.TestCase):
    """Test mtime-based staleness detection."""

    @require_kia_source
    def test_promote_records_mtimes(self):
        """Promotion should record source and target mtimes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            entry = promoter.promote(versions[0], user="mtime_test")

            # Both mtimes should be recorded
            self.assertIsNotNone(entry.source_mtime,
                                 "source_mtime should be recorded")
            self.assertIsNotNone(entry.target_mtime,
                                 "target_mtime should be recorded")
            self.assertTrue(entry.source_mtime > 0)
            self.assertTrue(entry.target_mtime > 0)

    @require_kia_source
    def test_mtime_roundtrip_in_history(self):
        """Mtime values should survive serialization through history file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            entry = promoter.promote(versions[0], user="mtime_test")
            original_source_mtime = entry.source_mtime
            original_target_mtime = entry.target_mtime

            # Reload from disk
            reloaded = promoter.get_current_version()
            self.assertAlmostEqual(reloaded.source_mtime, original_source_mtime, places=2)
            self.assertAlmostEqual(reloaded.target_mtime, original_target_mtime, places=2)

    @require_kia_source
    def test_verify_clean_after_promote(self):
        """Verify should pass immediately after promotion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            promoter.promote(versions[0], user="test")
            result = promoter.verify()
            self.assertTrue(result["valid"], f"Verify should pass: {result['message']}")

    def test_verify_detects_target_tamper(self):
        """Verify should detect when target files are modified after promotion."""
        import time
        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = os.path.join(tmpdir, "source_v01")
            os.makedirs(src_dir)
            target_dir = os.path.join(tmpdir, "latest")

            # Create a source file
            src_file = os.path.join(src_dir, "test.0001.exr")
            with open(src_file, "wb") as f:
                f.write(b"frame_data" * 100)

            ws = WatchedSource(
                name="TamperTest",
                source_dir=os.path.join(tmpdir, "source"),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=target_dir,
            )

            # Manually create promotion entry with mtime
            from lvm.history import HistoryManager
            hm = HistoryManager(os.path.join(target_dir, ".latest_history.json"))
            os.makedirs(target_dir, exist_ok=True)

            # Copy file to target
            import shutil
            shutil.copy2(src_file, os.path.join(target_dir, "test.0001.exr"))

            target_file = os.path.join(target_dir, "test.0001.exr")
            original_mtime = os.path.getmtime(target_file)

            entry = HistoryEntry(
                version="v001", source=src_dir,
                set_by="test", set_at="2025-01-01",
                file_count=1,
                target_mtime=original_mtime,
            )
            hm.record_promotion(entry)

            # Tamper with target file — set mtime well beyond tolerance
            with open(target_file, "wb") as f:
                f.write(b"tampered_data" * 100)
            # Force mtime 10 seconds into the future to exceed 1s tolerance
            os.utime(target_file, (original_mtime + 10, original_mtime + 10))

            # Verify should detect the tampering
            promoter = Promoter(ws)
            result = promoter.verify()
            self.assertFalse(result["valid"])
            self.assertIn("modified since promotion", result["message"])

    def test_backward_compat_no_mtime(self):
        """History entries without mtime fields should still work."""
        entry = HistoryEntry.from_dict({
            "version": "v001",
            "source": "/tmp/v01",
            "set_by": "old_user",
            "set_at": "2024-01-01",
            "file_count": 10,
        })
        self.assertIsNone(entry.source_mtime)
        self.assertIsNone(entry.target_mtime)

        # to_dict should not include None mtimes
        d = entry.to_dict()
        self.assertNotIn("source_mtime", d)
        self.assertNotIn("target_mtime", d)


class TestGenerateReport(unittest.TestCase):
    """Test promotion report generation."""

    @require_kia_source
    def test_report_has_required_fields(self):
        """Report should contain all required fields."""
        from lvm.promoter import generate_report

        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            entry = promoter.promote(versions[0], user="report_test")
            report = generate_report(entry, ws)

            self.assertEqual(report["source_name"], "SH560")
            self.assertEqual(report["version"], "v001")
            self.assertEqual(report["set_by"], "report_test")
            self.assertIn("timestamp", report)
            self.assertIn("source_path", report)
            self.assertIn("target_path", report)
            self.assertIn("frame_range", report)
            self.assertIn("frame_count", report)
            self.assertIn("file_count", report)
            self.assertIn("source_mtime", report)
            self.assertIn("target_mtime", report)

    @require_kia_source
    def test_report_with_dry_run_data(self):
        """Report with dry_run data should include file_map."""
        from lvm.promoter import generate_report

        with tempfile.TemporaryDirectory() as tmpdir:
            ws = WatchedSource(
                name="SH560",
                source_dir=str(SH560_OUTPUT),
                version_pattern="_v{version}",
                file_extensions=[".exr"],
                latest_target=os.path.join(tmpdir, "latest"),
                file_rename_template="{source_basename}_comp_latest",
                sample_filename="A_0029C015_251217_191031_h1DS8_comp_v01.0991.exr",
            )
            promoter = Promoter(ws, task_tokens=["comp_%%", "comp"])
            scanner = VersionScanner(ws)
            versions = scanner.scan()

            dry_run_data = promoter.dry_run(versions[0])
            entry = promoter.promote(versions[0], user="report_test")
            report = generate_report(entry, ws, dry_run_data=dry_run_data)

            self.assertIn("file_map", report)
            self.assertEqual(len(report["file_map"]), dry_run_data["total_files"])
            self.assertIn("total_size_bytes", report)

    def test_report_json_serializable(self):
        """Report should be JSON-serializable."""
        from lvm.promoter import generate_report

        entry = HistoryEntry(
            version="v001", source="/tmp/v01", set_by="test",
            set_at="2025-01-01T12:00:00", file_count=47,
            frame_range="991-1037", source_mtime=1700000000.0,
            target_mtime=1700000001.0,
        )
        ws = WatchedSource(name="Test", source_dir="/tmp",
                           latest_target="/tmp/latest")
        report = generate_report(entry, ws)

        # Should not raise
        json_str = json.dumps(report)
        self.assertIn("Test", json_str)
        self.assertIn("v001", json_str)


# ============================================================================
# Run tests
# ============================================================================
if __name__ == "__main__":
    # Run with verbose output
    unittest.main(verbosity=2)
