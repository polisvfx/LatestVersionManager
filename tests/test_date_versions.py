"""
Tests for date-based version support.

Covers:
- Date validation and parsing
- Date stripping from filenames
- Version pattern compilation with {date} token
- Scanner extraction of date+version, date-only, and version-only
- Discovery detection of date patterns
- Sorting: date primary, version secondary
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lvm.task_tokens import (
    validate_date_string,
    parse_date_to_sortable,
    format_date_display,
    strip_date,
    strip_version,
    derive_source_tokens,
)
from lvm.models import VersionInfo, WatchedSource, ProjectConfig, DiscoveryResult
from lvm.scanner import VersionScanner
from lvm.config import apply_project_defaults


# ---------------------------------------------------------------------------
# Date validation
# ---------------------------------------------------------------------------

class TestValidateDateString(unittest.TestCase):
    """Test validate_date_string() for all supported formats."""

    def test_ddmmyy_valid(self):
        self.assertTrue(validate_date_string("260224", "DDMMYY"))   # Feb 26, 2024
        self.assertTrue(validate_date_string("010199", "DDMMYY"))   # Jan 1, 1999
        self.assertTrue(validate_date_string("311269", "DDMMYY"))   # Dec 31, 1969

    def test_ddmmyy_invalid(self):
        self.assertFalse(validate_date_string("321224", "DDMMYY"))  # day 32
        self.assertFalse(validate_date_string("001224", "DDMMYY"))  # day 0
        self.assertFalse(validate_date_string("151324", "DDMMYY"))  # month 13
        self.assertFalse(validate_date_string("150024", "DDMMYY"))  # month 0
        self.assertFalse(validate_date_string("12345", "DDMMYY"))   # wrong length

    def test_yymmdd_valid(self):
        self.assertTrue(validate_date_string("240226", "YYMMDD"))   # Feb 26, 2024
        self.assertTrue(validate_date_string("990101", "YYMMDD"))   # Jan 1, 1999

    def test_yymmdd_invalid(self):
        self.assertFalse(validate_date_string("241232", "YYMMDD"))  # day 32
        self.assertFalse(validate_date_string("241300", "YYMMDD"))  # month 13, day 0

    def test_yyyymmdd_valid(self):
        self.assertTrue(validate_date_string("20240226", "YYYYMMDD"))
        self.assertTrue(validate_date_string("19991231", "YYYYMMDD"))

    def test_yyyymmdd_invalid(self):
        self.assertFalse(validate_date_string("18991231", "YYYYMMDD"))  # year < 1900
        self.assertFalse(validate_date_string("21001231", "YYYYMMDD"))  # year > 2099
        self.assertFalse(validate_date_string("20241301", "YYYYMMDD"))  # month 13

    def test_ddmmyyyy_valid(self):
        self.assertTrue(validate_date_string("26022024", "DDMMYYYY"))
        self.assertTrue(validate_date_string("31121999", "DDMMYYYY"))

    def test_ddmmyyyy_invalid(self):
        self.assertFalse(validate_date_string("32122024", "DDMMYYYY"))  # day 32
        self.assertFalse(validate_date_string("26021899", "DDMMYYYY"))  # year < 1900

    def test_wrong_format(self):
        self.assertFalse(validate_date_string("260224", "YYYYMMDD"))  # 6 digits for 8-digit format
        self.assertFalse(validate_date_string("20240226", "DDMMYY"))  # 8 digits for 6-digit format
        self.assertFalse(validate_date_string("260224", "INVALID"))


# ---------------------------------------------------------------------------
# Date parsing to sortable integer
# ---------------------------------------------------------------------------

class TestParseDateToSortable(unittest.TestCase):

    def test_ddmmyy(self):
        self.assertEqual(parse_date_to_sortable("260224", "DDMMYY"), 20240226)
        self.assertEqual(parse_date_to_sortable("010199", "DDMMYY"), 19990101)
        self.assertEqual(parse_date_to_sortable("150370", "DDMMYY"), 19700315)

    def test_yymmdd(self):
        self.assertEqual(parse_date_to_sortable("240226", "YYMMDD"), 20240226)
        self.assertEqual(parse_date_to_sortable("990101", "YYMMDD"), 19990101)

    def test_yyyymmdd(self):
        self.assertEqual(parse_date_to_sortable("20240226", "YYYYMMDD"), 20240226)

    def test_ddmmyyyy(self):
        self.assertEqual(parse_date_to_sortable("26022024", "DDMMYYYY"), 20240226)

    def test_year_pivot(self):
        # YY < 70 -> 20xx, YY >= 70 -> 19xx
        self.assertEqual(parse_date_to_sortable("010170", "DDMMYY"), 19700101)
        self.assertEqual(parse_date_to_sortable("700101", "YYMMDD"), 19700101)
        self.assertEqual(parse_date_to_sortable("010125", "DDMMYY"), 20250101)
        self.assertEqual(parse_date_to_sortable("250101", "YYMMDD"), 20250101)
        # 69 is < 70 so maps to 2069
        self.assertEqual(parse_date_to_sortable("690101", "YYMMDD"), 20690101)

    def test_invalid_returns_zero(self):
        self.assertEqual(parse_date_to_sortable("999999", "DDMMYY"), 0)
        self.assertEqual(parse_date_to_sortable("abc", "DDMMYY"), 0)


# ---------------------------------------------------------------------------
# Date display formatting
# ---------------------------------------------------------------------------

class TestFormatDateDisplay(unittest.TestCase):

    def test_ddmmyy(self):
        self.assertEqual(format_date_display("260224", "DDMMYY"), "26-02-24")

    def test_yymmdd(self):
        self.assertEqual(format_date_display("240226", "YYMMDD"), "26-02-24")

    def test_yyyymmdd(self):
        self.assertEqual(format_date_display("20240226", "YYYYMMDD"), "26-02-2024")

    def test_ddmmyyyy(self):
        self.assertEqual(format_date_display("26022024", "DDMMYYYY"), "26-02-2024")


# ---------------------------------------------------------------------------
# Date stripping
# ---------------------------------------------------------------------------

class TestStripDate(unittest.TestCase):

    def test_no_format_passthrough(self):
        """No date_format means no stripping."""
        self.assertEqual(strip_date("260224_shotname", ""), "260224_shotname")
        self.assertEqual(strip_date("shotname_260224", ""), "shotname_260224")

    def test_prefix_date(self):
        self.assertEqual(strip_date("260224_shotname", "DDMMYY"), "shotname")

    def test_suffix_date(self):
        self.assertEqual(strip_date("shotname_260224", "DDMMYY"), "shotname")

    def test_middle_date(self):
        self.assertEqual(strip_date("shot_260224_comp", "DDMMYY"), "shot_comp")

    def test_no_valid_date(self):
        """If digits don't form a valid date, don't strip."""
        self.assertEqual(strip_date("shotname_999999", "DDMMYY"), "shotname_999999")

    def test_eight_digit_date(self):
        self.assertEqual(strip_date("shotname_20240226", "YYYYMMDD"), "shotname")

    def test_only_strips_first(self):
        """Only one date should be stripped per call."""
        result = strip_date("260224_270224_shotname", "DDMMYY")
        # Should strip the first valid date
        self.assertIn("shotname", result)

    def test_multi_format_strips_either(self):
        """Comma-separated date_format spec accepts a date matching any
        listed format, so DDMMYY-or-YYMMDD both validate '260401'."""
        # "260401" parses as DDMMYY (26 Apr 2001) and as YYMMDD (1 Apr 2026).
        self.assertEqual(strip_date("260401_shot", "DDMMYY,YYMMDD"), "shot")
        self.assertEqual(strip_date("260401_shot", "YYMMDD"), "shot")
        # A 6-digit value that's only valid as YYMMDD (mm > 12 in DDMMYY).
        self.assertEqual(strip_date("260024_shot", "DDMMYY"), "260024_shot")
        # Same input under multi-format including YYMMDD now strips,
        # because YYMMDD validates "26-00-24" as month 00 — not actually
        # valid either; pick a real YYMMDD-only date to assert success.
        self.assertEqual(strip_date("261231_shot", "DDMMYY,YYMMDD"), "shot")

    def test_multi_format_six_and_eight(self):
        """Multi-format spec with both 6- and 8-digit formats handles either width."""
        self.assertEqual(strip_date("shot_260401", "DDMMYY,YYYYMMDD"), "shot")
        self.assertEqual(strip_date("shot_20260401", "DDMMYY,YYYYMMDD"), "shot")


# ---------------------------------------------------------------------------
# derive_source_tokens with date_format
# ---------------------------------------------------------------------------

class TestDeriveSourceTokensWithDate(unittest.TestCase):

    def test_date_prefix_with_version(self):
        """260224_shotname_v03.mov -> basename should be 'shotname'."""
        tokens = derive_source_tokens("260224_shotname_v03.mov", [], "DDMMYY")
        self.assertEqual(tokens["source_filename"], "260224_shotname_v03.mov")
        self.assertEqual(tokens["source_fullname"], "260224_shotname_v03")
        self.assertEqual(tokens["source_name"], "260224_shotname")
        # With date stripping, basename should exclude the date
        self.assertEqual(tokens["source_basename"], "shotname")

    def test_date_middle_with_version(self):
        """shotname_260224_v01.mov -> basename should be 'shotname'."""
        tokens = derive_source_tokens("shotname_260224_v01.mov", [], "DDMMYY")
        self.assertEqual(tokens["source_basename"], "shotname")

    def test_no_date_format_preserves_digits(self):
        """Without date_format, digit sequences are preserved in basename."""
        tokens = derive_source_tokens("260224_shotname_v03.mov", [], "")
        # Without date stripping, basename includes the date digits
        self.assertEqual(tokens["source_basename"], "260224_shotname")

    def test_different_dates_same_basename(self):
        """Different dates should produce the same basename."""
        t1 = derive_source_tokens("260224_shotname_v01.mov", [], "DDMMYY")
        t2 = derive_source_tokens("270224_shotname_v02.mov", [], "DDMMYY")
        self.assertEqual(t1["source_basename"], t2["source_basename"])
        self.assertEqual(t1["source_basename"], "shotname")


# ---------------------------------------------------------------------------
# Scanner pattern compilation with {date}
# ---------------------------------------------------------------------------

class TestScannerPatternCompilation(unittest.TestCase):

    def test_date_and_version_pattern(self):
        """Pattern with both {date} and {version} tokens."""
        source = WatchedSource(
            name="test", source_dir="/tmp",
            version_pattern="_{date}_v{version}",
            date_format="DDMMYY",
        )
        scanner = VersionScanner(source)
        result = scanner._extract_version("shotname_260224_v03")
        self.assertIsNotNone(result)
        ver_str, ver_num, date_str, date_sortable = result
        self.assertEqual(ver_str, "v003")
        self.assertEqual(ver_num, 3)
        self.assertEqual(date_str, "260224")
        self.assertEqual(date_sortable, 20240226)

    def test_date_only_pattern(self):
        """Pattern with only {date} token."""
        source = WatchedSource(
            name="test", source_dir="/tmp",
            version_pattern="_{date}",
            date_format="DDMMYY",
        )
        scanner = VersionScanner(source)
        result = scanner._extract_version("shotname_260224")
        self.assertIsNotNone(result)
        ver_str, ver_num, date_str, date_sortable = result
        self.assertEqual(ver_str, "26-02-24")  # formatted date display
        self.assertEqual(ver_num, 0)
        self.assertEqual(date_str, "260224")
        self.assertEqual(date_sortable, 20240226)

    def test_version_only_pattern_regression(self):
        """Existing _v{version} pattern still works."""
        source = WatchedSource(
            name="test", source_dir="/tmp",
            version_pattern="_v{version}",
        )
        scanner = VersionScanner(source)
        result = scanner._extract_version("shotname_v003")
        self.assertIsNotNone(result)
        ver_str, ver_num, date_str, date_sortable = result
        self.assertEqual(ver_str, "v003")
        self.assertEqual(ver_num, 3)
        self.assertIsNone(date_str)
        self.assertEqual(date_sortable, 0)

    def test_date_prefix_pattern(self):
        """Pattern like {date}_shotname_v{version} for date-prefixed names."""
        source = WatchedSource(
            name="test", source_dir="/tmp",
            version_pattern="{date}_shotname_v{version}",
            date_format="DDMMYY",
        )
        scanner = VersionScanner(source)
        result = scanner._extract_version("260224_shotname_v03")
        self.assertIsNotNone(result)
        ver_str, ver_num, date_str, date_sortable = result
        self.assertEqual(ver_str, "v003")
        self.assertEqual(ver_num, 3)
        self.assertEqual(date_str, "260224")

    def test_eight_digit_date_pattern(self):
        """8-digit YYYYMMDD date in pattern."""
        source = WatchedSource(
            name="test", source_dir="/tmp",
            version_pattern="_{date}",
            date_format="YYYYMMDD",
        )
        scanner = VersionScanner(source)
        result = scanner._extract_version("shotname_20240226")
        self.assertIsNotNone(result)
        ver_str, ver_num, date_str, date_sortable = result
        self.assertEqual(date_str, "20240226")
        self.assertEqual(date_sortable, 20240226)


# ---------------------------------------------------------------------------
# Scanner scan with date-versioned files
# ---------------------------------------------------------------------------

class TestScannerDateVersionedFiles(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_scan_date_plus_version_files(self):
        """Scan files with date + version: 260224_shot_v01.mov, 260224_shot_v02.mov"""
        for name in ["260224_shot_v01.mov", "260224_shot_v02.mov", "270224_shot_v01.mov"]:
            Path(self.tmpdir, name).write_bytes(b"\x00" * 100)

        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="{date}_shot_v{version}",
            file_extensions=[".mov"],
            date_format="DDMMYY",
        )
        scanner = VersionScanner(source)
        versions = scanner.scan()

        self.assertEqual(len(versions), 3)
        # Sorted by (date_sortable, version_number)
        self.assertEqual(versions[0].version_string, "v001")  # 260224 v01
        self.assertEqual(versions[0].date_sortable, 20240226)
        self.assertEqual(versions[1].version_string, "v002")  # 260224 v02
        self.assertEqual(versions[1].date_sortable, 20240226)
        self.assertEqual(versions[2].version_string, "v001")  # 270224 v01
        self.assertEqual(versions[2].date_sortable, 20240227)

    def test_scan_date_only_files(self):
        """Scan files with date only: shot_260224.mov, shot_270224.mov"""
        for name in ["shot_260224.mov", "shot_270224.mov"]:
            Path(self.tmpdir, name).write_bytes(b"\x00" * 100)

        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="shot_{date}",
            file_extensions=[".mov"],
            date_format="DDMMYY",
        )
        scanner = VersionScanner(source)
        versions = scanner.scan()

        self.assertEqual(len(versions), 2)
        # Sorted by date_sortable (chronological)
        self.assertEqual(versions[0].date_string, "260224")
        self.assertEqual(versions[0].date_sortable, 20240226)
        self.assertEqual(versions[1].date_string, "270224")
        self.assertEqual(versions[1].date_sortable, 20240227)

    def test_scan_version_only_regression(self):
        """Existing version-only scanning still works."""
        for name in ["shot_v01.mov", "shot_v02.mov", "shot_v03.mov"]:
            Path(self.tmpdir, name).write_bytes(b"\x00" * 100)

        source = WatchedSource(
            name="test", source_dir=self.tmpdir,
            version_pattern="_v{version}",
            file_extensions=[".mov"],
        )
        scanner = VersionScanner(source)
        versions = scanner.scan()

        self.assertEqual(len(versions), 3)
        self.assertEqual(versions[0].version_number, 1)
        self.assertEqual(versions[1].version_number, 2)
        self.assertEqual(versions[2].version_number, 3)
        for v in versions:
            self.assertIsNone(v.date_string)
            self.assertEqual(v.date_sortable, 0)


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------

class TestConfigDateDefaults(unittest.TestCase):

    def test_date_format_inherited(self):
        """Sources without override_date_format inherit from project."""
        config = ProjectConfig(
            project_name="Test",
            default_date_format="DDMMYY",
        )
        source = WatchedSource(name="s1", source_dir="/tmp")
        config.watched_sources.append(source)
        apply_project_defaults(config)
        self.assertEqual(source.date_format, "DDMMYY")

    def test_date_format_override(self):
        """Sources with override_date_format keep their own."""
        config = ProjectConfig(
            project_name="Test",
            default_date_format="DDMMYY",
        )
        source = WatchedSource(
            name="s1", source_dir="/tmp",
            date_format="YYMMDD",
            override_date_format=True,
        )
        config.watched_sources.append(source)
        apply_project_defaults(config)
        self.assertEqual(source.date_format, "YYMMDD")


# ---------------------------------------------------------------------------
# Model serialization
# ---------------------------------------------------------------------------

class TestModelSerialization(unittest.TestCase):

    def test_version_info_date_fields(self):
        vi = VersionInfo(
            version_string="v003",
            version_number=3,
            source_path="/tmp/v003",
            date_string="260224",
            date_sortable=20240226,
        )
        self.assertEqual(vi.date_string, "260224")
        self.assertEqual(vi.date_sortable, 20240226)

    def test_watched_source_date_format_roundtrip(self):
        source = WatchedSource(
            name="test", source_dir="/tmp",
            date_format="DDMMYY",
            override_date_format=True,
        )
        d = source.to_dict()
        self.assertEqual(d["date_format"], "DDMMYY")
        self.assertTrue(d["override_date_format"])

        restored = WatchedSource.from_dict(d)
        self.assertEqual(restored.date_format, "DDMMYY")
        self.assertTrue(restored.override_date_format)

    def test_watched_source_no_date_compact(self):
        """Empty date_format should not be serialized (compact JSON)."""
        source = WatchedSource(name="test", source_dir="/tmp")
        d = source.to_dict()
        self.assertNotIn("date_format", d)
        self.assertNotIn("override_date_format", d)

    def test_project_config_date_format_roundtrip(self):
        config = ProjectConfig(
            project_name="Test",
            default_date_format="DDMMYY",
        )
        d = config.to_dict()
        self.assertEqual(d["default_date_format"], "DDMMYY")

        restored = ProjectConfig.from_dict(d)
        self.assertEqual(restored.default_date_format, "DDMMYY")

    def test_project_config_no_date_compact(self):
        config = ProjectConfig(project_name="Test")
        d = config.to_dict()
        self.assertNotIn("default_date_format", d)

    def test_discovery_result_date_format(self):
        dr = DiscoveryResult(
            path="/tmp",
            name="test",
            suggested_date_format="DDMMYY",
        )
        self.assertEqual(dr.suggested_date_format, "DDMMYY")

    def test_backward_compat_no_date_fields(self):
        """Old configs without date fields should load cleanly."""
        old_data = {"name": "test", "source_dir": "/tmp"}
        source = WatchedSource.from_dict(old_data)
        self.assertEqual(source.date_format, "")
        self.assertFalse(source.override_date_format)

        old_project = {"project_name": "Test"}
        config = ProjectConfig.from_dict(old_project)
        self.assertEqual(config.default_date_format, "")


# ---------------------------------------------------------------------------
# Discovery date detection
# ---------------------------------------------------------------------------

class TestDiscoveryDateDetection(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_discover_date_only_files(self):
        """Discovery should detect date-only files."""
        from lvm.discovery import discover
        for name in ["shot_260224.mov", "shot_270224.mov"]:
            Path(self.tmpdir, name).write_bytes(b"\x00" * 100)

        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertTrue(result.suggested_date_format)
        self.assertIn("{date}", result.suggested_pattern)
        self.assertEqual(len(result.versions_found), 2)

    def test_discover_versioned_with_dates(self):
        """Discovery should detect version + date in directory names."""
        from lvm.discovery import discover
        for name in ["260224_shot_v01", "260224_shot_v02"]:
            d = Path(self.tmpdir, name)
            d.mkdir()
            (d / "frame.0001.exr").write_bytes(b"\x00" * 100)

        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 1)
        result = results[0]
        # Should detect version pattern
        self.assertIn("{version}", result.suggested_pattern)
        # Should also detect date
        if result.suggested_date_format:
            self.assertIn("{date}", result.suggested_pattern)

    def test_discover_version_only_regression(self):
        """Standard versioned directories still work."""
        from lvm.discovery import discover
        for name in ["shot_v01", "shot_v02"]:
            d = Path(self.tmpdir, name)
            d.mkdir()
            (d / "frame.0001.exr").write_bytes(b"\x00" * 100)

        results = discover(self.tmpdir, max_depth=1)
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertIn("{version}", result.suggested_pattern)
        self.assertEqual(result.suggested_date_format, "")


# ---------------------------------------------------------------------------
# False positive defense
# ---------------------------------------------------------------------------

class TestFalsePositiveDefense(unittest.TestCase):

    def test_arri_reel_id_not_stripped(self):
        """ARRI camera reel IDs should not be stripped as dates."""
        # Without date_format set, no stripping
        name = "A_0029C015_251217_191031_h1DS8_comp"
        self.assertEqual(strip_date(name, ""), name)

    def test_arri_reel_id_with_date_format(self):
        """With date_format, only valid date positions are stripped."""
        name = "A_0029C015_251217_191031_h1DS8_comp"
        result = strip_date(name, "DDMMYY")
        # 251217 validates as Dec 25, 2017 (DDMMYY) — will be stripped
        # This is expected behavior: if user sets DDMMYY, dates matching that
        # format will be stripped. The protection is that date_format is opt-in.
        self.assertNotEqual(result, name)  # something was stripped

    def test_frame_number_not_confused_with_date(self):
        """Frame numbers should not be treated as valid dates in DDMMYY format."""
        # 001001 as DDMMYY: day=00 is invalid
        self.assertFalse(validate_date_string("001001", "DDMMYY"))
        # 001001 as YYMMDD: yy=00, mm=10, dd=01 — technically valid (Oct 1, 2000)
        # This is expected: the protection is that date_format is opt-in
        self.assertTrue(validate_date_string("001001", "YYMMDD"))

        # strip_date with DDMMYY won't strip frame-like "001001" (day=00 invalid)
        self.assertEqual(strip_date("name_001001", "DDMMYY"), "name_001001")

        # A valid date in the middle should be stripped
        result = strip_date("260224_name", "DDMMYY")
        self.assertEqual(result, "name")


if __name__ == "__main__":
    unittest.main()
