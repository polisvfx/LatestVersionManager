"""
Tests for src/lvm/timecode.py — unit tests using synthetic fixtures.

Covers _decode_smpte_timecode, _is_valid_timecode_string,
_read_exr_timecode, _read_dpx_timecode, ffprobe extraction,
and populate_timecodes.
"""

import json
import os
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root and src to path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "src"))

from lvm.timecode import (
    _decode_smpte_timecode,
    _is_valid_timecode_string,
    _read_exr_timecode,
    _read_dpx_timecode,
    _extract_timecode_ffprobe,
    extract_timecode,
    populate_timecodes,
)
from lvm.models import VersionInfo


# ---------------------------------------------------------------------------
# Helpers for building synthetic binary headers
# ---------------------------------------------------------------------------

def _pack_smpte(hours, minutes, seconds, frames):
    """Pack HH:MM:SS:FF into SMPTE 12M 8-byte little-endian format."""
    tc = (
        (frames % 10) |
        ((frames // 10) << 4) |
        ((seconds % 10) << 8) |
        ((seconds // 10) << 12) |
        ((minutes % 10) << 16) |
        ((minutes // 10) << 20) |
        ((hours % 10) << 24) |
        ((hours // 10) << 28)
    )
    return struct.pack("<II", tc, 0)


def _make_exr_with_timecode(hours, minutes, seconds, frames):
    """Build a minimal valid EXR header with a timeCode attribute."""
    magic = b"\x76\x2f\x31\x01"
    version = b"\x02\x00\x00\x00"
    attr_name = b"timeCode\x00"
    attr_type = b"timecode\x00"
    attr_size = struct.pack("<I", 8)
    attr_value = _pack_smpte(hours, minutes, seconds, frames)
    end_marker = b"\x00"
    return magic + version + attr_name + attr_type + attr_size + attr_value + end_marker


def _make_dpx_be(hours, minutes, seconds, frames):
    """Build a minimal big-endian DPX file with timecode at offset 1920."""
    header = bytearray(2048)
    header[0:4] = b"SDPX"
    tc = (
        ((hours // 10) << 28) | ((hours % 10) << 24) |
        ((minutes // 10) << 20) | ((minutes % 10) << 16) |
        ((seconds // 10) << 12) | ((seconds % 10) << 8) |
        ((frames // 10) << 4) | (frames % 10)
    )
    struct.pack_into(">I", header, 1920, tc)
    return bytes(header)


def _write_temp(data, suffix):
    """Write data to a temp file and return the path (caller must delete)."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.write(fd, data)
    os.close(fd)
    return path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDecodeSmpteTimecode(unittest.TestCase):

    def test_zero(self):
        self.assertEqual(_decode_smpte_timecode(_pack_smpte(0, 0, 0, 0)), "00:00:00:00")

    def test_one_hour(self):
        self.assertEqual(_decode_smpte_timecode(_pack_smpte(1, 0, 0, 0)), "01:00:00:00")

    def test_typical(self):
        self.assertEqual(_decode_smpte_timecode(_pack_smpte(10, 30, 15, 12)), "10:30:15:12")

    def test_max_valid(self):
        self.assertEqual(_decode_smpte_timecode(_pack_smpte(23, 59, 59, 29)), "23:59:59:29")

    def test_invalid_hours(self):
        data = _pack_smpte(24, 0, 0, 0)
        self.assertIsNone(_decode_smpte_timecode(data))

    def test_too_short(self):
        self.assertIsNone(_decode_smpte_timecode(b"\x00" * 4))

    def test_empty(self):
        self.assertIsNone(_decode_smpte_timecode(b""))


class TestIsValidTimecodeString(unittest.TestCase):

    def test_valid_colon(self):
        self.assertTrue(_is_valid_timecode_string("01:00:00:00"))

    def test_valid_semicolon_drop_frame(self):
        self.assertTrue(_is_valid_timecode_string("01:00:00;00"))

    def test_valid_high_values(self):
        self.assertTrue(_is_valid_timecode_string("23:59:59:29"))

    def test_too_short(self):
        self.assertFalse(_is_valid_timecode_string("1:0:0:0"))

    def test_non_numeric(self):
        self.assertFalse(_is_valid_timecode_string("ab:cd:ef:gh"))

    def test_wrong_part_count(self):
        self.assertFalse(_is_valid_timecode_string("01:00:00"))

    def test_empty(self):
        self.assertFalse(_is_valid_timecode_string(""))

    def test_none(self):
        self.assertFalse(_is_valid_timecode_string(None))


class TestReadExrTimecode(unittest.TestCase):

    def test_reads_timecode(self):
        data = _make_exr_with_timecode(1, 0, 0, 0)
        tmp = _write_temp(data, ".exr")
        try:
            self.assertEqual(_read_exr_timecode(Path(tmp)), "01:00:00:00")
        finally:
            os.unlink(tmp)

    def test_typical_timecode(self):
        data = _make_exr_with_timecode(10, 30, 15, 12)
        tmp = _write_temp(data, ".exr")
        try:
            self.assertEqual(_read_exr_timecode(Path(tmp)), "10:30:15:12")
        finally:
            os.unlink(tmp)

    def test_wrong_magic(self):
        data = b"\x00\x00\x00\x00" + b"\x00" * 50
        tmp = _write_temp(data, ".exr")
        try:
            self.assertIsNone(_read_exr_timecode(Path(tmp)))
        finally:
            os.unlink(tmp)

    def test_missing_file(self):
        self.assertIsNone(_read_exr_timecode(Path("/nonexistent/file.exr")))


class TestReadDpxTimecode(unittest.TestCase):

    def test_reads_be_timecode(self):
        data = _make_dpx_be(1, 30, 0, 0)
        tmp = _write_temp(data, ".dpx")
        try:
            self.assertEqual(_read_dpx_timecode(Path(tmp)), "01:30:00:00")
        finally:
            os.unlink(tmp)

    def test_undefined_timecode(self):
        header = bytearray(2048)
        header[0:4] = b"SDPX"
        struct.pack_into(">I", header, 1920, 0xFFFFFFFF)
        tmp = _write_temp(bytes(header), ".dpx")
        try:
            self.assertIsNone(_read_dpx_timecode(Path(tmp)))
        finally:
            os.unlink(tmp)

    def test_wrong_magic(self):
        header = bytearray(2048)
        header[0:4] = b"XXXX"
        tmp = _write_temp(bytes(header), ".dpx")
        try:
            self.assertIsNone(_read_dpx_timecode(Path(tmp)))
        finally:
            os.unlink(tmp)

    def test_missing_file(self):
        self.assertIsNone(_read_dpx_timecode(Path("/nonexistent/file.dpx")))

    def test_le_magic(self):
        """Test little-endian DPX (XPDS magic)."""
        header = bytearray(2048)
        header[0:4] = b"XPDS"
        # Pack LE timecode for 02:00:00:00
        tc = (
            ((0) << 28) | ((2) << 24) |
            ((0) << 20) | ((0) << 16) |
            ((0) << 12) | ((0) << 8) |
            ((0) << 4) | (0)
        )
        struct.pack_into("<I", header, 1920, tc)
        tmp = _write_temp(bytes(header), ".dpx")
        try:
            result = _read_dpx_timecode(Path(tmp))
            self.assertEqual(result, "02:00:00:00")
        finally:
            os.unlink(tmp)


class TestExtractTimecodeFFprobe(unittest.TestCase):

    def test_ffprobe_not_found(self):
        with patch("lvm.timecode.find_ffprobe", return_value=None):
            self.assertIsNone(_extract_timecode_ffprobe(Path("test.mov")))

    def test_format_level_timecode(self):
        output = json.dumps({
            "format": {"tags": {"timecode": "01:00:00:00"}},
            "streams": [],
        })
        mock_result = MagicMock(returncode=0, stdout=output)
        with patch("lvm.timecode.find_ffprobe", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.run", return_value=mock_result):
                self.assertEqual(_extract_timecode_ffprobe(Path("test.mov")), "01:00:00:00")

    def test_stream_level_timecode(self):
        output = json.dumps({
            "format": {"tags": {}},
            "streams": [{"tags": {"timecode": "02:00:00:00"}}],
        })
        mock_result = MagicMock(returncode=0, stdout=output)
        with patch("lvm.timecode.find_ffprobe", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.run", return_value=mock_result):
                self.assertEqual(_extract_timecode_ffprobe(Path("test.mov")), "02:00:00:00")

    def test_no_timecode(self):
        output = json.dumps({"format": {"tags": {}}, "streams": []})
        mock_result = MagicMock(returncode=0, stdout=output)
        with patch("lvm.timecode.find_ffprobe", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.run", return_value=mock_result):
                self.assertIsNone(_extract_timecode_ffprobe(Path("test.mov")))

    def test_ffprobe_error(self):
        mock_result = MagicMock(returncode=1, stdout="")
        with patch("lvm.timecode.find_ffprobe", return_value="/usr/bin/ffprobe"):
            with patch("subprocess.run", return_value=mock_result):
                self.assertIsNone(_extract_timecode_ffprobe(Path("test.mov")))


class TestExtractTimecode(unittest.TestCase):

    def test_routes_exr(self):
        data = _make_exr_with_timecode(1, 0, 0, 0)
        tmp = _write_temp(data, ".exr")
        try:
            self.assertEqual(extract_timecode(Path(tmp)), "01:00:00:00")
        finally:
            os.unlink(tmp)

    def test_routes_dpx(self):
        data = _make_dpx_be(10, 0, 0, 0)
        tmp = _write_temp(data, ".dpx")
        try:
            self.assertEqual(extract_timecode(Path(tmp)), "10:00:00:00")
        finally:
            os.unlink(tmp)

    def test_routes_mov_to_ffprobe(self):
        with patch("lvm.timecode.find_ffprobe", return_value=None):
            self.assertIsNone(extract_timecode(Path("test.mov")))


class TestPopulateTimecodes(unittest.TestCase):

    def _make_version(self, source_path, timecode=None):
        return VersionInfo(
            version_string="v001",
            version_number=1,
            source_path=source_path,
            start_timecode=timecode,
        )

    def test_skips_already_populated(self):
        v = self._make_version("/fake/path", timecode="01:00:00:00")
        with patch("lvm.timecode.extract_timecode_for_version") as mock:
            populate_timecodes([v])
            mock.assert_not_called()
        self.assertEqual(v.start_timecode, "01:00:00:00")

    def test_populates_none_timecodes(self):
        v = self._make_version("/fake/path", timecode=None)
        with patch("lvm.timecode.extract_timecode_for_version", return_value="02:00:00:00"):
            populate_timecodes([v])
        self.assertEqual(v.start_timecode, "02:00:00:00")

    def test_handles_empty_list(self):
        populate_timecodes([])  # should not raise


if __name__ == "__main__":
    unittest.main(verbosity=2)
