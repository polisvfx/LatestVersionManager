"""Tests for container render-completion checks (src/lvm/render_check.py)
and their use by Promoter.promote()."""

import os
import shutil
import struct
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lvm import render_check as rc
from lvm.models import VersionInfo, WatchedSource
from lvm.promoter import Promoter, PromotionError


def _atom(kind: bytes, payload: bytes = b"") -> bytes:
    return struct.pack(">I4s", 8 + len(payload), kind) + payload


FTYP = _atom(b"ftyp", b"qt  \x00\x00\x02\x00qt  ")


def finished_mov() -> bytes:
    return FTYP + _atom(b"wide") + _atom(b"mdat", b"\x00" * 64) + _atom(b"moov", b"\x00" * 32)


def rendering_mov_size_zero() -> bytes:
    # mdat size placeholder 0, no moov yet
    return FTYP + _atom(b"wide") + struct.pack(">I4s", 0, b"mdat") + b"\x00" * 64


def rendering_mov_overrun() -> bytes:
    # mdat size reserved larger than what has been written so far
    return FTYP + struct.pack(">I4s", 8 + 10_000, b"mdat") + b"\x00" * 64


def rendering_mov_64bit_placeholder() -> bytes:
    return FTYP + struct.pack(">I4sQ", 1, b"mdat", 0) + b"\x00" * 64


_MXF_PREFIX = bytes.fromhex("060E2B34020501010D01020101")


def mxf_header(status: int) -> bytes:
    return _MXF_PREFIX + bytes([0x02, status, 0x00]) + b"\x83\x00\x00\x10" + b"\x00" * 16


def mxf_footer() -> bytes:
    return _MXF_PREFIX + bytes([0x04, 0x04, 0x00]) + b"\x83\x00\x00\x10" + b"\x00" * 16


def mxf_rip() -> bytes:
    body = bytes.fromhex("060E2B34020501010D01020101110100") + b"\x83\x00\x00\x0c" + b"\x00" * 12
    return body + struct.pack(">I", len(body) + 4)


def avi(riff_size=None, payload=b"\x00" * 40) -> bytes:
    body = b"AVI " + payload
    size = len(body) if riff_size is None else riff_size
    return struct.pack("<4sI", b"RIFF", size) + body


class _TmpDirTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_render_"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, name: str, data: bytes) -> Path:
        p = self.tmp / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return p


class TestQuickTimeStructure(_TmpDirTest):
    def test_finished_mov_is_complete(self):
        p = self.write("a_v001.mov", finished_mov())
        self.assertEqual(rc.container_structure_status(p)[0], rc.COMPLETE)

    def test_moov_first_faststart_is_complete(self):
        data = FTYP + _atom(b"moov", b"\x00" * 32) + _atom(b"mdat", b"\x00" * 64)
        p = self.write("a_v001.mp4", data)
        self.assertEqual(rc.container_structure_status(p)[0], rc.COMPLETE)

    def test_size_zero_mdat_without_moov_is_incomplete(self):
        p = self.write("a_v001.mov", rendering_mov_size_zero())
        self.assertEqual(rc.container_structure_status(p)[0], rc.INCOMPLETE)

    def test_mdat_past_eof_is_incomplete(self):
        p = self.write("a_v001.mov", rendering_mov_overrun())
        status, reason = rc.container_structure_status(p)
        self.assertEqual(status, rc.INCOMPLETE)
        self.assertIn("past end of file", reason)

    def test_64bit_placeholder_is_incomplete(self):
        p = self.write("a_v001.mov", rendering_mov_64bit_placeholder())
        self.assertEqual(rc.container_structure_status(p)[0], rc.INCOMPLETE)

    def test_missing_moov_is_incomplete(self):
        p = self.write("a_v001.mov", FTYP + _atom(b"mdat", b"\x00" * 64))
        self.assertIn("moov", rc.container_structure_status(p)[1])

    def test_empty_file_is_incomplete(self):
        p = self.write("a_v001.mov", b"")
        self.assertEqual(rc.container_structure_status(p)[0], rc.INCOMPLETE)

    def test_fragmented_mp4_is_unknown(self):
        data = FTYP + _atom(b"moov", b"\x00" * 8) + _atom(b"moof", b"\x00" * 8) + _atom(b"mdat")
        p = self.write("a_v001.mp4", data)
        self.assertEqual(rc.container_structure_status(p)[0], rc.UNKNOWN)

    def test_non_quicktime_bytes_are_unknown_not_incomplete(self):
        # Placeholder bytes like those used throughout the test suite.
        p = self.write("a_v001.mov", b"mov content v001")
        self.assertEqual(rc.container_structure_status(p)[0], rc.UNKNOWN)


class TestMxfStructure(_TmpDirTest):
    def test_closed_complete_header_is_complete(self):
        p = self.write("a.mxf", mxf_header(0x04) + b"\x00" * 100)
        self.assertEqual(rc.container_structure_status(p)[0], rc.COMPLETE)

    def test_open_header_without_footer_is_incomplete(self):
        p = self.write("a.mxf", mxf_header(0x01) + b"\x00" * 100)
        self.assertEqual(rc.container_structure_status(p)[0], rc.INCOMPLETE)

    def test_open_header_with_footer_is_complete(self):
        p = self.write("a.mxf", mxf_header(0x01) + b"\x00" * 100 + mxf_footer())
        self.assertEqual(rc.container_structure_status(p)[0], rc.COMPLETE)

    def test_open_header_with_rip_is_complete(self):
        p = self.write("a.mxf", mxf_header(0x02) + b"\x00" * 100 + mxf_rip())
        self.assertEqual(rc.container_structure_status(p)[0], rc.COMPLETE)

    def test_run_in_before_header(self):
        p = self.write("a.mxf", b"\xff" * 300 + mxf_header(0x01) + b"\x00" * 50)
        self.assertEqual(rc.container_structure_status(p)[0], rc.INCOMPLETE)

    def test_non_mxf_is_unknown(self):
        p = self.write("a.mxf", b"not an mxf at all")
        self.assertEqual(rc.container_structure_status(p)[0], rc.UNKNOWN)


class TestAviStructure(_TmpDirTest):
    def test_finished_avi_is_complete(self):
        p = self.write("a.avi", avi())
        self.assertEqual(rc.container_structure_status(p)[0], rc.COMPLETE)

    def test_zero_riff_size_is_incomplete(self):
        p = self.write("a.avi", avi(riff_size=0))
        self.assertEqual(rc.container_structure_status(p)[0], rc.INCOMPLETE)

    def test_riff_past_eof_is_incomplete(self):
        p = self.write("a.avi", avi(riff_size=100_000))
        self.assertEqual(rc.container_structure_status(p)[0], rc.INCOMPLETE)


class TestCombinedVerdict(_TmpDirTest):
    def test_sequences_are_ignored(self):
        p = self.write("a_v001.1001.exr", b"")
        self.assertIsNone(rc.render_in_progress_reason(p))

    def test_finished_container_passes(self):
        p = self.write("a_v001.mov", finished_mov())
        self.assertIsNone(rc.render_in_progress_reason(p, settle_seconds=60))

    def test_settle_applies_only_to_unknown_structure(self):
        p = self.write("a_v001.mkv", b"\x1a\x45\xdf\xa3 matroska")
        self.assertIsNone(rc.render_in_progress_reason(p))
        self.assertIsNotNone(rc.render_in_progress_reason(p, settle_seconds=60))
        old = time.time() - 120
        os.utime(p, (old, old))
        self.assertIsNone(rc.render_in_progress_reason(p, settle_seconds=60))

    def test_future_mtime_from_clock_skew_does_not_block(self):
        p = self.write("a_v001.mkv", b"x")
        future = time.time() + 3600
        os.utime(p, (future, future))
        self.assertIsNone(rc.render_in_progress_reason(p, settle_seconds=10))

    @unittest.skipUnless(sys.platform == "win32", "Windows share modes")
    def test_open_write_handle_detected(self):
        p = self.write("a_v001.mov", finished_mov())
        self.assertFalse(rc.is_open_for_writing(p))
        with open(p, "ab"):
            self.assertTrue(rc.is_open_for_writing(p))
            self.assertIn("open for writing", rc.render_in_progress_reason(p))
        self.assertFalse(rc.is_open_for_writing(p))

    @unittest.skipUnless(sys.platform == "win32", "Windows share modes")
    def test_reader_does_not_count_as_writer(self):
        p = self.write("a_v001.mov", finished_mov())
        with open(p, "rb"):
            self.assertFalse(rc.is_open_for_writing(p))


class TestPromoterBlocksUnfinishedRenders(_TmpDirTest):
    def setUp(self):
        super().setUp()
        self.source_dir = self.tmp / "renders"
        self.target_dir = self.tmp / "online"
        self.source_dir.mkdir()
        self.target_dir.mkdir()

    def _promoter(self):
        src = WatchedSource(
            name="Comp", source_dir=str(self.source_dir),
            latest_target=str(self.target_dir), version_pattern="_v{version}",
            file_extensions=[".mov", ".exr"], link_mode="copy",
            override_latest_target=True,
        )
        return Promoter(src)

    def test_flat_single_file_rendering_is_blocked(self):
        p = self.write("renders/comp_v002.mov", rendering_mov_size_zero())
        version = VersionInfo("v002", 2, str(p), file_count=1)
        with self.assertRaises(PromotionError) as ctx:
            self._promoter().promote(version, user="t")
        self.assertIn("Still rendering", str(ctx.exception))
        self.assertEqual(list(self.target_dir.glob("*.mov")), [])

    def test_force_overrides(self):
        p = self.write("renders/comp_v002.mov", rendering_mov_size_zero())
        version = VersionInfo("v002", 2, str(p), file_count=1)
        self._promoter().promote(version, user="t", force=True)
        self.assertEqual(len(list(self.target_dir.glob("*.mov"))), 1)

    def test_finished_file_promotes(self):
        p = self.write("renders/comp_v002.mov", finished_mov())
        version = VersionInfo("v002", 2, str(p), file_count=1)
        self._promoter().promote(version, user="t")
        self.assertEqual(len(list(self.target_dir.glob("*.mov"))), 1)

    def test_container_inside_version_folder_is_checked(self):
        vdir = self.source_dir / "comp_v003"
        self.write("renders/comp_v003/comp_v003.mov", rendering_mov_overrun())
        version = VersionInfo("v003", 3, str(vdir), file_count=1)
        unfinished = self._promoter().find_unfinished_renders(version)
        self.assertEqual([p.name for p, _ in unfinished], ["comp_v003.mov"])

    def test_flat_layout_only_checks_this_version(self):
        self.write("renders/comp_v001.mov", rendering_mov_size_zero())  # other version
        self.write("renders/comp_v002.mov", finished_mov())
        version = VersionInfo("v002", 2, str(self.source_dir), file_count=1)
        self.assertEqual(self._promoter().find_unfinished_renders(version), [])


if __name__ == "__main__":
    unittest.main()
