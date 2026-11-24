"""Unit tests for lvm.thumbnail — tool caching, cache key invalidation, atomic writes, fallback."""

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from lvm import thumbnail


class TestThumbnail(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_thumb_"))
        self.cache = self.tmp / "cache"
        # Save and reset global caches
        self._saved = (
            thumbnail._oiiotool_path,
            thumbnail._oiiotool_checked,
            thumbnail._ffmpeg_path,
            thumbnail._ffmpeg_checked,
        )

    def tearDown(self):
        (
            thumbnail._oiiotool_path,
            thumbnail._oiiotool_checked,
            thumbnail._ffmpeg_path,
            thumbnail._ffmpeg_checked,
        ) = self._saved
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_cache_key_changes_with_mtime_or_size(self):
        k1 = thumbnail._cache_key("/a/b", "v001", 1000, 50)
        k2 = thumbnail._cache_key("/a/b", "v001", 2000, 50)
        k3 = thumbnail._cache_key("/a/b", "v001", 1000, 60)
        self.assertNotEqual(k1, k2)
        self.assertNotEqual(k1, k3)

    def test_returns_none_when_no_tools_available(self):
        seq = self.tmp / "seq"
        seq.mkdir()
        (seq / "shot.1001.exr").write_bytes(b"frame")
        with mock.patch.object(thumbnail, "find_oiiotool", return_value=None), \
             mock.patch.object(thumbnail, "find_ffmpeg", return_value=None):
            res = thumbnail.get_thumbnail(str(seq), "v001", [".exr"], str(self.cache))
        self.assertIsNone(res)

    def test_sequence_picks_middle_frame_and_writes_atomically(self):
        seq = self.tmp / "seq"
        seq.mkdir()
        for f in (1001, 1002, 1003):
            (seq / f"shot.{f}.exr").write_bytes(b"exr")

        picked = []

        def fake_oiio(tool, input_file, output_path):
            picked.append(input_file.name)
            output_path.write_bytes(b"\xff\xd8\xff\xd9")
            return True

        with mock.patch.object(thumbnail, "find_oiiotool", return_value="oiiotool"), \
             mock.patch.object(thumbnail, "find_ffmpeg", return_value=None), \
             mock.patch.object(thumbnail, "_generate_with_oiiotool", side_effect=fake_oiio):
            out1 = thumbnail.get_thumbnail(str(seq), "v001", [".exr"], str(self.cache))
            # Second call hits cache without re-invoking oiiotool
            out2 = thumbnail.get_thumbnail(str(seq), "v001", [".exr"], str(self.cache))

        self.assertEqual(picked, ["shot.1002.exr"])
        self.assertEqual(out1, out2)
        self.assertTrue(Path(out1).is_file())
        # Ensure no leftover .tmp files in cache dir
        self.assertEqual([p.name for p in self.cache.glob("*.tmp*")], [])

    def test_ffmpeg_container_falls_back_to_frame_zero_for_short_clips(self):
        clip = self.tmp / "short.mov"
        clip.write_bytes(b"mov")
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            out_path = Path(cmd[-1])
            proc = mock.MagicMock()
            if "-ss" in cmd:
                proc.returncode = 1  # -ss 00:00:01 fails on <1s clip
            else:
                out_path.write_bytes(b"\xff\xd8\xff\xd9")
                proc.returncode = 0
            return proc

        with mock.patch.object(thumbnail, "find_oiiotool", return_value=None), \
             mock.patch.object(thumbnail, "find_ffmpeg", return_value="ffmpeg"), \
             mock.patch("subprocess.run", side_effect=fake_run):
            out = thumbnail.get_thumbnail(str(clip), "v001", [".mov"], str(self.cache))

        self.assertIsNotNone(out)
        self.assertEqual(len(calls), 2)
        self.assertIn("-ss", calls[0])
        self.assertNotIn("-ss", calls[1])


if __name__ == "__main__":
    unittest.main()
