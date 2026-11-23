"""Smoke test for tools/bench_scan.py so the benchmark doesn't rot as the
scanner and promoter APIs change. Tiny tree, local temp dir. Portable."""

import contextlib
import io
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "tools"))

import bench_scan  # noqa: E402


class TestBenchScan(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_bench_test_"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _main(self, *extra):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = bench_scan.main(["--root", str(self.tmp), "--sources", "2",
                                  "--versions", "2", "--frames", "3", *extra])
        return rc, out.getvalue()

    def test_all_phases_run_and_succeed(self):
        rc, out = self._main()
        self.assertEqual(rc, 0)
        for phase in ("scan-cold", "scan-warm", "promote-copy", "verify",
                      "dry-run", "repromote", "promote-link"):
            self.assertIn(phase, out)
        self.assertNotIn("failed:", out)
        self.assertIn("reused 2 folder(s)", out)  # older version per source
        self.assertFalse((self.tmp / bench_scan.TREE_NAME).exists())

    def test_keep_then_reuse(self):
        self._main("--keep")
        self.assertTrue((self.tmp / bench_scan.TREE_NAME / "src1" / "online").is_dir())
        rc, out = self._main("--reuse")
        self.assertEqual(rc, 0)
        self.assertNotIn("Generated", out)

    def test_profile_writes_stats(self):
        prof = self.tmp / "prof"
        self._main("--profile", str(prof))
        self.assertTrue((prof / "scan-cold.prof").is_file())
        self.assertIn("cumulative", (prof / "promote-copy.txt").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
