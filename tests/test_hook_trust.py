"""Unit tests for lvm.hook_trust — per-project hook fingerprinting and approval."""

import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from lvm.hook_trust import (
    add_trusted_hook,
    collect_source_hooks,
    hook_fingerprint,
    is_hook_trusted,
)
from lvm.models import WatchedSource


class TestHookTrust(unittest.TestCase):
    def test_empty_hooks_are_always_trusted(self):
        self.assertTrue(is_hook_trusted("/proj/a.json", "", "", []))
        self.assertTrue(is_hook_trusted("/proj/a.json", "   ", "  ", []))

    def test_fingerprint_isolated_by_project_and_command(self):
        fp1 = hook_fingerprint("/proj/a.json", "echo pre", "echo post")
        fp2 = hook_fingerprint("/proj/b.json", "echo pre", "echo post")
        fp3 = hook_fingerprint("/proj/a.json", "echo pre2", "echo post")
        self.assertNotEqual(fp1, fp2)
        self.assertNotEqual(fp1, fp3)

    def test_add_trusted_hook_idempotent(self):
        trusted = add_trusted_hook("/proj/a.json", "echo pre", "", [])
        self.assertTrue(is_hook_trusted("/proj/a.json", "echo pre", "", trusted))
        self.assertFalse(is_hook_trusted("/proj/a.json", "echo other", "", trusted))
        trusted2 = add_trusted_hook("/proj/a.json", "echo pre", "", trusted)
        self.assertEqual(trusted, trusted2)

    def test_collect_source_hooks(self):
        s1 = WatchedSource(name="a", source_dir="/a", pre_promote_cmd="echo 1")
        s2 = WatchedSource(name="b", source_dir="/b")
        s3 = WatchedSource(name="c", source_dir="/c", post_promote_cmd="echo 2")
        self.assertEqual(
            collect_source_hooks([s1, s2, s3]),
            [("a", "echo 1", ""), ("c", "", "echo 2")],
        )


if __name__ == "__main__":
    unittest.main()
