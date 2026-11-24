"""Unit tests for lvm.updater — version parsing, URL/checksum verification, ZIP extraction, cancellation."""

import hashlib
import shutil
import sys
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from unittest import mock

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from lvm import updater


class TestUpdaterVersionAndSecurity(unittest.TestCase):
    def test_parse_version_and_is_newer_prereleases(self):
        self.assertEqual(updater.parse_version("v1.2.3"), ((1, 2, 3), 1, ()))
        self.assertTrue(updater.is_newer("1.2.0", "1.2.0-rc1"))
        self.assertFalse(updater.is_newer("1.2.0-rc1", "1.2.0"))
        self.assertTrue(updater.is_newer("1.2.0-rc2", "1.2.0-rc1"))
        self.assertTrue(updater.is_newer("2.0.0", "1.9.9"))
        self.assertFalse(updater.is_newer("1.0.0", "1.0.0"))

    def test_sanitize_asset_name(self):
        self.assertEqual(updater.sanitize_asset_name("LVM-win.zip"), "LVM-win.zip")
        with self.assertRaises(updater.UpdateError):
            updater.sanitize_asset_name("../evil.zip")
        with self.assertRaises(updater.UpdateError):
            updater.sanitize_asset_name("sub/evil.zip")
        with self.assertRaises(updater.UpdateError):
            updater.sanitize_asset_name("")

    def test_validate_github_url_allowlist(self):
        # Allowed GitHub HTTPS hosts
        updater.validate_github_url("https://github.com/polisvfx/LatestVersionManager/releases/download/v1.0/app.zip")
        updater.validate_github_url("https://objects.githubusercontent.com/some/asset.zip")
        # Reject HTTP and non-GitHub hosts
        with self.assertRaises(updater.UpdateError):
            updater.validate_github_url("http://github.com/polisvfx/a.zip")
        with self.assertRaises(updater.UpdateError):
            updater.validate_github_url("https://evil.example.com/a.zip")
        with self.assertRaises(updater.UpdateError):
            updater.validate_github_url("")

    def test_parse_and_compute_sha256(self):
        tmp = Path(tempfile.mkdtemp(prefix="lvm_upd_"))
        try:
            f = tmp / "asset.zip"
            f.write_bytes(b"hello update")
            digest = hashlib.sha256(b"hello update").hexdigest()
            parsed = updater.parse_sha256sums(f"{digest}  asset.zip\n")
            self.assertEqual(parsed["asset.zip"], digest)
            self.assertEqual(updater.compute_file_sha256(f), digest)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestUpdaterExtractAndDownload(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_upd_ext_"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _release(self, checksums_url: str = "") -> updater.ReleaseInfo:
        return updater.ReleaseInfo(
            tag_name="v2.0.0",
            version="2.0.0",
            name="Release 2.0.0",
            body="",
            html_url="https://github.com/polisvfx/LatestVersionManager/releases/tag/v2.0.0",
            asset_url="https://github.com/polisvfx/LatestVersionManager/releases/download/v2.0.0/app.zip",
            asset_name="app.zip",
            asset_size=10,
            checksums_url=checksums_url,
        )

    def test_extract_update_valid_and_rejects_traversal(self):
        good_zip = self.tmp / "good.zip"
        with zipfile.ZipFile(good_zip, "w") as zf:
            zf.writestr("LatestVersionManager/app.exe", b"binary")
            zf.writestr("LatestVersionManager/readme.txt", b"ok")
        out = updater.extract_update(good_zip, self.tmp / "out_good")
        self.assertTrue((out / "app.exe").is_file())

        bad_zip = self.tmp / "bad.zip"
        with zipfile.ZipFile(bad_zip, "w") as zf:
            zf.writestr("../../outside.txt", b"pwn")
        with self.assertRaises(updater.UpdateError):
            updater.extract_update(bad_zip, self.tmp / "out_bad")

    def test_extract_update_rejects_symlink_member(self):
        sym_zip = self.tmp / "sym.zip"
        with zipfile.ZipFile(sym_zip, "w") as zf:
            zi = zipfile.ZipInfo("LatestVersionManager/link")
            zi.create_system = 3  # Unix
            zi.external_attr = (0o120777 << 16)
            zf.writestr(zi, "/etc/passwd")
        with self.assertRaises(updater.UpdateError):
            updater.extract_update(sym_zip, self.tmp / "out_sym")

    def test_download_update_fails_closed_without_checksum_asset(self):
        info = self._release(checksums_url="")
        with self.assertRaises(updater.UpdateDownloadError) as ctx:
            updater.download_update(info, str(self.tmp))
        self.assertIn("SHA256SUMS", str(ctx.exception))

    def test_download_update_respects_cancel_event(self):
        info = self._release(
            checksums_url="https://github.com/polisvfx/LatestVersionManager/releases/download/v2.0.0/SHA256SUMS.txt"
        )
        ev = threading.Event()
        ev.set()
        with mock.patch.object(updater, "fetch_expected_sha256", return_value="a" * 64):
            with self.assertRaises(updater.UpdateDownloadError) as ctx:
                updater.download_update(info, str(self.tmp), cancel_event=ev)
        self.assertIn("cancel", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
