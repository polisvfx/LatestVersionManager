"""Regression tests for P1 security, locking, updater, recovery, and hook-trust hardening."""

import hashlib
import json
import os
import stat
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lvm import hooks, hook_trust, updater, templates
from lvm.lockfile import PromoteLock, LockHeldError
from lvm.models import ProjectConfig, VersionInfo, WatchedSource
from lvm.promoter import BACKUP_DIRNAME, Promoter, PromotionError, _SWAP_COMMITTED_MARKER


class TestHooksSecurity(unittest.TestCase):
    """Shell-token quoting, single-pass substitution, and hook trust."""

    def test_single_pass_substitution_prevents_chained_expansion(self):
        out = hooks._substitute_tokens(
            "echo {source_name} {version}",
            {"source_name": "{version}", "version": "v005"},
            quote=False,
        )
        self.assertEqual(out, "echo {version} v005")

    def test_posix_shell_quoting_neutralizes_metacharacters(self):
        q = hooks.quote_shell_token("shot; rm -rf /; $(whoami)", platform_name="linux")
        self.assertEqual(q, "'shot; rm -rf /; $(whoami)'")

    def test_windows_shell_quoting_neutralizes_ampersand_and_percent(self):
        q = hooks.quote_shell_token("Film & VFX %TEMP%", platform_name="win32")
        self.assertTrue(q.startswith('"') and q.endswith('"'))
        self.assertIn("%%TEMP%%", q)

    def test_hook_trust_fingerprinting_and_detection(self):
        fp = hook_trust.hook_fingerprint("/proj/show.json", "echo pre", "echo post")
        self.assertEqual(len(fp), 64)
        self.assertFalse(hook_trust.is_hook_trusted("/proj/show.json", "echo pre", "echo post", []))
        trusted = hook_trust.add_trusted_hook("/proj/show.json", "echo pre", "echo post", [])
        self.assertTrue(hook_trust.is_hook_trusted("/proj/show.json", "echo pre", "echo post", trusted))
        # Editing command invalidates trust
        self.assertFalse(hook_trust.is_hook_trusted("/proj/show.json", "echo evil", "echo post", trusted))
        # Different project path invalidates trust
        self.assertFalse(hook_trust.is_hook_trusted("/other/show.json", "echo pre", "echo post", trusted))
        # Empty hooks are always trusted
        self.assertTrue(hook_trust.is_hook_trusted("/proj/show.json", "", "", []))


class TestUpdaterSecurity(unittest.TestCase):
    """URL validation, asset sanitization, SHA-256 verification, zip guards, script quoting."""

    def test_parse_version_semver_prerelease_and_trailing_zeros(self):
        self.assertTrue(updater.is_newer("1.2.0", "1.2.0-beta.1"))
        self.assertFalse(updater.is_newer("1.2.0-beta.1", "1.2.0"))
        self.assertFalse(updater.is_newer("1.0.0", "1.0"))
        self.assertFalse(updater.is_newer("1.0", "1.0.0"))
        self.assertTrue(updater.is_newer("v1.10.0", "v1.9.0"))

    def test_validate_github_url_rejects_non_https_and_foreign_hosts(self):
        self.assertTrue(updater.validate_github_url("https://github.com/polisvfx/LatestVersionManager/releases"))
        with self.assertRaises(updater.UpdateError):
            updater.validate_github_url("http://github.com/a/b")
        with self.assertRaises(updater.UpdateError):
            updater.validate_github_url("https://evil.example.com/payload.zip")
        with self.assertRaises(updater.UpdateError):
            updater.validate_github_url("file:///etc/passwd")

    def test_sanitize_asset_name_blocks_traversal(self):
        self.assertEqual(updater.sanitize_asset_name("LVM-windows.zip"), "LVM-windows.zip")
        for bad in ("../evil.zip", "/abs/evil.zip", "sub\\evil.zip", "..", ""):
            with self.assertRaises(updater.UpdateError):
                updater.sanitize_asset_name(bad)

    def test_parse_sha256sums_and_fail_closed_when_missing(self):
        digest = "a" * 64
        parsed = updater.parse_sha256sums(f"{digest}  LatestVersionManager-windows.zip\n# comment\n")
        self.assertEqual(parsed["LatestVersionManager-windows.zip"], digest)

        rel = updater.ReleaseInfo(
            tag_name="v2.0.0", version="2.0.0", name="v2.0.0", body="",
            html_url="https://github.com/polisvfx/LatestVersionManager/releases/v2.0.0",
            asset_url="https://github.com/polisvfx/LatestVersionManager/releases/download/v2.0.0/LVM-windows.zip",
            asset_name="LVM-windows.zip", asset_size=10, checksums_url="",
        )
        with self.assertRaises(updater.UpdateDownloadError):
            updater.fetch_expected_sha256(rel)

    def test_extract_update_rejects_traversal_and_symlinks(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            zpath = root / "evil.zip"
            with zipfile.ZipFile(zpath, "w") as zf:
                zf.writestr("../escape.txt", "boom")
            with self.assertRaises(updater.UpdateError):
                updater.extract_update(zpath, root / "out")

            zsym = root / "sym.zip"
            with zipfile.ZipFile(zsym, "w") as zf:
                zi = zipfile.ZipInfo("LatestVersionManager/link")
                zi.external_attr = (0o120777 << 16)
                zf.writestr(zi, "/etc/passwd")
            with self.assertRaises(updater.UpdateError):
                updater.extract_update(zsym, root / "out2")

    def test_unix_and_windows_updater_scripts_quote_special_paths(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            extracted = root / "upd"
            install = root / "Film & VFX $(id)"
            extracted.mkdir()
            install.mkdir()
            unix_script = updater._create_unix_updater(extracted, install, install / "LVM", 12345)
            unix_text = unix_script.read_text(encoding="utf-8")
            # shlex.quote uses single quotes so $(id) cannot execute
            self.assertIn("INSTALL_DIR='", unix_text)
            self.assertNotIn("|| true\n    log \"Re-sign done", unix_text)
            unix_script.unlink(missing_ok=True)

            win_script = updater._create_windows_updater(extracted, install, install / "LVM.exe", 12345)
            win_text = win_script.read_text(encoding="utf-8")
            self.assertIn("DisableDelayedExpansion", win_text)
            self.assertNotIn("\npause\n", win_text.lower())
            self.assertNotIn("\\\\lvm_update.log", win_text)
            win_script.unlink(missing_ok=True)


class TestPromoterAndLockHardening(unittest.TestCase):
    """Path traversal in _remap_filename, committed backup recovery, read-only files, lock tokens."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="lvm_p1_")
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_remap_filename_blocks_path_traversal(self):
        src = WatchedSource(
            name="../../evil",
            source_dir=str(self.root),
            latest_target=str(self.root / "out"),
            file_rename_template="{source_title}_latest",
        )
        p = Promoter(src)
        with self.assertRaises(PromotionError):
            p._remap_filename("shot_v001.1001.exr")

    def test_committed_backup_is_not_resurrected(self):
        target = self.root / "latest"
        target.mkdir()
        (target / "live.1001.exr").write_bytes(b"new")
        backup = target / BACKUP_DIRNAME
        backup.mkdir()
        (backup / "retired_old.1099.exr").write_bytes(b"old")
        (backup / _SWAP_COMMITTED_MARKER).write_text("ok", encoding="utf-8")

        src = WatchedSource(name="s", source_dir=str(self.root), latest_target=str(target))
        Promoter(src)._cleanup_orphaned_promotion_dirs(target)

        self.assertFalse(backup.exists())
        self.assertFalse((target / "retired_old.1099.exr").exists())
        self.assertEqual((target / "live.1001.exr").read_bytes(), b"new")

    def test_uncommitted_backup_overwrites_torn_files(self):
        target = self.root / "latest"
        target.mkdir()
        (target / "shot.1001.exr").write_bytes(b"half-new")
        backup = target / BACKUP_DIRNAME
        backup.mkdir()
        (backup / "shot.1001.exr").write_bytes(b"complete-old")

        src = WatchedSource(name="s", source_dir=str(self.root), latest_target=str(target))
        Promoter(src)._cleanup_orphaned_promotion_dirs(target)

        self.assertFalse(backup.exists())
        self.assertEqual((target / "shot.1001.exr").read_bytes(), b"complete-old")

    def test_readonly_target_files_can_be_repromoted(self):
        renders = self.root / "renders"
        target = self.root / "latest"
        v1 = renders / "shot_v001"
        v2 = renders / "shot_v002"
        v1.mkdir(parents=True)
        v2.mkdir(parents=True)
        target.mkdir()
        f1 = v1 / "shot_v001.1001.exr"
        f2 = v2 / "shot_v002.1001.exr"
        f1.write_bytes(b"v1")
        f2.write_bytes(b"v2")

        src = WatchedSource(
            name="shot", source_dir=str(renders), latest_target=str(target),
            version_pattern="_v{version}", file_extensions=[".exr"],
        )
        p = Promoter(src)
        p.promote(VersionInfo("v001", 1, str(v1), file_count=1, total_size_bytes=2), user="t")

        # Mark target file read-only (as CopyFileExW does for read-only sources)
        live = target / "shot.1001.exr"
        os.chmod(live, stat.S_IREAD)
        try:
            p.promote(VersionInfo("v002", 2, str(v2), file_count=1, total_size_bytes=2), user="t")
            self.assertEqual(live.read_bytes(), b"v2")
        finally:
            if live.exists():
                os.chmod(live, stat.S_IWRITE | stat.S_IREAD)

    def test_lock_release_does_not_delete_taken_over_lock(self):
        lock_a = PromoteLock(self.root, stale_after=300.0)
        lock_a.acquire()
        # Simulate another holder taking over the lock with a different token
        other_payload = {
            "pid": os.getpid(),
            "host": "other-host",
            "user": "other",
            "started_at": "2026-11-24T00:00:00",
            "token": "different-holder-token",
        }
        lock_a.path.write_text(json.dumps(other_payload), encoding="utf-8")

        lock_a.release()
        # The other holder's lock file MUST still exist!
        self.assertTrue(lock_a.path.exists())
        data = json.loads(lock_a.path.read_text(encoding="utf-8"))
        self.assertEqual(data["token"], "different-holder-token")


class TestTemplatesHardening(unittest.TestCase):
    """Non-shadowing templates, default resets, and deep copies."""

    def test_apply_template_resets_default_fields_and_deep_copies(self):
        with tempfile.TemporaryDirectory() as td:
            src_cfg = ProjectConfig(project_name="CopyDefault", default_link_mode="copy",
                                    name_whitelist=["comp"])
            tpl_path = templates.save_template(src_cfg, "Base", location="project", project_dir=td)
            tpl_data = templates.load_template(tpl_path)

            target_cfg = ProjectConfig(project_name="Target", default_link_mode="symlink",
                                       block_incomplete_sequences=False)
            templates.apply_template(target_cfg, tpl_data)
            self.assertEqual(target_cfg.default_link_mode, "copy")
            self.assertTrue(target_cfg.block_incomplete_sequences)

            # Mutating target_cfg must not mutate template_data
            target_cfg.name_whitelist.append("extra")
            self.assertEqual(tpl_data["name_whitelist"], ["comp"])

    def test_project_template_does_not_shadow_user_template(self):
        with tempfile.TemporaryDirectory() as td:
            user_dir = Path(td) / "user_tpl"
            proj_dir = Path(td) / "proj"
            user_dir.mkdir()
            proj_dir.mkdir()
            with patch.object(templates, "get_user_templates_dir", return_value=user_dir):
                templates.save_template(ProjectConfig(project_name="Mine"), "Studio", location="user")
                templates.save_template(ProjectConfig(project_name="External", pre_promote_cmd="evil"),
                                        "Studio", location="project", project_dir=str(proj_dir))
                listed = templates.list_templates(str(proj_dir))
            locs = [(t["name"], t["location"]) for t in listed]
            self.assertIn(("Studio", "user"), locs)
            self.assertIn(("Studio", "project"), locs)


if __name__ == "__main__":
    unittest.main()
