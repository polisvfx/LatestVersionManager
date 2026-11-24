"""Unit tests for the CLI entry point (main.py)."""

import argparse
import io
import json
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

import main as cli
from lvm.config import load_config, save_config
from lvm.models import ProjectConfig, WatchedSource


class TestCLIWorkflow(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_cli_"))
        self.renders = self.tmp / "renders" / "shot010"
        self.online = self.tmp / "online" / "shot010"
        (self.renders / "shot010_v001").mkdir(parents=True)
        (self.renders / "shot010_v002").mkdir(parents=True)
        for v in ("v001", "v002"):
            for f in (1001, 1002):
                (self.renders / f"shot010_{v}" / f"shot010_{v}.{f}.exr").write_bytes(v.encode())
        self.online.mkdir(parents=True)

        self.cfg_path = self.tmp / "proj.json"
        cfg = ProjectConfig(
            project_name="DemoShow",
            latest_path_template="",
            timecode_mode="never",
            watched_sources=[
                WatchedSource(
                    name="shot010",
                    source_dir=str(self.renders),
                    latest_target=str(self.online),
                    override_latest_target=True,
                    file_extensions=[".exr"],
                    sample_filename="shot010_v001.1001.exr",
                )
            ],
        )
        save_config(cfg, str(self.cfg_path))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run_stdout(self, fn, args) -> str:
        buf = io.StringIO()
        with redirect_stdout(buf):
            fn(args)
        return buf.getvalue()

    def test_cmd_setup_respects_output_directory(self):
        out_dir = self.tmp / "nested" / "sub"
        out_dir.mkdir(parents=True)
        out_file = out_dir / "custom_proj.json"
        args = argparse.Namespace(
            name="MyShow", dir=None, whitelist="comp", blacklist="wip",
            tasks="comp,grade", output=str(out_file), template="",
        )
        self._run_stdout(cli.cmd_setup, args)
        self.assertTrue(out_file.is_file())
        loaded = load_config(str(out_file))
        self.assertEqual(loaded.project_name, "MyShow")
        self.assertEqual(loaded.task_tokens, ["comp", "grade"])

    def test_cmd_discover_with_no_timecode(self):
        args = argparse.Namespace(
            directory=str(self.tmp / "renders"), depth=3, extensions=[".exr"],
            whitelist="", blacklist="", no_timecode=True,
        )
        with mock.patch.object(cli, "populate_timecodes") as mock_tc:
            out = self._run_stdout(cli.cmd_discover, args)
        mock_tc.assert_not_called()
        self.assertIn("shot010", out)

    def test_cmd_scan_status_validate(self):
        args = argparse.Namespace(config=str(self.cfg_path))
        scan_out = self._run_stdout(cli.cmd_scan, args)
        self.assertIn("v001", scan_out)
        self.assertIn("v002", scan_out)

        status_out = self._run_stdout(cli.cmd_status, args)
        self.assertIn("NOT SET", status_out)

        # Ensure {source_title} in latest_path_template produces zero warnings
        cfg = load_config(str(self.cfg_path))
        cfg.latest_path_template = "{project_root}/{source_title}_latest"
        save_config(cfg, str(self.cfg_path))
        val_out = self._run_stdout(cli.cmd_validate, args)
        self.assertNotIn("Unknown tokens", val_out)

    def test_promote_history_verify_undo_and_no_hooks(self):
        # Add a failing hook to verify --no-hooks suppresses it
        cfg = load_config(str(self.cfg_path))
        cfg.watched_sources[0].pre_promote_cmd = "exit 9"
        cfg.watched_sources[0].override_pre_promote_cmd = True
        save_config(cfg, str(self.cfg_path))

        # Dry run
        dry_args = argparse.Namespace(
            config=str(self.cfg_path), source_name="shot010", version="v001",
            yes=True, force=False, dry_run=True, no_hooks=True, report=None,
        )
        dry_out = self._run_stdout(cli.cmd_promote, dry_args)
        self.assertIn("Dry Run", dry_out)

        # Promote v001 with --no-hooks and report
        rep = self.tmp / "report.json"
        p1 = argparse.Namespace(
            config=str(self.cfg_path), source_name="shot010", version="v001",
            yes=True, force=False, dry_run=False, no_hooks=True, report=str(rep),
        )
        self._run_stdout(cli.cmd_promote, p1)
        self.assertTrue(rep.is_file())
        self.assertEqual(json.loads(rep.read_text(encoding="utf-8"))["version"], "v001")

        # Verify succeeds (exit 0 / no SystemExit)
        self._run_stdout(cli.cmd_verify, argparse.Namespace(config=str(self.cfg_path)))

        # Promote-all advances to v002 with --no-hooks
        pa = argparse.Namespace(
            config=str(self.cfg_path), yes=True, force=False, dry_run=False,
            no_hooks=True, report=None,
        )
        pa_out = self._run_stdout(cli.cmd_promote_all, pa)
        self.assertIn("1/1 succeeded", pa_out)

        # History shows v002 then v001
        hist_out = self._run_stdout(
            cli.cmd_history,
            argparse.Namespace(config=str(self.cfg_path), source_name="shot010"),
        )
        self.assertIn("v002", hist_out)
        self.assertIn("v001", hist_out)

        # Clear failing hook so rollback works cleanly
        cfg = load_config(str(self.cfg_path))
        cfg.watched_sources[0].pre_promote_cmd = ""
        save_config(cfg, str(self.cfg_path))

        rb = argparse.Namespace(
            config=str(self.cfg_path), source_name="shot010", yes=True, report=None,
        )
        rb_out = self._run_stdout(cli.cmd_rollback, rb)
        self.assertIn("now at v001", rb_out)

    def test_promote_unknown_source_exits_nonzero(self):
        args = argparse.Namespace(
            config=str(self.cfg_path), source_name="nonexistent", version="v001",
            yes=True, force=False, dry_run=False, no_hooks=False, report=None,
        )
        with self.assertRaises(SystemExit) as ctx:
            self._run_stdout(cli.cmd_promote, args)
        self.assertEqual(ctx.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
