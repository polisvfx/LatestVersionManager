"""Unit tests for src.lvm.nle_bridge.

These tests don't require DaVinci Resolve to be installed — they mock
the path probes and the subprocess call so the logic is exercised
hermetically.
"""

import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

# Make the package importable without installing
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.lvm import nle_bridge  # noqa: E402


class TestPathProbes(unittest.TestCase):

    def test_modules_path_returns_none_when_dir_missing(self):
        with mock.patch.object(Path, "is_dir", return_value=False):
            self.assertIsNone(nle_bridge.resolve_modules_path())

    def test_modules_path_returns_path_when_dir_exists(self):
        with mock.patch.object(Path, "is_dir", return_value=True):
            result = nle_bridge.resolve_modules_path()
            self.assertIsNotNone(result)
            self.assertIn("Modules", str(result))

    def test_lib_path_returns_none_when_no_candidate_exists(self):
        with mock.patch.object(Path, "is_file", return_value=False):
            self.assertIsNone(nle_bridge.resolve_script_lib_path())

    def test_lib_path_returns_first_existing(self):
        with mock.patch.object(Path, "is_file", return_value=True):
            result = nle_bridge.resolve_script_lib_path()
            self.assertIsNotNone(result)
            # All platforms reference fusionscript by name
            self.assertIn("fusionscript", result.name.lower())

    def test_companions_dir_points_to_repo(self):
        d = nle_bridge.companions_dir()
        self.assertTrue(d.name == "companions")
        # Sanity: parent is the repo root and the resolve script lives under it
        self.assertTrue(
            (d / "resolve" / "lvm_restore_versions.py").is_file(),
            "companion script not where the bridge expects it",
        )

    def test_resolve_script_path(self):
        p = nle_bridge.resolve_script_path()
        self.assertEqual(p.name, "lvm_restore_versions.py")
        self.assertEqual(p.parent.name, "resolve")


class TestIsAvailable(unittest.TestCase):

    def test_false_when_modules_missing(self):
        with mock.patch.object(nle_bridge, "resolve_modules_path", return_value=None), \
             mock.patch.object(nle_bridge, "resolve_script_lib_path",
                               return_value=Path("/x")):
            self.assertFalse(nle_bridge.is_resolve_external_available())

    def test_false_when_lib_missing(self):
        with mock.patch.object(nle_bridge, "resolve_modules_path",
                               return_value=Path("/x/Modules")), \
             mock.patch.object(nle_bridge, "resolve_script_lib_path",
                               return_value=None):
            self.assertFalse(nle_bridge.is_resolve_external_available())

    def test_true_when_both_present(self):
        with mock.patch.object(nle_bridge, "resolve_modules_path",
                               return_value=Path("/x/Modules")), \
             mock.patch.object(nle_bridge, "resolve_script_lib_path",
                               return_value=Path("/y/fusionscript.dll")):
            self.assertTrue(nle_bridge.is_resolve_external_available())


class TestRunResolveSync(unittest.TestCase):
    """Mock subprocess.run; assert env vars and arguments are correct."""

    def setUp(self):
        self.modules = Path("C:/fake/Resolve/Developer/Scripting/Modules")
        self.lib = Path("C:/fake/Resolve/fusionscript.dll")

    def _patch_paths(self):
        return mock.patch.multiple(
            nle_bridge,
            resolve_modules_path=mock.MagicMock(return_value=self.modules),
            resolve_script_lib_path=mock.MagicMock(return_value=self.lib),
        )

    def test_pre_flight_modules_missing(self):
        with mock.patch.object(nle_bridge, "resolve_modules_path", return_value=None):
            r = nle_bridge.run_resolve_sync()
        self.assertFalse(r.ok)
        self.assertIn("modules folder", r.error)

    def test_pre_flight_lib_missing(self):
        with mock.patch.object(nle_bridge, "resolve_modules_path",
                               return_value=self.modules), \
             mock.patch.object(nle_bridge, "resolve_script_lib_path",
                               return_value=None):
            r = nle_bridge.run_resolve_sync()
        self.assertFalse(r.ok)
        self.assertIn("fusionscript", r.error)

    def test_pre_flight_script_missing(self):
        with self._patch_paths(), \
             mock.patch.object(Path, "is_file", return_value=False):
            r = nle_bridge.run_resolve_sync()
        self.assertFalse(r.ok)
        self.assertIn("Companion script missing", r.error)

    def test_subprocess_env_and_command(self):
        completed = mock.MagicMock(returncode=0, stdout="ok\n", stderr="")
        with self._patch_paths(), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch("subprocess.run", return_value=completed) as run:
            r = nle_bridge.run_resolve_sync(python_executable="python-test")

        self.assertTrue(r.ok)
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "ok\n")
        self.assertEqual(r.stderr, "")
        self.assertIsNone(r.error)

        args, kwargs = run.call_args
        cmd = args[0]
        self.assertEqual(cmd[0], "python-test")
        self.assertTrue(cmd[1].endswith("lvm_restore_versions.py"))

        env = kwargs["env"]
        # API points at parent of the modules folder
        self.assertEqual(env["RESOLVE_SCRIPT_API"], str(self.modules.parent))
        self.assertEqual(env["RESOLVE_SCRIPT_LIB"], str(self.lib))
        # PYTHONPATH must include the modules folder
        self.assertIn(str(self.modules), env["PYTHONPATH"].split(os.pathsep))

    def test_pythonpath_preserves_existing(self):
        completed = mock.MagicMock(returncode=0, stdout="", stderr="")
        existing = "X:/already/here"
        with self._patch_paths(), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch.dict(os.environ, {"PYTHONPATH": existing}, clear=False), \
             mock.patch("subprocess.run", return_value=completed) as run:
            nle_bridge.run_resolve_sync()
        env = run.call_args.kwargs["env"]
        parts = env["PYTHONPATH"].split(os.pathsep)
        self.assertIn(existing, parts)
        self.assertIn(str(self.modules), parts)

    def test_subprocess_timeout_returns_error(self):
        with self._patch_paths(), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch(
                 "subprocess.run",
                 side_effect=subprocess.TimeoutExpired(cmd="x", timeout=5)):
            r = nle_bridge.run_resolve_sync(timeout=5)
        self.assertFalse(r.ok)
        self.assertIn("timed out", r.error)

    def test_subprocess_oserror_returns_error(self):
        with self._patch_paths(), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch("subprocess.run", side_effect=OSError("nope")):
            r = nle_bridge.run_resolve_sync()
        self.assertFalse(r.ok)
        self.assertIn("Could not launch", r.error)

    def test_nonzero_exit_marks_not_ok_but_no_error(self):
        completed = mock.MagicMock(returncode=2, stdout="", stderr="boom")
        with self._patch_paths(), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch("subprocess.run", return_value=completed):
            r = nle_bridge.run_resolve_sync()
        self.assertFalse(r.ok)
        self.assertEqual(r.returncode, 2)
        self.assertEqual(r.stderr, "boom")
        self.assertIsNone(r.error)


class TestCompanionRenameClips(unittest.TestCase):
    """Sanity-check the contract that the bridge depends on: the companion
    script exposes ``rename_clips(resolve, log)`` and handles a missing
    Resolve gracefully.
    """

    @classmethod
    def setUpClass(cls):
        import importlib.util
        path = nle_bridge.resolve_script_path()
        spec = importlib.util.spec_from_file_location("lvm_companion_resolve",
                                                       str(path))
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)

    def test_module_exposes_rename_clips(self):
        self.assertTrue(callable(getattr(self.module, "rename_clips", None)))

    def test_rename_clips_with_no_resolve_returns_error_stats(self):
        logged = []
        stats = self.module.rename_clips(
            None, log=lambda lvl, msg: logged.append((lvl, msg)))
        self.assertFalse(stats["ok"])
        self.assertEqual(stats["errors"], 1)
        self.assertEqual(stats["renamed"], 0)
        self.assertTrue(any(lvl == "error" for lvl, _ in logged))

    def test_rename_clips_with_no_open_project_returns_error_stats(self):
        fake_resolve = mock.MagicMock()
        fake_resolve.GetProjectManager.return_value.GetCurrentProject.return_value = None
        logged = []
        stats = self.module.rename_clips(
            fake_resolve, log=lambda lvl, msg: logged.append((lvl, msg)))
        self.assertFalse(stats["ok"])
        self.assertEqual(stats["errors"], 1)
        self.assertTrue(any("No project is open" in msg for _, msg in logged))


class TestRunResolveInProcess(unittest.TestCase):
    """The frozen-build hotfix path. Mocks DaVinciResolveScript + the
    companion module so no Resolve install is needed.
    """

    def test_modules_missing_logs_error(self):
        logged = []
        with mock.patch.object(nle_bridge, "resolve_modules_path", return_value=None):
            stats = nle_bridge.run_resolve_in_process(
                lambda lvl, msg: logged.append((lvl, msg)))
        self.assertFalse(stats["ok"])
        self.assertEqual(stats["errors"], 1)
        self.assertTrue(any(lvl == "error" and "modules not found" in msg.lower()
                            for lvl, msg in logged))

    def test_resolve_not_running_logs_error(self):
        modules = Path("C:/fake/Modules")
        fake_dvr = mock.MagicMock()
        fake_dvr.scriptapp.return_value = None
        logged = []

        with mock.patch.object(nle_bridge, "resolve_modules_path",
                                return_value=modules), \
             mock.patch.object(nle_bridge, "resolve_script_lib_path",
                                return_value=Path("/lib/fusionscript.dll")), \
             mock.patch.dict(sys.modules, {"DaVinciResolveScript": fake_dvr}):
            stats = nle_bridge.run_resolve_in_process(
                lambda lvl, msg: logged.append((lvl, msg)))

        self.assertFalse(stats["ok"])
        self.assertTrue(any("Could not connect" in msg for _, msg in logged))

    def test_calls_companion_rename_clips(self):
        """When everything is wired up, run_resolve_in_process delegates to
        the companion script's rename_clips() and returns its stats."""
        modules = Path("C:/fake/Modules")
        resolve_handle = mock.MagicMock()
        fake_dvr = mock.MagicMock()
        fake_dvr.scriptapp.return_value = resolve_handle

        # Stand-in for the loaded companion module
        fake_module = mock.MagicMock()
        fake_module.rename_clips.return_value = {
            "renamed": 5, "idempotent": 2, "no_match": 100,
            "errors": 0, "ok": True,
        }
        spec_mock = mock.MagicMock()
        spec_mock.loader = mock.MagicMock()

        logged = []

        with mock.patch.object(nle_bridge, "resolve_modules_path",
                                return_value=modules), \
             mock.patch.object(nle_bridge, "resolve_script_lib_path",
                                return_value=Path("/lib/fusionscript.dll")), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch.dict(sys.modules, {"DaVinciResolveScript": fake_dvr}), \
             mock.patch("importlib.util.spec_from_file_location",
                        return_value=spec_mock), \
             mock.patch("importlib.util.module_from_spec",
                        return_value=fake_module):
            stats = nle_bridge.run_resolve_in_process(
                lambda lvl, msg: logged.append((lvl, msg)))

        # rename_clips was called with the real Resolve handle and our
        # log callback (so the companion can stream progress through).
        fake_module.rename_clips.assert_called_once()
        call_args = fake_module.rename_clips.call_args
        self.assertIs(call_args[0][0], resolve_handle)

        self.assertTrue(stats["ok"])
        self.assertEqual(stats["renamed"], 5)
        self.assertEqual(stats["errors"], 0)

    def test_companion_module_load_failure_returns_error_stats(self):
        modules = Path("C:/fake/Modules")
        fake_dvr = mock.MagicMock()
        fake_dvr.scriptapp.return_value = mock.MagicMock()  # Resolve "running"
        spec_mock = mock.MagicMock()
        spec_mock.loader.exec_module.side_effect = SyntaxError("bad")

        logged = []
        with mock.patch.object(nle_bridge, "resolve_modules_path",
                                return_value=modules), \
             mock.patch.object(nle_bridge, "resolve_script_lib_path",
                                return_value=Path("/lib/fusionscript.dll")), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch.dict(sys.modules, {"DaVinciResolveScript": fake_dvr}), \
             mock.patch("importlib.util.spec_from_file_location",
                        return_value=spec_mock), \
             mock.patch("importlib.util.module_from_spec",
                        return_value=mock.MagicMock()):
            stats = nle_bridge.run_resolve_in_process(
                lambda lvl, msg: logged.append((lvl, msg)))
        self.assertFalse(stats["ok"])
        self.assertEqual(stats["errors"], 1)
        self.assertTrue(any("failed to load" in msg for _, msg in logged))


if __name__ == "__main__":
    unittest.main()
