"""NLE bridge — launch companion scripts in DaVinci Resolve / Adobe Premiere
from outside the application.

Resolve: spawn ``python`` with ``RESOLVE_SCRIPT_API``, ``RESOLVE_SCRIPT_LIB``,
and ``PYTHONPATH`` configured, then run
``companions/resolve/lvm_restore_versions.py`` against a Resolve instance the
user already has open. External Python scripting is documented as
DaVinci Resolve **Studio**-only — Free users still have the in-NLE path
(Workspace → Scripts → Edit → lvm_restore_versions).

Premiere: scope for the v1.5 CEP panel; not implemented here.
"""

__all__ = [
    "resolve_modules_path",
    "resolve_script_lib_path",
    "is_resolve_external_available",
    "is_resolve_running",
    "invalidate_resolve_running_cache",
    "companions_dir",
    "resolve_script_path",
    "prepare_resolve_command",
    "run_resolve_sync",
    "run_resolve_in_process",
    # Premiere
    "lvm_data_dir",
    "premiere_trigger_dir",
    "premiere_heartbeat_path",
    "premiere_panel_install_dir",
    "premiere_panel_source_dir",
    "is_premiere_panel_alive",
    "write_premiere_trigger",
    "PREMIERE_HEARTBEAT_MAX_AGE",
    # Installer
    "is_premiere_panel_installed",
    "is_premiere_debug_mode_enabled",
    "install_premiere_panel",
    "uninstall_premiere_panel",
    "PREMIERE_CSXS_VERSIONS",
]

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional


def resolve_modules_path() -> Optional[Path]:
    """Return the standard DaVinciResolveScript modules folder if it exists."""
    if sys.platform == "darwin":
        path = Path("/Library/Application Support/Blackmagic Design/"
                    "DaVinci Resolve/Developer/Scripting/Modules")
    elif sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
        program_data = os.environ.get("PROGRAMDATA", r"C:\ProgramData")
        path = (Path(program_data) / "Blackmagic Design" / "DaVinci Resolve" /
                "Support" / "Developer" / "Scripting" / "Modules")
    elif sys.platform.startswith("linux"):
        path = Path("/opt/resolve/Developer/Scripting/Modules")
    else:
        return None
    return path if path.is_dir() else None


def resolve_script_lib_path() -> Optional[Path]:
    """Return the absolute path of the fusionscript shared library, if found.

    DaVinciResolveScript loads this via ctypes when scripting from outside
    Resolve. Without it set on ``RESOLVE_SCRIPT_LIB`` the import succeeds
    but ``scriptapp("Resolve")`` returns ``None``.
    """
    if sys.platform == "darwin":
        candidates = [
            Path("/Applications/DaVinci Resolve/DaVinci Resolve.app/"
                 "Contents/Libraries/Fusion/fusionscript.so"),
        ]
    elif sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
        program_files = os.environ.get("ProgramFiles", r"C:\Program Files")
        candidates = [
            Path(program_files) / "Blackmagic Design" / "DaVinci Resolve" /
            "fusionscript.dll",
        ]
    elif sys.platform.startswith("linux"):
        candidates = [
            Path("/opt/resolve/libs/Fusion/fusionscript.so"),
        ]
    else:
        return None
    for c in candidates:
        if c.is_file():
            return c
    return None


def is_resolve_external_available() -> bool:
    """Best-effort check that external Resolve scripting is usable here.

    True only when both the modules folder and the fusionscript library
    exist on disk. Doesn't probe whether Resolve is actually running —
    that's checked at run time by the companion script itself.
    """
    return resolve_modules_path() is not None and resolve_script_lib_path() is not None


# ---- Resolve running-process probe ------------------------------------
#
# is_resolve_external_available() only tells us Resolve is *installed*.
# The UI also wants to know whether it's *running right now* — otherwise
# clicking "Sync Resolve" silently fails after a few seconds while the
# script tries to connect. Premiere has this via heartbeat; Resolve
# doesn't expose anything cheaper than enumerating processes, so we shell
# out and cache the result.

_RESOLVE_RUNNING_TTL = 10.0  # seconds; longer than any UI refresh
_resolve_running_cache: tuple[float, bool] = (0.0, False)


def _check_resolve_process() -> bool:
    """Return True when a Resolve process is currently running.

    Decodes subprocess output with ``errors="replace"`` because Windows
    ``tasklist`` emits localised header text in the system code page,
    and Python's reader thread will silently crash on undecodable bytes
    if the default cp1252 decoder is left to its own devices — leaving
    ``proc.stdout`` empty and producing a false "not running" reading.
    """
    try:
        if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
            proc = subprocess.run(
                ["tasklist", "/FI", "IMAGENAME eq Resolve.exe", "/NH", "/FO", "CSV"],
                capture_output=True, text=True, timeout=4,
                encoding="utf-8", errors="replace",
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            return "Resolve.exe" in (proc.stdout or "")
        if sys.platform == "darwin":
            proc = subprocess.run(
                ["pgrep", "-x", "Resolve"],
                capture_output=True, text=True, timeout=4,
                encoding="utf-8", errors="replace",
            )
            return proc.returncode == 0
        if sys.platform.startswith("linux"):
            proc = subprocess.run(
                ["pgrep", "-f", "resolve"],
                capture_output=True, text=True, timeout=4,
                encoding="utf-8", errors="replace",
            )
            return proc.returncode == 0
    except (subprocess.TimeoutExpired, OSError, FileNotFoundError):
        return False
    return False


def is_resolve_running(force: bool = False) -> bool:
    """Return True when DaVinci Resolve is currently running.

    Cached for 10 s so periodic UI refresh ticks don't keep spawning
    ``tasklist`` / ``pgrep``. Pass ``force=True`` to bypass the cache
    (e.g. right before launching a sync, to fail fast).
    """
    global _resolve_running_cache
    import time
    now = time.monotonic()
    cached_at, cached_value = _resolve_running_cache
    if not force and (now - cached_at) < _RESOLVE_RUNNING_TTL:
        return cached_value

    value = _check_resolve_process()
    _resolve_running_cache = (now, value)
    return value


def invalidate_resolve_running_cache() -> None:
    """Drop the cached running-state. Call after operations that change
    Resolve's lifecycle (e.g. successful sync) so the next refresh
    reads fresh state."""
    global _resolve_running_cache
    _resolve_running_cache = (0.0, False)


def companions_dir() -> Path:
    """Return the bundled companions/ folder path."""
    # src/lvm/nle_bridge.py -> repo root -> companions/
    return Path(__file__).resolve().parents[2] / "companions"


def resolve_script_path() -> Path:
    return companions_dir() / "resolve" / "lvm_restore_versions.py"


def _build_resolve_env(modules: Path, lib: Path) -> dict:
    env = os.environ.copy()
    env["RESOLVE_SCRIPT_API"] = str(modules.parent)
    env["RESOLVE_SCRIPT_LIB"] = str(lib)
    parts = [str(modules)]
    existing = env.get("PYTHONPATH", "")
    if existing:
        parts.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env


def prepare_resolve_command(
    python_executable: Optional[str] = None,
) -> "ResolveCommand":
    """Resolve install paths and return a launch-ready command + env.

    The returned object exposes ``cmd``, ``env``, and ``error``. When
    ``error`` is non-empty the launch is impossible (Studio missing,
    fusionscript not found, companion script absent); ``cmd`` and ``env``
    are empty in that case.
    """
    modules = resolve_modules_path()
    lib = resolve_script_lib_path()
    script = resolve_script_path()

    if not modules:
        return ResolveCommand(
            cmd=[], env={}, error=(
                "DaVinciResolveScript modules folder not found. "
                "Install DaVinci Resolve Studio, or use the in-NLE path "
                "(Workspace -> Scripts -> Edit -> lvm_restore_versions)."
            ),
        )
    if not lib:
        return ResolveCommand(
            cmd=[], env={},
            error="fusionscript library not found at the expected location.",
        )
    if not script.is_file():
        return ResolveCommand(
            cmd=[], env={},
            error=f"Companion script missing: {script}",
        )

    interpreter = python_executable or sys.executable
    return ResolveCommand(
        cmd=[interpreter, str(script)],
        env=_build_resolve_env(modules, lib),
        error="",
    )


def run_resolve_sync(timeout: float = 600.0,
                      python_executable: Optional[str] = None) -> "ResolveSyncResult":
    """Run the Resolve companion script and return the captured output.

    Resolve must already be running. Studio-only — call
    :func:`is_resolve_external_available` first.

    Args:
        timeout: kill the subprocess after this many seconds. Defaults to
            ten minutes — Resolve API calls can be slow on large media
            pools, so don't set this aggressively.
        python_executable: override the interpreter used. Defaults to
            ``sys.executable`` (the same one running LVM).
    """
    prep = prepare_resolve_command(python_executable)
    if prep.error:
        return ResolveSyncResult(
            ok=False, returncode=1, stdout="", stderr="", error=prep.error,
        )

    try:
        proc = subprocess.run(
            prep.cmd,
            env=prep.env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return ResolveSyncResult(
            ok=False, returncode=1, stdout="", stderr="",
            error=f"Companion script timed out after {timeout:.0f}s. "
                  "Is DaVinci Resolve running and responsive?",
        )
    except (OSError, FileNotFoundError) as e:
        return ResolveSyncResult(
            ok=False, returncode=1, stdout="", stderr="",
            error=f"Could not launch Python interpreter: {e}",
        )

    return ResolveSyncResult(
        ok=(proc.returncode == 0),
        returncode=proc.returncode,
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
        error=None,
    )


class ResolveCommand:
    """Prepared subprocess invocation for the Resolve companion script.

    Returned by :func:`prepare_resolve_command`. When ``error`` is
    non-empty the launch is impossible; ``cmd`` and ``env`` are empty.
    """

    __slots__ = ("cmd", "env", "error")

    def __init__(self, cmd: list, env: dict, error: str):
        self.cmd = cmd
        self.env = env
        self.error = error


class ResolveSyncResult:
    """Outcome of a ``run_resolve_sync`` call.

    Attributes mirror the subprocess's exit code and captured streams.
    ``error`` is set only for pre-flight failures (paths missing, timeout,
    interpreter not found); when the subprocess ran but exited non-zero,
    ``ok`` is False but ``error`` stays None and the failure message lives
    in ``stderr``.
    """

    __slots__ = ("ok", "returncode", "stdout", "stderr", "error")

    def __init__(self, ok: bool, returncode: int, stdout: str, stderr: str,
                 error: Optional[str]):
        self.ok = ok
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.error = error

    def __repr__(self) -> str:  # pragma: no cover - debug only
        return (f"ResolveSyncResult(ok={self.ok}, rc={self.returncode}, "
                f"stdout={len(self.stdout)} chars, stderr={len(self.stderr)} chars, "
                f"error={self.error!r})")


def run_resolve_in_process(log) -> dict:
    """Run the Resolve rename inside this Python process.

    Loads the bundled companion script as a module, ensures
    ``DaVinciResolveScript`` is on ``sys.path``, connects to a running
    Resolve, and calls ``rename_clips``. Avoids subprocess entirely so
    frozen PyInstaller builds work — ``sys.executable`` in a frozen build
    is the LVM ``.exe`` itself, not a Python interpreter, so spawning
    ``[sys.executable, script]`` opens a second LVM instance instead of
    running the script.

    Args:
        log: ``log(level, message)`` callable. ``level`` is one of
            ``"info"``, ``"warning"``, ``"error"``.

    Returns:
        Stats dict from :func:`rename_clips`. Always returns a dict; check
        ``stats["ok"]`` to see if the rename ran. ``stats["errors"]`` is
        non-zero on any failure including pre-flight.
    """
    stats_failed = {"renamed": 0, "idempotent": 0, "no_match": 0,
                    "errors": 1, "ok": False}

    modules = resolve_modules_path()
    if not modules:
        log("error",
            "DaVinci Resolve scripting modules not found. Install "
            "DaVinci Resolve Studio (or use the in-Resolve script: "
            "Workspace -> Scripts -> Edit -> lvm_restore_versions).")
        return stats_failed

    modules_str = str(modules)
    if modules_str not in sys.path:
        sys.path.insert(0, modules_str)

    try:
        import DaVinciResolveScript as dvr_script  # type: ignore
    except ImportError as e:
        log("error",
            f"Could not import DaVinciResolveScript from {modules_str}: {e}")
        return stats_failed

    # The library load is keyed off RESOLVE_SCRIPT_LIB; ensure it's set so
    # ctypes can find fusionscript.dll/.so.
    lib = resolve_script_lib_path()
    if lib and not os.environ.get("RESOLVE_SCRIPT_LIB"):
        os.environ["RESOLVE_SCRIPT_LIB"] = str(lib)

    try:
        resolve_app = dvr_script.scriptapp("Resolve")
    except Exception as e:
        log("error", f"DaVinciResolveScript.scriptapp raised: {e}")
        return stats_failed

    if not resolve_app:
        log("error",
            "Could not connect to DaVinci Resolve. Is Resolve Studio running?")
        return stats_failed

    # Load the companion script as a module and call its rename_clips().
    # Bundled at companions/resolve/lvm_restore_versions.py both in source
    # and frozen PyInstaller builds (see lvm.spec datas).
    import importlib.util
    script_path = resolve_script_path()
    if not script_path.is_file():
        log("error", f"Companion script missing at {script_path}")
        return stats_failed

    spec = importlib.util.spec_from_file_location(
        "lvm_companion_resolve", str(script_path),
    )
    if spec is None or spec.loader is None:
        log("error", f"Could not load companion script from {script_path}")
        return stats_failed

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        log("error", f"Companion script failed to load: {e}")
        return stats_failed

    rename_fn = getattr(module, "rename_clips", None)
    if rename_fn is None:
        log("error", "Companion script doesn't expose rename_clips().")
        return stats_failed

    try:
        return rename_fn(resolve_app, log)
    except Exception as e:
        log("error", f"Rename run raised: {e}")
        return stats_failed


# ---------------------------------------------------------------------------
# Adobe Premiere Pro — file-based trigger bridge.
#
# Premiere doesn't expose external Python scripting like Resolve Studio. The
# only practical way for LVM to drive a running Premiere is via an installed
# CEP panel that watches a directory for trigger files. LVM writes a small
# JSON trigger via temp+rename; the panel polls the directory, runs the
# rename via CSInterface.evalScript, and deletes the trigger.
#
# Detection: the panel writes a heartbeat file every ~10s. LVM treats the
# panel as "alive" when the heartbeat exists and was modified recently.
# ---------------------------------------------------------------------------

PREMIERE_HEARTBEAT_MAX_AGE = 60.0  # seconds; panel beats every ~10s


def lvm_data_dir() -> Path:
    """Per-user LVM data directory.

    Mirrors the path the panel's main.js computes — keep them in sync.
    """
    if sys.platform == "darwin":
        return (Path.home() / "Library" / "Application Support" / "LVM")
    if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / "LVM"
        return Path.home() / "AppData" / "Roaming" / "LVM"
    # linux / other unix
    xdg = os.environ.get("XDG_DATA_HOME")
    if xdg:
        return Path(xdg) / "LVM"
    return Path.home() / ".local" / "share" / "LVM"


def premiere_trigger_dir() -> Path:
    return lvm_data_dir() / "triggers"


def premiere_heartbeat_path() -> Path:
    return lvm_data_dir() / "heartbeat" / "premiere.json"


def premiere_panel_source_dir() -> Path:
    """Path to the bundled CEP panel folder LVM ships."""
    return companions_dir() / "premiere" / "lvm_panel"


def premiere_panel_install_dir() -> Optional[Path]:
    """Where Adobe expects user-installed CEP extensions on this OS.

    Returns the directory the panel should be copied into. Doesn't check
    whether it actually contains the panel — see :func:`is_premiere_panel_alive`
    for runtime detection.
    """
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "Adobe" / \
               "CEP" / "extensions" / "com.polisvfx.lvm.panel"
    if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / "Adobe" / "CEP" / "extensions" / \
                   "com.polisvfx.lvm.panel"
        return Path.home() / "AppData" / "Roaming" / "Adobe" / "CEP" / \
               "extensions" / "com.polisvfx.lvm.panel"
    # Linux: Adobe doesn't ship Premiere on Linux; return None so the
    # caller knows there's no realistic path here.
    return None


def is_premiere_panel_alive(max_age_seconds: float = PREMIERE_HEARTBEAT_MAX_AGE) -> bool:
    """True when the panel has written a heartbeat within *max_age_seconds*.

    Indicates that Premiere is running with the panel loaded and able to
    process triggers. Stale heartbeats (Premiere closed) read as not alive.
    """
    hb = premiere_heartbeat_path()
    try:
        st = hb.stat()
    except (OSError, FileNotFoundError):
        return False
    import time
    return (time.time() - st.st_mtime) <= max_age_seconds


def write_premiere_trigger(payload: Optional[dict] = None) -> Path:
    """Drop a trigger JSON into the panel's watch directory.

    Atomic temp+rename so the panel never reads a half-written file.
    Returns the final trigger path.
    """
    import json
    import time
    import uuid

    trig_dir = premiere_trigger_dir()
    trig_dir.mkdir(parents=True, exist_ok=True)

    body = dict(payload or {})
    body.setdefault("id", uuid.uuid4().hex)
    body.setdefault("issued_at", time.strftime("%Y-%m-%dT%H:%M:%S",
                                                time.localtime()))
    body.setdefault("source_app", "lvm")

    final = trig_dir / f"{body['id']}.json"
    tmp = trig_dir / f"{body['id']}.json.tmp"
    tmp.write_text(json.dumps(body), encoding="utf-8")
    tmp.replace(final)
    return final


# ---------------------------------------------------------------------------
# Premiere panel one-click installer.
#
# Adobe gates unsigned CEP extensions behind a per-user PlayerDebugMode flag
# (registry on Windows, defaults on macOS) keyed by CSXS major version.
# Premiere version → CSXS version (approximate, Adobe doesn't always bump it):
#   Premiere 2019      → CSXS 9
#   Premiere 2020-2023 → CSXS 10
#   Premiere 2024+     → CSXS 11
#   Premiere future    → CSXS 12 (preemptive)
# Setting all of these is harmless — keys for unused CSXS versions are
# ignored by Premiere and don't affect anything else on the system.
# ---------------------------------------------------------------------------

PREMIERE_CSXS_VERSIONS = (9, 10, 11, 12)


def is_premiere_panel_installed() -> bool:
    """True when the panel is present in Adobe's CEP extensions folder."""
    install_dir = premiere_panel_install_dir()
    if install_dir is None:
        return False
    manifest = install_dir / "CSXS" / "manifest.xml"
    return manifest.is_file()


def is_premiere_debug_mode_enabled() -> bool:
    """True when *any* CSXS PlayerDebugMode flag is on for the current user.

    Premiere only consults the CSXS version it was built against, but we
    can't know which one the user has installed without launching Premiere,
    so a true return here means at least one of the candidate versions is
    enabled — sufficient to avoid prompting the user again.
    """
    if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
        try:
            import winreg
        except ImportError:
            return False
        for v in PREMIERE_CSXS_VERSIONS:
            try:
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                                     fr"Software\Adobe\CSXS.{v}") as key:
                    value, _ = winreg.QueryValueEx(key, "PlayerDebugMode")
                    if str(value).strip() in ("1", "1.0"):
                        return True
            except OSError:
                continue
        return False

    if sys.platform == "darwin":
        for v in PREMIERE_CSXS_VERSIONS:
            try:
                proc = subprocess.run(
                    ["defaults", "read", f"com.adobe.CSXS.{v}", "PlayerDebugMode"],
                    capture_output=True, text=True, timeout=3,
                )
            except (OSError, subprocess.TimeoutExpired):
                continue
            if proc.returncode == 0 and proc.stdout.strip() == "1":
                return True
        return False

    return False


def _set_premiere_debug_mode_windows(enable: bool, log) -> int:
    """Toggle PlayerDebugMode for every candidate CSXS version on Windows.

    Returns the number of CSXS versions actually written/cleared.
    """
    try:
        import winreg
    except ImportError:
        log("error", "winreg unavailable on this platform — cannot set "
                     "PlayerDebugMode automatically.")
        return 0

    touched = 0
    for v in PREMIERE_CSXS_VERSIONS:
        sub = fr"Software\Adobe\CSXS.{v}"
        try:
            if enable:
                with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, sub, 0,
                                         winreg.KEY_SET_VALUE) as key:
                    # PlayerDebugMode is documented as a string "1", but the
                    # widely-followed community guidance also accepts a
                    # DWORD. We write the string form Adobe uses.
                    winreg.SetValueEx(key, "PlayerDebugMode", 0, winreg.REG_SZ, "1")
                touched += 1
                log("info", f"  CSXS.{v}: PlayerDebugMode = 1")
            else:
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER, sub, 0,
                                     winreg.KEY_SET_VALUE) as key:
                    try:
                        winreg.DeleteValue(key, "PlayerDebugMode")
                        touched += 1
                        log("info", f"  CSXS.{v}: PlayerDebugMode cleared")
                    except FileNotFoundError:
                        pass  # Value didn't exist; nothing to do.
        except OSError as e:
            log("warning", f"  CSXS.{v}: registry write failed: {e}")
    return touched


def _set_premiere_debug_mode_macos(enable: bool, log) -> int:
    """Toggle PlayerDebugMode via `defaults` for each candidate CSXS version.

    Returns the number of CSXS versions actually written/cleared.
    """
    touched = 0
    for v in PREMIERE_CSXS_VERSIONS:
        domain = f"com.adobe.CSXS.{v}"
        try:
            if enable:
                args = ["defaults", "write", domain, "PlayerDebugMode", "1"]
            else:
                args = ["defaults", "delete", domain, "PlayerDebugMode"]
            proc = subprocess.run(args, capture_output=True, text=True, timeout=5)
        except (OSError, subprocess.TimeoutExpired) as e:
            log("warning", f"  CSXS.{v}: defaults call failed: {e}")
            continue
        if proc.returncode == 0:
            touched += 1
            verb = "set" if enable else "cleared"
            log("info", f"  CSXS.{v}: PlayerDebugMode {verb}")
    return touched


def _copy_panel_tree(src: Path, dst: Path, log) -> int:
    """Recursively copy the panel folder. Returns file count copied."""
    import shutil
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)

    count = 0
    for _root, _dirs, files in os.walk(dst):
        count += len(files)
    log("info", f"  Copied {count} files into {dst}")
    return count


def install_premiere_panel(log=None) -> dict:
    """One-click panel install: copy files + enable PlayerDebugMode.

    Args:
        log: optional ``log(level, message)`` callable (info/warning/error).

    Returns a status dict::

        {
          "ok": bool,
          "files_copied": int,
          "csxs_flags_set": int,
          "install_dir": str,
          "needs_premiere_restart": bool,  # True when Premiere may already be open
          "error": Optional[str],
        }
    """
    if log is None:
        log = lambda lvl, msg: None

    install_dir = premiere_panel_install_dir()
    if install_dir is None:
        return {"ok": False, "files_copied": 0, "csxs_flags_set": 0,
                "install_dir": "", "needs_premiere_restart": False,
                "error": "Adobe Premiere isn't supported on this OS."}

    src = premiere_panel_source_dir()
    if not (src / "CSXS" / "manifest.xml").is_file():
        return {"ok": False, "files_copied": 0, "csxs_flags_set": 0,
                "install_dir": str(install_dir), "needs_premiere_restart": False,
                "error": f"Bundled panel source not found at {src}. "
                         "Reinstall LVM."}

    log("info", f"Installing LVM Premiere panel to {install_dir}")
    install_dir.parent.mkdir(parents=True, exist_ok=True)

    try:
        files = _copy_panel_tree(src, install_dir, log)
    except OSError as e:
        return {"ok": False, "files_copied": 0, "csxs_flags_set": 0,
                "install_dir": str(install_dir), "needs_premiere_restart": False,
                "error": f"File copy failed: {e}"}

    log("info", "Enabling unsigned CEP extensions (PlayerDebugMode):")
    if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
        flags = _set_premiere_debug_mode_windows(True, log)
    elif sys.platform == "darwin":
        flags = _set_premiere_debug_mode_macos(True, log)
    else:
        flags = 0

    return {
        "ok": True,
        "files_copied": files,
        "csxs_flags_set": flags,
        "install_dir": str(install_dir),
        "needs_premiere_restart": True,
        "error": None,
    }


def uninstall_premiere_panel(log=None, *, clear_debug_mode: bool = False) -> dict:
    """Remove the installed panel folder.

    By default leaves PlayerDebugMode enabled so any other unsigned CEP
    panels keep working. Pass ``clear_debug_mode=True`` to also clear the
    registry/defaults entries this installer set.
    """
    import shutil
    if log is None:
        log = lambda lvl, msg: None

    install_dir = premiere_panel_install_dir()
    if install_dir is None:
        return {"ok": False, "files_removed": False, "csxs_flags_cleared": 0,
                "error": "Adobe Premiere isn't supported on this OS."}

    removed = False
    if install_dir.exists():
        try:
            shutil.rmtree(install_dir)
            removed = True
            log("info", f"Removed panel directory: {install_dir}")
        except OSError as e:
            return {"ok": False, "files_removed": False, "csxs_flags_cleared": 0,
                    "error": f"Could not remove panel directory: {e}"}
    else:
        log("info", f"Panel directory wasn't present: {install_dir}")

    cleared = 0
    if clear_debug_mode:
        log("info", "Clearing PlayerDebugMode flags:")
        if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
            cleared = _set_premiere_debug_mode_windows(False, log)
        elif sys.platform == "darwin":
            cleared = _set_premiere_debug_mode_macos(False, log)

    return {
        "ok": True,
        "files_removed": removed,
        "csxs_flags_cleared": cleared,
        "error": None,
    }
