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
    "companions_dir",
    "resolve_script_path",
    "prepare_resolve_command",
    "run_resolve_sync",
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
