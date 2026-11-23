"""
Elevation helpers for Windows symlink support.

Provides utilities to detect admin privileges, Developer Mode,
and to relaunch the application with elevated (UAC) privileges.
"""

import os
import sys
import platform
import logging
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

LINK_MODES = ("copy", "symlink", "hardlink")


def is_windows() -> bool:
    return platform.system() == "Windows"


def is_admin() -> bool:
    """Check if the current process has administrator privileges."""
    if not is_windows():
        return os.geteuid() == 0
    try:
        import ctypes
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def is_developer_mode() -> bool:
    """Check if Windows Developer Mode is enabled (allows symlinks without admin)."""
    if not is_windows():
        return True  # non-Windows can always symlink
    try:
        import winreg
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\AppModelUnlock",
        ) as key:
            value, _ = winreg.QueryValueEx(key, "AllowDevelopmentWithoutDevLicense")
        return bool(value)
    except Exception:
        return False


def can_create_symlinks() -> bool:
    """Test whether the current process can actually create symlinks."""
    if not is_windows():
        return True
    # Quick path: admin or dev mode
    if is_admin() or is_developer_mode():
        return True
    # Empirical test — try creating one in temp
    try:
        tmp = Path(tempfile.mkdtemp())
        target = tmp / "test_target.tmp"
        link = tmp / "test_link.tmp"
        target.write_text("test")
        try:
            link.symlink_to(target)
            return True
        except OSError:
            return False
        finally:
            link.unlink(missing_ok=True)
            target.unlink(missing_ok=True)
            tmp.rmdir()
    except Exception:
        return False


def can_create_hardlinks() -> bool:
    """Test whether hardlinks can be created (same volume, NTFS)."""
    if not is_windows():
        return True
    try:
        tmp = Path(tempfile.mkdtemp())
        target = tmp / "test_target.tmp"
        link = tmp / "test_link.tmp"
        target.write_text("test")
        try:
            os.link(str(target), str(link))
            return True
        except OSError:
            return False
        finally:
            link.unlink(missing_ok=True)
            target.unlink(missing_ok=True)
            tmp.rmdir()
    except Exception:
        return False


def restart_elevated() -> bool:
    """Relaunch the current process with elevated (UAC) privileges.

    Returns True if the elevation was launched (caller should exit).
    Returns False if elevation failed or was declined.
    """
    if not is_windows():
        logger.warning("Elevation restart is only supported on Windows")
        return False

    try:
        import ctypes
        import subprocess
        from ctypes import wintypes

        shell32 = ctypes.WinDLL("shell32", use_last_error=True)
        shell32.ShellExecuteW.argtypes = [
            wintypes.HWND,
            wintypes.LPCWSTR,
            wintypes.LPCWSTR,
            wintypes.LPCWSTR,
            wintypes.LPCWSTR,
            ctypes.c_int,
        ]
        shell32.ShellExecuteW.restype = ctypes.c_void_p

        argv = sys.argv[1:] if getattr(sys, "frozen", False) else sys.argv
        params = subprocess.list2cmdline(argv) if argv else None

        # ShellExecuteW returns an HINSTANCE > 32 on success
        result = shell32.ShellExecuteW(
            None,            # hwnd
            "runas",         # lpOperation — triggers UAC
            sys.executable,  # lpFile — python.exe / LatestVersionManager.exe
            params,          # lpParameters
            None,            # lpDirectory
            1,               # nShowCmd — SW_SHOWNORMAL
        )
        return bool(result and result > 32)
    except Exception as e:
        logger.error(f"Failed to restart elevated: {e}")
        return False


def check_link_mode_available(mode: str) -> tuple[bool, str]:
    """Check if the requested link mode is available.

    Returns (available, reason). If not available, reason explains why
    and suggests alternatives.
    """
    if mode == "copy":
        return True, ""

    if mode == "hardlink":
        if can_create_hardlinks():
            return True, ""
        return False, (
            "Hardlinks are not available (requires NTFS and files on the same volume)."
        )

    if mode == "symlink":
        if can_create_symlinks():
            return True, ""
        if is_windows():
            return False, (
                "Symlinks require Administrator privileges or Developer Mode.\n\n"
                "Options:\n"
                "  - Restart the app with elevated privileges (UAC prompt)\n"
                "  - Enable Developer Mode in Windows Settings\n"
                "  - Use hardlink or copy mode instead"
            )
        return False, "Symlink creation failed unexpectedly."

    return False, f"Unknown link mode: {mode}"
