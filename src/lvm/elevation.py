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
        return os.getuid() == 0
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
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\AppModelUnlock",
        )
        value, _ = winreg.QueryValueEx(key, "AllowDevelopmentWithoutDevLicense")
        winreg.CloseKey(key)
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
        # ShellExecuteW returns an HINSTANCE > 32 on success
        result = ctypes.windll.shell32.ShellExecuteW(
            None,            # hwnd
            "runas",         # lpOperation — triggers UAC
            sys.executable,  # lpFile — python.exe / pythonw.exe
            " ".join(f'"{a}"' for a in sys.argv),  # lpParameters
            None,            # lpDirectory
            1,               # nShowCmd — SW_SHOWNORMAL
        )
        return result > 32
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
