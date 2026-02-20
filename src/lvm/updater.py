"""
Auto-updater — checks GitHub Releases for new versions and applies updates.

All network access is explicit (no background polling).  The module uses
only Python stdlib so it adds no new dependencies.
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Callable
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)

GITHUB_REPO = "polisvfx/LatestVersionManager"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
USER_AGENT = "LatestVersionManager-Updater/1.0"
REQUEST_TIMEOUT = 15  # seconds
CHUNK_SIZE = 65536    # 64 KB


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class UpdateError(Exception):
    """Base class for update errors."""

class UpdateCheckError(UpdateError):
    """Failed to check for updates (network error, API error)."""

class UpdateDownloadError(UpdateError):
    """Failed to download the update."""


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass
class ReleaseInfo:
    """Parsed info from a GitHub Release."""
    tag_name: str       # e.g. "v1.2.3"
    version: str        # e.g. "1.2.3" (tag stripped of leading 'v')
    name: str           # release title
    body: str           # release notes (markdown)
    html_url: str       # URL to view release in browser
    asset_url: str      # download URL for the platform-appropriate ZIP
    asset_name: str     # e.g. "LatestVersionManager-windows.zip"
    asset_size: int     # bytes


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def parse_version(version_string: str) -> tuple[int, ...]:
    """Parse '0.1.0' or 'v0.1.0' into a comparable tuple (0, 1, 0)."""
    cleaned = version_string.lstrip("vV")
    parts = []
    for part in cleaned.split("."):
        match = re.match(r"(\d+)", part)
        if match:
            parts.append(int(match.group(1)))
    return tuple(parts) if parts else (0,)


def is_newer(remote_version: str, local_version: str) -> bool:
    """Return True if *remote_version* is strictly newer than *local_version*."""
    return parse_version(remote_version) > parse_version(local_version)


# ---------------------------------------------------------------------------
# Platform helpers
# ---------------------------------------------------------------------------

def _get_platform_suffix() -> str:
    """Return the asset filename suffix for the current platform."""
    if sys.platform == "win32":
        return "windows.zip"
    elif sys.platform == "darwin":
        return "macos.zip"
    else:
        return "linux.zip"


def is_frozen() -> bool:
    """Return True if running inside a PyInstaller-frozen bundle."""
    return getattr(sys, "frozen", False)


def get_install_dir() -> Optional[Path]:
    """Return the root installation directory, or None if running from source."""
    if not is_frozen():
        return None
    return Path(sys.executable).parent


# ---------------------------------------------------------------------------
# Check for update
# ---------------------------------------------------------------------------

def check_for_update(current_version: str) -> Optional[ReleaseInfo]:
    """Query GitHub for the latest release and return *ReleaseInfo* if newer.

    Returns ``None`` when the app is already up-to-date or no matching
    platform asset is found.  Raises *UpdateCheckError* on network errors.
    """
    suffix = _get_platform_suffix()

    req = Request(GITHUB_API_URL, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github+json",
    })

    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 403:
            raise UpdateCheckError(
                "GitHub API rate limit exceeded.  Please try again later."
            ) from exc
        raise UpdateCheckError(
            f"GitHub API returned HTTP {exc.code}."
        ) from exc
    except URLError as exc:
        raise UpdateCheckError(
            "Could not connect to GitHub.  Please check your internet connection."
        ) from exc
    except Exception as exc:
        raise UpdateCheckError(f"Unexpected error: {exc}") from exc

    tag = data.get("tag_name", "")
    if not tag:
        logger.warning("GitHub release has no tag_name")
        return None

    if not is_newer(tag, current_version):
        return None

    # Find the platform-appropriate asset
    asset_url = asset_name = ""
    asset_size = 0
    for asset in data.get("assets", []):
        name = asset.get("name", "")
        if name.endswith(suffix):
            asset_url = asset.get("browser_download_url", "")
            asset_name = name
            asset_size = asset.get("size", 0)
            break

    if not asset_url:
        logger.warning("No matching asset for platform suffix '%s'", suffix)
        return None

    version = tag.lstrip("vV")

    return ReleaseInfo(
        tag_name=tag,
        version=version,
        name=data.get("name", tag),
        body=data.get("body", ""),
        html_url=data.get("html_url", ""),
        asset_url=asset_url,
        asset_name=asset_name,
        asset_size=asset_size,
    )


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_update(
    release: ReleaseInfo,
    dest_dir: str,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Path:
    """Download the release asset ZIP to *dest_dir*.

    *progress_callback*, if provided, is called as
    ``progress_callback(bytes_downloaded, total_bytes)`` periodically.

    Returns the path to the downloaded file.
    Raises *UpdateDownloadError* on failure.
    """
    dest = Path(dest_dir) / release.asset_name
    tmp_path = dest.with_suffix(".part")

    req = Request(release.asset_url, headers={"User-Agent": USER_AGENT})

    try:
        with urlopen(req, timeout=60) as resp:
            total = int(resp.headers.get("Content-Length", release.asset_size))
            downloaded = 0

            with open(tmp_path, "wb") as f:
                while True:
                    chunk = resp.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback and total > 0:
                        progress_callback(downloaded, total)

            # Rename to final name
            if dest.exists():
                dest.unlink()
            tmp_path.rename(dest)

    except (UpdateDownloadError, KeyboardInterrupt):
        raise
    except Exception as exc:
        # Clean up partial download
        for p in (tmp_path, dest):
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
        raise UpdateDownloadError(f"Download failed: {exc}") from exc

    logger.info("Downloaded %s (%d bytes)", dest.name, downloaded)
    return dest


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_update(zip_path: Path, extract_dir: Path) -> Path:
    """Extract the update ZIP and return the path to the application folder."""
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
    except Exception as exc:
        raise UpdateError(f"Failed to extract update: {exc}") from exc

    # The ZIP contains a top-level LatestVersionManager/ folder
    app_dir = extract_dir / "LatestVersionManager"
    if app_dir.is_dir():
        return app_dir

    # Fallback: if the ZIP extracted directly (no subfolder)
    return extract_dir


# ---------------------------------------------------------------------------
# Updater script generation
# ---------------------------------------------------------------------------

def create_updater_script(
    extracted_dir: Path,
    install_dir: Path,
    executable_path: Path,
    pid: int,
) -> Path:
    """Create a platform-specific script that replaces files and restarts.

    Returns the path to the generated script.
    """
    if sys.platform == "win32":
        return _create_windows_updater(extracted_dir, install_dir, executable_path, pid)
    else:
        return _create_unix_updater(extracted_dir, install_dir, executable_path, pid)


def _create_windows_updater(
    extracted_dir: Path,
    install_dir: Path,
    executable_path: Path,
    pid: int,
) -> Path:
    script = tempfile.NamedTemporaryFile(
        mode="w", suffix=".cmd", prefix="lvm_update_",
        dir=tempfile.gettempdir(), delete=False,
    )
    backup_dir = install_dir.parent / f"{install_dir.name}_backup"
    script.write(f"""@echo off
setlocal
title Updating Latest Version Manager...

set "PID={pid}"
set "INSTALL_DIR={install_dir}"
set "UPDATE_DIR={extracted_dir}"
set "BACKUP_DIR={backup_dir}"
set "EXE_NAME={executable_path.name}"

echo Waiting for Latest Version Manager to close...
:waitloop
timeout /t 1 /nobreak >NUL
tasklist /FI "PID eq %PID%" 2>NUL | find /I "%PID%" >NUL
if not errorlevel 1 goto waitloop

echo Backing up current installation...
if exist "%BACKUP_DIR%" rmdir /S /Q "%BACKUP_DIR%"
rename "%INSTALL_DIR%" "{install_dir.name}_backup"
if errorlevel 1 (
    echo ERROR: Could not rename current installation.
    echo Please close any programs using files in %INSTALL_DIR% and try again.
    pause
    exit /b 1
)

echo Installing update...
move "%UPDATE_DIR%" "%INSTALL_DIR%"
if errorlevel 1 (
    echo ERROR: Could not install update. Restoring backup...
    rename "%BACKUP_DIR%" "{install_dir.name}"
    pause
    exit /b 1
)

echo Cleaning up...
rmdir /S /Q "%BACKUP_DIR%" 2>NUL

echo Starting Latest Version Manager...
start "" "%INSTALL_DIR%\\%EXE_NAME%"

echo Update complete.
timeout /t 2 /nobreak >NUL
del "%~f0"
""")
    script.close()
    return Path(script.name)


def _create_unix_updater(
    extracted_dir: Path,
    install_dir: Path,
    executable_path: Path,
    pid: int,
) -> Path:
    script = tempfile.NamedTemporaryFile(
        mode="w", suffix=".sh", prefix="lvm_update_",
        dir=tempfile.gettempdir(), delete=False,
    )
    backup_dir = install_dir.parent / f"{install_dir.name}_backup"
    script.write(f"""#!/bin/bash
PID={pid}
INSTALL_DIR="{install_dir}"
UPDATE_DIR="{extracted_dir}"
BACKUP_DIR="{backup_dir}"
EXE_NAME="{executable_path.name}"

echo "Waiting for Latest Version Manager to close..."
while kill -0 "$PID" 2>/dev/null; do sleep 1; done

echo "Backing up current installation..."
rm -rf "$BACKUP_DIR"
mv "$INSTALL_DIR" "$BACKUP_DIR"

echo "Installing update..."
mv "$UPDATE_DIR" "$INSTALL_DIR"
if [ $? -ne 0 ]; then
    echo "ERROR: Could not install update. Restoring backup..."
    mv "$BACKUP_DIR" "$INSTALL_DIR"
    exit 1
fi

chmod +x "$INSTALL_DIR/$EXE_NAME"

echo "Cleaning up..."
rm -rf "$BACKUP_DIR"

echo "Starting Latest Version Manager..."
"$INSTALL_DIR/$EXE_NAME" &

rm -- "$0"
""")
    script.close()
    os.chmod(script.name, 0o755)
    return Path(script.name)


# ---------------------------------------------------------------------------
# Launch updater
# ---------------------------------------------------------------------------

def launch_updater(script_path: Path):
    """Launch the updater script as a detached process.

    The caller is responsible for exiting the application after this.
    """
    if sys.platform == "win32":
        subprocess.Popen(
            ["cmd", "/c", str(script_path)],
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
            close_fds=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        subprocess.Popen(
            ["bash", str(script_path)],
            start_new_session=True,
            close_fds=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    logger.info("Updater script launched: %s", script_path)
