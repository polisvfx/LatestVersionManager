"""
Auto-updater — checks GitHub Releases for new versions and applies updates.

All network access is explicit (no background polling).  The module uses
only Python stdlib so it adds no new dependencies.
"""

import hashlib
import json
import logging
import os
import re
import shlex
import ssl
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Callable
from urllib.parse import urlparse
from urllib.request import (
    HTTPRedirectHandler,
    HTTPSHandler,
    Request,
    build_opener,
)
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)

GITHUB_REPO = "polisvfx/LatestVersionManager"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
USER_AGENT = "LatestVersionManager-Updater/1.0"
REQUEST_TIMEOUT = 15  # seconds
CHUNK_SIZE = 65536    # 64 KB
CHECKSUMS_ASSET_NAME = "SHA256SUMS.txt"

ALLOWED_DOWNLOAD_HOSTS = frozenset({
    "api.github.com",
    "github.com",
    "objects.githubusercontent.com",
    "release-assets.githubusercontent.com",
    "github-releases.githubusercontent.com",
})

# Defense-in-depth limits against zip bombs
MAX_EXTRACT_BYTES = 2 * 1024 * 1024 * 1024  # 2 GiB
MAX_EXTRACT_FILES = 25000


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class UpdateError(Exception):
    """Base class for update errors."""


class UpdateCheckError(UpdateError):
    """Failed to check for updates (network error, API error)."""


class UpdateDownloadError(UpdateError):
    """Failed to download or verify the update."""


class UpdateCancelled(UpdateDownloadError):
    """Raised when the user cancels a download."""


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
    checksums_url: str = ""  # download URL for SHA256SUMS.txt (if published)


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

_SEMVER_RE = re.compile(
    r"^[vV]?(\d+(?:\.\d+)*)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$"
)


def parse_version(version_string: str) -> tuple[tuple[int, ...], int, tuple]:
    """Parse a version string into a comparable tuple respecting semver pre-releases.

    Examples:
        ``1.2.0-beta.1`` < ``1.2.0``
        ``1.0`` == ``1.0.0``
        ``v1.9.0`` < ``v1.10.0``
    """
    cleaned = (version_string or "").strip()
    m = _SEMVER_RE.match(cleaned)
    if m:
        nums = [int(p) for p in m.group(1).split(".")]
        pre_str = m.group(2)
    else:
        nums = [int(p) for p in re.findall(r"\d+", cleaned)] or [0]
        pre_str = None

    # Strip trailing zeros so 1.0 == 1.0.0
    while len(nums) > 1 and nums[-1] == 0:
        nums.pop()

    if not pre_str:
        # Final release sorts AFTER any pre-release of the same core version
        return (tuple(nums), 1, ())

    pre_parts = []
    for part in pre_str.split("."):
        if part.isdigit():
            pre_parts.append((0, int(part)))
        else:
            pre_parts.append((1, part.lower()))
    return (tuple(nums), 0, tuple(pre_parts))


def is_newer(remote_version: str, local_version: str) -> bool:
    """Return True if *remote_version* is strictly newer than *local_version*."""
    return parse_version(remote_version) > parse_version(local_version)


# ---------------------------------------------------------------------------
# URL & filename validation
# ---------------------------------------------------------------------------

def validate_github_url(url: str) -> str:
    """Ensure *url* is an HTTPS URL pointing at an allowed GitHub host."""
    if not url:
        raise UpdateError("Empty download URL.")
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https":
        raise UpdateError(f"Refusing non-HTTPS update URL: {url!r}")
    if host not in ALLOWED_DOWNLOAD_HOSTS:
        raise UpdateError(f"Refusing update URL from untrusted host: {host!r}")
    return url


def sanitize_asset_name(name: str) -> str:
    """Return a safe single-segment filename for a release asset."""
    if not name or "/" in name or "\\" in name or "\x00" in name:
        raise UpdateError(f"Unsafe release asset name: {name!r}")
    base = Path(name).name
    if not base or base in (".", "..") or base != name:
        raise UpdateError(f"Unsafe release asset name: {name!r}")
    return base


class _SafeGitHubRedirectHandler(HTTPRedirectHandler):
    """Reject any HTTP redirect to non-HTTPS or untrusted hosts."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_github_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _open_github_url(req: Request, *, timeout: int, context: ssl.SSLContext):
    validate_github_url(req.full_url)
    opener = build_opener(
        HTTPSHandler(context=context),
        _SafeGitHubRedirectHandler(),
    )
    return opener.open(req, timeout=timeout)


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


def _get_ssl_context() -> ssl.SSLContext:
    """Create an SSL context that works in PyInstaller bundles on all platforms."""
    ctx = ssl.create_default_context()

    if sys.platform == "darwin" and is_frozen():
        stats = ctx.cert_store_stats()
        if stats["x509_ca"] == 0:
            _MACOS_CERT_PATHS = (
                "/etc/ssl/cert.pem",
                "/usr/local/etc/openssl/cert.pem",
                "/usr/local/etc/openssl@3/cert.pem",
                "/usr/local/etc/openssl@1.1/cert.pem",
                "/opt/homebrew/etc/openssl/cert.pem",
                "/opt/homebrew/etc/openssl@3/cert.pem",
            )
            for cert_path in _MACOS_CERT_PATHS:
                if os.path.isfile(cert_path):
                    try:
                        ctx.load_verify_locations(cert_path)
                        logger.debug("Loaded CA certs from %s", cert_path)
                        break
                    except Exception:
                        continue

            if ctx.cert_store_stats()["x509_ca"] == 0:
                try:
                    import certifi
                    ctx.load_verify_locations(certifi.where())
                    logger.debug("Loaded CA certs from certifi")
                except ImportError:
                    logger.warning(
                        "No CA certificates found — SSL verification may fail "
                        "on this macOS system."
                    )

    return ctx


# ---------------------------------------------------------------------------
# Check for update
# ---------------------------------------------------------------------------

def check_for_update(current_version: str) -> Optional[ReleaseInfo]:
    """Query GitHub for the latest release and return *ReleaseInfo* if newer.

    Returns ``None`` when the app is already up-to-date.
    Raises *UpdateCheckError* on network errors or when a newer version
    exists but no compatible download asset is found for this platform.
    """
    suffix = _get_platform_suffix()

    req = Request(GITHUB_API_URL, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/vnd.github+json",
    })

    try:
        ctx = _get_ssl_context()
        with _open_github_url(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
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

    asset_url = asset_name = ""
    asset_size = 0
    checksums_url = ""
    for asset in data.get("assets", []):
        name = asset.get("name", "")
        url = asset.get("browser_download_url", "")
        if name == CHECKSUMS_ASSET_NAME and url:
            try:
                checksums_url = validate_github_url(url)
            except UpdateError:
                pass
        elif not asset_url and name.endswith(suffix):
            try:
                asset_name = sanitize_asset_name(name)
                asset_url = validate_github_url(url)
                asset_size = int(asset.get("size", 0) or 0)
            except (UpdateError, ValueError):
                asset_url = asset_name = ""
                asset_size = 0

    if not asset_url:
        version = tag.lstrip("vV")
        raise UpdateCheckError(
            f"A new version (v{version}) is available but no download was "
            f"found for this platform.  Visit the release page to update "
            f"manually:\n{data.get('html_url', '')}"
        )

    html_url = data.get("html_url", "")
    if html_url:
        try:
            validate_github_url(html_url)
        except UpdateError:
            html_url = f"https://github.com/{GITHUB_REPO}/releases"

    version = tag.lstrip("vV")

    return ReleaseInfo(
        tag_name=tag,
        version=version,
        name=data.get("name", tag),
        body=data.get("body", ""),
        html_url=html_url,
        asset_url=asset_url,
        asset_name=asset_name,
        asset_size=asset_size,
        checksums_url=checksums_url,
    )


# ---------------------------------------------------------------------------
# Checksum verification
# ---------------------------------------------------------------------------

def parse_sha256sums(text: str) -> dict[str, str]:
    """Parse standard ``sha256sum`` output into ``{filename: lowercase_hex}``."""
    out: dict[str, str] = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([0-9a-fA-F]{64})\s+\*?(.+)$", line)
        if not m:
            continue
        digest = m.group(1).lower()
        name = Path(m.group(2).strip()).name
        out[name] = digest
    return out


def fetch_expected_sha256(release: ReleaseInfo) -> str:
    """Download ``SHA256SUMS.txt`` for *release* and return the expected hex digest.

    Fails closed when the release does not publish checksums or omits the asset.
    """
    if not release.checksums_url:
        raise UpdateDownloadError(
            f"Release {release.tag_name} does not include {CHECKSUMS_ASSET_NAME}. "
            f"Refusing unverified update — please download manually from:\n"
            f"{release.html_url}"
        )
    req = Request(release.checksums_url, headers={"User-Agent": USER_AGENT})
    try:
        ctx = _get_ssl_context()
        with _open_github_url(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            raw = resp.read(256 * 1024).decode("utf-8", errors="replace")
    except Exception as exc:
        raise UpdateDownloadError(
            f"Could not download {CHECKSUMS_ASSET_NAME}: {exc}"
        ) from exc

    sums = parse_sha256sums(raw)
    expected = sums.get(release.asset_name)
    if not expected:
        raise UpdateDownloadError(
            f"{CHECKSUMS_ASSET_NAME} does not list an entry for {release.asset_name}."
        )
    return expected


def compute_file_sha256(path: Path) -> str:
    """Compute lowercase SHA-256 hex digest of *path*."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest().lower()


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_update(
    release: ReleaseInfo,
    dest_dir: str,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    cancel_event=None,
    verify_checksum: bool = True,
) -> Path:
    """Download the release asset ZIP to *dest_dir* and verify its SHA-256.

    Raises *UpdateDownloadError* (or *UpdateCancelled*) on failure.
    """
    safe_name = sanitize_asset_name(release.asset_name)
    validate_github_url(release.asset_url)

    expected_sha = fetch_expected_sha256(release) if verify_checksum else None

    dest = Path(dest_dir) / safe_name
    tmp_path = dest.with_suffix(".part")

    req = Request(release.asset_url, headers={"User-Agent": USER_AGENT})

    try:
        ctx = _get_ssl_context()
        with _open_github_url(req, timeout=60, context=ctx) as resp:
            total = int(resp.headers.get("Content-Length", release.asset_size) or 0)
            downloaded = 0
            hasher = hashlib.sha256()

            with open(tmp_path, "wb") as f:
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        raise UpdateCancelled("Download cancelled by user.")
                    chunk = resp.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                    hasher.update(chunk)
                    downloaded += len(chunk)
                    if progress_callback and total > 0:
                        progress_callback(downloaded, total)

            actual_sha = hasher.hexdigest().lower()
            if expected_sha is not None and actual_sha != expected_sha:
                raise UpdateDownloadError(
                    f"Checksum mismatch for {safe_name} "
                    f"(expected {expected_sha[:12]}..., got {actual_sha[:12]}...)."
                )

            os.replace(tmp_path, dest)

    except (UpdateDownloadError, KeyboardInterrupt):
        for p in (tmp_path, dest):
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        raise
    except Exception as exc:
        for p in (tmp_path, dest):
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        raise UpdateDownloadError(f"Download failed: {exc}") from exc

    logger.info("Downloaded and verified %s (%d bytes)", dest.name, downloaded)
    return dest


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_update(zip_path: Path, extract_dir: Path) -> Path:
    """Safely extract the update ZIP and return the path to the application folder.

    Validates every archive entry against path traversal, symlinks, and zip-bomb
    decompressed size/file-count caps.
    """
    extract_root = extract_dir.resolve()
    extract_root.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    file_count = 0

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            infos = zf.infolist()
            if len(infos) > MAX_EXTRACT_FILES:
                raise UpdateError(
                    f"Archive contains too many entries ({len(infos)} > {MAX_EXTRACT_FILES})."
                )
            for info in infos:
                # Reject symlinks inside ZIP (Unix mode 0o120000 in upper 16 bits)
                unix_mode = (info.external_attr >> 16) & 0o170000
                if unix_mode == 0o120000:
                    raise UpdateError(f"Refusing symlink entry in update archive: {info.filename!r}")

                rel = Path(info.filename)
                if rel.is_absolute() or ".." in rel.parts:
                    raise UpdateError(f"Refusing unsafe path in update archive: {info.filename!r}")

                target = (extract_root / rel).resolve()
                if target != extract_root and not target.is_relative_to(extract_root):
                    raise UpdateError(f"Archive entry escapes target dir: {info.filename!r}")

                if info.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue

                file_count += 1
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info, "r") as src, open(target, "wb") as dst:
                    while True:
                        buf = src.read(CHUNK_SIZE)
                        if not buf:
                            break
                        total_bytes += len(buf)
                        if total_bytes > MAX_EXTRACT_BYTES:
                            raise UpdateError("Update archive exceeds maximum safe extracted size.")
                        dst.write(buf)

                # Restore POSIX executable bits when present
                perm = (info.external_attr >> 16) & 0o777
                if perm and os.name != "nt":
                    try:
                        os.chmod(target, perm)
                    except OSError:
                        pass
    except UpdateError:
        raise
    except Exception as exc:
        raise UpdateError(f"Failed to extract update: {exc}") from exc

    # macOS .app bundle ZIP (ditto -c -k --keepParent LatestVersionManager.app)
    app_bundle = extract_root / "LatestVersionManager.app"
    if app_bundle.is_dir():
        return app_bundle

    # Standard PyInstaller COLLECT folder ZIP
    app_dir = extract_root / "LatestVersionManager"
    if app_dir.is_dir():
        return app_dir

    return extract_root


# ---------------------------------------------------------------------------
# Updater script generation
# ---------------------------------------------------------------------------

def _find_app_bundle(install_dir: Path) -> Optional[Path]:
    """Walk up from *install_dir* looking for a ``.app`` bundle directory."""
    for parent in (install_dir, *install_dir.parents):
        if parent.suffix == ".app":
            return parent
    return None


def _cmd_safe_set(var_name: str, value: str) -> str:
    """Emit a ``set "VAR=VAL"`` line safe under ``DisableDelayedExpansion``.

    Rejects newlines/NUL and escapes ``%`` as ``%%`` so cmd.exe cannot expand
    attacker-controlled env vars; callers must always reference ``"%VAR%"``
    inside double quotes.
    """
    cleaned = str(value).replace("\x00", "").replace("\r", "").replace("\n", "")
    if '"' in cleaned:
        raise UpdateError(f"Unsafe quote character in updater path: {cleaned!r}")
    escaped = cleaned.replace("%", "%%")
    return f'set "{var_name}={escaped}"'


def create_updater_script(
    extracted_dir: Path,
    install_dir: Path,
    executable_path: Path,
    pid: int,
    cleanup_dir: Optional[Path] = None,
) -> Path:
    """Create a platform-specific script that replaces files and restarts."""
    if sys.platform == "win32":
        return _create_windows_updater(
            extracted_dir, install_dir, executable_path, pid, cleanup_dir=cleanup_dir
        )
    else:
        return _create_unix_updater(
            extracted_dir, install_dir, executable_path, pid, cleanup_dir=cleanup_dir
        )


def _create_windows_updater(
    extracted_dir: Path,
    install_dir: Path,
    executable_path: Path,
    pid: int,
    cleanup_dir: Optional[Path] = None,
) -> Path:
    work_dir = Path(tempfile.mkdtemp(prefix="lvm_updater_"))
    script_path = work_dir / "lvm_update.cmd"
    backup_dir = install_dir.parent / f"{install_dir.name}_backup"
    log_file = work_dir / "lvm_update.log"
    pid_tmp = work_dir / "pidcheck.tmp"
    cleanup_str = str(cleanup_dir) if cleanup_dir else ""

    lines = [
        "@echo off",
        "setlocal DisableDelayedExpansion",
        "title Updating Latest Version Manager...",
        "",
        _cmd_safe_set("LOG", str(log_file)),
        _cmd_safe_set("PIDTMP", str(pid_tmp)),
        _cmd_safe_set("PID", str(int(pid))),
        _cmd_safe_set("INSTALL_DIR", str(install_dir)),
        _cmd_safe_set("UPDATE_DIR", str(extracted_dir)),
        _cmd_safe_set("BACKUP_DIR", str(backup_dir)),
        _cmd_safe_set("EXE_NAME", executable_path.name),
        _cmd_safe_set("CLEANUP_DIR", cleanup_str),
        "",
        'echo [%date% %time%] Update started > "%LOG%"',
        'echo PID="%PID%" >> "%LOG%"',
        'echo INSTALL_DIR="%INSTALL_DIR%" >> "%LOG%"',
        'echo UPDATE_DIR="%UPDATE_DIR%" >> "%LOG%"',
        "",
        "set /a WAITED=0",
        ":waitloop",
        "if %WAITED% GEQ 60 goto waitdone",
        "timeout /t 1 /nobreak >NUL",
        "set /a WAITED+=1",
        'tasklist /FI "PID eq %PID%" /NH >"%PIDTMP%" 2>&1',
        'findstr /R /C:" %PID% " "%PIDTMP%" >NUL 2>NUL',
        "if not errorlevel 1 goto waitloop",
        ":waitdone",
        'del "%PIDTMP%" 2>NUL',
        'echo [%date% %time%] Process exited (waited %WAITED%s). >> "%LOG%"',
        "",
        'if exist "%BACKUP_DIR%" rmdir /S /Q "%BACKUP_DIR%"',
        'robocopy "%INSTALL_DIR%" "%BACKUP_DIR%" /MIR /R:1 /W:1 /NFL /NDL /NJH /NJS >NUL 2>NUL',
        'set "BRC=%ERRORLEVEL%"',
        'if %BRC% GEQ 8 (',
        '    echo [%date% %time%] ERROR: backup robocopy failed with code %BRC% >> "%LOG%"',
        "    exit /b 1",
        ")",
        'echo [%date% %time%] Backup copied. >> "%LOG%"',
        "",
        'robocopy "%UPDATE_DIR%" "%INSTALL_DIR%" /MIR /R:5 /W:2 /NFL /NDL /NJH /NJS >> "%LOG%" 2>&1',
        'set "RC=%ERRORLEVEL%"',
        'echo [%date% %time%] robocopy exit code: %RC% >> "%LOG%"',
        "if %RC% GEQ 8 (",
        '    echo [%date% %time%] ERROR: robocopy failed with code %RC%; restoring backup >> "%LOG%"',
        '    robocopy "%BACKUP_DIR%" "%INSTALL_DIR%" /MIR /R:3 /W:1 /NFL /NDL /NJH /NJS >NUL 2>NUL',
        "    exit /b 1",
        ")",
        'echo [%date% %time%] Files installed. >> "%LOG%"',
        "",
        'rmdir /S /Q "%BACKUP_DIR%" 2>NUL',
        'if not "%CLEANUP_DIR%"=="" rmdir /S /Q "%CLEANUP_DIR%" 2>NUL',
        "",
        'start "" "%INSTALL_DIR%\\%EXE_NAME%"',
        'echo [%date% %time%] Update complete. >> "%LOG%"',
        "timeout /t 2 /nobreak >NUL",
        'del "%~f0"',
        "",
    ]
    script_path.write_text("\r\n".join(lines), encoding="utf-8")
    return script_path


def _create_unix_updater(
    extracted_dir: Path,
    install_dir: Path,
    executable_path: Path,
    pid: int,
    cleanup_dir: Optional[Path] = None,
) -> Path:
    work_dir = Path(tempfile.mkdtemp(prefix="lvm_updater_"))
    script_path = work_dir / "lvm_update.sh"
    log_file = work_dir / "lvm_update.log"

    app_bundle = _find_app_bundle(install_dir)
    # When updating a macOS .app bundle with an extracted .app bundle, swap the
    # entire .app bundle so Info.plist, Frameworks, and signatures stay intact.
    swap_target = app_bundle if (app_bundle and extracted_dir.suffix == ".app") else install_dir
    backup_dir = swap_target.parent / f"{swap_target.name}_backup"

    q = shlex.quote
    script_body = f"""#!/bin/bash
set -u
PID={int(pid)}
INSTALL_DIR={q(str(swap_target))}
UPDATE_DIR={q(str(extracted_dir))}
BACKUP_DIR={q(str(backup_dir))}
EXE_NAME={q(executable_path.name)}
APP_BUNDLE={q(str(app_bundle) if app_bundle else "")}
SWAPPING_BUNDLE={1 if (app_bundle and extracted_dir.suffix == ".app") else 0}
CLEANUP_DIR={q(str(cleanup_dir) if cleanup_dir else "")}
LOG={q(str(log_file))}

log() {{ echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG"; }}

log "Update started (PID=$PID)"

WAITED=0
while kill -0 "$PID" 2>/dev/null; do
    if [ "$WAITED" -ge 60 ]; then
        log "Timed out waiting for PID $PID"
        break
    fi
    sleep 1
    WAITED=$((WAITED + 1))
done
sleep 1
log "Process exited (waited ${{WAITED}}s)."

rm -rf "$BACKUP_DIR"
if ! cp -a "$INSTALL_DIR" "$BACKUP_DIR" 2>>"$LOG"; then
    log "ERROR: backup copy failed; aborting update."
    exit 1
fi
log "Backup copied."

install_files() {{
    if [ "$SWAPPING_BUNDLE" -eq 1 ]; then
        rm -rf "$INSTALL_DIR" && cp -a "$UPDATE_DIR" "$INSTALL_DIR"
    elif command -v rsync >/dev/null 2>&1; then
        rsync -a --delete "$UPDATE_DIR/" "$INSTALL_DIR/"
    else
        rm -rf "$INSTALL_DIR" && mkdir -p "$INSTALL_DIR" && cp -a "$UPDATE_DIR/." "$INSTALL_DIR/"
    fi
}}

if ! install_files 2>>"$LOG"; then
    log "ERROR: install failed; restoring backup."
    rm -rf "$INSTALL_DIR" && cp -a "$BACKUP_DIR" "$INSTALL_DIR" 2>>"$LOG"
    exit 1
fi
log "Files installed."

if [ "$SWAPPING_BUNDLE" -eq 0 ]; then
    chmod +x "$INSTALL_DIR/$EXE_NAME" 2>/dev/null || true
    find "$INSTALL_DIR" \\( -name "*.so" -o -name "*.dylib" \\) -exec chmod +x {{}} + 2>/dev/null || true
fi

if [ -n "$APP_BUNDLE" ] && [ -d "$APP_BUNDLE" ]; then
    # Only ad-hoc sign if the bundle does not already carry a valid signature
    if ! codesign --verify --deep --strict "$APP_BUNDLE" 2>/dev/null; then
        log "Bundle unsigned/modified; applying ad-hoc signature..."
        if codesign --force --deep --sign - "$APP_BUNDLE" 2>>"$LOG"; then
            log "Ad-hoc sign succeeded."
        else
            log "WARNING: ad-hoc codesign returned rc=$?."
        fi
    fi
fi

rm -rf "$BACKUP_DIR"
if [ -n "$CLEANUP_DIR" ] && [ -d "$CLEANUP_DIR" ]; then
    rm -rf "$CLEANUP_DIR" 2>/dev/null || true
fi

if [ -n "$APP_BUNDLE" ] && [ -d "$APP_BUNDLE" ]; then
    log "Opening $APP_BUNDLE"
    open "$APP_BUNDLE" &
else
    log "Starting $INSTALL_DIR/$EXE_NAME"
    "$INSTALL_DIR/$EXE_NAME" &
fi

log "Update complete."
rm -- "$0"
"""
    script_path.write_text(script_body, encoding="utf-8")
    os.chmod(script_path, 0o700)
    return script_path


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
