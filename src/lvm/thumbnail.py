"""
Thumbnail extraction for version previews.

Uses oiiotool (preferred for VFX) or ffmpeg (fallback) to extract a
representative frame from sequences or containers.
Caches results in .lvm_cache/ next to the project file.
"""

import hashlib
import logging
import os
import shutil
import subprocess
import sys
import threading
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _app_dir() -> Optional[Path]:
    """Return the directory containing the running executable (frozen) or script."""
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent
    return None


def _subprocess_kwargs() -> dict:
    """Return platform-specific kwargs to suppress console windows on Windows."""
    kwargs = {}
    if sys.platform == "win32":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = subprocess.SW_HIDE
        kwargs["startupinfo"] = si
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    return kwargs


_oiiotool_path: Optional[str] = None
_oiiotool_checked: bool = False
_ffmpeg_path: Optional[str] = None
_ffmpeg_checked: bool = False
_tool_lock = threading.Lock()

# Extensions that are image sequences (need frame selection)
_SEQUENCE_EXTENSIONS = {".exr", ".dpx", ".tiff", ".tif", ".png", ".jpg", ".jpeg"}
# Extensions that are container files (need time-based extraction)
_CONTAINER_EXTENSIONS = {".mov", ".mxf", ".mp4", ".avi"}


def find_oiiotool() -> Optional[str]:
    """Locate oiiotool on PATH or next to the executable. Caches result."""
    global _oiiotool_path, _oiiotool_checked
    if _oiiotool_checked:
        return _oiiotool_path
    with _tool_lock:
        if _oiiotool_checked:
            return _oiiotool_path
        # Check next to the executable (portable / manually placed)
        app_dir = _app_dir()
        if app_dir:
            name = "oiiotool.exe" if sys.platform == "win32" else "oiiotool"
            candidate = app_dir / name
            if candidate.is_file():
                _oiiotool_path = str(candidate)
                _oiiotool_checked = True
                logger.debug(f"Found bundled oiiotool: {_oiiotool_path}")
                return _oiiotool_path
        # Fall back to system PATH
        _oiiotool_path = shutil.which("oiiotool")
        _oiiotool_checked = True
        if _oiiotool_path:
            logger.debug(f"Found oiiotool: {_oiiotool_path}")
        return _oiiotool_path


def find_ffmpeg() -> Optional[str]:
    """Locate ffmpeg: bundled (imageio-ffmpeg) → next to exe → system PATH."""
    global _ffmpeg_path, _ffmpeg_checked
    if _ffmpeg_checked:
        return _ffmpeg_path
    with _tool_lock:
        if _ffmpeg_checked:
            return _ffmpeg_path
        # 1. Try bundled ffmpeg from imageio-ffmpeg package
        try:
            from imageio_ffmpeg import get_ffmpeg_exe
            bundled = get_ffmpeg_exe()
            if bundled and Path(bundled).is_file():
                _ffmpeg_path = bundled
                _ffmpeg_checked = True
                logger.debug(f"Found bundled ffmpeg (imageio-ffmpeg): {_ffmpeg_path}")
                return _ffmpeg_path
        except (ImportError, RuntimeError):
            pass
        # 2. Check next to the executable (portable / manually placed)
        app_dir = _app_dir()
        if app_dir:
            name = "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg"
            candidate = app_dir / name
            if candidate.is_file():
                _ffmpeg_path = str(candidate)
                _ffmpeg_checked = True
                logger.debug(f"Found portable ffmpeg: {_ffmpeg_path}")
                return _ffmpeg_path
        # 3. Fall back to system PATH
        _ffmpeg_path = shutil.which("ffmpeg")
        _ffmpeg_checked = True
        if _ffmpeg_path:
            logger.debug(f"Found ffmpeg: {_ffmpeg_path}")
        return _ffmpeg_path


def _cache_key(
    source_path: str,
    version_string: str,
    mtime_ns: int = 0,
    size: int = 0,
) -> str:
    """Generate a cache filename from source path, version, and file fingerprint."""
    payload = f"{source_path}:{version_string}:{mtime_ns}:{size}"
    h = hashlib.md5(payload.encode()).hexdigest()[:12]
    return f"thumb_{h}.jpg"


def _generate_with_oiiotool(oiiotool: str, input_file: Path, output: Path) -> bool:
    """Generate a JPEG thumbnail using oiiotool."""
    try:
        cmd = [
            oiiotool,
            "-i", str(input_file),
            "--resize", "320x0",
            "--colorconvert", "scene_linear", "sRGB",
            "-o", str(output),
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=15, **_subprocess_kwargs())
        if result.returncode == 0 and output.exists() and output.stat().st_size > 0:
            return True
        # Retry without color conversion (may not have OCIO config)
        cmd = [
            oiiotool,
            "-i", str(input_file),
            "--resize", "320x0",
            "-o", str(output),
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=15, **_subprocess_kwargs())
        return result.returncode == 0 and output.exists() and output.stat().st_size > 0
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.debug(f"oiiotool thumbnail failed: {e}")
        return False


def _generate_with_ffmpeg(ffmpeg: str, input_file: Path, output: Path, is_container: bool = False) -> bool:
    """Generate a JPEG thumbnail using ffmpeg."""
    try:
        if is_container:
            cmd = [
                ffmpeg, "-y", "-v", "quiet",
                "-ss", "00:00:01",
                "-i", str(input_file),
                "-vf", "scale=320:-1",
                "-frames:v", "1",
                str(output),
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=15, **_subprocess_kwargs())
            if result.returncode == 0 and output.exists() and output.stat().st_size > 0:
                return True
            # Fallback to frame 0 for clips shorter than 1 second
        cmd = [
            ffmpeg, "-y", "-v", "quiet",
            "-i", str(input_file),
            "-vf", "scale=320:-1",
            "-frames:v", "1",
            str(output),
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=15, **_subprocess_kwargs())
        return result.returncode == 0 and output.exists() and output.stat().st_size > 0
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.debug(f"ffmpeg thumbnail failed: {e}")
        return False


def get_thumbnail(
    source_path: str,
    version_string: str,
    extensions: list[str],
    cache_dir: str,
) -> Optional[str]:
    """Get or generate a thumbnail for a version.

    Args:
        source_path: Path to version folder or single file.
        version_string: Version string for cache key.
        extensions: Allowed file extensions.
        cache_dir: Directory for cached thumbnails.

    Returns:
        Path to JPEG thumbnail, or None if unavailable.
    """
    source = Path(source_path)
    input_file: Optional[Path] = None
    is_container = False
    mtime_ns = 0
    size = 0

    try:
        if source.is_dir():
            # Pick the middle frame from the sequence using os.scandir
            valid_ext = {e.lower() for e in extensions}
            entries = []
            with os.scandir(source) as it:
                for entry in it:
                    if not entry.is_file():
                        continue
                    dot = entry.name.rfind(".")
                    if dot >= 0 and entry.name[dot:].lower() in valid_ext:
                        entries.append(entry)
            if not entries:
                return None
            entries.sort(key=lambda e: e.name)
            mid_entry = entries[len(entries) // 2]
            input_file = Path(mid_entry.path)
            try:
                st = mid_entry.stat()
                mtime_ns, size = st.st_mtime_ns, st.st_size
            except OSError:
                pass
        elif source.is_file():
            input_file = source
            is_container = input_file.suffix.lower() in _CONTAINER_EXTENSIONS
            try:
                st = source.stat()
                mtime_ns, size = st.st_mtime_ns, st.st_size
            except OSError:
                pass
        else:
            return None
    except OSError as e:
        logger.debug(f"Could not inspect source for thumbnail {source_path}: {e}")
        return None

    cache_path = Path(cache_dir) / _cache_key(source_path, version_string, mtime_ns, size)
    if cache_path.exists():
        return str(cache_path)

    oiiotool = find_oiiotool()
    ffmpeg = find_ffmpeg()
    if oiiotool is None and ffmpeg is None:
        return None

    # Generate thumbnail atomically via a unique temp file so concurrent
    # workers never read a half-written JPEG.
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    tmp_path = Path(cache_dir) / f".{cache_path.stem}.{os.getpid()}.{threading.get_ident()}.tmp.jpg"
    ext = input_file.suffix.lower()
    generated = False
    try:
        if is_container:
            if ffmpeg and _generate_with_ffmpeg(ffmpeg, input_file, tmp_path, is_container=True):
                generated = True
        elif ext in _SEQUENCE_EXTENSIONS:
            if oiiotool and _generate_with_oiiotool(oiiotool, input_file, tmp_path):
                generated = True
            elif ffmpeg and _generate_with_ffmpeg(ffmpeg, input_file, tmp_path):
                generated = True
        else:
            if ffmpeg and _generate_with_ffmpeg(ffmpeg, input_file, tmp_path):
                generated = True

        if generated:
            os.replace(tmp_path, cache_path)
            return str(cache_path)
    except OSError as e:
        logger.debug(f"Thumbnail cache write failed for {input_file}: {e}")
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

    logger.debug(f"Could not generate thumbnail for {input_file}")
    return None
