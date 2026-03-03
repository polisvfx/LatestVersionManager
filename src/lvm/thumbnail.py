"""
Thumbnail extraction for version previews.

Uses oiiotool (preferred for VFX) or ffmpeg (fallback) to extract a
representative frame from sequences or containers.
Caches results in .lvm_cache/ next to the project file.
"""

import hashlib
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


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

# Extensions that are image sequences (need frame selection)
_SEQUENCE_EXTENSIONS = {".exr", ".dpx", ".tiff", ".tif", ".png", ".jpg", ".jpeg"}
# Extensions that are container files (need time-based extraction)
_CONTAINER_EXTENSIONS = {".mov", ".mxf", ".mp4", ".avi"}


def find_oiiotool() -> Optional[str]:
    """Locate oiiotool on PATH. Caches result."""
    global _oiiotool_path, _oiiotool_checked
    if _oiiotool_checked:
        return _oiiotool_path
    _oiiotool_checked = True
    _oiiotool_path = shutil.which("oiiotool")
    if _oiiotool_path:
        logger.debug(f"Found oiiotool: {_oiiotool_path}")
    return _oiiotool_path


def find_ffmpeg() -> Optional[str]:
    """Locate ffmpeg on PATH. Caches result."""
    global _ffmpeg_path, _ffmpeg_checked
    if _ffmpeg_checked:
        return _ffmpeg_path
    _ffmpeg_checked = True
    _ffmpeg_path = shutil.which("ffmpeg")
    if _ffmpeg_path:
        logger.debug(f"Found ffmpeg: {_ffmpeg_path}")
    return _ffmpeg_path


def _cache_key(source_path: str, version_string: str) -> str:
    """Generate a cache filename from source path + version."""
    h = hashlib.md5(f"{source_path}:{version_string}".encode()).hexdigest()[:12]
    return f"thumb_{h}.jpg"


def _generate_with_oiiotool(oiiotool: str, input_file: Path, output: Path) -> bool:
    """Generate a JPEG thumbnail using oiiotool."""
    try:
        cmd = [
            oiiotool,
            str(input_file),
            "--resize", "320x0",
            "--colorconvert", "scene_linear", "sRGB",
            "-o", str(output),
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=15, **_subprocess_kwargs())
        if result.returncode == 0 and output.exists():
            return True
        # Retry without color conversion (may not have OCIO config)
        cmd = [
            oiiotool,
            str(input_file),
            "--resize", "320x0",
            "-o", str(output),
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=15, **_subprocess_kwargs())
        return result.returncode == 0 and output.exists()
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
        else:
            cmd = [
                ffmpeg, "-y", "-v", "quiet",
                "-i", str(input_file),
                "-vf", "scale=320:-1",
                "-frames:v", "1",
                str(output),
            ]
        result = subprocess.run(cmd, capture_output=True, timeout=15, **_subprocess_kwargs())
        return result.returncode == 0 and output.exists()
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
    cache_path = Path(cache_dir) / _cache_key(source_path, version_string)
    if cache_path.exists():
        return str(cache_path)

    oiiotool = find_oiiotool()
    ffmpeg = find_ffmpeg()
    if oiiotool is None and ffmpeg is None:
        return None

    source = Path(source_path)
    input_file = None
    is_container = False

    if source.is_dir():
        # Pick the middle frame from the sequence
        valid_ext = set(e.lower() for e in extensions)
        files = sorted(
            f for f in source.iterdir()
            if f.is_file() and f.suffix.lower() in valid_ext
        )
        if not files:
            return None
        mid = len(files) // 2
        input_file = files[mid]
    elif source.is_file():
        input_file = source
        is_container = input_file.suffix.lower() in _CONTAINER_EXTENSIONS
    else:
        return None

    # Generate thumbnail
    Path(cache_dir).mkdir(parents=True, exist_ok=True)

    ext = input_file.suffix.lower()

    if is_container:
        # Containers: only ffmpeg can handle these
        if ffmpeg and _generate_with_ffmpeg(ffmpeg, input_file, cache_path, is_container=True):
            return str(cache_path)
    elif ext in _SEQUENCE_EXTENSIONS:
        # Image sequences: try oiiotool first (handles ACES/linear), then ffmpeg
        if oiiotool and _generate_with_oiiotool(oiiotool, input_file, cache_path):
            return str(cache_path)
        if ffmpeg and _generate_with_ffmpeg(ffmpeg, input_file, cache_path):
            return str(cache_path)
    else:
        # Unknown type: try ffmpeg as fallback
        if ffmpeg and _generate_with_ffmpeg(ffmpeg, input_file, cache_path):
            return str(cache_path)

    logger.debug(f"Could not generate thumbnail for {input_file}")
    return None
