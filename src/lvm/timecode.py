"""
Timecode extraction from media files.

Extracts starting timecode using:
- Native EXR header parsing (reads timeCode and nuke/input/timecode attributes)
- Native DPX header parsing (reads timecode from header)
- ffprobe for container formats (MOV, MXF, MP4, etc.)

Gracefully degrades when ffprobe is not available or files lack timecode.
"""

import json
import logging
import shutil
import struct
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Cached ffprobe path
_ffprobe_path: Optional[str] = None
_ffprobe_checked: bool = False

# Extensions handled by native parsers (no ffprobe needed)
_NATIVE_TC_EXTENSIONS = {".exr", ".dpx"}


# ---------------------------------------------------------------------------
# Native EXR timecode parsing
# ---------------------------------------------------------------------------

def _read_exr_timecode(file_path: Path) -> Optional[str]:
    """Read timecode from an EXR file header.

    EXR stores timecode in two possible attributes:
    - 'timeCode' (type 'timecode'): SMPTE 12M packed format (8 bytes)
    - 'nuke/input/timecode' (type 'string'): human-readable string written by Nuke

    Returns a timecode string (e.g. "01:00:00:00") or None.
    """
    try:
        with open(file_path, "rb") as f:
            # Validate EXR magic number
            magic = f.read(4)
            if magic != b"\x76\x2f\x31\x01":
                return None
            f.read(4)  # skip version

            tc_smpte = None
            tc_nuke_str = None

            # Parse attribute headers until empty name (null byte)
            while True:
                name = _read_null_string(f)
                if not name:
                    break  # end of header

                attr_type = _read_null_string(f)
                size_data = f.read(4)
                if len(size_data) < 4:
                    break
                size = struct.unpack("<I", size_data)[0]

                if name == "timeCode" and attr_type == "timecode" and size == 8:
                    value = f.read(8)
                    tc_smpte = _decode_smpte_timecode(value)
                elif name == "nuke/input/timecode" and attr_type == "string" and size <= 32:
                    value = f.read(size)
                    tc_nuke_str = value.rstrip(b"\x00").decode("ascii", errors="replace").strip()
                    if not _is_valid_timecode_string(tc_nuke_str):
                        tc_nuke_str = None
                else:
                    # Skip this attribute's value
                    f.seek(size, 1)

                # Stop early if we found both
                if tc_smpte and tc_nuke_str:
                    break

            # Prefer the SMPTE timecode, fall back to Nuke string
            return tc_smpte or tc_nuke_str

    except (OSError, struct.error) as e:
        logger.debug(f"EXR timecode read error for {file_path}: {e}")
        return None


def _read_null_string(f) -> Optional[str]:
    """Read a null-terminated string from a file. Returns empty string for end-of-header."""
    chars = []
    while True:
        c = f.read(1)
        if not c or c == b"\x00":
            break
        chars.append(c)
    return b"".join(chars).decode("ascii", errors="replace") if chars else ""


def _decode_smpte_timecode(data: bytes) -> Optional[str]:
    """Decode SMPTE 12M timecode from 8 bytes (two uint32, little-endian).

    The first uint32 contains the time value in BCD:
    - bits 0-3: frame units, 4-5: frame tens
    - bits 8-11: second units, 12-14: second tens
    - bits 16-19: minute units, 20-22: minute tens
    - bits 24-27: hour units, 28-29: hour tens
    """
    if len(data) < 8:
        return None
    try:
        tc_packed = struct.unpack("<I", data[:4])[0]
        frames = (tc_packed & 0x0F) + ((tc_packed >> 4) & 0x03) * 10
        seconds = ((tc_packed >> 8) & 0x0F) + ((tc_packed >> 12) & 0x07) * 10
        minutes = ((tc_packed >> 16) & 0x0F) + ((tc_packed >> 20) & 0x07) * 10
        hours = ((tc_packed >> 24) & 0x0F) + ((tc_packed >> 28) & 0x03) * 10

        # Sanity check
        if hours > 23 or minutes > 59 or seconds > 59 or frames > 59:
            return None

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}:{frames:02d}"
    except struct.error:
        return None


def _is_valid_timecode_string(s: str) -> bool:
    """Check if a string looks like a timecode (HH:MM:SS:FF or HH:MM:SS;FF)."""
    if not s or len(s) < 8:
        return False
    parts = s.replace(";", ":").split(":")
    if len(parts) != 4:
        return False
    return all(p.isdigit() for p in parts)


# ---------------------------------------------------------------------------
# Native DPX timecode parsing
# ---------------------------------------------------------------------------

def _read_dpx_timecode(file_path: Path) -> Optional[str]:
    """Read timecode from a DPX file header.

    DPX stores timecode as a 32-bit SMPTE value at offset 1920 (big-endian)
    or at offset 1920 (little-endian depending on magic).
    """
    try:
        with open(file_path, "rb") as f:
            magic = f.read(4)
            if magic == b"SDPX":
                byte_order = ">"  # big-endian
            elif magic == b"XPDS":
                byte_order = "<"  # little-endian
            else:
                return None

            # TV header timecode is at offset 1920
            f.seek(1920)
            tc_data = f.read(4)
            if len(tc_data) < 4:
                return None

            tc_packed = struct.unpack(f"{byte_order}I", tc_data)[0]

            # DPX timecode 0xFFFFFFFF means undefined
            if tc_packed == 0xFFFFFFFF:
                return None

            # DPX uses BCD encoding similar to SMPTE but big-endian nibble order:
            # bits 28-31: hours tens, 24-27: hours units
            # bits 20-23: minutes tens, 16-19: minutes units
            # bits 12-15: seconds tens, 8-11: seconds units
            # bits 4-7: frames tens, 0-3: frames units
            hours = ((tc_packed >> 28) & 0x0F) * 10 + ((tc_packed >> 24) & 0x0F)
            minutes = ((tc_packed >> 20) & 0x0F) * 10 + ((tc_packed >> 16) & 0x0F)
            seconds = ((tc_packed >> 12) & 0x0F) * 10 + ((tc_packed >> 8) & 0x0F)
            frames = ((tc_packed >> 4) & 0x0F) * 10 + (tc_packed & 0x0F)

            if hours > 23 or minutes > 59 or seconds > 59 or frames > 59:
                return None

            return f"{hours:02d}:{minutes:02d}:{seconds:02d}:{frames:02d}"

    except (OSError, struct.error) as e:
        logger.debug(f"DPX timecode read error for {file_path}: {e}")
        return None


# ---------------------------------------------------------------------------
# ffprobe-based timecode (for MOV, MXF, MP4, etc.)
# ---------------------------------------------------------------------------

def find_ffprobe() -> Optional[str]:
    """Locate ffprobe on PATH. Caches the result after first call."""
    global _ffprobe_path, _ffprobe_checked
    if _ffprobe_checked:
        return _ffprobe_path

    _ffprobe_checked = True
    _ffprobe_path = shutil.which("ffprobe")
    if _ffprobe_path is None:
        logger.info("ffprobe not found on PATH — timecode extraction for container formats disabled")
    else:
        logger.debug(f"Found ffprobe: {_ffprobe_path}")
    return _ffprobe_path


def _extract_timecode_ffprobe(file_path: Path) -> Optional[str]:
    """Extract timecode from a media file using ffprobe.

    Returns a timecode string (e.g. "01:00:00:00") or None.
    """
    ffprobe = find_ffprobe()
    if ffprobe is None:
        return None

    try:
        result = subprocess.run(
            [
                ffprobe,
                "-v", "quiet",
                "-print_format", "json",
                "-show_entries", "format_tags=timecode:stream_tags=timecode",
                str(file_path),
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            logger.debug(f"ffprobe returned {result.returncode} for {file_path}")
            return None

        data = json.loads(result.stdout)

        # Check format-level tags first
        tc = (data.get("format", {}).get("tags", {}).get("timecode")
              or data.get("format", {}).get("tags", {}).get("TIMECODE"))
        if tc:
            return tc

        # Check stream-level tags
        for stream in data.get("streams", []):
            tc = (stream.get("tags", {}).get("timecode")
                  or stream.get("tags", {}).get("TIMECODE"))
            if tc:
                return tc

        return None

    except subprocess.TimeoutExpired:
        logger.debug(f"ffprobe timed out for {file_path}")
        return None
    except (json.JSONDecodeError, OSError) as e:
        logger.debug(f"ffprobe error for {file_path}: {e}")
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_timecode(file_path: Path) -> Optional[str]:
    """Extract starting timecode from a single media file.

    Uses native parsing for EXR and DPX files (fast, no dependencies).
    Falls back to ffprobe for container formats (MOV, MXF, MP4, etc.).

    Returns a timecode string (e.g. "01:00:00:00") or None.
    """
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext == ".exr":
        return _read_exr_timecode(path)
    elif ext == ".dpx":
        return _read_dpx_timecode(path)
    else:
        return _extract_timecode_ffprobe(path)


def populate_timecodes(versions: list) -> None:
    """Populate start_timecode on a list of VersionInfo objects that have None timecode.

    This is the lazy-loading entry point: call this when you actually need
    to display timecodes, not during the initial scan.

    Args:
        versions: List of VersionInfo objects. Those with start_timecode=None
                  will have their timecode extracted from the source files.
    """
    for v in versions:
        if v.start_timecode is None:
            v.start_timecode = extract_timecode_for_version(Path(v.source_path))


def extract_timecode_for_version(source_path: Path, files: Optional[list[Path]] = None) -> Optional[str]:
    """Extract starting timecode for a version.

    Args:
        source_path: Path to version folder (sequence) or single file.
        files: Optional pre-collected list of media files in the folder.
               If provided, uses the first file. If None and source_path is
               a directory, finds the first media file by sorted iteration.

    Returns:
        Timecode string or None.
    """
    source = Path(source_path)

    if source.is_file():
        return extract_timecode(source)

    if source.is_dir():
        # Use provided file list or find first file
        if files:
            return extract_timecode(files[0])
        # Fallback: iterate directory for first media file
        for f in sorted(source.iterdir()):
            if f.is_file() and not f.name.startswith("."):
                tc = extract_timecode(f)
                if tc is not None:
                    return tc

    return None
