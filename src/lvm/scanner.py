"""
Version Scanner - detects and parses versions from watched folders.

Handles both:
- File sequences (e.g. hero_comp_v003/hero_comp_v003.1001.exr ... .1120.exr)
- Single files (e.g. hero_comp_v003.mov)
"""

import os
import re
import logging
from pathlib import Path
from typing import Optional

from .models import VersionInfo, WatchedSource

logger = logging.getLogger(__name__)


class VersionScanner:
    """Scans a watched source directory for available versions."""

    # Common frame padding patterns: name.1001.exr, name.%04d.exr, name_1001.exr
    FRAME_PATTERNS = [
        re.compile(r"[._](\d{3,8})\.\w+$"),        # name.1001.exr or name_1001.exr
    ]

    def __init__(self, watched_source: WatchedSource):
        self.source = watched_source
        self._version_regex = self._compile_version_pattern(watched_source.version_pattern)

    @staticmethod
    def _compile_version_pattern(pattern: str) -> re.Pattern:
        """
        Compile the version pattern into a regex.

        Supports two formats:
        - Raw regex with a capture group:  r"_v(\\d+)"
        - Template style:  "hero_comp_v{version}"
          which gets converted to:  hero_comp_v(\\d+)
        """
        if "{version}" in pattern:
            regex_str = re.escape(pattern).replace(r"\{version\}", r"(\d+)")
            return re.compile(regex_str)
        else:
            return re.compile(pattern)

    def scan(self) -> list[VersionInfo]:
        """
        Scan the source directory and return all detected versions,
        sorted by version number (ascending).

        Timecode extraction is lazy - not performed during scan.
        """
        source_path = Path(self.source.source_dir)
        if not source_path.exists():
            logger.warning(f"Source directory does not exist: {source_path}")
            return []

        versions = []

        for entry in sorted(source_path.iterdir()):
            version_info = None

            if entry.is_dir():
                version_info = self._scan_version_folder(entry)
            elif entry.is_file():
                version_info = self._scan_version_file(entry)

            if version_info:
                versions.append(version_info)

        versions.sort(key=lambda v: v.version_number)
        return versions

    def _extract_version(self, name: str) -> Optional[tuple[str, int]]:
        """
        Extract version string and number from a name.
        Returns (version_string, version_number) or None.
        """
        match = self._version_regex.search(name)
        if not match:
            return None
        version_num = int(match.group(1))
        version_str = f"v{version_num:03d}"
        return version_str, version_num

    def _scan_version_folder(self, folder: Path) -> Optional[VersionInfo]:
        """Scan a version folder for file sequences or single files."""
        result = self._extract_version(folder.name)
        if result is None:
            return None

        version_str, version_num = result

        # Collect files matching our extensions using os.scandir
        files = self._collect_files(folder)
        if not files:
            logger.debug(f"No matching files in version folder: {folder}")
            return None

        frame_range, frame_count = self._detect_frame_range(files)
        total_size = sum(f.stat().st_size for f in files)

        return VersionInfo(
            version_string=version_str,
            version_number=version_num,
            source_path=str(folder),
            frame_range=frame_range,
            frame_count=frame_count,
            file_count=len(files),
            total_size_bytes=total_size,
            start_timecode=None,  # Lazy: extracted on demand via timecode module
        )

    def _scan_version_file(self, filepath: Path) -> Optional[VersionInfo]:
        """Scan a single versioned file (e.g. a .mov or .mxf)."""
        if filepath.suffix.lower() not in self.source.file_extensions:
            return None

        result = self._extract_version(filepath.name)
        if result is None:
            return None

        version_str, version_num = result

        return VersionInfo(
            version_string=version_str,
            version_number=version_num,
            source_path=str(filepath),
            frame_range=None,
            frame_count=1,
            file_count=1,
            total_size_bytes=filepath.stat().st_size,
            start_timecode=None,  # Lazy: extracted on demand via timecode module
        )

    def _collect_files(self, folder: Path) -> list[Path]:
        """Collect all files in a folder matching the configured extensions.

        Uses os.scandir for faster directory iteration.
        """
        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)
        files = []
        try:
            with os.scandir(folder) as it:
                for entry in it:
                    if entry.is_file(follow_symlinks=False):
                        name = entry.name
                        dot_idx = name.rfind(".")
                        if dot_idx >= 0:
                            suffix = name[dot_idx:].lower()
                            if suffix in valid_extensions:
                                files.append(Path(entry.path))
        except PermissionError:
            pass
        files.sort()
        return files

    def _detect_frame_range(self, files: list[Path]) -> tuple[Optional[str], int]:
        """
        Detect frame range from a list of files.
        Returns (range_string, frame_count).
        """
        if not files:
            return None, 0

        if len(files) == 1:
            return None, 1

        frames = []
        for f in files:
            for pattern in self.FRAME_PATTERNS:
                match = pattern.search(f.name)
                if match:
                    frames.append(int(match.group(1)))
                    break

        if not frames:
            return None, len(files)

        frames.sort()
        first = frames[0]
        last = frames[-1]
        expected_count = last - first + 1
        actual_count = len(frames)

        range_str = f"{first}-{last}"
        if actual_count != expected_count:
            range_str += f" ({actual_count}/{expected_count} frames, gaps detected)"

        return range_str, actual_count

    def get_latest_version(self) -> Optional[VersionInfo]:
        """Return the highest version number found."""
        versions = self.scan()
        return versions[-1] if versions else None


# ---------------------------------------------------------------------------
# Standalone helpers for manual version import
# ---------------------------------------------------------------------------

# Frame pattern for detecting sequence siblings
_FRAME_RE = re.compile(r"[._](\d{3,8})\.\w+$")


def detect_sequence_from_file(
    filepath: Path, extensions: list[str]
) -> tuple[list[Path], Optional[str], int]:
    """Given a single file, detect the full frame sequence it belongs to.

    Finds all sibling files in the same directory that share the same base name
    (everything before the frame number) and the same extension.

    Args:
        filepath: Path to any frame in a sequence, or a single movie file.
        extensions: Allowed file extensions (e.g. [".exr", ".mov"]).

    Returns:
        (sorted_file_list, frame_range_string_or_None, frame_count)
    """
    filepath = Path(filepath)
    if not filepath.is_file():
        return [filepath], None, 0

    match = _FRAME_RE.search(filepath.name)
    if not match:
        # Single file (movie, etc.) — no sequence
        return [filepath], None, 1

    # Determine the base pattern: everything before the frame number
    frame_start = match.start()
    separator = filepath.name[frame_start]  # '.' or '_'
    base_prefix = filepath.name[:frame_start + 1]  # include separator
    ext = filepath.suffix.lower()

    # Build a regex to match siblings: same prefix, frame digits, same extension
    sibling_re = re.compile(
        re.escape(base_prefix) + r"\d{" + str(len(match.group(1))) + r"}" + re.escape(ext) + "$",
        re.IGNORECASE,
    )

    valid_ext = set(e.lower() for e in extensions)
    parent = filepath.parent
    files = []
    try:
        with os.scandir(parent) as it:
            for entry in it:
                if entry.is_file(follow_symlinks=False):
                    name = entry.name
                    dot_idx = name.rfind(".")
                    if dot_idx >= 0 and name[dot_idx:].lower() in valid_ext:
                        if sibling_re.match(name):
                            files.append(Path(entry.path))
    except PermissionError:
        pass

    if not files:
        files = [filepath]

    files.sort()

    # Detect frame range
    frames = []
    for f in files:
        m = _FRAME_RE.search(f.name)
        if m:
            frames.append(int(m.group(1)))
    frames.sort()

    if len(frames) < 2:
        return files, None, len(files)

    first, last = frames[0], frames[-1]
    expected = last - first + 1
    actual = len(frames)
    range_str = f"{first}-{last}"
    if actual != expected:
        range_str += f" ({actual}/{expected} frames, gaps detected)"

    return files, range_str, actual


def scan_directory_as_version(
    folder: Path, extensions: list[str]
) -> tuple[list[Path], Optional[str], int]:
    """Scan a directory for media files and detect frame range.

    Used for drag-and-drop of directories as manual versions.

    Returns:
        (sorted_file_list, frame_range_string_or_None, frame_count)
    """
    valid_ext = set(e.lower() for e in extensions)
    files = []
    try:
        with os.scandir(folder) as it:
            for entry in it:
                if entry.is_file(follow_symlinks=False):
                    name = entry.name
                    dot_idx = name.rfind(".")
                    if dot_idx >= 0 and name[dot_idx:].lower() in valid_ext:
                        files.append(Path(entry.path))
    except PermissionError:
        pass

    files.sort()
    if not files:
        return files, None, 0

    # Detect frame range
    frames = []
    for f in files:
        m = _FRAME_RE.search(f.name)
        if m:
            frames.append(int(m.group(1)))
    frames.sort()

    if len(frames) < 2:
        return files, None, len(files)

    first, last = frames[0], frames[-1]
    expected = last - first + 1
    actual = len(frames)
    range_str = f"{first}-{last}"
    if actual != expected:
        range_str += f" ({actual}/{expected} frames, gaps detected)"

    return files, range_str, actual


def create_manual_version(
    source_path: str,
    version_number: int,
    file_count: int,
    total_size_bytes: int,
    frame_range: Optional[str] = None,
    frame_count: int = 0,
) -> VersionInfo:
    """Create a VersionInfo for a manually imported version.

    Args:
        source_path: Path to the directory (sequence) or file (single).
        version_number: Assigned version number.
        file_count: Number of media files.
        total_size_bytes: Total size in bytes.
        frame_range: Frame range string or None.
        frame_count: Number of frames.
    """
    return VersionInfo(
        version_string=f"v{version_number:03d}",
        version_number=version_number,
        source_path=source_path,
        frame_range=frame_range,
        frame_count=frame_count,
        file_count=file_count,
        total_size_bytes=total_size_bytes,
    )
