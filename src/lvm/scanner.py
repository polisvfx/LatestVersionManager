"""
Version Scanner - detects and parses versions from watched folders.

Handles both:
- File sequences (e.g. hero_comp_v003/hero_comp_v003.1001.exr ... .1120.exr)
- Single files (e.g. hero_comp_v003.mov)
"""

__all__ = [
    "VersionScanner", "detect_sequence_from_file",
    "scan_directory_as_version", "create_manual_version",
]

import os
import re
import logging
from pathlib import Path
from typing import Optional

from .models import VersionInfo, WatchedSource
from .task_tokens import derive_source_tokens, parse_date_to_sortable, format_date_display

logger = logging.getLogger(__name__)


class VersionScanner:
    """Scans a watched source directory for available versions."""

    # Common frame padding patterns: name.1001.exr, name.%04d.exr, name_1001.exr
    FRAME_PATTERNS = [
        re.compile(r"[._](\d+)\.\w+$"),        # name.1001.exr or name_1001.exr
    ]

    def __init__(self, watched_source: WatchedSource, task_tokens: list[str] = None):
        self.source = watched_source
        self._date_format = getattr(watched_source, "date_format", "")
        self._version_regex = self._compile_version_pattern(
            watched_source.version_pattern, self._date_format)
        self._task_tokens = task_tokens or []
        self._expected_basename = self._compute_expected_basename()

    def _compute_expected_basename(self) -> str:
        """Compute the expected basename from the source's sample_filename.

        Returns empty string if no sample_filename is set (disables filtering).
        """
        sample = self.source.sample_filename
        if not sample:
            return ""
        tokens = derive_source_tokens(sample, self._task_tokens, self._date_format)
        return tokens["source_basename"]

    def _strip_extension_suffix(self, name: str) -> str:
        """Strip trailing extension-like suffix from directory names.

        Handles the convention where version directories include the file type
        as an underscore suffix, e.g. 'hero_comp_v001_exr' for .exr sequences.
        """
        name_lower = name.lower()
        for ext in self.source.file_extensions:
            suffix = "_" + ext.lstrip(".")  # ".exr" -> "_exr"
            if name_lower.endswith(suffix.lower()):
                return name[:len(name) - len(suffix)]
        return name

    def _matches_basename(self, entry_name: str) -> bool:
        """Check if an entry's basename matches the expected basename.

        If no expected basename is set, always returns True (no filtering).
        Strips trailing extension suffixes (e.g. '_exr') from directory names
        before comparison.
        """
        if not self._expected_basename:
            return True
        clean_name = self._strip_extension_suffix(entry_name)
        tokens = derive_source_tokens(clean_name, self._task_tokens, self._date_format)
        return tokens["source_basename"] == self._expected_basename

    @staticmethod
    def _compile_version_pattern(pattern: str, date_format: str = "") -> re.Pattern:
        """
        Compile the version pattern into a regex.

        Supports three token types:
        - {version}: matches \\d+ (version number)
        - {date}: matches \\d{6} or \\d{8} (date string)
        - Raw regex: used directly
        """
        if "{version}" in pattern or "{date}" in pattern:
            regex_str = re.escape(pattern)
            if r"\{version\}" in regex_str:
                regex_str = regex_str.replace(r"\{version\}", r"(\d+)")
            if r"\{date\}" in regex_str:
                if date_format in ("DDMMYYYY", "YYYYMMDD"):
                    regex_str = regex_str.replace(r"\{date\}", r"(\d{8})")
                else:
                    regex_str = regex_str.replace(r"\{date\}", r"(\d{6})")
            return re.compile(regex_str)
        else:
            return re.compile(pattern)

    def scan(self) -> list[VersionInfo]:
        """
        Scan the source directory and return all detected versions,
        sorted by version number (ascending).

        Timecode extraction is lazy - not performed during scan.
        Uses os.scandir for faster directory listing — DirEntry caches
        is_dir/is_file from the OS listing, avoiding per-entry stat() over SMB.
        """
        source_path = Path(self.source.source_dir)
        if not source_path.exists():
            logger.warning(f"Source directory does not exist: {source_path}")
            return []

        versions = []
        # Collect flat versioned files for grouping by (version_number, date)
        versioned_files: dict[tuple, dict] = {}  # (ver_num, date_sortable) -> {info, files}

        # Use os.scandir instead of Path.iterdir to avoid per-entry stat calls
        raw_entries = []
        try:
            with os.scandir(source_path) as it:
                for entry in it:
                    raw_entries.append(entry)
        except (PermissionError, OSError):
            return []
        raw_entries.sort(key=lambda e: e.name)

        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)

        for entry in raw_entries:
            if entry.is_dir(follow_symlinks=False):
                if not self._matches_basename(entry.name):
                    continue
                version_info = self._scan_version_folder(Path(entry.path))
                if version_info:
                    versions.append(version_info)
            elif entry.is_file(follow_symlinks=False):
                if not self._matches_basename(entry.name):
                    continue
                # Group flat files by version+date to detect frame sequences
                name = entry.name
                dot_idx = name.rfind(".")
                if dot_idx < 0 or name[dot_idx:].lower() not in valid_extensions:
                    continue
                result = self._extract_version(name)
                if result is None:
                    continue
                ver_str, ver_num, date_str, date_sortable = result
                group_key = (ver_num, date_sortable)
                if group_key not in versioned_files:
                    versioned_files[group_key] = {
                        "ver_str": ver_str, "ver_num": ver_num,
                        "date_str": date_str, "date_sortable": date_sortable,
                        "files": [], "entries": [],
                    }
                versioned_files[group_key]["files"].append(Path(entry.path))
                versioned_files[group_key]["entries"].append(entry)

        # Process grouped flat files — use cached DirEntry.stat() where available
        for group_key, group in versioned_files.items():
            files = group["files"]
            entries = group.get("entries", [])
            if len(files) == 1:
                # Single file — no frame sequence
                try:
                    file_size = entries[0].stat(follow_symlinks=False).st_size if entries else files[0].stat().st_size
                except OSError:
                    file_size = 0
                versions.append(VersionInfo(
                    version_string=group["ver_str"],
                    version_number=group["ver_num"],
                    source_path=str(files[0]),
                    frame_range=None,
                    frame_count=1,
                    file_count=1,
                    total_size_bytes=file_size,
                    start_timecode=None,
                    date_string=group["date_str"],
                    date_sortable=group["date_sortable"],
                ))
            else:
                # Multiple files — detect frame sequences
                frame_range, frame_count, sub_sequences = self._detect_frame_range(files)
                total_size = 0
                for e in entries:
                    try:
                        total_size += e.stat(follow_symlinks=False).st_size
                    except OSError:
                        pass
                versions.append(VersionInfo(
                    version_string=group["ver_str"],
                    version_number=group["ver_num"],
                    source_path=str(files[0].parent),
                    frame_range=frame_range,
                    frame_count=frame_count,
                    sub_sequences=sub_sequences,
                    file_count=len(files),
                    total_size_bytes=total_size,
                    start_timecode=None,
                    date_string=group["date_str"],
                    date_sortable=group["date_sortable"],
                ))

        versions.sort(key=lambda v: (v.date_sortable, v.version_number))
        return versions

    def _extract_version(self, name: str) -> Optional[tuple[str, int, Optional[str], int]]:
        """
        Extract version and/or date info from a name.

        Returns (version_string, version_number, date_string, date_sortable) or None.
        - date_string: raw date digits from filename, or None
        - date_sortable: YYYYMMDD int for sorting (0 when no date)
        """
        match = self._version_regex.search(name)
        if not match:
            return None

        pattern = self.source.version_pattern
        has_version = "{version}" in pattern
        has_date = "{date}" in pattern

        groups = match.groups()

        if has_version and has_date:
            # Both tokens: determine capture group order from template positions
            date_pos = pattern.index("{date}")
            ver_pos = pattern.index("{version}")
            if date_pos < ver_pos:
                date_str, ver_str = groups[0], groups[1]
            else:
                ver_str, date_str = groups[0], groups[1]
            version_num = int(ver_str)
            version_string = f"v{version_num:03d}"
            date_sortable = parse_date_to_sortable(date_str, self._date_format)
            return version_string, version_num, date_str, date_sortable

        elif has_date and not has_version:
            # Date-only: date IS the version
            date_str = groups[0]
            date_sortable = parse_date_to_sortable(date_str, self._date_format)
            display = format_date_display(date_str, self._date_format)
            return display, 0, date_str, date_sortable

        else:
            # Version-only or raw regex (existing behavior)
            version_num = int(match.group(1))
            version_string = f"v{version_num:03d}"
            return version_string, version_num, None, 0

    def _scan_version_folder(self, folder: Path) -> Optional[VersionInfo]:
        """Scan a version folder for file sequences or single files."""
        result = self._extract_version(folder.name)
        if result is None:
            return None

        version_str, version_num, date_str, date_sortable = result

        # Collect files matching our extensions using os.scandir
        files, total_size = self._collect_files_with_stats(folder)
        if not files:
            logger.debug(f"No matching files in version folder: {folder}")
            return None

        frame_range, frame_count, sub_sequences = self._detect_frame_range(files)

        return VersionInfo(
            version_string=version_str,
            version_number=version_num,
            source_path=str(folder),
            frame_range=frame_range,
            frame_count=frame_count,
            sub_sequences=sub_sequences,
            file_count=len(files),
            total_size_bytes=total_size,
            start_timecode=None,  # Lazy: extracted on demand via timecode module
            date_string=date_str,
            date_sortable=date_sortable,
        )

    def _scan_version_file(self, filepath: Path) -> Optional[VersionInfo]:
        """Scan a single versioned file (e.g. a .mov or .mxf)."""
        if filepath.suffix.lower() not in self.source.file_extensions:
            return None

        result = self._extract_version(filepath.name)
        if result is None:
            return None

        version_str, version_num, date_str, date_sortable = result

        return VersionInfo(
            version_string=version_str,
            version_number=version_num,
            source_path=str(filepath),
            frame_range=None,
            frame_count=1,
            file_count=1,
            total_size_bytes=filepath.stat().st_size,
            start_timecode=None,  # Lazy: extracted on demand via timecode module
            date_string=date_str,
            date_sortable=date_sortable,
        )

    def _collect_files(self, folder: Path) -> list[Path]:
        """Collect all files in a folder matching the configured extensions.

        Uses os.scandir for faster directory iteration.
        """
        files, _ = self._collect_files_with_stats(folder)
        return files

    def _collect_files_with_stats(self, folder: Path) -> tuple[list[Path], int]:
        """Collect files and total size in a single os.scandir pass.

        Uses DirEntry.stat() which on Windows leverages cached stat data
        from FindFirstFile/FindNextFile — no extra round-trips over SMB.

        Returns (sorted_file_list, total_size_bytes).
        """
        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)
        files = []
        total_size = 0
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
                                try:
                                    total_size += entry.stat(follow_symlinks=False).st_size
                                except OSError:
                                    pass
        except PermissionError:
            pass
        files.sort()
        return files, total_size

    def _detect_frame_range(self, files: list[Path]) -> tuple[Optional[str], int, list[dict]]:
        """
        Detect frame range from a list of files, grouping by sequence prefix.
        Returns (primary_range_string, primary_frame_count, sub_sequences_list).
        The sub_sequences_list is empty when there is only one sequence group.
        """
        if not files:
            return None, 0, []

        if len(files) == 1:
            return None, 1, []

        groups = _group_files_by_sequence(files)

        # Single group (or no frame-number files): fast path
        if len(groups) <= 1:
            group_files = next(iter(groups.values()))
            range_str, count = _detect_frame_range_for_group(group_files)
            return range_str, count, []

        # Multiple groups: compute per-group ranges
        group_info = []
        for prefix, group_files in sorted(groups.items()):
            range_str, count = _detect_frame_range_for_group(group_files)
            display_name = prefix.rstrip("._") if prefix else "(non-sequence)"
            group_info.append({
                "name": display_name,
                "prefix": prefix,
                "file_count": len(group_files),
                "frame_range": range_str,
                "frame_count": count,
            })

        # Select primary: largest group by file_count
        primary = max(group_info, key=lambda g: g["file_count"])
        sub_sequences = [g for g in group_info if g is not primary]

        return primary["frame_range"], primary["frame_count"], sub_sequences

    def get_latest_version(self) -> Optional[VersionInfo]:
        """Return the highest version number found."""
        versions = self.scan()
        return versions[-1] if versions else None


# ---------------------------------------------------------------------------
# Standalone helpers for manual version import
# ---------------------------------------------------------------------------

# Frame pattern for detecting sequence siblings
_FRAME_RE = re.compile(r"[._](\d+)\.\w+$")


def _detect_padding(digit_str: str) -> int:
    """Return the padding width for a frame number string.

    If the string has leading zeros, padding = len(digit_str).
    Otherwise padding = 0 (unpadded).
    """
    if digit_str.startswith("0") and len(digit_str) > 1:
        return len(digit_str)
    return 0


def _group_files_by_sequence(
    files: list[Path],
    frame_re: re.Pattern = None,
) -> dict[str, list[Path]]:
    """Group files by their sequence prefix (everything before the frame number).

    Files that don't match the frame pattern are placed in a "" (empty string) group.

    Example:
        sh490_comp_v034.1001.exr       -> prefix "sh490_comp_v034."
        sh490_comp_v034_Alpha.1001.exr -> prefix "sh490_comp_v034_Alpha."
    """
    if frame_re is None:
        frame_re = _FRAME_RE
    groups: dict[str, list[Path]] = {}
    for f in files:
        match = frame_re.search(f.name)
        if match:
            # Include everything up to and including the separator before frame digits
            prefix = f.name[:match.start() + 1]
        else:
            prefix = ""
        groups.setdefault(prefix, []).append(f)
    return groups


def _detect_frame_range_for_group(
    files: list[Path],
    frame_re: re.Pattern = None,
) -> tuple[Optional[str], int]:
    """Compute frame range for a single homogeneous sequence group.

    Returns (range_string, frame_count).
    Preserves padding in the range display (e.g. "00991-01120").
    """
    if frame_re is None:
        frame_re = _FRAME_RE

    if not files:
        return None, 0
    if len(files) == 1:
        return None, 1

    frames = []
    max_digit_width = 0
    has_leading_zeros = False
    for f in files:
        match = frame_re.search(f.name)
        if match:
            digit_str = match.group(1)
            max_digit_width = max(max_digit_width, len(digit_str))
            if digit_str.startswith("0") and len(digit_str) > 1:
                has_leading_zeros = True
            frames.append(int(digit_str))

    if not frames:
        return None, len(files)

    # Determine padding: use max digit width if any frame has leading zeros
    padding = max_digit_width if has_leading_zeros else 0

    frames.sort()
    first, last = frames[0], frames[-1]
    expected = last - first + 1
    actual = len(frames)

    if padding > 0:
        range_str = f"{str(first).zfill(padding)}-{str(last).zfill(padding)}"
    else:
        range_str = f"{first}-{last}"
    if actual != expected:
        # Compute which frames are missing for diagnostic detail
        frame_set = set(frames)
        missing = sorted(set(range(first, last + 1)) - frame_set)
        missing_str = _format_frame_gaps(missing, padding, max_items=10)
        range_str += f" ({actual}/{expected} frames, gaps detected"
        if missing_str:
            range_str += f"; missing: {missing_str}"
        range_str += ")"

    return range_str, actual


def _format_frame_gaps(missing: list[int], padding: int = 0, max_items: int = 10) -> str:
    """Format a list of missing frame numbers as compact ranges.

    Examples: "1005-1008, 1020" or "01005-01008, 01020" with padding.
    Limits output to *max_items* ranges to avoid huge strings.
    """
    if not missing:
        return ""

    def _fmt(n: int) -> str:
        return str(n).zfill(padding) if padding else str(n)

    ranges = []
    start = missing[0]
    end = missing[0]
    for n in missing[1:]:
        if n == end + 1:
            end = n
        else:
            ranges.append((start, end))
            start = end = n
    ranges.append((start, end))

    parts = []
    for s, e in ranges[:max_items]:
        if s == e:
            parts.append(_fmt(s))
        else:
            parts.append(f"{_fmt(s)}-{_fmt(e)}")

    result = ", ".join(parts)
    if len(ranges) > max_items:
        result += f", ... ({len(ranges) - max_items} more)"
    return result


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

    # Detect frame range (grouped by sequence prefix to avoid false gaps)
    groups = _group_files_by_sequence(files)
    if len(groups) <= 1:
        group_files = next(iter(groups.values()))
        range_str, count = _detect_frame_range_for_group(group_files)
    else:
        # Multiple sequences: report primary (largest group)
        best_range, best_count, best_size = None, 0, 0
        for prefix, group_files in groups.items():
            r, c = _detect_frame_range_for_group(group_files)
            if len(group_files) > best_size:
                best_range, best_count, best_size = r, c, len(group_files)
        range_str, count = best_range, best_count

    return files, range_str, count


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
