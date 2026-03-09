"""
Discovery Scanner - walks a directory tree and reports versioned content.

Finds directories or files that contain version patterns (v01, v002, v0051, etc.)
and reports what it found. Does NOT modify any project files — report only.
"""

import os
import re
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Callable

from .models import DiscoveryResult, VersionInfo

logger = logging.getLogger(__name__)

# Matches common version patterns: _v01, _v002, _v0051, .v3, -v10, _V004, etc.
VERSION_RE = re.compile(r"[._\-]v(\d+)", re.IGNORECASE)

# Frame number in filenames: name.1001.exr, name_1001.exr
FRAME_RE = re.compile(r"[._](\d{3,8})\.\w+$")

# Date patterns: 6-digit DDMMYY/YYMMDD or 8-digit, bounded by dividers or string edges
DATE_RE = re.compile(r"(?:^|(?<=[._\-]))(\d{6}|\d{8})(?=[._\-]|$)")

# Common VFX/media extensions
MEDIA_EXTENSIONS = {
    ".exr", ".dpx", ".tiff", ".tif", ".png", ".jpg", ".jpeg",
    ".mov", ".mxf", ".mp4", ".avi", ".wav", ".aiff",
}


def _estimate_dir_count(root: Path, max_depth: int) -> int:
    """Quick estimate of directory count by scanning the first 2 depth levels."""
    count = 0
    try:
        for entry in os.scandir(root):
            if entry.is_dir() and not entry.name.startswith("."):
                count += 1
                if max_depth > 1:
                    try:
                        for sub in os.scandir(entry.path):
                            if sub.is_dir() and not sub.name.startswith("."):
                                count += 1
                    except (PermissionError, OSError):
                        pass
    except (PermissionError, OSError):
        pass
    return max(count, 1)


class _ProgressTracker:
    """Thread-safe progress counter with throttled callback."""

    def __init__(self, callback: Optional[Callable], estimated_total: int):
        self._callback = callback
        self.estimated_total = estimated_total
        self._count = 0
        self._lock = threading.Lock()
        self._last_callback_time = 0.0

    def increment(self, current_path: str):
        if self._callback is None:
            return
        with self._lock:
            self._count += 1
            now = time.monotonic()
            # Throttle callbacks to max once per 100ms
            if now - self._last_callback_time < 0.1:
                return
            self._last_callback_time = now
            count = self._count
        self._callback(current_path, count, self.estimated_total)

    @property
    def count(self) -> int:
        return self._count


def discover(
    root_dir: str,
    max_depth: int = 4,
    extensions: Optional[list] = None,
    whitelist: Optional[list] = None,
    blacklist: Optional[list] = None,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> list[DiscoveryResult]:
    """Scan a directory tree for versioned content.

    Walks directories up to max_depth levels deep. For each directory that
    contains versioned subdirectories or versioned files, creates a
    DiscoveryResult with the detected versions.

    Args:
        root_dir: Root directory to start scanning from.
        max_depth: Maximum depth of directory traversal.
        extensions: File extensions to look for. None means use MEDIA_EXTENSIONS.
        whitelist: If provided, only keep results whose name or relative path
                   contains at least one of these keywords (case-insensitive).
        blacklist: If provided, drop results whose name or relative path
                   contains any of these keywords (case-insensitive).
        progress_callback: Optional callback(current_path, dirs_scanned, estimated_total)
                          called periodically during scan for progress reporting.

    Returns:
        List of DiscoveryResult, one per location that has versioned content.
    """
    root = Path(root_dir).resolve()
    if not root.exists():
        logger.warning(f"Directory does not exist: {root}")
        return []

    valid_extensions = set(extensions) if extensions else MEDIA_EXTENSIONS

    # Phase 1: Quick pre-count for progress estimation
    estimated = 0
    if progress_callback:
        progress_callback("Estimating scan size...", 0, 0)
        estimated = _estimate_dir_count(root, max_depth)

    tracker = _ProgressTracker(progress_callback, estimated)

    # Phase 2: Parallel walk of top-level subdirectories
    results = []
    # First process the root itself (depth 0)
    _walk_for_versions(root, root, 0, max_depth, valid_extensions, results,
                       visited={root}, progress=tracker)

    # Apply whitelist/blacklist filtering
    if whitelist or blacklist:
        results = _apply_filters(results, root, whitelist, blacklist)

    results.sort(key=lambda r: r.path)
    return results


def _apply_filters(
    results: list,
    root: Path,
    whitelist: Optional[list],
    blacklist: Optional[list],
) -> list:
    """Filter discovery results by whitelist and blacklist keywords."""
    wl = [kw.lower() for kw in whitelist] if whitelist else []
    bl = [kw.lower() for kw in blacklist] if blacklist else []

    filtered = []
    for result in results:
        # Build search text from name, relative path, and sample filename
        try:
            rel_path = str(Path(result.path).relative_to(root))
        except ValueError:
            rel_path = result.path
        parts = [result.name, rel_path]
        if result.sample_filename:
            parts.append(result.sample_filename)
        search_text = " ".join(parts).lower()

        # Blacklist: skip if any keyword matches
        if bl and any(kw in search_text for kw in bl):
            continue

        # Whitelist: keep only if at least one keyword matches
        if wl and not any(kw in search_text for kw in wl):
            continue

        filtered.append(result)

    return filtered


def _scan_version_dir(vdir: Path, ver_num: int, extensions: set) -> VersionInfo:
    """Scan a single versioned directory for its metadata.

    Runs file collection, frame detection, and size computation.
    Timecode extraction is deferred (lazy) - not done during discovery.
    """
    ver_str = f"v{ver_num:03d}"
    files = _collect_media_files(vdir, extensions)
    frame_range, frame_count, sub_sequences = _detect_frame_range(files)

    total_size = 0
    for f in files:
        try:
            total_size += f.stat().st_size
        except OSError:
            pass

    return VersionInfo(
        version_string=ver_str,
        version_number=ver_num,
        source_path=str(vdir),
        frame_range=frame_range,
        frame_count=frame_count,
        sub_sequences=sub_sequences,
        file_count=len(files),
        total_size_bytes=total_size,
        start_timecode=None,  # Lazy: extracted on demand, not during discovery
    )


def _walk_for_versions(
    current: Path,
    root: Path,
    depth: int,
    max_depth: int,
    extensions: set,
    results: list,
    visited: set = None,
    progress: _ProgressTracker = None,
):
    """Recursively walk directories looking for versioned content."""
    if visited is None:
        visited = set()
    if depth > max_depth:
        return

    if progress is not None:
        progress.increment(str(current))

    try:
        entries = sorted(current.iterdir())
    except PermissionError:
        logger.debug(f"Permission denied: {current}")
        return

    versioned_dirs = []
    versioned_files = []
    dated_dirs = []       # dirs with date but no version
    dated_files = []      # files with date but no version
    subdirs = []

    for entry in entries:
        if entry.name.startswith("."):
            continue

        if entry.is_dir():
            ver_match = VERSION_RE.search(entry.name)
            if ver_match:
                versioned_dirs.append((entry, ver_match))
            else:
                date_match = DATE_RE.search(entry.name)
                if date_match and _is_plausible_date(date_match.group(1)):
                    dated_dirs.append((entry, date_match))
                else:
                    subdirs.append(entry)
        elif entry.is_file():
            if entry.suffix.lower() in extensions:
                ver_match = VERSION_RE.search(entry.stem)
                if ver_match:
                    versioned_files.append((entry, ver_match))
                else:
                    date_match = DATE_RE.search(entry.stem)
                    if date_match and _is_plausible_date(date_match.group(1)):
                        dated_files.append((entry, date_match))

    # If this directory contains versioned subdirectories, report it
    if versioned_dirs:
        found_extensions = set()

        # Check if versioned dirs also contain date patterns
        first_dir_name = versioned_dirs[0][0].name
        first_ver_match = versioned_dirs[0][1]
        first_date_match = DATE_RE.search(first_dir_name)
        if first_date_match and not _is_plausible_date(first_date_match.group(1)):
            first_date_match = None

        # Parallel scan of version directories using ThreadPoolExecutor
        versions = []
        worker_count = min(8, len(versioned_dirs))
        if worker_count > 1:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_entry = {}
                for vdir, match in versioned_dirs:
                    ver_num = int(match.group(1))
                    future = executor.submit(_scan_version_dir, vdir, ver_num, extensions)
                    future_to_entry[future] = (vdir, match)

                for future in as_completed(future_to_entry):
                    try:
                        vi = future.result()
                        # Detect date in this dir name
                        vdir, vmatch = future_to_entry[future]
                        _populate_date_on_vi(vi, vdir.name)
                        versions.append(vi)
                        for f in _collect_media_files(vdir, extensions):
                            found_extensions.add(f.suffix.lower())
                    except Exception as e:
                        vdir = future_to_entry[future][0]
                        logger.debug(f"Error scanning {vdir}: {e}")
        else:
            # Single version dir, no need for thread overhead
            for vdir, match in versioned_dirs:
                ver_num = int(match.group(1))
                vi = _scan_version_dir(vdir, ver_num, extensions)
                _populate_date_on_vi(vi, vdir.name)
                versions.append(vi)
                for f in _collect_media_files(vdir, extensions):
                    found_extensions.add(f.suffix.lower())

        versions.sort(key=lambda v: (v.date_sortable, v.version_number))

        # Suggest a version pattern (with possible date token)
        suggested_pattern = _suggest_pattern(
            first_dir_name, ver_match=first_ver_match, date_match=first_date_match)

        # Detect date format if date was found
        suggested_date_fmt = ""
        if first_date_match:
            suggested_date_fmt = _detect_date_format(first_date_match.group(1))

        # Grab a representative filename from the first non-empty version folder
        sample_filename = ""
        for vdir, _match in versioned_dirs:
            sample_files = _collect_media_files(vdir, extensions)
            if sample_files:
                sample_filename = sample_files[0].name
                break

        results.append(DiscoveryResult(
            path=str(current),
            name=current.name,
            versions_found=versions,
            suggested_pattern=suggested_pattern,
            suggested_extensions=sorted(found_extensions),
            sample_filename=sample_filename,
            suggested_date_format=suggested_date_fmt,
        ))

    # If this directory contains versioned single files, report it
    if versioned_files and not versioned_dirs:
        versions = []
        found_extensions = set()
        seen_versions = {}

        first_file_stem = versioned_files[0][0].stem
        first_ver_match = versioned_files[0][1]
        first_date_match = DATE_RE.search(first_file_stem)
        if first_date_match and not _is_plausible_date(first_date_match.group(1)):
            first_date_match = None

        for vfile, match in versioned_files:
            ver_num = int(match.group(1))
            ver_str = f"v{ver_num:03d}"
            found_extensions.add(vfile.suffix.lower())

            if ver_num in seen_versions:
                # Multiple files for same version - increment count
                try:
                    seen_versions[ver_num].file_count += 1
                    seen_versions[ver_num].total_size_bytes += vfile.stat().st_size
                except OSError:
                    pass
            else:
                try:
                    file_size = vfile.stat().st_size
                except OSError:
                    file_size = 0
                vi = VersionInfo(
                    version_string=ver_str,
                    version_number=ver_num,
                    source_path=str(vfile),
                    file_count=1,
                    total_size_bytes=file_size,
                    start_timecode=None,  # Lazy: extracted on demand
                )
                _populate_date_on_vi(vi, vfile.stem)
                seen_versions[ver_num] = vi
                versions.append(vi)

        versions.sort(key=lambda v: (v.date_sortable, v.version_number))
        suggested_pattern = _suggest_pattern(
            first_file_stem, ver_match=first_ver_match, date_match=first_date_match)

        suggested_date_fmt = ""
        if first_date_match:
            suggested_date_fmt = _detect_date_format(first_date_match.group(1))

        results.append(DiscoveryResult(
            path=str(current),
            name=current.name,
            versions_found=versions,
            suggested_pattern=suggested_pattern,
            suggested_extensions=sorted(found_extensions),
            sample_filename=versioned_files[0][0].name,
            suggested_date_format=suggested_date_fmt,
        ))

    # If this directory contains date-only subdirectories (no version pattern), report it
    if dated_dirs and not versioned_dirs:
        found_extensions = set()
        versions = []

        first_dir_name = dated_dirs[0][0].name
        first_date_match = dated_dirs[0][1]
        guessed_fmt = _detect_date_format(first_date_match.group(1))

        for ddir, dmatch in dated_dirs:
            date_str = dmatch.group(1)
            fmt = _detect_date_format(date_str)
            from .task_tokens import parse_date_to_sortable, format_date_display
            date_sortable = parse_date_to_sortable(date_str, fmt)
            display = format_date_display(date_str, fmt)

            files = _collect_media_files(ddir, extensions)
            frame_range, frame_count, sub_sequences = _detect_frame_range(files)
            total_size = 0
            for f in files:
                try:
                    total_size += f.stat().st_size
                except OSError:
                    pass
                found_extensions.add(f.suffix.lower())

            if files:
                vi = VersionInfo(
                    version_string=display,
                    version_number=0,
                    source_path=str(ddir),
                    frame_range=frame_range,
                    frame_count=frame_count,
                    sub_sequences=sub_sequences,
                    file_count=len(files),
                    total_size_bytes=total_size,
                    start_timecode=None,
                    date_string=date_str,
                    date_sortable=date_sortable,
                )
                versions.append(vi)

        if versions:
            versions.sort(key=lambda v: v.date_sortable)
            suggested_pattern = _suggest_pattern(
                first_dir_name, ver_match=None, date_match=first_date_match)

            sample_filename = ""
            for ddir_entry, _ in dated_dirs:
                sample_files = _collect_media_files(ddir_entry, extensions)
                if sample_files:
                    sample_filename = sample_files[0].name
                    break

            results.append(DiscoveryResult(
                path=str(current),
                name=current.name,
                versions_found=versions,
                suggested_pattern=suggested_pattern,
                suggested_extensions=sorted(found_extensions),
                sample_filename=sample_filename,
                suggested_date_format=guessed_fmt,
            ))

    # If this directory contains date-only single files (no version pattern), report it
    if dated_files and not versioned_files and not versioned_dirs and not dated_dirs:
        from .task_tokens import parse_date_to_sortable, format_date_display
        versions = []
        found_extensions = set()
        seen_dates = {}

        first_file_stem = dated_files[0][0].stem
        first_date_match = dated_files[0][1]
        guessed_fmt = _detect_date_format(first_date_match.group(1))

        for dfile, dmatch in dated_files:
            date_str = dmatch.group(1)
            fmt = _detect_date_format(date_str)
            date_sortable = parse_date_to_sortable(date_str, fmt)
            display = format_date_display(date_str, fmt)
            found_extensions.add(dfile.suffix.lower())

            if date_str in seen_dates:
                try:
                    seen_dates[date_str].file_count += 1
                    seen_dates[date_str].total_size_bytes += dfile.stat().st_size
                except OSError:
                    pass
            else:
                try:
                    file_size = dfile.stat().st_size
                except OSError:
                    file_size = 0
                vi = VersionInfo(
                    version_string=display,
                    version_number=0,
                    source_path=str(dfile),
                    file_count=1,
                    total_size_bytes=file_size,
                    start_timecode=None,
                    date_string=date_str,
                    date_sortable=date_sortable,
                )
                seen_dates[date_str] = vi
                versions.append(vi)

        if versions:
            versions.sort(key=lambda v: v.date_sortable)
            suggested_pattern = _suggest_pattern(
                first_file_stem, ver_match=None, date_match=first_date_match)

            results.append(DiscoveryResult(
                path=str(current),
                name=current.name,
                versions_found=versions,
                suggested_pattern=suggested_pattern,
                suggested_extensions=sorted(found_extensions),
                sample_filename=dated_files[0][0].name,
                suggested_date_format=guessed_fmt,
            ))

    # Recurse into non-versioned subdirectories (with symlink loop protection)
    for subdir in subdirs:
        try:
            real_path = subdir.resolve()
        except OSError:
            continue
        if real_path in visited:
            logger.debug(f"Skipping already-visited path (symlink loop?): {subdir}")
            continue
        visited.add(real_path)
        _walk_for_versions(subdir, root, depth + 1, max_depth, extensions, results, visited, progress)


def _populate_date_on_vi(vi: VersionInfo, name: str):
    """If name contains a date pattern, populate date fields on a VersionInfo."""
    date_match = DATE_RE.search(name)
    if not date_match:
        return
    digits = date_match.group(1)
    if not _is_plausible_date(digits):
        return
    from .task_tokens import parse_date_to_sortable
    fmt = _detect_date_format(digits)
    vi.date_string = digits
    vi.date_sortable = parse_date_to_sortable(digits, fmt)


def _collect_media_files(folder: Path, extensions: set) -> list[Path]:
    """Collect media files in a folder (non-recursive) using os.scandir for speed."""
    files = []
    try:
        with os.scandir(folder) as it:
            for entry in it:
                if entry.is_file(follow_symlinks=False):
                    name = entry.name
                    dot_idx = name.rfind(".")
                    if dot_idx >= 0:
                        suffix = name[dot_idx:].lower()
                        if suffix in extensions:
                            files.append(Path(entry.path))
    except PermissionError:
        pass
    files.sort()
    return files


def _detect_frame_range(files: list[Path]) -> tuple[Optional[str], int, list]:
    """Detect frame range from a list of files, grouping by sequence prefix.

    Returns (primary_range_string, primary_frame_count, sub_sequences_list).
    """
    from .scanner import _group_files_by_sequence, _detect_frame_range_for_group

    if not files:
        return None, 0, []
    if len(files) == 1:
        return None, 1, []

    groups = _group_files_by_sequence(files, FRAME_RE)

    if len(groups) <= 1:
        group_files = next(iter(groups.values()))
        range_str, count = _detect_frame_range_for_group(group_files, FRAME_RE)
        return range_str, count, []

    # Multiple groups: compute per-group ranges
    group_info = []
    for prefix, group_files in sorted(groups.items()):
        range_str, count = _detect_frame_range_for_group(group_files, FRAME_RE)
        display_name = prefix.rstrip("._") if prefix else "(non-sequence)"
        group_info.append({
            "name": display_name,
            "prefix": prefix,
            "file_count": len(group_files),
            "frame_range": range_str,
            "frame_count": count,
        })

    primary = max(group_info, key=lambda g: g["file_count"])
    sub_sequences = [g for g in group_info if g is not primary]

    return primary["frame_range"], primary["frame_count"], sub_sequences


def _detect_date_format(date_str: str) -> str:
    """Guess date format from a 6- or 8-digit string.

    Heuristic for 6-digit:
    - If first two digits > 31, likely YYMMDD (e.g. 240226).
    - If first two digits <= 31 and middle two <= 12, likely DDMMYY (e.g. 260224).

    Heuristic for 8-digit:
    - If first four digits >= 1900, likely YYYYMMDD (e.g. 20240226).
    - Otherwise likely DDMMYYYY (e.g. 26022024).
    """
    if len(date_str) == 8:
        first_four = int(date_str[:4])
        if 1900 <= first_four <= 2099:
            return "YYYYMMDD"
        return "DDMMYYYY"

    if len(date_str) == 6:
        first_two = int(date_str[:2])
        mid_two = int(date_str[2:4])
        if first_two > 31:
            return "YYMMDD"
        if first_two <= 31 and mid_two <= 12:
            return "DDMMYY"
        # Ambiguous — default
        return "YYMMDD"

    return ""


def _is_plausible_date(digits: str) -> bool:
    """Quick check if a digit string could be a date (without knowing format).

    Tries all common formats and returns True if any produces a valid date.
    """
    from .task_tokens import validate_date_string
    if len(digits) == 6:
        return (validate_date_string(digits, "DDMMYY") or
                validate_date_string(digits, "YYMMDD"))
    elif len(digits) == 8:
        return (validate_date_string(digits, "YYYYMMDD") or
                validate_date_string(digits, "DDMMYYYY"))
    return False


def _suggest_pattern(name: str, ver_match: re.Match = None,
                     date_match: re.Match = None) -> str:
    """Suggest a version pattern from matched patterns.

    Cases:
    - Version only: "_v{version}" (existing behavior)
    - Date + version: includes both {date} and {version} tokens
    - Date only: "_{date}" or "{date}_" depending on position
    """
    if ver_match and date_match:
        # Both present — build pattern with both tokens
        ver_prefix = name[ver_match.start():ver_match.start(1)]
        date_start = date_match.start(1)
        # Find the divider before the date (if any)
        if date_start > 0 and name[date_start - 1] in "_.-":
            date_prefix = name[date_start - 1]
        else:
            date_prefix = ""

        if date_match.start() < ver_match.start():
            # Date comes before version: {date}_..._v{version}
            return date_prefix + "{date}" + ver_prefix + "{version}"
        else:
            # Version comes before date
            return ver_prefix + "{version}" + date_prefix + "{date}"

    elif date_match and not ver_match:
        # Date only
        date_start = date_match.start(1)
        if date_start > 0 and name[date_start - 1] in "_.-":
            return name[date_start - 1] + "{date}"
        return "{date}"

    elif ver_match:
        # Version only (existing behavior)
        start = ver_match.start()
        prefix_before_v = name[start:ver_match.start(1)]
        return prefix_before_v + "{version}"

    return "_v{version}"


def format_discovery_report(results: list[DiscoveryResult], root_dir: str = "") -> str:
    """Format discovery results as a human-readable report."""
    if not results:
        return "No versioned content found."

    lines = []
    root = Path(root_dir).resolve() if root_dir else None

    for result in results:
        display_path = result.path
        if root:
            try:
                display_path = str(Path(result.path).relative_to(root))
            except ValueError:
                pass

        lines.append(f"\n  {display_path}/")
        lines.append(f"    Name: {result.name}")
        lines.append(f"    Versions: {len(result.versions_found)}")
        lines.append(f"    Pattern: {result.suggested_pattern}")
        if result.suggested_extensions:
            lines.append(f"    Extensions: {' '.join(result.suggested_extensions)}")

        for v in result.versions_found:
            size = v.total_size_human
            frames = f"  frames: {v.frame_range}" if v.frame_range else ""
            tc = f"  TC: {v.start_timecode}" if v.start_timecode else ""
            lines.append(f"      {v.version_string}  |  {v.file_count} files  |  {size}{frames}{tc}")

    total_versions = sum(len(r.versions_found) for r in results)
    header = f"Found {len(results)} versioned location(s) with {total_versions} total version(s):"
    return header + "\n" + "\n".join(lines)
