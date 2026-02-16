"""
Discovery Scanner - walks a directory tree and reports versioned content.

Finds directories or files that contain version patterns (v01, v002, v0051, etc.)
and reports what it found. Does NOT modify any project files — report only.
"""

import os
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from .models import DiscoveryResult, VersionInfo

logger = logging.getLogger(__name__)

# Matches common version patterns: _v01, _v002, _v0051, .v3, -v10, _V004, etc.
VERSION_RE = re.compile(r"[._\-]v(\d+)", re.IGNORECASE)

# Frame number in filenames: name.1001.exr, name_1001.exr
FRAME_RE = re.compile(r"[._](\d{3,8})\.\w+$")

# Common VFX/media extensions
MEDIA_EXTENSIONS = {
    ".exr", ".dpx", ".tiff", ".tif", ".png", ".jpg", ".jpeg",
    ".mov", ".mxf", ".mp4", ".avi", ".wav", ".aiff",
}


def discover(
    root_dir: str,
    max_depth: int = 4,
    extensions: Optional[list] = None,
    whitelist: Optional[list] = None,
    blacklist: Optional[list] = None,
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

    Returns:
        List of DiscoveryResult, one per location that has versioned content.
    """
    root = Path(root_dir).resolve()
    if not root.exists():
        logger.warning(f"Directory does not exist: {root}")
        return []

    valid_extensions = set(extensions) if extensions else MEDIA_EXTENSIONS
    results = []

    _walk_for_versions(root, root, 0, max_depth, valid_extensions, results)

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
    frame_range, frame_count = _detect_frame_range(files)

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
):
    """Recursively walk directories looking for versioned content."""
    if depth > max_depth:
        return

    try:
        entries = sorted(current.iterdir())
    except PermissionError:
        logger.debug(f"Permission denied: {current}")
        return

    versioned_dirs = []
    versioned_files = []
    subdirs = []

    for entry in entries:
        if entry.name.startswith("."):
            continue

        if entry.is_dir():
            match = VERSION_RE.search(entry.name)
            if match:
                versioned_dirs.append((entry, match))
            else:
                subdirs.append(entry)
        elif entry.is_file():
            if entry.suffix.lower() in extensions:
                match = VERSION_RE.search(entry.stem)
                if match:
                    versioned_files.append((entry, match))

    # If this directory contains versioned subdirectories, report it
    if versioned_dirs:
        found_extensions = set()

        # Parallel scan of version directories using ThreadPoolExecutor
        versions = []
        worker_count = min(8, len(versioned_dirs))
        if worker_count > 1:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_match = {}
                for vdir, match in versioned_dirs:
                    ver_num = int(match.group(1))
                    future = executor.submit(_scan_version_dir, vdir, ver_num, extensions)
                    future_to_match[future] = vdir

                for future in as_completed(future_to_match):
                    try:
                        vi = future.result()
                        versions.append(vi)
                        # Collect extensions
                        vdir = future_to_match[future]
                        for f in _collect_media_files(vdir, extensions):
                            found_extensions.add(f.suffix.lower())
                    except Exception as e:
                        vdir = future_to_match[future]
                        logger.debug(f"Error scanning {vdir}: {e}")
        else:
            # Single version dir, no need for thread overhead
            for vdir, match in versioned_dirs:
                ver_num = int(match.group(1))
                vi = _scan_version_dir(vdir, ver_num, extensions)
                versions.append(vi)
                for f in _collect_media_files(vdir, extensions):
                    found_extensions.add(f.suffix.lower())

        versions.sort(key=lambda v: v.version_number)

        # Suggest a version pattern from the first versioned dir name
        suggested_pattern = _suggest_pattern(versioned_dirs[0][0].name, versioned_dirs[0][1])

        # Grab a representative filename from the first version folder
        sample_filename = ""
        first_vdir = versioned_dirs[0][0]
        sample_files = _collect_media_files(first_vdir, extensions)
        if sample_files:
            sample_filename = sample_files[0].name

        results.append(DiscoveryResult(
            path=str(current),
            name=current.name,
            versions_found=versions,
            suggested_pattern=suggested_pattern,
            suggested_extensions=sorted(found_extensions),
            sample_filename=sample_filename,
        ))

    # If this directory contains versioned single files, report it
    if versioned_files and not versioned_dirs:
        versions = []
        found_extensions = set()
        seen_versions = {}

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
                seen_versions[ver_num] = vi
                versions.append(vi)

        versions.sort(key=lambda v: v.version_number)
        suggested_pattern = _suggest_pattern(versioned_files[0][0].stem, versioned_files[0][1])

        results.append(DiscoveryResult(
            path=str(current),
            name=current.name,
            versions_found=versions,
            suggested_pattern=suggested_pattern,
            suggested_extensions=sorted(found_extensions),
            sample_filename=versioned_files[0][0].name,
        ))

    # Recurse into non-versioned subdirectories
    for subdir in subdirs:
        _walk_for_versions(subdir, root, depth + 1, max_depth, extensions, results)


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


def _detect_frame_range(files: list[Path]) -> tuple[Optional[str], int]:
    """Detect frame range from a list of files."""
    if not files:
        return None, 0
    if len(files) == 1:
        return None, 1

    frames = []
    for f in files:
        match = FRAME_RE.search(f.name)
        if match:
            frames.append(int(match.group(1)))

    if not frames:
        return None, len(files)

    frames.sort()
    first, last = frames[0], frames[-1]
    expected = last - first + 1
    actual = len(frames)

    range_str = f"{first}-{last}"
    if actual != expected:
        range_str += f" ({actual}/{expected} frames, gaps detected)"
    return range_str, actual


def _suggest_pattern(name: str, match: re.Match) -> str:
    """Suggest a version pattern from a matched name.

    e.g. "hero_comp_v003" -> "hero_comp_v{version}"
    e.g. "shot010_comp_v02" -> "_v{version}"
    """
    start = match.start()
    # Find where the version token starts (after the separator char)
    prefix_before_v = name[start:match.start(1)]
    return prefix_before_v + "{version}"


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
