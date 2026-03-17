"""
Promoter - copies, symlinks, or hardlinks a version to the "latest" target location.

This is the core operation: take a detected version and make it the
current "latest" that Resolve/Nuke is reading from.
"""

import os
import re
import shutil
import logging
import platform
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Callable

from .models import VersionInfo, WatchedSource, HistoryEntry
from .history import HistoryManager
from .hooks import run_pre_promote_hook, run_post_promote_hook, HookError
from .task_tokens import derive_source_tokens
from .config import _expand_group_token
from .fast_copy import smart_copy
from .scanner import _group_files_by_sequence

logger = logging.getLogger(__name__)

# Pre-compiled module-level regex patterns (avoid re-compiling per call)
_VERSION_RE = re.compile(r"[._\-]v\d+", re.IGNORECASE)
_DATE_RE = re.compile(r"(?:^|(?<=[._\-]))(\d{6}|\d{8})(?=[._\-]|$)")
_DOUBLE_DIVIDER_RE = re.compile(r"([_.\-]){2,}")
_FRAME_EXT_RE = re.compile(r"([._])(\d+)\.(\w+)$")

# Number of threads for parallel file copy operations — adapts to CPU count
_COPY_WORKERS = min(os.cpu_count() or 4, 8)


def _resolve_unc_safe(path: Path) -> Path:
    """Resolve a path without breaking UNC paths on Windows.

    Path.resolve() on Windows converts UNC paths (\\\\server\\share\\...)
    to extended-length syntax (\\\\?\\UNC\\server\\share\\...) which breaks
    symlink/hardlink creation over SMB. This resolves the path and then
    converts back to standard UNC format if needed.
    """
    resolved = path.resolve()
    if platform.system() != "Windows":
        return resolved
    s = str(resolved)
    # Fix \\?\UNC\server\share -> \\server\share
    if s.startswith("\\\\?\\UNC\\"):
        return Path("\\\\" + s[8:])
    # Fix \\?\C:\... -> C:\... (local extended-length paths)
    if s.startswith("\\\\?\\"):
        return Path(s[4:])
    return resolved


class PromotionError(Exception):
    """Raised when a promotion fails."""
    pass


def has_frame_gaps(version: VersionInfo) -> bool:
    """Return True if the version's frame_range indicates gaps."""
    return version.frame_range is not None and "gaps detected" in version.frame_range


class Promoter:
    """Handles promoting a version to the latest target."""

    def __init__(self, watched_source: WatchedSource, task_tokens: list = None, project_name: str = ""):
        self.source = watched_source
        self.task_tokens = task_tokens or []
        self.project_name = project_name

        if not watched_source.latest_target:
            raise PromotionError(
                f"Source '{watched_source.name}' has no latest target path configured."
            )

        self.history = HistoryManager(
            os.path.join(watched_source.latest_target, watched_source.history_filename)
        )
        # Cache valid extensions once — immutable for this Promoter's lifetime
        self._valid_extensions = frozenset(
            ext.lower() for ext in watched_source.file_extensions
        )
        # Cache derived tokens for file renaming
        self._rename_tokens = None
        self._cancelled = threading.Event()

    def cancel(self):
        """Signal that the current promotion should be aborted at the next checkpoint."""
        self._cancelled.set()

    def detect_obsolete_layers(self, version: VersionInfo) -> list[dict]:
        """Return layers present in the target but absent from the new version.

        A "layer" is a distinct sequence prefix (e.g. ``beauty.``, ``matte.``)
        as determined by :func:`_group_files_by_sequence`.  If the target
        directory contains layers that the incoming *version* does not provide,
        those layers are returned so the caller can ask the user what to do.

        Each returned dict has:
            ``"name"``    – human-readable display name
            ``"prefix"``  – raw prefix key for passing to *keep_layers*
            ``"file_count"`` – number of files belonging to this layer
        """
        target_dir = Path(self.source.latest_target)
        if not target_dir.exists():
            return []

        valid_extensions = self._valid_extensions

        # Layers currently in the target directory
        target_files = sorted(
            f for f in target_dir.iterdir()
            if f.is_file() and f.suffix.lower() in valid_extensions
        )
        if not target_files:
            return []
        target_groups = _group_files_by_sequence(target_files)
        target_layer_names = set(target_groups.keys())

        # Layers in the incoming version
        source_path = Path(version.source_path)
        if source_path.is_dir():
            source_files = sorted(
                f for f in source_path.iterdir()
                if f.is_file() and f.suffix.lower() in valid_extensions
            )
            if source_path == Path(self.source.source_dir):
                source_files = self._filter_version_files(source_files, version)
        else:
            source_files = [source_path] if source_path.is_file() else []

        if not source_files:
            return []
        # Remap source filenames to match target naming (strip version/date)
        # so that layer prefixes are comparable between source and target.
        remapped_source_files = [
            f.with_name(self._remap_filename(f.name))
            for f in source_files
        ]
        source_groups = _group_files_by_sequence(remapped_source_files)
        source_layer_names = set(source_groups.keys())

        obsolete_prefixes = target_layer_names - source_layer_names
        result = []
        for prefix in sorted(obsolete_prefixes):
            result.append({
                "name": prefix.rstrip("._") or "(non-sequence)",
                "prefix": prefix,
                "file_count": len(target_groups[prefix]),
            })
        return result

    def promote(
        self,
        version: VersionInfo,
        user: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        force: bool = False,
        pinned: bool = False,
        keep_layers: Optional[set[str]] = None,
    ) -> HistoryEntry:
        """
        Promote a version to be the current "latest".

        Args:
            version: The version to promote.
            user: Username to record. Defaults to OS login name.
            progress_callback: Optional callback(current_file, total_files, filename)
                               for UI progress updates.
            force: If True, skip sequence completeness validation.
            keep_layers: Optional set of raw prefixes whose files should be
                         preserved in the target directory instead of being
                         cleared.  Pass ``None`` (default) to clear everything.

        Returns:
            The HistoryEntry that was recorded.

        Raises:
            PromotionError: If the promotion fails or is cancelled.
        """
        self._cancelled.clear()
        if user is None:
            try:
                user = os.getlogin()
            except OSError:
                user = os.environ.get("USER", os.environ.get("USERNAME", "unknown"))

        target_dir = Path(self.source.latest_target)
        source_path = Path(version.source_path)

        logger.info(f"Promoting {version.version_string}: {source_path} -> {target_dir}")

        # Validate source exists
        if not source_path.exists():
            raise PromotionError(f"Source no longer exists: {source_path}")

        # Sequence completeness validation
        if not force and self.source.block_incomplete_sequences and has_frame_gaps(version):
            raise PromotionError(
                f"Sequence has frame gaps: {version.frame_range}. "
                f"Use force to override, or disable block_incomplete_sequences."
            )

        # Run pre-promote hook
        try:
            run_pre_promote_hook(self.source, version, user, self.project_name)
        except HookError as e:
            raise PromotionError(f"Pre-promote hook failed:\n{e}")

        # Create target directory if needed
        target_dir.mkdir(parents=True, exist_ok=True)

        # Scan target directory once — reuse for locked-file and clear checks
        target_entries = self._scan_target_media(target_dir)

        # Smart locked file detection: only check if target has files
        if self._target_has_media_files(target_dir, cached_entries=target_entries):
            locked = self._check_locked_files(target_dir, cached_entries=target_entries)
            if locked:
                raise PromotionError(
                    f"Cannot overwrite - these files appear to be locked/in use:\n"
                    + "\n".join(f"  {f}" for f in locked[:10])
                )

        try:
            if source_path.is_dir():
                self._promote_sequence(source_path, target_dir, version, progress_callback, keep_layers=keep_layers)
            else:
                self._promote_single_file(source_path, target_dir, progress_callback)
        except PromotionError:
            raise
        except Exception as e:
            raise PromotionError(f"File operation failed: {e}") from e

        # Record in history with mtime snapshots
        entry = HistoryEntry.from_version_info(version, user)
        version_files = self._get_version_source_files(source_path, version)
        entry.source_mtime = self._get_max_mtime(source_path, files=version_files)
        entry.target_mtime = self._get_max_mtime(target_dir)
        entry.pinned = pinned
        # Extract clip frame count for container files
        if source_path.is_file() and source_path.suffix.lower() in (".mov", ".mxf", ".mp4", ".avi"):
            try:
                from .timecode import extract_clip_frame_count
                entry.clip_frame_count = extract_clip_frame_count(source_path)
            except Exception:
                pass
        self.history.record_promotion(entry)

        # Run post-promote hook
        run_post_promote_hook(self.source, version, user, self.project_name)

        logger.info(f"Promotion complete: {version.version_string}")
        return entry

    def _scan_target_media(self, target_dir: Path) -> list:
        """Single os.scandir pass to collect media DirEntry objects from target.

        Returns a list of os.DirEntry filtered to files matching valid extensions.
        Reuse this result instead of scanning the target directory multiple times.
        """
        entries = []
        try:
            with os.scandir(target_dir) as it:
                for entry in it:
                    if entry.is_file(follow_symlinks=False):
                        name = entry.name
                        dot_idx = name.rfind(".")
                        if dot_idx >= 0 and name[dot_idx:].lower() in self._valid_extensions:
                            entries.append(entry)
        except (PermissionError, OSError):
            pass
        return entries

    def _target_has_media_files(self, target_dir: Path, cached_entries: list = None) -> bool:
        """Quick check if target directory contains any media files.

        When *cached_entries* is provided, uses the pre-scanned list instead
        of hitting the filesystem again.
        """
        if cached_entries is not None:
            return len(cached_entries) > 0
        # Fallback: quick scan with early exit
        try:
            with os.scandir(target_dir) as it:
                for entry in it:
                    if entry.is_file(follow_symlinks=False):
                        name = entry.name
                        dot_idx = name.rfind(".")
                        if dot_idx >= 0 and name[dot_idx:].lower() in self._valid_extensions:
                            return True
        except (PermissionError, OSError):
            pass
        return False

    def _filter_version_files(
        self, files: list[Path], version: VersionInfo
    ) -> list[Path]:
        """Filter files to only those belonging to a specific version.

        When a watched source uses flat versioned files (all versions in the
        same directory), the file list contains every version's files.  This
        method keeps only the files whose embedded version/date matches the
        target *version*.
        """
        from .scanner import VersionScanner
        from .task_tokens import parse_date_to_sortable

        pattern = self.source.version_pattern
        if not pattern:
            return files

        date_format = getattr(self.source, "date_format", "")
        regex = VersionScanner._compile_version_pattern(pattern, date_format)

        has_version = "{version}" in pattern
        has_date = "{date}" in pattern

        filtered = []
        for f in files:
            match = regex.search(f.name)
            if not match:
                continue
            groups = match.groups()

            if has_version and has_date:
                date_pos = pattern.index("{date}")
                ver_pos = pattern.index("{version}")
                if date_pos < ver_pos:
                    date_str, ver_str = groups[0], groups[1]
                else:
                    ver_str, date_str = groups[0], groups[1]
                if (int(ver_str) != version.version_number
                        or parse_date_to_sortable(date_str, date_format) != version.date_sortable):
                    continue
            elif has_date and not has_version:
                if parse_date_to_sortable(groups[0], date_format) != version.date_sortable:
                    continue
            else:
                if int(match.group(1)) != version.version_number:
                    continue

            filtered.append(f)

        return filtered if filtered else files

    def _promote_sequence(
        self,
        source_dir: Path,
        target_dir: Path,
        version: VersionInfo,
        progress_callback: Optional[Callable],
        keep_layers: Optional[set[str]] = None,
    ):
        """Copy/symlink a folder of frames to the target."""
        valid_extensions = self._valid_extensions
        source_files = sorted(
            f for f in source_dir.iterdir()
            if f.is_file() and f.suffix.lower() in valid_extensions
        )

        # When source_dir is the watched source root (flat file layout),
        # it contains files from ALL versions — filter to only the target version.
        if source_dir == Path(self.source.source_dir):
            source_files = self._filter_version_files(source_files, version)

        if not source_files:
            raise PromotionError(f"No matching files found in {source_dir}")

        # Clear existing files in target (only matching extensions),
        # but preserve files belonging to layers the user chose to keep.
        self._clear_target(target_dir, valid_extensions, keep_layers=keep_layers)

        total = len(source_files)
        mode = self.source.link_mode

        # Use parallel copy for large sequences in copy mode
        if mode == "copy" and total > 10:
            self._parallel_copy(source_files, target_dir, total, progress_callback)
        else:
            # Sequential for symlink/hardlink (fast already) or small sets
            for i, src_file in enumerate(source_files):
                if self._cancelled.is_set():
                    raise PromotionError("Promotion cancelled by user.")
                target_name = self._remap_filename(src_file.name)
                target_file = target_dir / target_name
                self._link_or_copy(src_file, target_file)
                if progress_callback:
                    progress_callback(i + 1, total, src_file.name)

    def _parallel_copy(
        self,
        source_files: list[Path],
        target_dir: Path,
        total: int,
        progress_callback: Optional[Callable],
    ):
        """Copy files in parallel using a thread pool."""
        completed = [0]  # mutable counter for closure

        def _copy_one(src_file: Path):
            if self._cancelled.is_set():
                return
            target_name = self._remap_filename(src_file.name)
            target_file = target_dir / target_name
            if target_file.exists() or target_file.is_symlink():
                target_file.unlink()
            smart_copy(src_file, target_file, cancel_event=self._cancelled)
            completed[0] += 1
            if progress_callback:
                progress_callback(completed[0], total, src_file.name)

        with ThreadPoolExecutor(max_workers=_COPY_WORKERS) as executor:
            futures = [executor.submit(_copy_one, f) for f in source_files]
            # Wait for all to complete, propagate exceptions
            for future in futures:
                future.result()

        if self._cancelled.is_set():
            raise PromotionError("Promotion cancelled by user.")

    def _promote_single_file(
        self,
        source_file: Path,
        target_dir: Path,
        progress_callback: Optional[Callable],
    ):
        """Copy/symlink a single file to the target."""
        target_name = self._remap_filename(source_file.name)
        target_file = target_dir / target_name

        if progress_callback:
            progress_callback(0, 1, source_file.name)

        self._link_or_copy(source_file, target_file)

        if progress_callback:
            progress_callback(1, 1, source_file.name)

    def _extract_layer_suffix(self, filename: str) -> str:
        """Extract the layer suffix from a filename relative to the base source name.

        Compares the file's version-stripped name against the cached base
        source_name to find additional naming info (like layer/pass names).

        Examples (base source_name="shot010_comp"):
            shot010_comp_v001.1001.exr           -> ""
            shot010_comp_alpha_v001.1001.exr      -> "_alpha"
            shot010_comp_v001_alpha.1001.exr       -> "_alpha"
        """
        from .task_tokens import strip_frame_and_ext, strip_version

        fullname = strip_frame_and_ext(filename)
        file_source_name = strip_version(fullname)
        base_source_name = self._rename_tokens["source_name"]

        if file_source_name == base_source_name:
            return ""

        if file_source_name.startswith(base_source_name):
            return file_source_name[len(base_source_name):]

        # Check if the base is a prefix after case-insensitive comparison
        if file_source_name.lower().startswith(base_source_name.lower()):
            return file_source_name[len(base_source_name):]

        return ""

    def _remap_filename(self, filename: str) -> str:
        """Remap a versioned filename using the file rename template.

        If file_rename_template is set, uses it to build the base name.
        Tokens: {source_title}, {source_name}, {source_basename}, {source_fullname}
        The frame number and extension are always preserved from the original.
        Layer suffixes (e.g. _alpha) are preserved for non-primary sequences.

        Examples (template="{source_name}"):
            hero_comp_v003.1001.exr -> hero_comp.1001.exr
            hero_comp_alpha_v003.1001.exr -> hero_comp_alpha.1001.exr
        Examples (template="{source_name}_latest"):
            hero_comp_v003.1001.exr -> hero_comp_latest.1001.exr
            hero_comp_v003_alpha.1001.exr -> hero_comp_latest_alpha.1001.exr
        """
        template = self.source.file_rename_template
        if not template:
            # Fallback: strip version (and date if configured) using pre-compiled regex
            result = _VERSION_RE.sub("", filename, count=1)
            date_fmt = getattr(self.source, "date_format", "")
            if date_fmt:
                from .task_tokens import strip_date
                # Strip date from the stem only (preserve extension)
                p = Path(result)
                stem = strip_date(p.stem, date_fmt)
                result = stem + p.suffix if p.suffix else stem
            result = _DOUBLE_DIVIDER_RE.sub(r"\1", result)
            return result

        # Parse the original filename into components
        frame_match = _FRAME_EXT_RE.search(filename)

        if frame_match:
            frame_sep = frame_match.group(1)
            frame_num = frame_match.group(2)
            ext = frame_match.group(3)
        else:
            frame_sep = ""
            frame_num = ""
            # Single file, no frame number - just extension
            p = Path(filename)
            ext = p.suffix.lstrip(".")

        # Derive source tokens (cached after first call)
        if self._rename_tokens is None:
            token_input = self.source.sample_filename or self.source.name
            date_fmt = getattr(self.source, "date_format", "")
            self._rename_tokens = derive_source_tokens(
                token_input, self.task_tokens, date_fmt,
                source_title=self.source.name)

        tokens = self._rename_tokens

        # Extract layer suffix for this specific file (e.g. "_alpha")
        layer_suffix = self._extract_layer_suffix(filename)

        # Expand template tokens
        base = template
        base = base.replace("{source_title}", tokens["source_title"])
        base = base.replace("{source_name}", tokens["source_name"])
        base = base.replace("{source_basename}", tokens["source_basename"])
        base = base.replace("{source_fullname}", tokens["source_fullname"])
        base = _expand_group_token(base, self.source.group)

        # Append layer suffix after template expansion to preserve original position
        if layer_suffix:
            base += layer_suffix
            base = _DOUBLE_DIVIDER_RE.sub(r"\1", base)

        # Reconstruct filename: base + frame + ext
        if frame_num:
            return f"{base}{frame_sep}{frame_num}.{ext}"
        else:
            return f"{base}.{ext}"

    def _clear_target(self, target_dir: Path, valid_extensions: set,
                       keep_layers: Optional[set[str]] = None):
        """Remove existing media files from the target directory (not the history file).

        When *keep_layers* is provided, files whose sequence prefix (as
        determined by :func:`_group_files_by_sequence`) is in the set are
        left untouched.
        """
        try:
            entries = list(target_dir.iterdir())
        except OSError as e:
            raise PromotionError(f"Cannot read target directory {target_dir}: {e}") from e

        # Build a set of prefixes to preserve
        if keep_layers:
            media_files = [f for f in entries if f.is_file() and f.suffix.lower() in valid_extensions]
            groups = _group_files_by_sequence(media_files) if media_files else {}
            keep_files: set[str] = set()
            for prefix, files in groups.items():
                if prefix in keep_layers:
                    keep_files.update(f.name for f in files)
        else:
            keep_files = set()

        for f in entries:
            if f.is_file() and f.suffix.lower() in valid_extensions:
                if f.name in keep_files:
                    continue
                try:
                    f.unlink()
                except PermissionError:
                    raise PromotionError(f"Cannot delete {f} - file may be in use")
            elif f.is_symlink():
                try:
                    f.unlink()
                except OSError as e:
                    logger.warning(f"Could not remove symlink {f}: {e}")

    def _link_or_copy(self, source: Path, target: Path):
        """Route to the correct file operation based on link_mode."""
        if target.exists() or target.is_symlink():
            target.unlink()

        mode = self.source.link_mode
        if mode == "symlink":
            self._create_symlink(source, target)
        elif mode == "hardlink":
            self._create_hardlink(source, target)
        else:
            smart_copy(source, target, cancel_event=self._cancelled)

    def _create_symlink(self, source: Path, target: Path):
        """Create a symlink, handling platform differences."""
        try:
            target.symlink_to(_resolve_unc_safe(source))
        except OSError as e:
            if platform.system() == "Windows":
                raise PromotionError(
                    f"Symlink creation failed. On Windows, symlinks require "
                    f"either Administrator privileges or Developer Mode enabled. "
                    f"Consider using copy mode instead. Error: {e}"
                ) from e
            raise

    def _create_hardlink(self, source: Path, target: Path):
        """Create a hardlink. Works on NTFS without elevation, but source and target must be on the same volume."""
        try:
            os.link(str(_resolve_unc_safe(source)), str(target))
        except OSError as e:
            raise PromotionError(
                f"Hardlink creation failed. Hardlinks require source and target "
                f"to be on the same drive/volume and a filesystem that supports them (NTFS). "
                f"Error: {e}"
            ) from e

    def _check_locked_files(self, target_dir: Path, cached_entries: list = None) -> list[str]:
        """
        Check for locked files in the target directory.
        Returns a list of filenames that appear to be locked.

        When *cached_entries* is provided (list of os.DirEntry from
        _scan_target_media), uses those instead of re-scanning.
        """
        locked = []

        if cached_entries is not None:
            files = [Path(e.path) for e in cached_entries]
        else:
            valid_extensions = self._valid_extensions
            files = [
                f for f in target_dir.iterdir()
                if f.is_file() and f.suffix.lower() in valid_extensions
            ]

        for f in files:
            try:
                # Try to open for writing - if it fails, the file is locked
                with open(f, "a"):
                    pass
            except (PermissionError, OSError):
                locked.append(f.name)

        return locked

    def get_current_version(self) -> Optional[HistoryEntry]:
        """Get the currently promoted version."""
        return self.history.get_current()

    def get_history(self) -> list[HistoryEntry]:
        """Get the full promotion history."""
        return self.history.get_history()

    def dry_run(self, version: VersionInfo) -> dict:
        """Preview the file mapping for a promotion without copying anything.

        Returns a dict with source_dir, target_dir, file_map, total_files,
        total_size_bytes, and link_mode.
        """
        source_path = Path(version.source_path)
        target_dir = Path(self.source.latest_target)
        valid_extensions = self._valid_extensions

        if source_path.is_dir():
            source_files = sorted(
                f for f in source_path.iterdir()
                if f.is_file() and f.suffix.lower() in valid_extensions
            )
        else:
            source_files = [source_path] if source_path.is_file() else []

        file_map = []
        total_size = 0
        for f in source_files:
            try:
                size = f.stat().st_size
            except OSError:
                size = 0
            target_name = self._remap_filename(f.name)
            file_map.append({
                "source": str(f),
                "target_name": target_name,
                "size_bytes": size,
            })
            total_size += size

        return {
            "source_dir": str(source_path),
            "target_dir": str(target_dir),
            "file_map": file_map,
            "total_files": len(file_map),
            "total_size_bytes": total_size,
            "link_mode": self.source.link_mode,
        }

    def _get_max_mtime(self, path: Path, files: list[Path] = None) -> Optional[float]:
        """Return the maximum mtime of media files in a directory or single file.

        When *files* is provided, only those files are checked instead of
        scanning the entire directory.  This is essential for flat layouts
        where the directory contains files from multiple versions.
        """
        max_mt = 0.0
        found = False
        try:
            if files is not None:
                for f in files:
                    try:
                        mt = f.stat().st_mtime
                        if mt > max_mt:
                            max_mt = mt
                        found = True
                    except OSError:
                        pass
            elif path.is_dir():
                valid_extensions = self._valid_extensions
                with os.scandir(path) as it:
                    for entry in it:
                        if entry.is_file(follow_symlinks=False):
                            name = entry.name
                            dot_idx = name.rfind(".")
                            if dot_idx >= 0 and name[dot_idx:].lower() in valid_extensions:
                                mt = entry.stat().st_mtime
                                if mt > max_mt:
                                    max_mt = mt
                                found = True
            elif path.is_file():
                max_mt = path.stat().st_mtime
                found = True
        except OSError:
            pass
        return max_mt if found else None

    def _get_version_source_files(self, source_path: Path, version: VersionInfo) -> Optional[list[Path]]:
        """Return filtered file list for flat layouts, or None for subfolder layouts.

        In a flat layout the source_path equals the watched source directory
        and contains files from every version.  This method filters to only
        the files belonging to *version* so that mtime checks are accurate.
        """
        if source_path.is_dir() and source_path == Path(self.source.source_dir):
            valid_extensions = self._valid_extensions
            all_files = sorted(
                f for f in source_path.iterdir()
                if f.is_file() and f.suffix.lower() in valid_extensions
            )
            return self._filter_version_files(all_files, version)
        return None

    @staticmethod
    def _extract_version_number(version_str: str) -> Optional[int]:
        """Extract integer version number from a version string like 'v003'."""
        m = re.search(r'(\d+)', version_str)
        return int(m.group(1)) if m else None

    def verify(self) -> dict:
        """Check integrity of the latest target vs history.

        Checks file count, source staleness (re-rendered since promotion),
        and target staleness (externally overwritten).

        Scans the target directory once and reuses the results for both
        the file-count integrity check and the mtime staleness check.
        """
        target_dir = Path(self.source.latest_target)
        if not target_dir.exists():
            return {"valid": True, "message": "Target directory doesn't exist yet."}

        # Single scan of target directory — reuse for file list and mtime
        target_entries = self._scan_target_media(target_dir)
        actual_files = [e.name for e in target_entries]

        # Basic file count check
        basic = self.history.verify_integrity(actual_files)
        if not basic["valid"]:
            return basic

        # Mtime-based staleness checks
        current = self.history.get_current()
        if not current:
            return basic

        # Check if source files changed since promotion (re-rendered)
        if current.source_mtime is not None:
            source_path = Path(current.source)
            # For flat layouts, filter to only the promoted version's files
            # so that new versions rendered into the same folder don't
            # trigger a false stale detection.
            version_files = None
            if source_path.is_dir() and source_path == Path(self.source.source_dir):
                ver_num = self._extract_version_number(current.version)
                if ver_num is not None:
                    stub = VersionInfo(current.version, ver_num, current.source)
                    version_files = self._get_version_source_files(source_path, stub)
            current_source_mtime = self._get_max_mtime(source_path, files=version_files)
            if current_source_mtime is not None and current_source_mtime > current.source_mtime + 1.0:
                return {
                    "valid": False,
                    "message": f"Source files for {current.version} modified since promotion "
                               f"— may have been re-rendered.",
                }

        # Check if target files were overwritten externally — use cached entries
        if current.target_mtime is not None:
            max_mt = 0.0
            for entry in target_entries:
                try:
                    mt = entry.stat(follow_symlinks=False).st_mtime
                    if mt > max_mt:
                        max_mt = mt
                except OSError:
                    pass
            current_target_mtime = max_mt if target_entries else None
            if current_target_mtime is not None and abs(current_target_mtime - current.target_mtime) > 1.0:
                return {
                    "valid": False,
                    "message": f"Target files modified since promotion "
                               f"— may have been overwritten externally.",
                }

        return basic


def generate_report(entry: HistoryEntry, source: WatchedSource, dry_run_data: dict = None) -> dict:
    """Generate a promotion report suitable for JSON export.

    Args:
        entry: The HistoryEntry from the promotion.
        source: The WatchedSource that was promoted.
        dry_run_data: Optional dry_run result dict to include file mapping.

    Returns:
        A dict suitable for JSON serialization.
    """
    report = {
        "timestamp": entry.set_at,
        "source_name": source.name,
        "version": entry.version,
        "source_path": entry.source,
        "target_path": source.latest_target,
        "set_by": entry.set_by,
        "frame_range": entry.frame_range,
        "frame_count": entry.frame_count,
        "file_count": entry.file_count,
        "link_mode": source.link_mode,
    }
    if entry.start_timecode:
        report["start_timecode"] = entry.start_timecode
    if entry.source_mtime is not None:
        report["source_mtime"] = entry.source_mtime
    if entry.target_mtime is not None:
        report["target_mtime"] = entry.target_mtime
    if dry_run_data:
        report["file_map"] = dry_run_data["file_map"]
        report["total_size_bytes"] = dry_run_data["total_size_bytes"]
    return report
