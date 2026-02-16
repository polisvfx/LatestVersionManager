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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Callable

from .models import VersionInfo, WatchedSource, HistoryEntry
from .history import HistoryManager
from .task_tokens import derive_source_tokens
from .config import _expand_group_token

logger = logging.getLogger(__name__)

# Pre-compiled module-level regex patterns (avoid re-compiling per call)
_VERSION_RE = re.compile(r"[._\-]v\d+", re.IGNORECASE)
_DOUBLE_DIVIDER_RE = re.compile(r"([_.\-]){2,}")
_FRAME_EXT_RE = re.compile(r"([._])(\d{3,8})\.(\w+)$")

# Number of threads for parallel file copy operations
_COPY_WORKERS = 4


class PromotionError(Exception):
    """Raised when a promotion fails."""
    pass


class Promoter:
    """Handles promoting a version to the latest target."""

    def __init__(self, watched_source: WatchedSource, task_tokens: list = None):
        self.source = watched_source
        self.task_tokens = task_tokens or []
        self.history = HistoryManager(
            os.path.join(watched_source.latest_target, watched_source.history_filename)
        )
        # Cache derived tokens for file renaming
        self._rename_tokens = None

    def promote(
        self,
        version: VersionInfo,
        user: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> HistoryEntry:
        """
        Promote a version to be the current "latest".

        Args:
            version: The version to promote.
            user: Username to record. Defaults to OS login name.
            progress_callback: Optional callback(current_file, total_files, filename)
                               for UI progress updates.

        Returns:
            The HistoryEntry that was recorded.

        Raises:
            PromotionError: If the promotion fails.
        """
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

        # Create target directory if needed
        target_dir.mkdir(parents=True, exist_ok=True)

        # Smart locked file detection: only check if target has files
        if self._target_has_media_files(target_dir):
            locked = self._check_locked_files(target_dir)
            if locked:
                raise PromotionError(
                    f"Cannot overwrite - these files appear to be locked/in use:\n"
                    + "\n".join(f"  {f}" for f in locked[:10])
                )

        try:
            if source_path.is_dir():
                self._promote_sequence(source_path, target_dir, version, progress_callback)
            else:
                self._promote_single_file(source_path, target_dir, progress_callback)
        except PromotionError:
            raise
        except Exception as e:
            raise PromotionError(f"File operation failed: {e}") from e

        # Record in history with mtime snapshots
        entry = HistoryEntry.from_version_info(version, user)
        entry.source_mtime = self._get_max_mtime(source_path)
        entry.target_mtime = self._get_max_mtime(target_dir)
        self.history.record_promotion(entry)

        logger.info(f"Promotion complete: {version.version_string}")
        return entry

    def _target_has_media_files(self, target_dir: Path) -> bool:
        """Quick check if target directory contains any media files.

        Uses os.scandir for fast iteration and returns early on first match.
        """
        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)
        try:
            with os.scandir(target_dir) as it:
                for entry in it:
                    if entry.is_file(follow_symlinks=False):
                        name = entry.name
                        dot_idx = name.rfind(".")
                        if dot_idx >= 0 and name[dot_idx:].lower() in valid_extensions:
                            return True
        except (PermissionError, OSError):
            pass
        return False

    def _promote_sequence(
        self,
        source_dir: Path,
        target_dir: Path,
        version: VersionInfo,
        progress_callback: Optional[Callable],
    ):
        """Copy/symlink a folder of frames to the target."""
        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)
        source_files = sorted(
            f for f in source_dir.iterdir()
            if f.is_file() and f.suffix.lower() in valid_extensions
        )

        if not source_files:
            raise PromotionError(f"No matching files found in {source_dir}")

        # Clear existing files in target (only matching extensions)
        self._clear_target(target_dir, valid_extensions)

        total = len(source_files)
        mode = self.source.link_mode

        # Use parallel copy for large sequences in copy mode
        if mode == "copy" and total > 10:
            self._parallel_copy(source_files, target_dir, total, progress_callback)
        else:
            # Sequential for symlink/hardlink (fast already) or small sets
            for i, src_file in enumerate(source_files):
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
            target_name = self._remap_filename(src_file.name)
            target_file = target_dir / target_name
            if target_file.exists() or target_file.is_symlink():
                target_file.unlink()
            shutil.copy2(src_file, target_file)
            completed[0] += 1
            if progress_callback:
                progress_callback(completed[0], total, src_file.name)

        with ThreadPoolExecutor(max_workers=_COPY_WORKERS) as executor:
            futures = [executor.submit(_copy_one, f) for f in source_files]
            # Wait for all to complete, propagate exceptions
            for future in futures:
                future.result()

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

    def _remap_filename(self, filename: str) -> str:
        """Remap a versioned filename using the file rename template.

        If file_rename_template is set, uses it to build the base name.
        Tokens: {source_name}, {source_basename}, {source_fullname}
        The frame number and extension are always preserved from the original.

        Examples (template="{source_name}"):
            hero_comp_v003.1001.exr -> hero_comp.1001.exr
        Examples (template="{source_name}_latest"):
            hero_comp_v003.1001.exr -> hero_comp_latest.1001.exr
        """
        template = self.source.file_rename_template
        if not template:
            # Fallback: just strip version using pre-compiled regex
            result = _VERSION_RE.sub("", filename, count=1)
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
            self._rename_tokens = derive_source_tokens(token_input, self.task_tokens)

        tokens = self._rename_tokens

        # Expand template tokens
        base = template
        base = base.replace("{source_name}", tokens["source_name"])
        base = base.replace("{source_basename}", tokens["source_basename"])
        base = base.replace("{source_fullname}", tokens["source_fullname"])
        base = _expand_group_token(base, self.source.group)

        # Reconstruct filename: base + frame + ext
        if frame_num:
            return f"{base}{frame_sep}{frame_num}.{ext}"
        else:
            return f"{base}.{ext}"

    def _clear_target(self, target_dir: Path, valid_extensions: set):
        """Remove existing media files from the target directory (not the history file)."""
        for f in target_dir.iterdir():
            if f.is_file() and f.suffix.lower() in valid_extensions:
                try:
                    f.unlink()
                except PermissionError:
                    raise PromotionError(f"Cannot delete {f} - file may be in use")
            elif f.is_symlink():
                f.unlink()

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
            shutil.copy2(source, target)

    def _create_symlink(self, source: Path, target: Path):
        """Create a symlink, handling platform differences."""
        try:
            target.symlink_to(source.resolve())
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
            os.link(str(source.resolve()), str(target))
        except OSError as e:
            raise PromotionError(
                f"Hardlink creation failed. Hardlinks require source and target "
                f"to be on the same drive/volume and a filesystem that supports them (NTFS). "
                f"Error: {e}"
            ) from e

    def _check_locked_files(self, target_dir: Path) -> list[str]:
        """
        Check for locked files in the target directory.
        Returns a list of filenames that appear to be locked.
        """
        locked = []
        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)

        for f in target_dir.iterdir():
            if not f.is_file() or f.suffix.lower() not in valid_extensions:
                continue
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
        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)

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

    def _get_max_mtime(self, path: Path) -> Optional[float]:
        """Return the maximum mtime of media files in a directory or single file."""
        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)
        max_mt = 0.0
        found = False
        try:
            if path.is_dir():
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

    def verify(self) -> dict:
        """Check integrity of the latest target vs history.

        Checks file count, source staleness (re-rendered since promotion),
        and target staleness (externally overwritten).
        """
        target_dir = Path(self.source.latest_target)
        if not target_dir.exists():
            return {"valid": True, "message": "Target directory doesn't exist yet."}

        valid_extensions = set(ext.lower() for ext in self.source.file_extensions)
        actual_files = [
            f.name for f in target_dir.iterdir()
            if f.is_file() and f.suffix.lower() in valid_extensions
        ]

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
            current_source_mtime = self._get_max_mtime(source_path)
            if current_source_mtime is not None and current_source_mtime > current.source_mtime + 1.0:
                return {
                    "valid": False,
                    "message": f"Source files for {current.version} modified since promotion "
                               f"— may have been re-rendered.",
                }

        # Check if target files were overwritten externally
        if current.target_mtime is not None:
            current_target_mtime = self._get_max_mtime(target_dir)
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
