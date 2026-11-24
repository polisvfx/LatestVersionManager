"""
Promoter - copies, symlinks, or hardlinks a version to the "latest" target location.

This is the core operation: take a detected version and make it the
current "latest" that Resolve/Nuke is reading from.
"""

__all__ = [
    "Promoter", "PromotionError", "has_frame_gaps",
    "generate_report", "validate_rename_template",
]

import json
import os
import re
import shutil
import stat
import logging
import platform
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Callable

from .models import VersionInfo, WatchedSource, HistoryEntry, has_media_extension, version_strings_match
from .history import HistoryManager
from .hooks import run_pre_promote_hook, run_post_promote_hook, HookError
from .task_tokens import derive_source_tokens, compose_nle_display_stem
from .config import _expand_group_token
from .fast_copy import smart_copy, CopyCancelled
from .lockfile import PromoteLock, LockHeldError, LOCK_FILENAME
from .scanner import _group_files_by_sequence
from .render_check import find_unfinished_renders, is_container_file, is_open_for_writing
from .patterns import (
    VERSION_STRIP_RE as _VERSION_RE,
    DOUBLE_DIVIDER_RE as _DOUBLE_DIVIDER_RE,
    FRAME_EXT_SPLIT_RE as _FRAME_EXT_RE,
)

logger = logging.getLogger(__name__)

# Number of threads for parallel file copy operations — adapts to CPU count
_COPY_WORKERS = min(os.cpu_count() or 4, 8)

# Renames during the swap run in parallel: over SMB each rename is a network
# round-trip, so a 2,000-frame swap done one rename at a time kept the
# target incomplete for tens of seconds. Small swaps stay sequential.
_SWAP_WORKERS = 8
_SWAP_PARALLEL_MIN = 5

# Hidden working directories used by staged (transactional) promotion.
# New files are fully written into the staging dir, then swapped into the
# target with same-volume renames; old files are parked in the backup dir
# until the swap succeeds. Both live inside the target directory so the
_SWAP_COMMITTED_MARKER = ".lvm_committed"


def _rmtree_force(path: Path, *, ignore_errors: bool = False) -> None:
    """``shutil.rmtree`` that clears read-only attributes on Windows before retry."""
    def _on_exc(func, p, exc):
        if isinstance(exc, PermissionError):
            try:
                os.chmod(p, stat.S_IWRITE | stat.S_IREAD)
                func(p)
                return
            except OSError:
                pass
        if not ignore_errors:
            raise exc

    if not path.exists() and not path.is_symlink():
        return
    try:
        shutil.rmtree(path, onexc=_on_exc)
    except OSError:
        if not ignore_errors:
            raise


def _unlink_force(path: Path) -> None:
    """Remove a single file or symlink, clearing Windows read-only bit if needed."""
    try:
        path.unlink(missing_ok=True)
    except PermissionError:
        try:
            if not path.is_symlink():
                os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
            path.unlink(missing_ok=True)
        except OSError:
            pass
    except OSError:
        pass


def _is_file_locked_win32(path_str: str, *, is_symlink: bool = False) -> bool:
    """Return True if *path_str* is held open by another process blocking rename/write.

    Uses Win32 ``CreateFileW`` with ``DELETE`` access + ``FILE_SHARE_READ |
    FILE_SHARE_DELETE`` so read-only files (which ``open(f, 'a')`` falsely
    flags) and non-writing readers do not produce false positives, and broken
    symlinks are never touched.
    """
    if is_symlink:
        return False
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.argtypes = [
            wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
            ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
        ]
        kernel32.CreateFileW.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        DELETE = 0x00010000
        FILE_SHARE_READ = 0x00000001
        FILE_SHARE_DELETE = 0x00000004
        OPEN_EXISTING = 3
        FILE_ATTRIBUTE_NORMAL = 0x80
        INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
        ERROR_SHARING_VIOLATION = 32
        ERROR_LOCK_VIOLATION = 33

        s = str(path_str)
        if len(s) >= 260 and not s.startswith("\\\\?\\"):
            s = os.path.abspath(s)
            if s.startswith("\\\\"):
                s = "\\\\?\\UNC\\" + s[2:]
            else:
                s = "\\\\?\\" + s

        handle = kernel32.CreateFileW(
            s,
            DELETE,
            FILE_SHARE_READ | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            None,
        )
        if handle == INVALID_HANDLE_VALUE:
            err = ctypes.get_last_error()
            return err in (ERROR_SHARING_VIOLATION, ERROR_LOCK_VIOLATION)
        kernel32.CloseHandle(handle)
        return False
    except Exception:
        return False
# renames never cross a volume boundary.
STAGING_DIRNAME = ".lvm_staging"
BACKUP_DIRNAME = ".lvm_backup"


def _human_size(n: float) -> str:
    """Format a byte count for error messages (e.g. '1.5 GB')."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{int(n)} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

# Valid tokens for file_rename_template
_VALID_RENAME_TOKENS = {
    "{source_title}", "{source_name}", "{source_basename}",
    "{source_fullname}", "{group}",
}
_TOKEN_RE = re.compile(r"\{[^}]+\}")


def validate_rename_template(template: str) -> list[str]:
    """Return a list of unknown tokens found in a file rename template.

    Known tokens: {source_title}, {source_name}, {source_basename},
    {source_fullname}, {group}.

    Returns an empty list if all tokens are valid.
    """
    if not template:
        return []
    found = _TOKEN_RE.findall(template)
    return [t for t in found if t not in _VALID_RENAME_TOKENS]


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


def _throttled_byte_progress(callback: Callable[[int, int, str], None], name: str,
                             interval: float = 0.1) -> Callable[[int, int], None]:
    """Wrap *callback(done, total, name)* so it runs at most every *interval*
    seconds — CopyFileExW reports every chunk, which would flood the GUI with
    signals. The final update (done >= total) is always delivered."""
    last = [float("-inf")]

    def report(done: int, total: int) -> None:
        now = time.monotonic()
        if done >= total or now - last[0] >= interval:
            last[0] = now
            callback(done, total, name)
    return report


class PromotionError(Exception):
    """Raised when a promotion fails."""
    pass


def has_frame_gaps(version: VersionInfo) -> bool:
    """Return True if the version's frame_range indicates gaps."""
    return version.frame_range is not None and "gaps detected" in version.frame_range


class Promoter:
    """Handles promoting a version to the latest target."""

    def __init__(self, watched_source: WatchedSource, task_tokens: list = None,
                 project_name: str = "", nle_rename_options: dict = None):
        self.source = watched_source
        self.task_tokens = task_tokens or []
        self.project_name = project_name
        # Per-project clip-rename composition. Optional — when None the
        # legacy companion fallback (full source basename) kicks in. Callers
        # that own a ProjectConfig should pass a dict with keys:
        #   include_version (bool, default True)
        #   include_frame   (bool)
        #   include_extension (bool)
        #   custom_enabled  (bool)
        #   custom_template (str)
        self.nle_rename_options = nle_rename_options

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
        # Duration of the last swap phase, for the promotion summary log line.
        self.last_swap_seconds: Optional[float] = None

        # Warn about unknown tokens in file rename template
        unknown = validate_rename_template(watched_source.file_rename_template)
        if unknown:
            logger.warning(
                "Source '%s' file_rename_template contains unknown tokens: %s",
                watched_source.name, ", ".join(unknown),
            )

    def cancel(self):
        """Signal that the current promotion should be aborted at the next checkpoint."""
        self._cancelled.set()

    def _media_entries(self, directory) -> list:
        """Media files in *directory* as ``os.DirEntry``, sorted by name.

        One ``os.scandir`` pass. ``Path.iterdir()`` + ``is_file()`` costs a
        stat per entry — one SMB round-trip per frame — while DirEntry
        carries the file type (and on Windows the stat data) from the
        listing itself. Raises OSError like ``iterdir`` would.
        """
        with os.scandir(directory) as it:
            entries = [
                e for e in it
                if has_media_extension(e.name, self._valid_extensions) and e.is_file()
            ]
        entries.sort(key=lambda e: e.name)
        return entries

    def _list_media(self, directory) -> list[Path]:
        """Media files in *directory* as sorted Paths (see :meth:`_media_entries`)."""
        return [Path(e.path) for e in self._media_entries(directory)]

    @staticmethod
    def _max_entry_mtime(entries) -> Optional[float]:
        """Newest mtime among DirEntries, using their cached stat where the
        platform provides one."""
        newest = None
        for e in entries:
            try:
                mt = e.stat().st_mtime
            except OSError:
                continue
            if newest is None or mt > newest:
                newest = mt
        return newest

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
        target_files = self._list_media(target_dir)
        # Other sources sharing this target are not "layers" of this one.
        target_files = self._drop_foreign_files(
            target_files, target_dir, self._compute_latest_basename(version)
        )
        if not target_files:
            return []
        target_groups = _group_files_by_sequence(target_files)
        target_layer_names = set(target_groups.keys())

        # Layers in the incoming version
        source_path = Path(version.source_path)
        if source_path.is_dir():
            source_files = self._list_media(source_path)
            if self._same_dir(source_path, Path(self.source.source_dir)):
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
        byte_progress_callback: Optional[Callable[[int, int, str], None]] = None,
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
            byte_progress_callback: Optional callback(bytes_copied,
                         total_bytes, filename) while a single-file version
                         (MOV/MXF/...) is copied, at most ~10 times a second.
                         Where the platform copy can't report bytes (macOS),
                         it isn't called.

        Returns:
            The HistoryEntry that was recorded.

        Raises:
            PromotionError: If the promotion fails or is cancelled.
        """
        self._cancelled.clear()
        self.last_swap_seconds = None
        started = time.monotonic()
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

        # Container files (MOV/MXF/...) are a single file the renderer keeps
        # writing to and finalises last — never publish one mid-render.
        if not force:
            unfinished = self.find_unfinished_renders(version)
            if unfinished:
                raise PromotionError(
                    "Still rendering — these files are not finished yet:\n"
                    + "\n".join(f"  {p.name}: {why}" for p, why in unfinished[:10])
                    + "\nWait for the render to finish, or use force to override."
                )

        # Run pre-promote hook
        try:
            run_pre_promote_hook(self.source, version, user, self.project_name)
        except HookError as e:
            raise PromotionError(f"Pre-promote hook failed:\n{e}")

        # Create target directory if needed
        target_dir.mkdir(parents=True, exist_ok=True)

        # Serialize writers: another LVM process (GUI + CLI, or a second
        # artist over SMB) promoting into the same target would otherwise
        # race the staged swap and the history sidecar.
        lock = PromoteLock(target_dir)
        try:
            lock.acquire()
        except LockHeldError as e:
            info = e.info or {}
            raise PromotionError(
                f"Another promotion is already writing to {target_dir} "
                f"(started by {info.get('user', '?')}@{info.get('host', '?')} "
                f"at {info.get('started_at', 'unknown time')}). "
                f"If that promotion crashed, its lock expires automatically "
                f"after a few minutes; to force it, delete {LOCK_FILENAME} "
                f"in the target folder."
            ) from e

        try:
            # Fail before touching the target if the history sidecar can't be
            # read (permissions, share dropped): promoting anyway would leave
            # files swapped with no history record, or overwrite the real
            # history with a single entry.
            try:
                self.history.load()
            except OSError as e:
                raise PromotionError(
                    f"Cannot read promotion history {self.history.path}: {e}"
                ) from e

            # Recover from any previous crashed promotion before touching
            # the target (restores a torn swap, discards dead staging).
            self._cleanup_orphaned_promotion_dirs(target_dir)

            self._preflight_disk_space(version, target_dir)

            # Scan target directory once — reuse for locked-file and clear checks.
            # Only inspect this source's own files: when several sources share a
            # latest_target dir (e.g. per-shot .mov outputs), another source's
            # file being locked must not block this source's promotion.
            target_entries = self._scan_target_media(target_dir)
            own_target_entries = self._filter_to_own_target_files(target_entries)

            if own_target_entries:
                locked = self._check_locked_files(target_dir, cached_entries=own_target_entries)
                if locked:
                    raise PromotionError(
                        f"Cannot overwrite - these files appear to be locked/in use:\n"
                        + "\n".join(f"  {f}" for f in locked[:10])
                    )

            try:
                if source_path.is_dir():
                    self._promote_sequence(source_path, target_dir, version, progress_callback, keep_layers=keep_layers)
                else:
                    self._promote_single_file(source_path, target_dir, progress_callback,
                                              byte_progress_callback)
            except PromotionError:
                raise
            except CopyCancelled:
                raise PromotionError(
                    "Promotion cancelled by user — target left unchanged."
                )
            except Exception as e:
                raise PromotionError(f"File operation failed: {e}") from e

            # Record in history with mtime snapshots
            entry = HistoryEntry.from_version_info(version, user)
            version_files = self._get_version_source_files(source_path, version)
            entry.source_mtime = self._get_max_mtime(source_path, files=version_files)
            # Restrict the target mtime snapshot to this source's own files —
            # otherwise sibling sources sharing the same latest_target would
            # bleed their mtimes into our record and cause spurious "modified
            # since promotion" warnings on the next verify().
            own_entries = self._filter_to_own_target_files(self._scan_target_media(target_dir))
            own_target_files = [Path(e.path) for e in own_entries]
            # From the listing's DirEntries — not one stat per frame.
            entry.target_mtime = (self._max_entry_mtime(own_entries) if own_entries
                                  else self._get_max_mtime(target_dir))
            entry.pinned = pinned
            # Capture the on-disk stem so NLE companion scripts can match this
            # sidecar to its clip even when several sources share a target dir.
            entry.latest_basename = self._compute_latest_basename(version, own_target_files)
            # Precompute the NLE clip-rename stem + frame/extension flags so the
            # companion scripts don't have to know about project settings.
            self._populate_nle_display_fields(entry, version)
            # Extract clip frame count for container files
            if source_path.is_file() and source_path.suffix.lower() in (".mov", ".mxf", ".mp4", ".avi"):
                try:
                    from .timecode import extract_clip_frame_count
                    entry.clip_frame_count = extract_clip_frame_count(source_path)
                except Exception:
                    pass
            self.history.record_promotion(entry)
        finally:
            lock.release()

        # Run post-promote hook AFTER releasing PromoteLock so slow hooks
        # don't block sibling promotions or deadlock if they invoke LVM CLI.
        run_post_promote_hook(self.source, version, user, self.project_name)

        elapsed = time.monotonic() - started
        size = getattr(version, "total_size_bytes", 0) or 0
        rate = f", {_human_size(size / elapsed)}/s" if size and elapsed > 0 else ""
        swap = (f", swap {self.last_swap_seconds:.2f}s"
                if self.last_swap_seconds is not None else "")
        logger.info(
            "Promotion complete: %s %s — %d file(s), %s, %s mode, %.1fs%s%s",
            self.source.name, version.version_string, version.file_count,
            _human_size(size), self.source.link_mode, elapsed, rate, swap,
        )
        return entry

    def find_unfinished_renders(self, version: VersionInfo,
                                settle_seconds: float = 0.0) -> list[tuple[Path, str]]:
        """Return ``(path, reason)`` for container files of *version* that
        still look like they are being rendered (see :mod:`render_check`).

        Frame-sequence files are ignored — gaps cover those. *settle_seconds*
        additionally treats recently modified files of unrecognised formats
        as unfinished; auto-promote uses it, manual promotion does not.
        """
        source_path = Path(version.source_path)
        if source_path.is_file():
            candidates = [source_path]
        elif source_path.is_dir():
            try:
                with os.scandir(source_path) as it:
                    candidates = [
                        Path(e.path) for e in it
                        if is_container_file(e.name)
                        and has_media_extension(e.name, self._valid_extensions)
                        and e.is_file()
                    ]
            except OSError:
                return []
            if candidates and self._same_dir(source_path, Path(self.source.source_dir)):
                # Flat layout: only this version's files.
                candidates = self._filter_version_files(sorted(candidates), version)
        else:
            return []
        return find_unfinished_renders(candidates, settle_seconds=settle_seconds)

    def newest_frames_open_for_writing(self, version: VersionInfo,
                                       count: int = 2) -> Optional[str]:
        """Name of one of the last *count* frames of *version* that another
        process still has open for writing, or None.

        Catches the frame a renderer is writing right now (Windows only —
        see :func:`render_check.is_open_for_writing`; elsewhere always None).
        Single-file versions return None; containers are covered by
        :meth:`find_unfinished_renders`.
        """
        source_path = Path(version.source_path)
        try:
            with os.scandir(source_path) as it:
                files = sorted(
                    Path(e.path) for e in it
                    if has_media_extension(e.name, self._valid_extensions)
                    and e.is_file()
                )
        except OSError:
            return None  # not a directory (single file) or unreadable
        if files and self._same_dir(source_path, Path(self.source.source_dir)):
            files = self._filter_version_files(files, version)
        for f in files[-count:]:
            try:
                if is_open_for_writing(f):
                    return f.name
            except OSError:
                continue
        return None

    def _scan_target_media(self, target_dir: Path) -> list:
        """Single os.scandir pass to collect media DirEntry objects from target.

        Returns a list of os.DirEntry filtered to files matching valid extensions.
        Reuse this result instead of scanning the target directory multiple times.
        """
        entries = []
        try:
            with os.scandir(target_dir) as it:
                for entry in it:
                    if (entry.is_file(follow_symlinks=False) or entry.is_symlink()) and has_media_extension(entry.name, self._valid_extensions):
                        entries.append(entry)
        except (PermissionError, OSError):
            pass
        return entries

    @staticmethod
    def _same_dir(a: Path, b: Path) -> bool:
        """Compare two directory paths tolerating symlinks, '..', and platform casing."""
        try:
            ra = str(a.resolve())
        except OSError:
            ra = os.path.abspath(str(a))
        try:
            rb = str(b.resolve())
        except OSError:
            rb = os.path.abspath(str(b))
        return os.path.normcase(ra) == os.path.normcase(rb)

    def _filter_version_files(
        self, files: list[Path], version: VersionInfo
    ) -> list[Path]:
        """Filter files to only those belonging to a specific version.

        When a watched source uses flat versioned files (all versions in the
        same directory), the file list contains every version's files.  This
        method keeps only the files whose embedded version/date (and basename,
        when ``sample_filename`` is set) matches the target *version*.
        """
        from .scanner import VersionScanner
        from .task_tokens import parse_date_to_sortable

        pattern = self.source.version_pattern
        if not pattern:
            return files

        date_format = getattr(self.source, "date_format", "")
        regex = VersionScanner._compile_version_pattern(pattern, date_format)
        scanner = VersionScanner(self.source, self.task_tokens)

        has_version = "{version}" in pattern
        has_date = "{date}" in pattern

        filtered = []
        for f in files:
            if not scanner._matches_basename(f.name):
                continue
            match = regex.search(f.name)
            if not match:
                continue
            groups = match.groups()

            try:
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
            except (IndexError, ValueError):
                continue

            filtered.append(f)

        return filtered

    def _promote_sequence(
        self,
        source_dir: Path,
        target_dir: Path,
        version: VersionInfo,
        progress_callback: Optional[Callable],
        keep_layers: Optional[set[str]] = None,
    ):
        """Copy/symlink a folder of frames to the target via a staging dir.

        All files are fully written into a hidden staging directory inside
        the target first, then swapped into place with same-volume renames
        (:meth:`_swap_staged_into_target`). A failure or cancellation
        mid-copy therefore never leaves the target partially populated —
        the old version stays live until the new one is complete.
        """
        valid_extensions = self._valid_extensions
        source_entries = {e.path: e for e in self._media_entries(source_dir)}
        source_files = [Path(p) for p in source_entries]

        # When source_dir is the watched source root (flat file layout),
        # it contains files from ALL versions — filter to only the target version.
        if self._same_dir(source_dir, Path(self.source.source_dir)):
            source_files = self._filter_version_files(source_files, version)

        if not source_files:
            raise PromotionError(f"No matching files found in {source_dir}")

        # What the staging dir must contain afterwards: staged name -> source
        # size. Two source files renamed to the same target name (or colliding
        # in case on Windows/macOS) would silently overwrite each other — refuse.
        expected: dict[str, Optional[int]] = {}
        seen_norm: dict[str, str] = {}
        for src_file in source_files:
            name = self._remap_filename(src_file.name)
            norm = name.casefold()
            if norm in seen_norm:
                raise PromotionError(
                    f"Two source files map to the same target name '{name}' "
                    f"(conflicts with '{seen_norm[norm]}'; check the file rename "
                    f"template) — nothing was promoted."
                )
            seen_norm[norm] = name
            entry = source_entries.get(str(src_file))
            expected[name] = self._entry_size(entry) if entry is not None else None

        total = len(source_files)
        mode = self.source.link_mode

        staging_dir = self._prepare_staging_dir(target_dir)
        try:
            # Use parallel copy for large sequences in copy mode
            if mode == "copy" and total > 10:
                self._parallel_copy(source_files, staging_dir, total, progress_callback)
            else:
                # Sequential for symlink/hardlink (fast already) or small sets
                for i, src_file in enumerate(source_files):
                    if self._cancelled.is_set():
                        raise PromotionError(
                            "Promotion cancelled by user — target left unchanged."
                        )
                    target_name = self._remap_filename(src_file.name)
                    self._link_or_copy(src_file, staging_dir / target_name)
                    if progress_callback:
                        progress_callback(i + 1, total, src_file.name)
            if self._cancelled.is_set():
                raise PromotionError(
                    "Promotion cancelled by user — target left unchanged."
                )
            self._verify_staged(staging_dir, expected)
        except BaseException:
            _rmtree_force(staging_dir, ignore_errors=True)
            raise

        # Staging is complete — retire the old files (only matching
        # extensions, minus layers the user chose to keep) and activate.
        files_to_remove = self._collect_removal_set(
            target_dir, valid_extensions, keep_layers=keep_layers,
            own_stem=self._compute_latest_basename(version),
        )
        self._swap_staged_into_target(staging_dir, target_dir, files_to_remove)

    def _parallel_copy(
        self,
        source_files: list[Path],
        target_dir: Path,
        total: int,
        progress_callback: Optional[Callable],
    ):
        """Copy files in parallel using a thread pool."""
        completed = 0
        counter_lock = threading.Lock()
        abort = threading.Event()

        def _copy_one(src_file: Path):
            nonlocal completed
            if self._cancelled.is_set() or abort.is_set():
                return
            target_name = self._remap_filename(src_file.name)
            # target_dir is the staging dir, emptied by _prepare_staging_dir,
            # so there is nothing to remove first (that check cost 1-2 stats
            # per frame over SMB).
            try:
                smart_copy(src_file, target_dir / target_name, cancel_event=self._cancelled)
            except BaseException:
                abort.set()
                raise
            with counter_lock:
                completed += 1
                done = completed
            if progress_callback:
                progress_callback(done, total, src_file.name)

        with ThreadPoolExecutor(max_workers=_COPY_WORKERS) as executor:
            futures = [executor.submit(_copy_one, f) for f in source_files]
            # Wait for all to complete, propagate first exception
            first_error: Optional[BaseException] = None
            for future in futures:
                try:
                    future.result()
                except BaseException as exc:
                    abort.set()
                    if first_error is None:
                        first_error = exc
            if first_error is not None:
                raise first_error

        if self._cancelled.is_set():
            raise PromotionError("Promotion cancelled by user — target left unchanged.")

    def _promote_single_file(
        self,
        source_file: Path,
        target_dir: Path,
        progress_callback: Optional[Callable],
        byte_progress_callback: Optional[Callable] = None,
    ):
        """Copy/symlink a single file to the target via the staging dir.

        Only this source's own remapped filename is replaced — other
        sources sharing the same target directory are never touched.
        """
        target_name = self._remap_filename(source_file.name)

        if progress_callback:
            progress_callback(0, 1, source_file.name)

        try:
            source_size: Optional[int] = os.stat(source_file).st_size
        except OSError:
            source_size = None

        staging_dir = self._prepare_staging_dir(target_dir)
        try:
            byte_cb = (_throttled_byte_progress(byte_progress_callback, source_file.name)
                       if byte_progress_callback else None)
            self._link_or_copy(source_file, staging_dir / target_name, progress_cb=byte_cb)
            if self._cancelled.is_set():
                raise PromotionError(
                    "Promotion cancelled by user — target left unchanged."
                )
            self._verify_staged(staging_dir, {target_name: source_size})
        except BaseException:
            _rmtree_force(staging_dir, ignore_errors=True)
            raise

        existing = target_dir / target_name
        files_to_remove = [existing] if (existing.exists() or existing.is_symlink()) else []
        self._swap_staged_into_target(staging_dir, target_dir, files_to_remove)

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

    def _populate_nle_display_fields(self, entry: HistoryEntry,
                                      version: VersionInfo) -> None:
        """Set entry.nle_display_stem and frame/extension flags from project options.

        No-ops when no rename options were provided (legacy callers / tests) —
        the companion scripts then fall back to the historical "use source
        basename verbatim" behaviour.
        """
        opts = self.nle_rename_options
        if not opts:
            return

        # Derive tokens from the *actual version being promoted* so the
        # recovered version digits (used when include_version is on) reflect
        # this promote, not whichever version happened to be the sample at
        # source-add time. source_name/basename are version-stripped during
        # derivation, so swapping the input only changes the version segment.
        # Falls back to the source's sample filename when the version has no
        # source_path (manual entries / tests).
        token_input = (
            Path(version.source_path).name if version.source_path
            else self.source.sample_filename
        )
        tokens = derive_source_tokens(
            token_input,
            self.task_tokens,
            getattr(self.source, "date_format", ""),
            source_title=self.source.name,
        )

        stem = compose_nle_display_stem(
            tokens,
            include_version=bool(opts.get("include_version", True)),
            custom_enabled=bool(opts.get("custom_enabled", False)),
            custom_template=opts.get("custom_template") or "{source_name}",
            group_token_expander=_expand_group_token,
            group=self.source.group,
        )

        entry.nle_display_stem = stem
        entry.nle_display_include_frame = bool(opts.get("include_frame", False))
        entry.nle_display_include_extension = bool(opts.get("include_extension", False))
        entry.nle_metadata_field = opts.get("metadata_field") or ""

    def _compute_latest_basename(self, version: VersionInfo,
                                  written_files: Optional[list] = None) -> str:
        """Return the stem of this source's latest output filename.

        The stem is what the on-disk file looks like minus the frame number
        (for sequences) and the extension. Example: a sequence written as
        ``hero_comp_latest.1001.exr`` gives ``hero_comp_latest``; a single
        ``hero_comp_latest.mov`` also gives ``hero_comp_latest``.

        Companion NLE scripts read this to match an imported clip back to
        the right sidecar, even when many sidecars (one per source) live in
        the same target directory.

        Prefers a name derived from a freshly-written target file, then
        falls back to remapping the source's sample filename, then to the
        version's source path basename.
        """
        sample_name = ""
        if written_files:
            try:
                sample_name = Path(written_files[0]).name
            except Exception:
                sample_name = ""
        if not sample_name and self.source.sample_filename:
            try:
                sample_name = self._remap_filename(self.source.sample_filename)
            except Exception:
                sample_name = ""
        if not sample_name:
            try:
                sample_name = self._remap_filename(Path(version.source_path).name)
            except Exception:
                return ""

        m = _FRAME_EXT_RE.search(sample_name)
        if m:
            return sample_name[:m.start()]
        return Path(sample_name).stem

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
            return self._validate_target_filename(result)

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

        # Expand template tokens in a single pass so a token value containing
        # another literal token is never re-expanded.
        _tok = {
            "{source_title}": tokens["source_title"],
            "{source_name}": tokens["source_name"],
            "{source_basename}": tokens["source_basename"],
            "{source_fullname}": tokens["source_fullname"],
        }
        base = re.sub(
            r"\{(?:source_title|source_name|source_basename|source_fullname)\}",
            lambda m: _tok[m.group(0)],
            template,
        )
        base = _expand_group_token(base, self.source.group)

        # Append layer suffix after template expansion to preserve original position
        if layer_suffix:
            base += layer_suffix
            base = _DOUBLE_DIVIDER_RE.sub(r"\1", base)

        # Reconstruct filename: base + frame + ext
        if frame_num:
            result = f"{base}{frame_sep}{frame_num}.{ext}"
        elif ext:
            result = f"{base}.{ext}"
        else:
            result = base
        return self._validate_target_filename(result)

    @staticmethod
    def _validate_target_filename(name: str) -> str:
        """Reject filenames that could escape the staging/target directory."""
        if (
            not name
            or name in (".", "..")
            or "/" in name
            or "\\" in name
            or "\x00" in name
            or Path(name).name != name
            or (len(name) >= 2 and name[1] == ":")
        ):
            raise PromotionError(
                f"Invalid remapped target filename {name!r} "
                f"(must be a single filename without path separators or '..')."
            )
        return name

    @staticmethod
    def _set_hidden_attribute(path: Path) -> None:
        """Best-effort: mark a dot-directory hidden on Windows so artists
        browsing the latest folder don't see promotion internals."""
        if platform.system() != "Windows":
            return
        try:
            import ctypes
            FILE_ATTRIBUTE_HIDDEN = 0x2
            ctypes.windll.kernel32.SetFileAttributesW(str(path), FILE_ATTRIBUTE_HIDDEN)
        except Exception:
            pass

    def _prepare_staging_dir(self, target_dir: Path) -> Path:
        """Create an empty staging directory inside the target."""
        staging_dir = target_dir / STAGING_DIRNAME
        if staging_dir.exists() or staging_dir.is_symlink():
            _rmtree_force(staging_dir, ignore_errors=False)
        try:
            staging_dir.mkdir(parents=True, exist_ok=True)
            with os.scandir(staging_dir) as it:
                if any(True for _ in it):
                    raise OSError("staging directory is not empty after cleanup")
        except OSError as e:
            raise PromotionError(
                f"Cannot create staging directory {staging_dir}: {e}"
            ) from e
        self._set_hidden_attribute(staging_dir)
        return staging_dir

    def _foreign_target_stems(self, target_dir: Path) -> Optional[list[str]]:
        """Output stems of *other* sources promoting into *target_dir*.

        Sources sharing a target each keep their own history sidecar there
        (see ``config._migrate_shared_history_filenames``), and every sidecar
        records the stem its source writes (``latest_basename``). Returns
        ``None`` when no other sidecar exists (the target belongs to this
        source alone), otherwise the list of known foreign stems — an
        unreadable sidecar or one written before ``latest_basename``
        existed contributes no stem but still marks the target as shared.
        """
        own_name = self.history.path.name
        shared = False
        stems: list[str] = []
        try:
            with os.scandir(target_dir) as it:
                sidecars = [
                    e.path for e in it
                    if e.name != own_name
                    and e.name.startswith(".latest_history")
                    and e.name.endswith(".json")
                    and e.is_file()
                ]
        except OSError:
            return None
        for path in sidecars:
            shared = True
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                stem = ((data or {}).get("current") or {}).get("latest_basename") or ""
            except (OSError, ValueError, AttributeError):
                stem = ""
            if stem:
                stems.append(stem)
        return stems if shared else None

    def _drop_foreign_files(self, files: list[Path], target_dir: Path,
                            own_stem: str) -> list[Path]:
        """Remove files that belong to another source sharing *target_dir*.

        Each file is attributed to the source whose output stem is the
        longest prefix of its name — so this source's layer files
        (``hero_comp_alpha.1001.exr`` for stem ``hero_comp``) stay its own,
        while ``hero_comp_bg.1001.exr`` goes to a source writing
        ``hero_comp_bg``. In a shared target, files matching no known stem
        are left alone: when in doubt, never delete. A target with no other
        sources' sidecars is returned unchanged.
        """
        foreign = self._foreign_target_stems(target_dir)
        if foreign is None:
            return list(files)
        own = (own_stem or "").casefold()
        candidates = [(s.casefold(), False) for s in foreign]
        if own:
            candidates.append((own, True))
        kept = []
        dropped = 0
        for f in files:
            name = f.name.casefold()
            best_len, is_own = -1, False
            for stem, mine in candidates:
                if not name.startswith(stem):
                    continue
                # Require a delimiter boundary (or exact match) so stem "shot1"
                # does not falsely match "shot10.exr".
                if len(name) > len(stem) and name[len(stem)] not in "._-":
                    continue
                # Ties go to this source: an equal foreign stem is usually
                # this source's own pre-migration sidecar (and a genuine
                # same-stem clash is flagged by conflicts.py anyway).
                if len(stem) > best_len or (mine and len(stem) == best_len):
                    best_len, is_own = len(stem), mine
            if is_own:
                kept.append(f)
            else:
                dropped += 1
        if dropped:
            logger.info(
                "Shared target %s: leaving %d file(s) of other sources untouched",
                target_dir, dropped,
            )
        return kept

    def _collect_removal_set(self, target_dir: Path, valid_extensions: frozenset,
                             keep_layers: Optional[set[str]] = None,
                             own_stem: str = "") -> list[Path]:
        """Files the swap phase should retire from the target directory.

        Media files matching the source's extensions (minus files belonging
        to layers in *keep_layers*) plus any symlinks. When other sources
        share the target, their files are excluded (see
        :meth:`_drop_foreign_files`). The history sidecar, the lock file,
        and the hidden staging/backup dirs are never included.
        """
        try:
            with os.scandir(target_dir) as it:
                entries = list(it)
        except OSError as e:
            raise PromotionError(f"Cannot read target directory {target_dir}: {e}") from e

        def is_media(e) -> bool:
            return has_media_extension(e.name, valid_extensions) and e.is_file()

        # Build a set of filenames to preserve
        if keep_layers:
            media_files = [Path(e.path) for e in entries if is_media(e)]
            groups = _group_files_by_sequence(media_files) if media_files else {}
            keep_files: set[str] = set()
            for prefix, files in groups.items():
                if prefix in keep_layers:
                    keep_files.update(f.name for f in files)
        else:
            keep_files = set()

        removal: list[Path] = []
        for e in entries:
            if is_media(e):
                if e.name not in keep_files:
                    removal.append(Path(e.path))
            elif e.is_symlink() and has_media_extension(e.name, valid_extensions):
                if e.name not in keep_files:
                    removal.append(Path(e.path))
        return self._drop_foreign_files(removal, target_dir, own_stem)

    @staticmethod
    def _entry_size(entry) -> Optional[int]:
        try:
            return entry.stat().st_size
        except OSError:
            return None

    def _verify_staged(self, staging_dir: Path, expected: dict) -> None:
        """Check the staging dir before anything in the target is touched.

        *expected* maps each staged name to the size of its source file
        (None = unknown, only presence is checked). One listing of the
        staging dir; sizes follow symlinks, so in symlink mode this also
        catches a link that doesn't resolve. On Windows the sizes come with
        the listing; elsewhere each one is a stat of a file just written.

        Raises PromotionError (the caller discards the staging dir, so the
        target keeps the previous version) when a file is missing, there is
        an unexpected extra file, or a size differs from its source — for
        example a copy cut short, or a source still being written.
        """
        try:
            with os.scandir(staging_dir) as it:
                staged = {e.name: e for e in it}
        except OSError as e:
            raise PromotionError(f"Cannot read staging directory {staging_dir}: {e}") from e

        problems = []
        for name in sorted(set(expected) - set(staged)):
            problems.append(f"{name}: missing from the staged copy")
        for name in sorted(set(staged) - set(expected)):
            problems.append(f"{name}: not expected in the staged copy")
        for name in sorted(set(expected) & set(staged)):
            want = expected[name]
            if want is None:
                continue
            got = self._entry_size(staged[name])
            if got is None:
                problems.append(f"{name}: staged file can't be read")
            elif got != want:
                problems.append(
                    f"{name}: {got:,} bytes staged, source has {want:,} "
                    f"(copy cut short, or the source is still being written)"
                )
        if problems:
            shown = "\n  ".join(problems[:10])
            more = f"\n  ... and {len(problems) - 10} more" if len(problems) > 10 else ""
            raise PromotionError(
                f"Staged copy failed verification — target left unchanged:\n  {shown}{more}"
            )
        logger.debug("Verified %d staged file(s) in %s", len(expected), staging_dir)

    @staticmethod
    def _run_renames(fn, names: list[str]) -> None:
        """Call *fn(name)* for every name, in parallel for larger sets.

        After the first failure no further names are started (renames
        already in flight finish), then the first exception is re-raised so
        the caller can roll back.
        """
        if len(names) < _SWAP_PARALLEL_MIN:
            for name in names:
                fn(name)
            return
        errors: list[BaseException] = []
        stop = threading.Event()

        def run(name):
            if stop.is_set():
                return
            try:
                fn(name)
            except BaseException as e:  # noqa: BLE001 - re-raised below
                stop.set()
                errors.append(e)

        with ThreadPoolExecutor(max_workers=min(_SWAP_WORKERS, len(names))) as executor:
            list(executor.map(run, names))
        if errors:
            raise errors[0]

    def _swap_staged_into_target(self, staging_dir: Path, target_dir: Path,
                                 files_to_remove: list[Path]) -> None:
        """Activate a fully-staged version, renaming each staged file into
        the target and parking whatever it replaces in a backup dir.

        Every step is a same-volume rename. Each name is swapped as one unit
        — old file to the backup, new file into place — so a frame is
        missing for the duration of one rename, not for the whole swap, and
        the renames run in parallel (:data:`_SWAP_WORKERS`). Old files that
        have no replacement (e.g. frames outside the new range) are retired
        after all new files are in place. While the swap runs the target
        holds a mix of old and new frames; how long that lasts is logged.

        Any existing file a staged file would overwrite is backed up, not
        only those in *files_to_remove*, so a rollback restores everything.
        On failure the placed files are removed and the backup is restored;
        even on a double fault (swap fails AND restore fails) the old files
        survive in the backup dir, which the next promotion restores via
        :meth:`_cleanup_orphaned_promotion_dirs`.
        """
        started = time.monotonic()
        backup_dir = target_dir / BACKUP_DIRNAME
        try:
            with os.scandir(staging_dir) as it:
                staged = sorted(e.name for e in it)
            with os.scandir(target_dir) as it:
                existing = {e.name for e in it} - {STAGING_DIRNAME, BACKUP_DIRNAME}
        except OSError as e:
            _rmtree_force(staging_dir, ignore_errors=True)
            raise PromotionError(f"Cannot read target directory {target_dir}: {e}") from e

        # Key by casefolded filename so case-only renames (e.g. Shot.exr -> shot.exr)
        # are matched on Windows NTFS, macOS APFS (where posixpath.normcase is a
        # no-op!), and Linux clients writing to case-insensitive SMB/NAS shares.
        existing_by_key: dict[str, list[str]] = {}
        for n in sorted(existing):
            existing_by_key.setdefault(n.casefold(), []).append(n)
        staged_by_key = {n.casefold(): n for n in staged}
        retire_by_key: dict[str, list[str]] = {}
        for f in files_to_remove:
            retire_by_key.setdefault(f.name.casefold(), []).append(f.name)

        backup_keys = set(retire_by_key) | (set(existing_by_key) & set(staged_by_key))
        leftovers = sorted(
            name
            for k in (set(retire_by_key) - set(staged_by_key))
            for name in retire_by_key[k]
        )
        replaced_count = sum(
            len(existing_by_key[k])
            for k in (backup_keys & set(staged_by_key))
            if k in existing_by_key
        )

        lock = threading.Lock()
        moved: list[str] = []    # exact names parked in backup_dir
        placed: list[str] = []   # staged names renamed into target_dir

        def swap_one(name: str) -> None:
            key = name.casefold()
            if key in backup_keys and key in existing_by_key:
                for old_name in existing_by_key[key]:
                    os.replace(target_dir / old_name, backup_dir / old_name)
                    with lock:
                        moved.append(old_name)
            os.replace(staging_dir / name, target_dir / name)
            with lock:
                placed.append(name)

        def retire_one(name: str) -> None:
            os.replace(target_dir / name, backup_dir / name)
            with lock:
                moved.append(name)

        try:
            if backup_keys:
                if backup_dir.exists():
                    _rmtree_force(backup_dir, ignore_errors=True)
                backup_dir.mkdir(parents=True, exist_ok=True)
                self._set_hidden_attribute(backup_dir)
            self._run_renames(swap_one, staged)
            self._run_renames(retire_one, leftovers)
        except BaseException as e:
            # Roll back: drop whatever was already placed, then return the
            # old files from the backup dir.
            for name in placed:
                _unlink_force(target_dir / name)
            restore_failed = False
            for name in moved:
                try:
                    os.replace(backup_dir / name, target_dir / name)
                except OSError as r:
                    restore_failed = True
                    logger.critical("Could not restore %s from backup: %s",
                                    target_dir / name, r)
            _rmtree_force(staging_dir, ignore_errors=True)
            if restore_failed:
                if isinstance(e, Exception):
                    raise PromotionError(
                        f"Failed to activate new version: {e}. Some previous files "
                        f"could not be restored and remain in {BACKUP_DIRNAME}; "
                        f"they will be recovered automatically on the next promotion."
                    ) from e
                raise
            _rmtree_force(backup_dir, ignore_errors=True)
            if isinstance(e, Exception):
                raise PromotionError(
                    f"Failed to activate new version; previous version restored: {e}"
                ) from e
            raise

        self.last_swap_seconds = time.monotonic() - started
        logger.info(
            "Swapped %d file(s) into %s in %.2fs (%d replaced, %d retired)",
            len(staged), target_dir, self.last_swap_seconds,
            replaced_count, len(leftovers),
        )
        _rmtree_force(staging_dir, ignore_errors=True)
        if backup_keys:
            # Mark backup_dir as committed BEFORE deleting so a failed rmtree
            # can never resurrect retired files on the next promotion.
            try:
                (backup_dir / _SWAP_COMMITTED_MARKER).write_text("ok", encoding="utf-8")
            except OSError:
                pass
            try:
                _rmtree_force(backup_dir, ignore_errors=False)
            except OSError as e:
                logger.warning(
                    "Could not remove backup dir %s (cleaned on next promote): %s",
                    backup_dir, e,
                )

    def _cleanup_orphaned_promotion_dirs(self, target_dir: Path) -> None:
        """Recover from a previous crashed promotion. Only runs under the lock.

        If ``.lvm_backup/.lvm_committed`` is present, the previous swap already
        completed and the backup dir is just leftover trash — delete it without
        moving retired files back. Otherwise a crash occurred mid-swap, so
        every backed-up file is restored to *target_dir* (overwriting any
        half-placed new files). Leftover staging is always discarded.
        """
        backup_dir = target_dir / BACKUP_DIRNAME
        if backup_dir.is_dir():
            committed = (backup_dir / _SWAP_COMMITTED_MARKER).exists()
            if committed:
                logger.info(
                    "Discarding already-committed backup dir from prior promotion: %s",
                    backup_dir,
                )
            else:
                restored = 0
                try:
                    for f in list(backup_dir.iterdir()):
                        if f.name == _SWAP_COMMITTED_MARKER:
                            continue
                        dest = target_dir / f.name
                        try:
                            os.replace(f, dest)
                            restored += 1
                        except OSError as e:
                            logger.warning("Could not restore %s from backup: %s", f.name, e)
                except OSError as e:
                    logger.warning("Could not read backup dir %s: %s", backup_dir, e)
                if restored:
                    logger.warning(
                        "Recovered %d file(s) from an interrupted promotion in %s",
                        restored, target_dir,
                    )
            _rmtree_force(backup_dir, ignore_errors=True)

        staging_dir = target_dir / STAGING_DIRNAME
        if staging_dir.exists() or staging_dir.is_symlink():
            logger.info(
                "Discarding leftover staging dir from interrupted promotion: %s",
                staging_dir,
            )
            _rmtree_force(staging_dir, ignore_errors=True)

    def _preflight_disk_space(self, version: VersionInfo, target_dir: Path) -> None:
        """Refuse to start a copy-mode promotion that cannot fit on the
        target volume.

        Peak usage during a staged promotion is exactly the new version's
        size — old files are renamed into the backup dir, never copied.
        Skipped for link modes and when the version's size is unknown
        (e.g. manually imported versions).
        """
        if self.source.link_mode != "copy":
            return
        need = getattr(version, "total_size_bytes", 0) or 0
        if need <= 0:
            logger.debug("Version size unknown — skipping disk space preflight")
            return
        try:
            free = shutil.disk_usage(target_dir).free
        except OSError as e:
            logger.debug("disk_usage failed for %s (%s) — skipping preflight", target_dir, e)
            return
        margin = max(64 * 1024 * 1024, need // 50)
        if free < need + margin:
            raise PromotionError(
                f"Not enough free space on the target volume: this version "
                f"needs ~{_human_size(need)} (+{_human_size(margin)} headroom) "
                f"but only {_human_size(free)} is free at {target_dir}"
            )

    def _link_or_copy(self, source: Path, target: Path, progress_cb=None):
        """Route to the correct file operation based on link_mode.

        *progress_cb(bytes_copied, total_bytes)* is passed to the copy (links
        are instant and report nothing).

        *target* is always inside the freshly emptied staging dir, so no
        existence check / unlink is needed (it cost 1-2 stats per frame).
        """
        mode = self.source.link_mode
        if mode == "symlink":
            self._create_symlink(source, target)
        elif mode == "hardlink":
            self._create_hardlink(source, target)
        else:
            smart_copy(source, target, cancel_event=self._cancelled, progress_cb=progress_cb)

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

        Raises PromotionError if the target directory itself is unreadable.
        """
        locked = []

        if os.name != "nt":
            # POSIX allows atomic rename/unlink over open files and open('a')
            # does not detect file locks; skip unnecessary per-file opens.
            return []

        if cached_entries is not None:
            for e in cached_entries:
                if _is_file_locked_win32(e.path, is_symlink=e.is_symlink()):
                    locked.append(e.name)
            return locked

        try:
            entries = self._media_entries(target_dir)
        except OSError as e:
            raise PromotionError(f"Cannot read target directory {target_dir}: {e}") from e

        for e in entries:
            if _is_file_locked_win32(e.path, is_symlink=e.is_symlink()):
                locked.append(e.name)

        return locked

    def get_current_version(self) -> Optional[HistoryEntry]:
        """Get the currently promoted version."""
        return self.history.get_current()

    def get_history(self) -> list[HistoryEntry]:
        """Get the full promotion history."""
        return self.history.get_history()

    def undo(
        self,
        n: int = 1,
        dry_run: bool = False,
        user: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ):
        """Undo the last N promotes by re-promoting the previous version.

        The targeted historical version is *re-promoted* as a new promote —
        hooks fire, and a new history entry is prepended (older entries are
        preserved, never deleted).

        Returns the new HistoryEntry on a real undo, or the dry_run dict
        when dry_run=True. Raises PromotionError when there isn't enough
        history or the source files for the target version are missing.
        """
        if n < 1:
            raise PromotionError(f"undo steps must be >= 1, got {n}")

        prev = self.history.get_previous(n)
        if prev is None:
            depth = len(self.history.get_history())
            raise PromotionError(
                f"Cannot undo {n} step(s): history only has {depth} entr"
                f"{'y' if depth == 1 else 'ies'}."
            )

        # Re-scan the source so we have a fresh VersionInfo with current
        # file paths / sizes — historical metadata alone is not enough.
        from .scanner import VersionScanner
        scanner = VersionScanner(self.source, self.task_tokens)
        versions = scanner.scan()

        target_version = None
        prev_source_norm = os.path.normcase(os.path.normpath(prev.source)) if prev.source else ""
        fallback_version = None
        for v in versions:
            if version_strings_match(v.version_string, prev.version, v.version_number):
                if prev_source_norm and os.path.normcase(os.path.normpath(v.source_path)) == prev_source_norm:
                    target_version = v
                    break
                if fallback_version is None:
                    fallback_version = v
        if target_version is None:
            target_version = fallback_version

        if target_version is None:
            raise PromotionError(
                f"Cannot undo: source files for {prev.version} no longer "
                f"exist on disk."
            )

        if dry_run:
            return self.dry_run(target_version)

        return self.promote(
            target_version,
            user=user,
            progress_callback=progress_callback,
        )

    def dry_run(self, version: VersionInfo) -> dict:
        """Preview the file mapping for a promotion without copying anything.

        Returns a dict with source_dir, target_dir, file_map, total_files,
        total_size_bytes, and link_mode.
        """
        source_path = Path(version.source_path)
        target_dir = Path(self.source.latest_target)
        valid_extensions = self._valid_extensions

        if source_path.is_dir():
            source_entries_map = {e.path: e for e in self._media_entries(source_path)}
            source_files = [Path(p) for p in source_entries_map]
            if self._same_dir(source_path, Path(self.source.source_dir)):
                source_files = self._filter_version_files(source_files, version)
            source_entries = [source_entries_map.get(str(f)) for f in source_files]
        else:
            source_entries = None
            source_files = [source_path] if source_path.is_file() else []

        file_map = []
        total_size = 0
        for i, f in enumerate(source_files):
            try:
                # DirEntry.stat() reuses the listing's data on Windows.
                entry = source_entries[i] if source_entries is not None else None
                st = entry.stat() if entry is not None else f.stat()
                size = st.st_size
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
                        if entry.is_file(follow_symlinks=False) or entry.is_symlink():
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
        if source_path.is_dir() and self._same_dir(source_path, Path(self.source.source_dir)):
            return self._filter_version_files(self._list_media(source_path), version)
        return None

    @staticmethod
    def _extract_version_number(version_str: str) -> Optional[int]:
        """Extract integer version number from a version string like 'v003'."""
        m = re.search(r'(\d+)', version_str)
        return int(m.group(1)) if m else None

    def _filter_to_own_target_files(self, target_entries: list) -> list:
        """Restrict target-dir entries to those this source's promotions
        would actually write.

        When several sources share a ``latest_target`` (e.g. a folder of
        per-shot .mov outputs), the directory listing contains files from
        every source. Counting all of them as "this source's promoted
        files" makes integrity / mtime checks fire spurious warnings. This
        helper returns only entries whose name matches what
        :meth:`_remap_filename` would produce for this source's own
        sample, treating frame numbers as wildcards for sequences.

        Returns the input list unchanged when the source has no
        ``sample_filename`` (we then can't tell which files are ours, so
        fall back to the legacy "all files" comparison).
        """
        sample = self.source.sample_filename
        if not sample:
            return list(target_entries)

        try:
            expected = self._remap_filename(sample)
        except Exception:
            return list(target_entries)

        if not is_container_file(expected):
            m = _FRAME_EXT_RE.search(expected)
            if m:
                sep = m.group(1)
                ext = m.group(3)
                stem = expected[:m.start()]
                own_re = re.compile(
                    re.escape(stem) + r"(?:[._\-].+)?" + re.escape(sep) + r"\d+\." + re.escape(ext) + r"$",
                    re.IGNORECASE,
                )
                matched = [e for e in target_entries if own_re.match(e.name)]
                if matched:
                    target_dir = Path(matched[0].path).parent
                    kept_paths = set(self._drop_foreign_files(
                        [Path(e.path) for e in matched], target_dir, stem
                    ))
                    return [e for e in matched if Path(e.path) in kept_paths]
                return []

        # Single-file source — exact name match (case-insensitive on Windows).
        expected_lower = expected.lower()
        return [e for e in target_entries if e.name.lower() == expected_lower]

    def verify(self) -> dict:
        """Check integrity of the latest target vs history.

        Checks file count, source staleness (re-rendered since promotion),
        and target staleness (externally overwritten).

        Scans the target directory once and reuses the results for both
        the file-count integrity check and the mtime staleness check.
        Files belonging to other sources sharing the target directory are
        filtered out via :meth:`_filter_to_own_target_files` so they don't
        trigger false integrity failures.
        """
        target_dir = Path(self.source.latest_target)
        if not target_dir.exists():
            return {"valid": True, "message": "Target directory doesn't exist yet."}

        # Single scan of target directory — reuse for file list and mtime
        target_entries = self._scan_target_media(target_dir)
        own_entries = self._filter_to_own_target_files(target_entries)
        actual_files = [e.name for e in own_entries]

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
            if source_path.is_dir() and self._same_dir(source_path, Path(self.source.source_dir)):
                ver_num = self._extract_version_number(current.version)
                date_sortable = 0
                date_fmt = getattr(self.source, "date_format", "")
                if date_fmt:
                    from .task_tokens import parse_date_to_sortable
                    # current.version may contain an ISO date like '2026-03-01' or raw digits
                    m_iso = re.search(r"(\d{4})-(\d{2})-(\d{2})", current.version or "")
                    if m_iso:
                        date_sortable = int(f"{m_iso.group(1)}{m_iso.group(2)}{m_iso.group(3)}")
                    else:
                        date_sortable = parse_date_to_sortable(current.version or "", date_fmt)
                if ver_num is not None or date_sortable:
                    stub = VersionInfo(
                        current.version,
                        ver_num if ver_num is not None else 0,
                        current.source,
                        date_sortable=date_sortable,
                    )
                    version_files = self._get_version_source_files(source_path, stub)
            current_source_mtime = self._get_max_mtime(source_path, files=version_files)
            if current_source_mtime is not None and current_source_mtime > current.source_mtime + 1.0:
                return {
                    "valid": False,
                    "message": f"Source files for {current.version} modified since promotion "
                               f"— may have been re-rendered.",
                }

        # Check if target files were overwritten externally — only inspect
        # this source's own files; other sources sharing the dir promote on
        # their own schedule and their mtimes shouldn't trip our check.
        if current.target_mtime is not None:
            max_mt = 0.0
            for entry in own_entries:
                try:
                    mt = entry.stat().st_mtime
                    if mt > max_mt:
                        max_mt = mt
                except OSError:
                    pass
            current_target_mtime = max_mt if own_entries else None
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
