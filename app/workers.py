"""Background QThread workers used by the GUI."""

import threading

from app._common import *  # noqa: F401,F403
from app._common import (
    _STATUS_MARKERS,
    _STATUS_LABELS,
    _STATUS_COLORS,
    _OVERRIDE_COLOR,
    _CONFLICT_COLOR,
    _DEFAULT_GROUP_COLOR_HEX,
    _GROUP_COLOR_CACHE,
    _REVEAL_LABEL,
    _BASE_DIR,
    _PLATFORM,
    _SINGLE_FILE_EXTS,
    _group_qcolor,
    _load_app_icon,
    _preview_sample_suffix,
    _expand_group_token,
    _resolve_group_root,
)


class PromoteWorker(QThread):
    """Runs the file copy in a background thread."""
    progress = Signal(int, int, str)   # current, total, filename
    finished = Signal(object)          # HistoryEntry on success
    error = Signal(str)                # error message

    def __init__(self, promoter: Promoter, version: VersionInfo, parent=None,
                 force=False, pinned=False, keep_layers=None):
        super().__init__(parent)
        self.promoter = promoter
        self.version = version
        self.force = force
        self.pinned = pinned
        self.keep_layers = keep_layers

    def cancel(self):
        """Request cancellation of the running promotion."""
        self.promoter.cancel()

    def run(self):
        try:
            entry = self.promoter.promote(
                self.version,
                progress_callback=self._on_progress,
                force=self.force,
                pinned=self.pinned,
                keep_layers=self.keep_layers,
            )
            self.finished.emit(entry)
        except PromotionError as e:
            self.error.emit(str(e))
        except Exception as e:
            self.error.emit(f"Unexpected error: {e}")

    def _on_progress(self, current, total, filename):
        self.progress.emit(current, total, filename)


# ---------------------------------------------------------------------------
# Worker thread for thumbnail generation
# ---------------------------------------------------------------------------


class ThumbnailWorker(QThread):
    finished = Signal(str)  # path to thumbnail or empty string

    def __init__(self, source_path, version_string, extensions, cache_dir, parent=None):
        super().__init__(parent)
        self.source_path = source_path
        self.version_string = version_string
        self.extensions = extensions
        self.cache_dir = cache_dir

    def run(self):
        from src.lvm.thumbnail import get_thumbnail
        result = get_thumbnail(self.source_path, self.version_string, self.extensions, self.cache_dir)
        self.finished.emit(result or "")


class ThumbnailQueueWorker(QThread):
    """Drains a queue of thumbnail requests for the version-tree icons.

    One long-lived thread, so at most one ffmpeg/oiiotool subprocess runs
    at a time. Requests are LIFO (newest first) and deduped by key, so
    re-requesting the selected version bumps it to the front of the queue.
    Disk-cache hits in get_thumbnail return instantly, which makes
    re-queues cheap.
    """

    ready = Signal(str, str)  # key (= version source_path), thumb path or ""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._pending: list[tuple] = []  # stack of (key, version, exts, cache_dir)
        self._cond = threading.Condition()
        self._stopped = False

    def request(self, source_path: str, version_string: str,
                extensions: list, cache_dir: str) -> None:
        """Queue a thumbnail (front of the stack). Dedupes by source_path."""
        with self._cond:
            self._pending = [p for p in self._pending if p[0] != source_path]
            self._pending.append(
                (source_path, version_string, list(extensions), cache_dir)
            )
            self._cond.notify()

    def clear_pending(self) -> None:
        """Drop queued requests (e.g. when another source is selected)."""
        with self._cond:
            self._pending.clear()

    def stop(self) -> None:
        """Stop the drain loop and wake it if it's waiting."""
        with self._cond:
            self._stopped = True
            self._cond.notify_all()

    def run(self):
        from src.lvm.thumbnail import get_thumbnail
        while True:
            with self._cond:
                while not self._pending and not self._stopped:
                    self._cond.wait()
                if self._stopped:
                    return
                key, version_string, extensions, cache_dir = self._pending.pop()
            try:
                result = get_thumbnail(key, version_string, extensions, cache_dir)
            except Exception:
                result = ""
            self.ready.emit(key, result or "")


# ---------------------------------------------------------------------------
# Worker threads for update checking / downloading
# ---------------------------------------------------------------------------


class UpdateCheckWorker(QThread):
    """Checks GitHub for a newer release in a background thread."""
    finished = Signal(object)  # ReleaseInfo or None
    error = Signal(str)

    def __init__(self, current_version: str, parent=None):
        super().__init__(parent)
        self.current_version = current_version

    def run(self):
        try:
            from src.lvm.updater import check_for_update
            release = check_for_update(self.current_version)
            self.finished.emit(release)
        except Exception as e:
            self.error.emit(str(e))



class UpdateDownloadWorker(QThread):
    """Downloads the update ZIP in a background thread."""
    progress = Signal(int, int)    # bytes_downloaded, total_bytes
    finished = Signal(str)         # path to downloaded ZIP
    error = Signal(str)

    def __init__(self, release_info, dest_dir: str, parent=None):
        super().__init__(parent)
        self.release_info = release_info
        self.dest_dir = dest_dir

    def run(self):
        try:
            from src.lvm.updater import download_update
            zip_path = download_update(
                self.release_info,
                self.dest_dir,
                progress_callback=lambda current, total: self.progress.emit(current, total),
            )
            self.finished.emit(str(zip_path))
        except Exception as e:
            self.error.emit(str(e))



class ScanWorker(QThread):
    """Scans project sources in a background thread."""
    progress = Signal(int, int, str)  # current_index, total, source_name
    finished = Signal(dict)           # {source_name: (versions, status_info)}
    error = Signal(str)

    def __init__(self, config: ProjectConfig, sources=None, previous_cache: dict[str, list] = None, parent=None):
        super().__init__(parent)
        self.config = config
        self._sources = sources or config.watched_sources
        self.previous_cache = previous_cache or {}

    def run(self):
        try:
            results = {}
            total = len(self._sources)
            tc_mode = self.config.timecode_mode

            def _scan_one(source):
                scanner = VersionScanner(source, self.config.task_tokens)
                versions = scanner.scan()
                return source.name, versions

            worker_count = min(8, total)
            if worker_count > 1:
                completed = 0
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    futures = {
                        executor.submit(_scan_one, s): s
                        for s in self._sources
                    }
                    for future in as_completed(futures):
                        name, versions = future.result()
                        results[name] = versions
                        completed += 1
                        self.progress.emit(completed, total, name)
            else:
                for i, source in enumerate(self._sources):
                    self.progress.emit(i + 1, total, source.name)
                    name, versions = _scan_one(source)
                    results[name] = versions

            # Phase 2: populate timecodes in one flat parallel pool over
            # all (source, version) pairs. Avoids the nested-pool trap of
            # 8 outer × 8 inner = 64 ffprobes saturating the box, and lets
            # one slow source not stall others.
            if tc_mode == "always":
                all_versions = [v for versions in results.values() for v in versions]
                populate_timecodes_parallel(all_versions, max_workers=8)

            self.finished.emit(results)
        except Exception as e:
            self.error.emit(str(e))



class StatusWorker(QThread):
    """Computes source statuses (verify, conflicts) in a background thread.

    Runs Promoter.verify() and conflict detection off the main thread so the
    UI stays responsive after scanning completes.  Per-source work is
    parallelised with a ThreadPoolExecutor for I/O-bound speedup.
    """
    finished = Signal(dict, dict, dict, dict)  # source_status, target_conflicts, promoters, scanners

    def __init__(self, config: ProjectConfig, versions_cache: dict,
                 sources=None, parent=None):
        super().__init__(parent)
        self._config = config
        self._versions_cache = versions_cache
        self._sources = sources or config.watched_sources

    def run(self):
        from src.lvm.conflicts import detect_target_conflicts

        def _compute_one(source):
            versions = self._versions_cache.get(source.name, [])
            scanner = VersionScanner(source, self._config.task_tokens)
            highest = versions[-1] if versions else None
            highest_ver = highest.version_string if highest else None
            highest_num = highest.version_number if highest else None
            current = None
            status = "no_target"
            integrity = None
            promoter = None

            if source.latest_target:
                promoter = Promoter(source, self._config.task_tokens, self._config.project_name,
                                    nle_rename_options=self._config.nle_rename_options())
                current = promoter.get_current_version()

                if not current:
                    status = "no_version"
                elif version_strings_match(highest_ver, current.version, highest_num):
                    integrity = promoter.verify()
                    if integrity["valid"]:
                        status = "highest"
                    elif "modified since promotion" in integrity.get("message", ""):
                        status = "stale"
                    else:
                        status = "integrity_fail"
                elif getattr(current, 'pinned', False) and not has_newer_versions_since(current, versions):
                    integrity = promoter.verify()
                    if not integrity["valid"] and "modified since promotion" in integrity.get("message", ""):
                        status = "stale"
                    else:
                        status = "deliberate"
                else:
                    integrity = promoter.verify()
                    if not integrity["valid"] and "modified since promotion" in integrity.get("message", ""):
                        status = "stale"
                    else:
                        status = "newer"

            status_info = {
                "current": current,
                "status": status,
                "has_overrides": source.has_overrides,
                "integrity": integrity,
            }
            return source.name, status_info, promoter, scanner

        source_status = {}
        promoters = {}
        scanners = {}

        sources = self._sources
        worker_count = min(8, len(sources))
        if worker_count > 1:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = {executor.submit(_compute_one, s): s for s in sources}
                for future in as_completed(futures):
                    name, status_info, promoter, scanner = future.result()
                    source_status[name] = status_info
                    if promoter:
                        promoters[name] = promoter
                    scanners[name] = scanner
        else:
            for source in sources:
                name, status_info, promoter, scanner = _compute_one(source)
                source_status[name] = status_info
                if promoter:
                    promoters[name] = promoter
                scanners[name] = scanner

        conflicts = detect_target_conflicts(self._config, self._config.task_tokens)
        target_conflicts = {}
        for target, name_a, name_b in conflicts:
            target_conflicts.setdefault(name_a, []).append(name_b)
            target_conflicts.setdefault(name_b, []).append(name_a)

        self.finished.emit(source_status, target_conflicts, promoters, scanners)



class SyncNamesWorker(QThread):
    """Runs the Resolve rename in-process on a worker thread.

    The previous design spawned ``[sys.executable, companion_script]`` as a
    subprocess. That breaks in PyInstaller frozen builds where
    ``sys.executable`` is the LVM ``.exe`` itself (not a Python interpreter)
    — the .exe ignores the script argument and just opens a second copy of
    LVM. Calling ``DaVinciResolveScript`` straight from this Python via
    ctypes works in both source and frozen builds and is faster too.
    """
    line = Signal(str, str)   # level ("info"|"warning"|"error"), text
    done = Signal(bool, str)  # ok, error

    def __init__(self, parent=None):
        super().__init__(parent)

    def run(self):
        from src.lvm.nle_bridge import run_resolve_in_process
        try:
            stats = run_resolve_in_process(self.line.emit)
        except Exception as e:
            self.done.emit(False, f"Unexpected error: {e}")
            return
        ok = bool(stats.get("ok")) and stats.get("errors", 0) == 0
        self.done.emit(ok, "")



class ProjectLoadWorker(QThread):
    """Loads a project config + scan cache off the UI thread.

    ``load_config`` does N × ``Path.resolve()`` syscalls (one per source for
    source_dir / latest_target / manual_versions / group root_dir). On SMB
    each is ~10-30ms, so a 50-source project freezes the UI for 1-3s if loaded
    synchronously. ``load_cache`` then deserialises VersionInfo entries — also
    pure I/O. Both run here so file open feels instant.
    """
    finished = Signal(object, object, str)  # config, cached_versions_or_None, path
    error = Signal(str, str)                 # message, path

    def __init__(self, path: str, parent=None):
        super().__init__(parent)
        self._path = path

    def run(self):
        try:
            config = load_config(self._path)
            from src.lvm.scan_cache import load_cache
            cached = load_cache(self._path, config.watched_sources) or None
            self.finished.emit(config, cached, self._path)
        except Exception as e:
            self.error.emit(str(e), self._path)


# ---------------------------------------------------------------------------
# Version tree with drag-and-drop support for manual version import
# ---------------------------------------------------------------------------

