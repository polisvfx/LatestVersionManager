"""Background QThread workers used by the GUI."""

import copy
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
    # bytes copied, total bytes, filename — floats, as Qt's int is 32-bit
    # and a single MOV/MXF can be far larger than 2 GB.
    byte_progress = Signal(float, float, str)
    completed = Signal(object)         # HistoryEntry on success
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
                byte_progress_callback=self._on_byte_progress,
            )
            self.completed.emit(entry)
        except PromotionError as e:
            self.error.emit(str(e))
        except Exception as e:
            self.error.emit(f"Unexpected error: {e}")

    def _on_progress(self, current, total, filename):
        self.progress.emit(current, total, filename)

    def _on_byte_progress(self, done, total, filename):
        self.byte_progress.emit(float(done), float(total), filename)


# ---------------------------------------------------------------------------
# Worker thread for thumbnail generation
# ---------------------------------------------------------------------------


class ThumbnailWorker(QThread):
    completed = Signal(int, str)  # request_id, path to thumbnail or empty string

    def __init__(self, source_path, version_string, extensions, cache_dir,
                 request_id: int = 0, parent=None):
        super().__init__(parent)
        self.source_path = source_path
        self.version_string = version_string
        self.extensions = list(extensions)
        self.cache_dir = cache_dir
        self.request_id = request_id

    def run(self):
        from src.lvm.thumbnail import get_thumbnail
        try:
            result = get_thumbnail(self.source_path, self.version_string, self.extensions, self.cache_dir)
        except Exception:
            result = ""
        self.completed.emit(self.request_id, result or "")


class ThumbnailQueueWorker(QThread):
    """Drains a queue of thumbnail requests for the version-tree icons."""

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
# Worker threads for update checking / downloading / extracting
# ---------------------------------------------------------------------------


class UpdateCheckWorker(QThread):
    """Checks GitHub for a newer release in a background thread."""
    completed = Signal(object)  # ReleaseInfo or None
    error = Signal(str)

    def __init__(self, current_version: str, parent=None):
        super().__init__(parent)
        self.current_version = current_version

    def run(self):
        try:
            from src.lvm.updater import check_for_update
            release = check_for_update(self.current_version)
            self.completed.emit(release)
        except Exception as e:
            self.error.emit(str(e))


class UpdateDownloadWorker(QThread):
    """Downloads and SHA-256 verifies the update ZIP in a background thread."""
    progress = Signal(int, int)    # bytes_downloaded, total_bytes
    completed = Signal(str)        # path to downloaded ZIP
    error = Signal(str)

    def __init__(self, release_info, dest_dir: str, parent=None):
        super().__init__(parent)
        self.release_info = release_info
        self.dest_dir = dest_dir
        self._cancel_event = threading.Event()

    def cancel(self):
        self._cancel_event.set()

    def run(self):
        try:
            from src.lvm.updater import download_update
            zip_path = download_update(
                self.release_info,
                self.dest_dir,
                progress_callback=lambda current, total: self.progress.emit(current, total),
                cancel_event=self._cancel_event,
            )
            self.completed.emit(str(zip_path))
        except Exception as e:
            self.error.emit(str(e))


class UpdateExtractWorker(QThread):
    """Extracts the verified update ZIP off the GUI thread."""
    completed = Signal(str)  # extracted app dir path
    error = Signal(str)

    def __init__(self, zip_path: str, extract_dir: str, parent=None):
        super().__init__(parent)
        self._zip_path = Path(zip_path)
        self._extract_dir = Path(extract_dir)

    def run(self):
        try:
            from src.lvm.updater import extract_update
            extracted = extract_update(self._zip_path, self._extract_dir)
            self.completed.emit(str(extracted))
        except Exception as e:
            self.error.emit(str(e))


class ScanWorker(QThread):
    """Scans project sources in a background thread.

    Takes immutable snapshots of *config* / *sources* on construction so
    GUI-thread edits cannot race with background scanning.
    """
    progress = Signal(int, int, str)  # current_index, total, source_name
    completed = Signal(dict)          # {source_name: list[VersionInfo]}
    error = Signal(str)

    def __init__(self, config: ProjectConfig, sources=None, previous_cache: dict[str, list] = None, parent=None):
        super().__init__(parent)
        self._task_tokens = list(config.task_tokens)
        self._tc_mode = config.timecode_mode
        raw_sources = config.watched_sources if sources is None else sources
        self._sources = [copy.deepcopy(s) for s in raw_sources]
        self.previous_cache = {
            k: list(v) for k, v in (previous_cache or {}).items()
        }

    def run(self):
        try:
            import time as _time
            started = _time.monotonic()
            results = {}
            total = len(self._sources)
            counts = {"reused": 0, "scanned": 0}
            counts_lock = threading.Lock()

            def _scan_one(source):
                try:
                    scanner = VersionScanner(source, self._task_tokens)
                    versions = scanner.scan(previous=self.previous_cache.get(source.name))
                    with counts_lock:
                        counts["reused"] += scanner.last_reused
                        counts["scanned"] += scanner.last_scanned
                    return source.name, versions
                except Exception as e:
                    logger.warning("Scan failed for '%s': %s", source.name, e)
                    return source.name, []

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

            if self._tc_mode == "always":
                all_versions = [v for versions in results.values() for v in versions]
                populate_timecodes_parallel(all_versions, max_workers=8)

            folders = counts["reused"] + counts["scanned"]
            logger.info(
                "Scanned %d source(s) in %.2fs — %d of %d version folder(s) unchanged and reused",
                total, _time.monotonic() - started, counts["reused"], folders,
            )
            self.completed.emit(results)
        except Exception as e:
            self.error.emit(str(e))


class StatusWorker(QThread):
    """Computes source statuses (verify, conflicts) in a background thread.

    ``completed`` is emitted on every exit path, including failures: the GUI
    only clears its ``_status_worker`` guard in the completion handlers.
    """
    completed = Signal(dict, dict, dict, dict)  # source_status, target_conflicts, promoters, scanners

    def __init__(self, config: ProjectConfig, versions_cache: dict,
                 sources=None, parent=None):
        super().__init__(parent)
        self._config = copy.deepcopy(config)
        self._versions_cache = {k: list(v) for k, v in (versions_cache or {}).items()}
        if sources is None:
            self._sources = list(self._config.watched_sources)
        else:
            self._sources = [copy.deepcopy(s) for s in sources]

    def run(self):
        source_status: dict = {}
        promoters: dict = {}
        scanners: dict = {}
        target_conflicts: dict = {}
        try:
            self._compute_all(source_status, promoters, scanners, target_conflicts)
        except Exception as e:
            logger.exception("Status computation failed: %s", e)
        finally:
            self.completed.emit(source_status, target_conflicts, promoters, scanners)

    def _compute_all(self, source_status: dict, promoters: dict,
                     scanners: dict, target_conflicts: dict) -> None:
        from src.lvm.conflicts import detect_target_conflicts

        sources = self._sources
        worker_count = min(8, len(sources))
        if worker_count > 1:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(self._compute_one_safe, s) for s in sources]
                results = [f.result() for f in as_completed(futures)]
        else:
            results = [self._compute_one_safe(s) for s in sources]

        for name, status_info, promoter, scanner in results:
            source_status[name] = status_info
            if promoter:
                promoters[name] = promoter
            scanners[name] = scanner

        conflicts = detect_target_conflicts(self._config, self._config.task_tokens)
        for target, name_a, name_b in conflicts:
            target_conflicts.setdefault(name_a, []).append(name_b)
            target_conflicts.setdefault(name_b, []).append(name_a)

    def _make_promoter(self, source):
        return Promoter(source, self._config.task_tokens, self._config.project_name,
                        nle_rename_options=self._config.nle_rename_options())

    def _compute_one_safe(self, source):
        """:meth:`_compute_one`, with failures reported as an ``"error"`` status."""
        try:
            return self._compute_one(source)
        except Exception as e:
            logger.warning("Status check failed for '%s': %s", source.name, e)
            promoter = None
            if source.latest_target:
                try:
                    promoter = self._make_promoter(source)
                except Exception:
                    promoter = None
            status_info = {
                "current": None,
                "status": "error",
                "has_overrides": source.has_overrides,
                "integrity": {"valid": False, "message": f"Status check failed: {e}"},
            }
            return (source.name, status_info, promoter,
                    VersionScanner(source, self._config.task_tokens))

    def _compute_one(self, source):
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
            promoter = self._make_promoter(source)
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
            "highest": highest_ver,
            "status": status,
            "has_overrides": source.has_overrides,
            "integrity": integrity,
        }
        return source.name, status_info, promoter, scanner


class AutoPromoteCheckWorker(QThread):
    """Runs the auto-promote checks for one source off the GUI thread."""
    decided = Signal(object)  # AutoPromoteDecision

    def __init__(self, config: ProjectConfig, source: WatchedSource,
                 waiting_on_render: bool, settle_seconds: float,
                 previous_settle=None, parent=None):
        super().__init__(parent)
        self._task_tokens = list(config.task_tokens)
        self._project_name = config.project_name
        self._nle_opts = config.nle_rename_options()
        self._source = copy.deepcopy(source)
        self._waiting_on_render = waiting_on_render
        self._settle_seconds = settle_seconds
        self._previous_settle = previous_settle

    def run(self):
        from src.lvm.auto_promote import AutoPromoteDecision, evaluate_auto_promote, ERROR
        try:
            promoter = Promoter(
                self._source, self._task_tokens, self._project_name,
                nle_rename_options=self._nle_opts,
            )
            scanner = VersionScanner(self._source, self._task_tokens)
            decision = evaluate_auto_promote(promoter, scanner, self._settle_seconds,
                                             previous=self._previous_settle)
        except Exception as e:
            decision = AutoPromoteDecision(self._source.name, ERROR, reason=f"check failed: {e}",
                                           log_level=logging.WARNING)
        decision.waiting_on_render = self._waiting_on_render
        self.decided.emit(decision)


class SyncNamesWorker(QThread):
    """Runs the Resolve rename in-process on a worker thread."""
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
    """Loads a project config + scan cache off the UI thread."""
    completed = Signal(object, object, str)  # config, cached_versions_or_None, path
    error = Signal(str, str)                 # message, path

    def __init__(self, path: str, parent=None):
        super().__init__(parent)
        self._path = path

    def run(self):
        try:
            config = load_config(self._path)
            from src.lvm.scan_cache import load_cache
            cached = load_cache(self._path, config.watched_sources, task_tokens=config.task_tokens) or None
            self.completed.emit(config, cached, self._path)
        except Exception as e:
            self.error.emit(str(e), self._path)
