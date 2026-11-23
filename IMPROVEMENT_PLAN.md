# LVM Improvement Plan

Analysis of LatestVersionManager at commit 3ee97da (branch `feat/container-render-check`), 2026-11-23. Analysis only: no code has been changed. Every finding was checked by reading the code. Impacts marked **(estimate)** are not measured yet; each one says how to measure it.


## 1. Architecture summary

**Entry points:** `main.py` is the argparse CLI: setup, discover, init, scan, status, promote, promote-all, history, rollback, undo, verify, validate, save-template and list-templates. `app.py` calls `app/_common.main()`, which creates the QApplication with the Fusion style and a dark stylesheet, then opens `app/main_window.MainWindow` (4.2k lines).

**Core package (`src/lvm/`):**

| Module | Role |
|---|---|
| models | `WatchedSource`, `ProjectConfig`, `VersionInfo`, `HistoryEntry` |
| config | load/save JSON, relative↔absolute paths, project defaults, history-filename migration |
| scanner | `os.scandir` of the source dir and each version folder, frame-range and gap detection |
| scan_cache | `.lvm_cache/scan_cache.json`, keyed by a per-source fingerprint |
| promoter | promotes through a staging dir, a swap and a backup dir, with a lock, disk preflight, render check and verify |
| fast_copy | CopyFileExW on Windows, clonefile/copyfile on macOS, copy_file_range on Linux |
| lockfile | leased `.lvm_promote.lock` with a heartbeat |
| history | sidecar JSON with an mtime cache and atomic save |
| render_check | detects MOV/MXF/AVI files that are still being written |
| hooks | pre/post shell hooks |
| watcher | watchdog Observer with a 2 s Qt debounce |
| timecode | EXR/DPX header parsing, ffprobe |
| nle_bridge | Resolve/Premiere sync |
| conflicts | detects sources whose latest targets overlap |
| Other | thumbnail, updater, templates, elevation, log_handler |

**Threading model:**

| Runs on | Work |
|---|---|
| GUI thread | All widget work. `_on_source_selected_by_name` (scan if not cached, plus lazy timecodes), `_try_auto_promote` (full scan, history read, obsolete-layer scan, render check), `_on_version_selected` (history read) |
| `ProjectLoadWorker` | Loads the config and the scan cache |
| `ScanWorker` | Runs `VersionScanner.scan()` with ≤8 threads |
| `StatusWorker` | Creates a `Promoter` per source and runs `verify()` and conflict detection with ≤8 threads |
| `PromoteWorker` | Runs `Promoter.promote()`. Files copy in a nested `ThreadPoolExecutor` (≤8) when there are more than 10 files |
| `ThumbnailQueueWorker` | Thumbnails |
| `SyncNamesWorker` | Resolve sync |
| `_io_executor` (1 thread) | Config and scan-cache writes |
| watchdog thread | Events go through `QMetaObject.invokeMethod` (queued) to the GUI thread |

**Data flow, from a file landing on disk to a recorded promotion:**
1. The watchdog `on_created` fires, but only for entries directly inside `source_dir` (`recursive=False`).
2. `_on_change_main_thread` restarts the 2 s single-shot timer. When it fires, `_flush_pending` emits `source_changed(name)`.
3. `MainWindow._on_watcher_change` drops the cached versions for that source, starts a background `ScanWorker` and `StatusWorker` for it, and calls `_try_auto_promote` synchronously.
4. Auto-promote gates: the checkbox is on, no promotion is running, a previous promotion exists, a newer version exists, the frame range is identical, there are no obsolete layers, the container render has finished (retried every 10 s), and the link mode is available.
5. `PromoteWorker` → `Promoter.promote`, which does:
   - gaps and render check
   - pre-hook
   - `PromoteLock`
   - orphan recovery
   - disk preflight
   - locked-file check
   - copy or link into `.lvm_staging`
   - move the old files to `.lvm_backup`, then rename the staged files in
   - `HistoryManager.record_promotion` (temp file + replace)
   - post-hook
   - release the lock
6. `_on_promote_finished` rescans the source and optionally auto-syncs the NLE.

**Persistence:**
- **Project JSON:** relative paths, written by `config.save_config` or `MainWindow._save_project` (a separate implementation of the same logic).
- **Per-target history sidecar:** `.latest_history*.json`.
- **Scan cache:** `.lvm_cache/` next to the project file.
- **QSettings** (`LatestVersionManager/LVM`): `last_project`, recent projects, thumbnail toggle.

## 2. Prioritized issues

Impact: H/M/L. Effort: S (≤½ day), M (1–3 days), L (>3 days). Risk is the chance that the fix causes a regression.

| # | Issue | Location | Category | Impact | Effort | Risk |
|---|---|---|---|---|---|---|
| 1 | An exception in `StatusWorker` permanently blocks every refresh | `app/workers.py:261-340`, `main_window.py:2373,2410,2511` | robustness | H | S | L |
| 2 | Promoting a sequence deletes other sources' media in a shared target dir | `promoter.py:840-872` (`_collect_removal_set`) | robustness (data loss) | H | S–M | M |
| 3 | Timecode lookup reads every frame header when frames carry no timecode. It runs on the GUI thread and repeats on every select and refresh | `timecode.py:449-458`, `main_window.py:3048-3051` | perf/UX | H | S | L |
| 4 | Auto-promote runs a full scan and other file I/O on the GUI thread | `main_window.py:3857,3865,3890,3903` | perf | H | M | M |
| 5 | Auto-promote of sequences is blind to frames landing inside version folders, and can publish a half-written last frame | `watcher.py:31-33,78`, `main_window.py:3876-3887` | robustness/feature | H | M | M |
| 6 | The watcher is unreliable on SMB: no polling fallback, and a dead observer goes unnoticed | `watcher.py:65-88,124-126` | robustness | H | M | L |
| 7 | GUI project save: non-atomic, duplicated logic that drops the `manual_versions` path relativisation, and the dirty flag clears before the write succeeds | `main_window.py:907-949`, `config.py:320` | robustness | H | S | L |
| 8 | `itemSelectionChanged` is re-connected on every source selection, so handlers pile up | `main_window.py:3199,3220` | perf/bug | M | S | L |
| 9 | History sidecar: Windows crash when backing up a corrupt file, `OSError` not handled, a fixed temp name, and verify checks only the file count | `history.py:59-78,94,188` | robustness | M | S | L |
| 10 | Refresh storm: a watcher event during a busy refresh turns into a full rescan of all sources, and a partial refresh rebuilds every list | `main_window.py:2373-2379,2472-2483` | perf/UX | M | M | M |
| 11 | Every scan re-lists every version folder. `ScanWorker.previous_cache` is accepted and never used | `workers.py:195-199`, `scanner.py:111-216` | perf | M–H (estimate) | M | M |
| 12 | The promote path stats every file over SMB (`Path.iterdir()` + `is_file()`) | `promoter.py:180-195,515-518,850-871,1148-1151,1226-1229` | perf | M (estimate) | S | L |
| 13 | Swap window: 2 SMB renames per file, so the target can sit empty or mixed for seconds | `promoter.py:874-938` | robustness | M (estimate) | L | H |
| 14 | No check after the copy that staged files are complete (size or count) | `promoter.py:531-560` | robustness | M | S | L |
| 15 | Log flood: an INFO line per copied file, the root logger at DEBUG, and every record goes through a Qt signal into `appendHtml` | `fast_copy.py:393`, `main_window.py:113`, `log_handler.py:25-28` | perf | L–M | S | L |
| 16 | Closing the app during a promotion: no prompt, no cancel, only a 2 s wait | `main_window.py:4135-4175` | robustness/UX | M | S | L |
| 17 | No byte-level progress for large single files (MOV/MXF) | `promoter.py:593-625`, `fast_copy.py:369` (`progress_cb` unused) | UX | M | S | L |
| 18 | Extra metadata round-trips after `CopyFileExW` (`copystat`) | `fast_copy.py:397-399` | perf | L (estimate) | S | L–M |
| 19 | The config has no schema version and no migration framework | `config.py:167-213`, `models.py` | feature | M | S–M | L |
| 20 | Small correctness bugs: fallback link mode restored on the wrong source, `select_source` overwritten, a progress counter shared between threads without a lock | `main_window.py:3667,2417`, `promoter.py:570-582` | robustness | L | S | L |
| 21 | Portable tests are missing from CI, and there is no performance benchmark | `.github/workflows/build.yml:98-106` | tests | M | S | L |
| 22 | Dead code | `watcher.py:29`, `scanner.py:293,317,394`, `promoter.py:429` | cleanup | L | S | L |
| 23 | Resolve: show the version in visible clip metadata through `SetMetadata` (only hidden third-party metadata is written today) | `companions/resolve/lvm_restore_versions.py:329-340` | feature | M | M | L |
| 24 | Nuke companion: reload Read nodes that point at a latest target after a promotion | new `companions/nuke/` | feature | M | M | L |
| 25 | Notifications for auto-promote results and failures (tray or OS toast) | `main_window.py:3930-3935`, `_on_promote_error` | UX | M | S–M | L |

## 3. Item details

**1. A `StatusWorker` exception blocks every refresh.** `StatusWorker.run` has no try/except and no error signal. `_status_worker` is reset only in the `finished` handlers (lines 2026, 2458, 2576). `HistoryManager.load` catches only JSON errors, so reading a sidecar while the SMB share drops raises `OSError`, which escapes `future.result()` and kills the worker. From then on, `_refresh_all`, `_refresh_selected_sources` and the watcher refreshes all return early because `self._status_worker is not None`. The UI looks alive but never updates again until you restart.
- **Fix:** wrap each source's work in `_compute_one` in try/except and mark the source `status="error"` with the message. Wrap `run()` as well and emit an `error` signal that resets `_status_worker` and re-enables the buttons. Add a unit test that injects an `OSError`.
- **Trade-off:** none worth noting.

**2. A sequence promotion deletes other sources' files in a shared target.** `conflicts.py` deliberately allows several sources to share a target dir when their filenames differ. `_promote_single_file` replaces only its own file, but `_promote_sequence` → `_collect_removal_set` retires **every** file with a matching extension, plus every symlink, into `.lvm_backup` and then deletes the backup. Two EXR sources sharing `.../online/` would wipe each other's frames on every promotion. In the GUI, the obsolete-layer dialog also lists the other source's sequences as "obsolete layers" of this one. The CLI passes `keep_layers=None` and deletes them without asking.
- **Fix:** filter the removal set with `_filter_to_own_target_files`, which already exists and is used for the lock check (line 322). Fall back to the current behaviour only when `sample_filename` is empty. Apply the same filter in `detect_obsolete_layers`. Add a test with two sources sharing a target.
- **Trade-off:** a source without a `sample_filename` still can't tell which files are its own. Log a warning in that case.

**3. Timecode lookup reads every frame (GUI thread).** `extract_timecode_for_version` sorts the folder listing and calls `extract_timecode` on **every** file until one returns a timecode. For EXRs without a timecode, that reads N headers per version. The default `timecode_mode` is `"lazy"`, which runs `populate_timecodes_parallel` inside `_on_source_selected_by_name` on the GUI thread. A `None` result is never cached. Every rescan creates fresh `VersionInfo` objects, so it all repeats after each watcher refresh and each re-selection. **Impact (estimate):** 2,000 frames × ~5–20 ms per SMB open and read ≈ 10–40 s per version, parallelised ×8, during which the UI is frozen.
- **Fix:** probe only the first file, and if needed the first file of each sub-sequence. Store a timecode-checked sentinel so "no timecode" is cached. Carry timecodes over from the previous cache when the source path and mtime haven't changed (see item 11). Move lazy population into a worker that updates column 5 of the tree when it finishes.
- **Measure:** time `_on_source_selected_by_name` against a test folder of 2,000 EXRs without timecode on the NAS.

**4. Auto-promote blocks the GUI.** `_try_auto_promote` calls `scanner.scan()` (line 3857) synchronously, right after `_on_watcher_change` has already started a background scan of the same source, so the work is done twice. It then reads the history, runs `detect_obsolete_layers` (two directory listings plus a stat per file), and runs the container render check (opens the file and reads its header and tail).
- **Fix:** move the decision into an `AutoPromoteCheckWorker`, or better, evaluate it in the completion handler of the partial refresh using the scan results it just produced. Only the final `_start_promotion` should run on the GUI thread.
- **Trade-off:** state handling becomes asynchronous, so the guards against a promotion already running must be re-checked when the result arrives.

**5. Auto-promote gaps for sequences.**
- **(a) Folder-per-version layout:** the watcher watches only `source_dir` non-recursively and handles only `on_created`. The new `v005/` folder fires once; 2 s later the scan sees a partial range, the frame-range gate rejects it, and nothing fires again because frames inside `v005/` raise no events. The render-wait retry covers containers only. Result: auto-promote of a sequence only works if the folder arrives already complete.
- **(b) Flat layout:** every frame fires an event. If a frame takes more than 2 s to render, each frame triggers a scan. When the last frame file appears, the ranges match, and the scan runs about 2 s after that file was **created**, possibly while a large EXR is still being written over the network, so a truncated last frame gets promoted.
- **(c) Renders that arrive by rename:** `on_moved` is ignored, so a folder or file renamed into place may never trigger.
- **Fix:** handle `on_moved` (and `on_modified` for flat layouts). Add a sequence settle rule: the version is ready when its file count and newest mtime are unchanged across two checks N seconds apart (reuse `RENDER_SETTLE_SECONDS`). While a version is incomplete, use the existing `_schedule_render_wait_retry` loop for sequences too. Optionally check that frame sizes are non-zero and close to the median. Allow a per-source "expected frame range" as an alternative gate.
- **Uncertain:** whether your renderers (Nuke Write, Resolve Deliver, Deadline) write to temp files and rename. Please confirm; it decides how much (c) matters.

**6. Watcher on SMB.**
- **What fails:** `watchdog.observers.Observer` uses the native API. On macOS, FSEvents does not report changes that other clients make on SMB mounts, and on Linux, inotify doesn't see remote changes on CIFS. On Windows, `ReadDirectoryChangesW` works over SMB but can overflow, and if the share drops, the observer thread can die silently. `is_running` would then report false while the UI still shows "Watching".
- **Fix:**
  - Add a per-project or per-source `watch_mode`: auto, native or polling. Auto picks `PollingObserver` (configurable interval, for example 10 s) for UNC or network paths on macOS and Linux.
  - Add a health `QTimer` that checks `observer.is_alive()`, updates the status label, restarts the observer with backoff, and runs a catch-up rescan.
  - Add a safety-net periodic rescan (for example every 5 min) while watching.
- **Trade-off:** polling a share with many sources is itself a listing cost. Keep it cheap by stat-ing only the top level and the newest version folder.
- **Measure:** run a test watch on the NAS from each OS and log the event latency.

**7. Project save.** `MainWindow._save_project` re-implements `save_config` and leaves out the relativisation of `manual_versions` paths, so GUI saves store those paths as absolute and they break when the project is moved. It writes with `open(p, "w")`, which is not atomic: a network drop or crash leaves truncated JSON. It also clears `_dirty` and shows "Saved" before the background write has finished, and a failure only reaches the log. `config.save_config` (line 320) is not atomic either.
- **Fix:** make `save_config` atomic (temp file + `os.replace` in the same directory, plus an `fsync`) and have the GUI call it on a snapshot. Clear the dirty flag in a completion callback, and on failure show the error and set the flag again.

**8. Signal connections pile up.** `version_tree.itemSelectionChanged.connect(self._on_version_selected)` and `history_tree...connect(...)` run on **every** source selection and are never disconnected. After you select K sources, each click runs `_on_version_selected` K times, and each run reads the history file.
- **Fix:** connect once in `_build_ui`.

**9. History sidecar.**
- `self.path.rename(backup)` (line 74) raises `FileExistsError` on Windows when a `.json.bak` already exists, so a second corruption crashes the read path. Use `os.replace` or a timestamped backup name.
- `open()` and `stat()` errors (`PermissionError`, SMB drop) aren't handled. Catch `OSError` and return a distinct "unreadable" state, which is not the same as "empty".
- The temp file always has the same name (`.json.tmp`). Use `tempfile.mkstemp` in the same directory, so that a CLI process and the GUI on two hosts (all already serialised by `PromoteLock` during promotion) can't collide on other write paths.
- `verify_integrity` compares file counts only. Add a total-size comparison, using the size that `HistoryEntry` records at promotion time.

**10. Refresh storm and UI rebuilds.**
- When a worker is busy, `_refresh_sources_by_name` sets `_rescan_after_cache = True`, which turns a one-source watcher event into a **full** rescan.
- Every partial refresh clears and rebuilds `source_list`, `version_tree` and `history_tree`, and disables the Promote buttons while it scans. During a long flat-layout render, the user faces a flickering list and a Promote button that keeps disabling itself.
- **Fix:**
  - Queue the names of pending sources as a set and run one follow-up partial scan for them.
  - Update the existing `QTreeWidgetItem` for each changed source in place, rebuilding only the version and history trees when the selected source is one of them.
  - Keep the selection and scroll position.
  - Don't disable Promote during background scans that the user didn't start.

**11. Incremental scanning.** Every `scan()` lists every version folder of every source. For S sources × V versions × F frames, that is S×V directory listings, each returning F entries over SMB, on every refresh. `ScanWorker.previous_cache` is passed in (lines 2083, 2524) but never read.
- **Fix:** record each version folder's mtime and size in `VersionInfo`, which also goes into the scan cache. On rescan, list only `source_dir`. For each version folder whose directory mtime (from the cached `DirEntry.stat()`) is unchanged, reuse the previous `VersionInfo`, including its timecode.
- **Caveat:** on SMB and NTFS a directory's mtime changes when entries are added or removed, not when a file's contents change. That is enough for "new frames arrived", and verify still checks content staleness.
- **Measure:** run `cProfile` on `ScanWorker.run` with 10 sources × 20 versions × 1,000 frames, local disk versus NAS, and compare cold and warm scans.

**12. Per-file stats on the promote path.** `Path.iterdir()` + `f.is_file()` + `f.suffix` costs a `stat` per entry, because `Path` objects don't carry `DirEntry`'s cached type. This happens in `_promote_sequence`, `_collect_removal_set` (`is_file` + `is_symlink`), `detect_obsolete_layers` (source and target), `dry_run` and `_get_version_source_files`.
- **Fix:** switch to `os.scandir` with `entry.is_file(follow_symlinks=False)`, as the scanner already does. Factor out one `_list_media(dir)` helper.
- **Impact (estimate):** about 1 SMB round-trip per frame, 2–3 times per promotion. Measure with timing around each phase of `promote()` (add debug timings).

**13. Swap window.** The docstring says the swap takes milliseconds, but it does 2 renames per file, and each is an SMB round-trip. **Estimate:** 2,000 frames × 2 × 5–20 ms ≈ 20–80 s during which a Resolve or Nuke reader sees missing or mixed frames.
- **Options:**
  - (a) Measure first, by adding timing logs around `_swap_staged_into_target`.
  - (b) Parallelise the renames, which shrinks the window but doesn't close it.
  - (c) When a source owns its target dir outright (it isn't shared and has no foreign files), swap the whole directory: rename `latest` → `.latest_old`, then `staging` → `latest`. That means 2 renames, but the staging dir has to move to a sibling of the target, and on Windows renaming a folder fails while Resolve holds file handles in it.
- **Recommendation:** do (a) and (b) now; treat (c) as an opt-in experiment. **High risk**, needs testing with Resolve.

**14. Post-copy check.** Before the swap, confirm that the staging dir contains exactly the expected filenames and that each size matches the source `DirEntry` size. This costs one `scandir` of the staging dir. Hashing is optional and per source, since xxhash over SMB is expensive.

**15. Log flood.**
- `fast_copy.smart_copy` logs `Server-side copy (same SMB server)` at INFO **for every file**. The message is also misleading, because `CopyFileExW` only *attempts* a server-side copy.
- `MainWindow` sets the root logger to DEBUG, so every debug record from every thread is formatted and emitted as a Qt signal, then added to a `QPlainTextEdit` with `appendHtml` (the block cap is 1,000).
- **Fix:** log one summary line per promotion (mode, files, bytes, seconds, MB/s). Leave the root logger at INFO unless the log filter is set to DEBUG. Batch log records into the widget on a 100 ms timer.

**16. Quitting mid-promotion.** `closeEvent` calls `worker.quit()`, which does nothing to a `QThread` that has no event loop, waits 2 s and exits. The promotion dies mid-copy (recovered on the next run) or mid-swap (recovered from the backup dir on the next promotion into that target). Until then, the target is left in a broken state.
- **Fix:** if `_worker` is running, ask "Cancel promotion and quit?", call `cancel()`, and wait until the worker finishes.

**17. Byte progress.** `smart_copy` takes `progress_cb`, but `_promote_single_file` never passes one, so a 40 GB MXF shows "0/1" for minutes.
- **Fix:** pass a throttled callback (for example at most 10 Hz) and report bytes.

**18. `copystat` after `CopyFileExW`.** `CopyFileExW` already copies timestamps and attributes. `shutil.copystat` adds a stat plus `utime` and `chmod` on the destination, which means extra SMB round-trips for every file.
- **Fix:** skip it on Windows after a successful `CopyFileExW`.
- **Verify first:** that `target_mtime` and staleness checks still behave the same; the existing `test_fast_copy` plus an mtime assertion would cover this.
- **Impact:** an estimate.

**19. Config schema versioning.** `ProjectConfig` has no version field. Migrations such as `_migrate_shared_history_filenames` run ad hoc on every load.
- **Fix:** add `config_version`, a table of `MIGRATIONS = {n: fn}`, and code that writes the new version on save. Add a warning when a file was saved by a newer LVM. The scan cache already uses `CACHE_VERSION`; follow the same pattern.

**20. Small bugs.**
- `_on_promote_finished` and `_on_promote_error` restore `_fallback_original_mode` on `_current_source`, which may no longer be the source that was promoted. Key it by `_promoting_source_name`.
- `_refresh_selected_sources` overwrites the `select_source` that the caller passed in (line 2417).
- `_parallel_copy` increments `completed[0] += 1` from up to 8 threads without a lock, so the progress count can be off. Use a lock or `itertools.count`.

**21. Tests and CI.**
- CI runs 9 suites. `test_render_check` (new on this branch), `test_nle_bridge` and `test_history_timeline` don't import QApplication directly, so they may be portable. I haven't confirmed this; `test_render_check` has 2 platform guards. Add them to the CI list.
- Add `tools/bench_scan.py`, which generates N versions × F zero-byte frames in a temp dir or a given NAS path, then times scan, verify, dry_run and promote (copy and hardlink) with `cProfile` output. Use it to check the estimates in items 3 and 11–13.

**22. Dead code.** Nothing in `app/`, `src/` or `main.py` uses these. Check `tests/` before removing them:
- `_FolderEventHandler._debounce_pending`
- `VersionScanner._scan_version_file`
- `VersionScanner._collect_files`
- `VersionScanner.get_latest_version`
- `Promoter._target_has_media_files`

**23. Resolve: visible metadata.** The companion already renames clips, sets status flags and stores a promotion marker with `SetThirdPartyMetadata` (hidden).
- **Proposal:** optionally also `SetMetadata` on a visible field (for example "Comments" or "Version"), with values such as `LVM v005 · 2026-11-20 · by mp`, so editors can see the version in the Metadata panel. Add a project option that picks the field.
- **Trade-off:** it overwrites a field that editors might also use.

**24. Nuke companion.** Read nodes already point at the latest path, but Nuke caches frames.
- **Proposal:** add a small `menu.py` add-on that watches the history sidecars of Read nodes whose `file` knob is under a latest target (or receives a post-hook trigger like the Premiere one) and runs `reload` on them, optionally setting a `label` with the version.
- **Scope:** keep it out of the core app and ship it under `companions/nuke/`.

**25. Notifications.** Auto-promote outcomes appear only in the status bar and the log.
- **Proposal:** add a `QSystemTrayIcon` message for: auto-promoted, waiting for render, skipped (and why), and failed. Make it opt-in per project.

**Already solid** (evidence that these areas are covered):
- Staged promotion with backup recovery
- Leased lock with a heartbeat and dead-PID takeover
- Disk preflight
- Locked-file check
- Container render check
- Native fast copy, including APFS clone, SMB server-side copy and `copy_file_range`
- Scan cache with fingerprints
- Async project load
- Hooks with env and token substitution
- PyInstaller spec, CI builds and an updater

Hardlink and symlink modes exist. Reflinks: macOS `clonefile` is used; on Linux `copy_file_range` reflinks on btrfs/XFS. **Uncertain:** whether Windows ReFS / Dev Drive block cloning kicks in automatically through `CopyFileExW` on your Windows build.

## 4. Recommended order

1. **Quick wins (S, low risk):** #1, #8, #7, #9, #3 (probe the first file, cache "none"), #2, #20, #15, #16, #22.
2. **High impact (M):** #4 (auto-promote off the GUI thread), #5 (sequence settle and moved events), #6 (polling and health checks), #10 (refresh queue, in-place updates).
3. **Measure, then optimise:** add the benchmark harness (#21), then #11 (incremental scan), #12, #14, #17, #18, then #13 (a) and (b).
4. **Larger features:** #19 (config versioning), #25 (notifications), #23 (Resolve SetMetadata), #24 (Nuke companion), #13 (c) (directory swap, opt-in).

## 5. Open questions

1. Do any of your projects share one latest target dir between several **sequence** sources? If so, #2 is urgent (data loss).
2. Which OS runs LVM while watching the NAS: Windows only, or macOS/Linux too? This decides how much #6 is worth.
3. Do your renderers write to temp files and rename, or write frames in place? Folder-per-version or flat? This affects #5.
4. For #13 (c): is a directory-level swap acceptable when Resolve may hold files open, or should LVM keep per-file renames only?
5. For #23: which visible Resolve metadata field may LVM own, if any?
6. Is a Nuke companion (#24) wanted, or is a manual "Reload" enough?
7. Should a full-content hash check be an option per source (#14), or are size and count enough?
