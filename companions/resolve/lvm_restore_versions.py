"""LVM Restore Versions — DaVinci Resolve companion script.

Walks the current project's media pool and renames each clip's display
name to the source version recorded in the LVM sidecar
(``.latest_history*.json``) found next to the clip on disk.

The on-disk file is not touched — only the clip's display name in
Resolve's media pool. Re-running is idempotent.

Every clip LVM manages also gets a status flag (one per clip):

* Green — watched by LVM, nothing to review.
* Red   — a new version was promoted since the last sync. The red flag
  stays until you clear it in Resolve (your "reviewed" acknowledgement);
  the next sync then turns the clip green.
* Sand  — stale: the promoted source was re-rendered, or the latest file
  was overwritten, after promotion. Resolve has no orange flag; Sand is
  its closest colour. Stale wins over red.

The other flag colours are left alone. Change the ``FLAG_*`` constants
below to use different colours, or set ``FLAG_CLIPS = False`` to disable.

Install (Windows):
    %APPDATA%\\Blackmagic Design\\DaVinci Resolve\\Support\\Fusion\\Scripts\\Edit\\

Install (macOS):
    ~/Library/Application Support/Blackmagic Design/DaVinci Resolve/Fusion/Scripts/Edit/

Install (Linux):
    ~/.local/share/DaVinciResolve/Fusion/Scripts/Edit/

Run from Workspace -> Scripts -> Edit -> lvm_restore_versions.

Compatible with the script API exposed by DaVinci Resolve 17+.
"""

import json
import os
import re
import sys
from pathlib import Path


def _default_modules_path():
    """Return Blackmagic's default DaVinciResolveScript module folder.

    Resolve usually puts this on sys.path itself, but some Windows installs
    (or non-standard Python configs) don't, so we add it explicitly.
    """
    if sys.platform.startswith("darwin"):
        return "/Library/Application Support/Blackmagic Design/DaVinci Resolve/Developer/Scripting/Modules/"
    if sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
        program_data = os.environ.get("PROGRAMDATA", r"C:\ProgramData")
        return os.path.join(
            program_data,
            "Blackmagic Design", "DaVinci Resolve",
            "Support", "Developer", "Scripting", "Modules",
        )
    if sys.platform.startswith("linux"):
        return "/opt/resolve/Developer/Scripting/Modules/"
    return ""


try:
    import DaVinciResolveScript as dvr_script  # type: ignore
except ImportError:
    fallback = _default_modules_path()
    if fallback and os.path.isdir(fallback) and fallback not in sys.path:
        sys.path.insert(0, fallback)
        try:
            import DaVinciResolveScript as dvr_script  # type: ignore
        except ImportError:
            dvr_script = None
    else:
        dvr_script = None


_VERSION_RE = re.compile(r"[._\-]v\d+", re.IGNORECASE)
_FRAME_EXT_RE = re.compile(r"([._])(\d+)\.(\w+)$")


# ---- status flags ---------------------------------------------------------
# Resolve's flag palette: Blue, Cyan, Green, Yellow, Red, Pink, Purple,
# Fuchsia, Rose, Lavender, Sky, Mint, Lemon, Sand, Cocoa, Cream.
FLAG_CLIPS = True
FLAG_WATCHED = "Green"
FLAG_PROMOTED = "Red"
FLAG_STALE = "Sand"   # closest to orange — Resolve has no "Orange" flag

STATUS_WATCHED = "watched"
STATUS_PROMOTED = "promoted"
STATUS_STALE = "stale"
_STATUS_FLAGS = {
    STATUS_WATCHED: FLAG_WATCHED,
    STATUS_PROMOTED: FLAG_PROMOTED,
    STATUS_STALE: FLAG_STALE,
}

# Third-party metadata key holding the promotion this clip was last synced
# to (``"<version>|<set_at>"``). Comparing it with the sidecar is how a new
# promotion is detected across syncs.
PROMOTION_METADATA_KEY = "LVM Promotion"

# Mtime slack, in seconds — matches Promoter.verify() in LVM.
_MTIME_TOLERANCE = 1.0
_IGNORED_FILES = {"thumbs.db", "desktop.ini"}


def _read_sidecar(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _derive_stem_from_source(source_path):
    """Fallback stem when latest_basename is absent.

    Strips the version segment (e.g. ``_v003``) from the source
    filename's stem and appends ``_latest`` — matches LVM's default
    rename template ``{source_basename}_latest``. Returns ``""`` when
    no version is detectable.
    """
    if not source_path:
        return ""
    base = os.path.basename(source_path)
    stem, _, _ = base.rpartition(".")
    stem = stem or base
    if not _VERSION_RE.search(stem):
        return ""
    stripped = _VERSION_RE.sub("", stem, count=1)
    stripped = re.sub(r"([_.\-]){2,}", r"\1", stripped).strip("._-")
    if not stripped:
        return ""
    return stripped + "_latest"


def _stem_matches_clip(stem, clip_basename):
    """True when *stem* names the on-disk clip (single file or sequence)."""
    if not stem or not clip_basename:
        return False
    name, _, ext = clip_basename.rpartition(".")
    if name == stem and ext:
        return True
    if clip_basename.startswith(stem + ".") or clip_basename.startswith(stem + "_"):
        return True
    return False


def _new_display_name(source_path, clip_basename, cur=None):
    """Compose the new display name for *clip_basename*.

    When the sidecar's ``current`` block (passed via *cur*) carries the
    precomputed ``nle_display_stem`` field, that stem is used together with
    the ``nle_display_include_frame`` / ``nle_display_include_extension``
    booleans — this is the path taken whenever LVM produced the sidecar.

    When those fields are absent (older sidecars), falls back to the legacy
    behaviour: source filename verbatim for single files; ``stem.frame.ext``
    for sequence frames. Keeps in-place upgrades working.
    """
    cur = cur or {}
    stem = cur.get("nle_display_stem") or ""
    frame_match = _FRAME_EXT_RE.search(clip_basename)

    if stem:
        include_frame = bool(cur.get("nle_display_include_frame", False))
        include_ext = bool(cur.get("nle_display_include_extension", False))
        name = stem
        if frame_match and include_frame:
            sep = frame_match.group(1)
            frame = frame_match.group(2)
            name = f"{name}{sep}{frame}"
        if include_ext:
            if frame_match:
                ext = frame_match.group(3)
            else:
                _, _, ext = clip_basename.rpartition(".")
            if ext:
                name = f"{name}.{ext}"
        return name

    # ---- legacy fallback (old sidecars without nle_display_stem) ----
    source_basename = os.path.basename(source_path) if source_path else ""
    if not source_basename:
        return ""
    if frame_match:
        sep = frame_match.group(1)
        frame = frame_match.group(2)
        ext = frame_match.group(3)
        source_stem, _, _ = source_basename.rpartition(".")
        source_stem = source_stem or source_basename
        return f"{source_stem}{sep}{frame}.{ext}"
    return source_basename


def _match_sidecar_to_clip(clip_path):
    """Return (sidecar_dict, sidecar_path) for the clip, or (None, None)."""
    clip_dir = os.path.dirname(clip_path)
    clip_basename = os.path.basename(clip_path)
    if not clip_dir or not clip_basename:
        return None, None

    # pathlib.Path.glob (rather than the stdlib ``glob`` module) keeps this
    # script working in PyInstaller frozen builds where ``glob`` may not be
    # bundled — the companion is shipped as a data file, not analysed for
    # imports, so any stdlib it uses must also live somewhere in the host
    # app's transitive imports. ``pathlib`` is used throughout LVM and is
    # always available.
    try:
        sc_paths = sorted(str(p) for p in Path(clip_dir).glob(".latest_history*.json"))
    except OSError:
        sc_paths = []

    for sc_path in sc_paths:
        data = _read_sidecar(sc_path)
        if not data:
            continue
        cur = data.get("current")
        if not cur:
            continue
        stem = cur.get("latest_basename") or _derive_stem_from_source(cur.get("source", ""))
        if _stem_matches_clip(stem, clip_basename):
            return data, sc_path
    return None, None


# ---- status flags ---------------------------------------------------------

def _clip_stem(cur):
    """On-disk stem of the latest file this sidecar describes."""
    return cur.get("latest_basename") or _derive_stem_from_source(cur.get("source", ""))


def _promotion_marker(cur):
    """Identity of a promotion — changes on every promote, even of the same version."""
    return f"{cur.get('version', '')}|{cur.get('set_at', '')}"


def _visible_files(directory):
    """Return DirEntry objects for the regular, non-hidden files in *directory*."""
    entries = []
    try:
        with os.scandir(directory) as it:
            for entry in it:
                name = entry.name
                if name.startswith(".") or name.lower() in _IGNORED_FILES:
                    continue
                try:
                    if entry.is_file():
                        entries.append(entry)
                except OSError:
                    continue
    except OSError:
        pass
    return entries


def _max_mtime(entries):
    best = None
    for entry in entries:
        try:
            mt = entry.stat().st_mtime
        except OSError:
            continue
        if best is None or mt > best:
            best = mt
    return best


def _source_max_mtime(source, version):
    """Newest mtime of the promoted version's source files, or None.

    When the source is a folder shared by several versions (flat layout),
    only files carrying the version token count, so a newer version
    rendered next to it doesn't read as a re-render.
    """
    if not source:
        return None
    if os.path.isdir(source):
        entries = _visible_files(source)
        token = (version or "").lower()
        if token:
            own = [e for e in entries if token in e.name.lower()]
            if own:
                entries = own
        return _max_mtime(entries)
    try:
        return os.stat(source).st_mtime
    except OSError:
        return None


def _stale_reason(cur, clip_dir):
    """Return why the promoted clip is stale, or ``""`` when it isn't.

    Mirrors LVM's ``Promoter.verify()`` staleness checks using only what
    the sidecar records: source re-rendered after promotion, or the latest
    file overwritten outside LVM.
    """
    source_mtime = cur.get("source_mtime")
    if source_mtime is not None:
        now = _source_max_mtime(cur.get("source", ""), cur.get("version", ""))
        if now is not None and now > source_mtime + _MTIME_TOLERANCE:
            return "source re-rendered since promotion"

    target_mtime = cur.get("target_mtime")
    stem = _clip_stem(cur)
    if target_mtime is not None and stem:
        own = [e for e in _visible_files(clip_dir) if _stem_matches_clip(stem, e.name)]
        now = _max_mtime(own)
        if now is not None and abs(now - target_mtime) > _MTIME_TOLERANCE:
            return "latest file overwritten since promotion"
    return ""


def _read_promotion_metadata(item):
    """Promotion marker stored on the clip by a previous sync, or ``""``."""
    try:
        value = item.GetThirdPartyMetadata(PROMOTION_METADATA_KEY)
    except Exception:
        return ""
    if isinstance(value, dict):
        value = value.get(PROMOTION_METADATA_KEY, "")
    return value if isinstance(value, str) else ""


def _write_promotion_metadata(item, marker):
    # Resolve's documented form takes one dict; very old builds took (key, value).
    try:
        item.SetThirdPartyMetadata({PROMOTION_METADATA_KEY: marker})
    except TypeError:
        try:
            item.SetThirdPartyMetadata(PROMOTION_METADATA_KEY, marker)
        except Exception:
            pass
    except Exception:
        pass


def _clip_flags(item):
    try:
        return list(item.GetFlagList() or [])
    except Exception:
        return []


def _classify_clip(cur, clip_dir, clip_name, new_name, stored_marker, flags,
                   stale_reason=None):
    """Return ``(status, reason)`` for a clip LVM manages.

    Priority: stale > promoted > watched. A clip already carrying the
    promoted flag keeps it until the user clears it in Resolve.

    A new promotion is detected by comparing the marker stored on the clip
    with the sidecar. Clips synced before markers existed (or on Resolve
    builds without third-party metadata) fall back to the rename: a clip
    whose name is neither the on-disk name nor the new display name was
    named after an older version by a previous sync.
    """
    if stale_reason is None:
        stale_reason = _stale_reason(cur, clip_dir)
    if stale_reason:
        return STATUS_STALE, stale_reason

    if stored_marker:
        promoted = stored_marker != _promotion_marker(cur)
    else:
        promoted = (bool(new_name) and clip_name != new_name
                    and not _stem_matches_clip(_clip_stem(cur), clip_name))
    if promoted:
        return STATUS_PROMOTED, f"promoted to {cur.get('version', '?')}"
    if FLAG_PROMOTED in flags:
        return STATUS_PROMOTED, ""
    return STATUS_WATCHED, ""


def _apply_status_flag(item, status, flags):
    """Leave exactly one LVM status flag on *item*. Returns True if anything changed.

    Flags in colours LVM doesn't use are not touched.
    """
    wanted = _STATUS_FLAGS[status]
    changed = False
    for color in set(_STATUS_FLAGS.values()):
        if color != wanted and color in flags:
            item.ClearFlags(color)
            changed = True
    if wanted not in flags:
        item.AddFlag(wanted)
        changed = True
    return changed


def _flag_clip(item, cur, clip_path, clip_name, new_name, stale_cache, cache_key):
    """Classify and flag one clip. Returns ``(status, reason, changed)``."""
    if cache_key not in stale_cache:
        stale_cache[cache_key] = _stale_reason(cur, os.path.dirname(clip_path))
    stored = _read_promotion_metadata(item)
    flags = _clip_flags(item)
    status, reason = _classify_clip(
        cur, os.path.dirname(clip_path), clip_name, new_name, stored, flags,
        stale_reason=stale_cache[cache_key],
    )
    changed = _apply_status_flag(item, status, flags)
    marker = _promotion_marker(cur)
    if stored != marker:
        _write_promotion_metadata(item, marker)
    return status, reason, changed


def _empty_stats(ok, errors=0):
    return {
        "renamed": 0, "idempotent": 0, "no_match": 0, "errors": errors,
        STATUS_WATCHED: 0, STATUS_PROMOTED: 0, STATUS_STALE: 0,
        "flag_errors": 0, "ok": ok,
    }


def _iter_clips(folder, log):
    """Recursively yield every MediaPoolItem under *folder*."""
    try:
        for clip in folder.GetClipList() or []:
            yield clip
        for sub in folder.GetSubFolderList() or []:
            for clip in _iter_clips(sub, log):
                yield clip
    except Exception as e:
        log("warning", f"failed to enumerate folder: {e}")


def _default_log(level, message):
    """Default logger for standalone runs — info to stdout, warnings/errors to stderr."""
    stream = sys.stderr if level in ("warning", "error") else sys.stdout
    print(message, file=stream)


def rename_clips(resolve, log=None):
    """Walk the open project's media pool and rename clips to source versions.

    Args:
        resolve: a DaVinci Resolve app handle (from
            ``DaVinciResolveScript.scriptapp("Resolve")``).
        log: optional ``log(level, message)`` callable. ``level`` is one of
            ``"info"``, ``"warning"``, ``"error"``. Defaults to printing to
            stdout/stderr so the standalone Workspace → Scripts entry still
            works unchanged.

    Also sets each matched clip's status flag (see the module docstring)
    unless ``FLAG_CLIPS`` is False.

    Returns a stats dict: ``{"renamed", "idempotent", "no_match", "errors",
    "watched", "promoted", "stale", "flag_errors", "ok"}``. ``ok`` is True
    when the rename completed (even with zero matches); False only when we
    couldn't talk to a project at all. Flag failures are counted in
    ``flag_errors`` only, so they don't fail the sync.
    """
    if log is None:
        log = _default_log

    if not resolve:
        log("error", "Could not connect to DaVinci Resolve.")
        return _empty_stats(ok=False, errors=1)

    project = resolve.GetProjectManager().GetCurrentProject()
    if not project:
        log("error", "No project is open.")
        return _empty_stats(ok=False, errors=1)

    media_pool = project.GetMediaPool()
    root = media_pool.GetRootFolder()

    renamed = 0
    skipped_match = 0
    skipped_idempotent = 0
    errors = 0
    processed = 0
    status_counts = {STATUS_WATCHED: 0, STATUS_PROMOTED: 0, STATUS_STALE: 0}
    flag_errors = 0
    stale_cache = {}
    HEARTBEAT_EVERY = 250

    for item in _iter_clips(root, log):
        processed += 1
        if processed % HEARTBEAT_EVERY == 0:
            log("info", f"  ...processed {processed} clips "
                f"(renamed={renamed}, skipped={skipped_match}, errors={errors})")

        try:
            clip_path = item.GetClipProperty("File Path") or ""
            clip_name = item.GetClipProperty("Clip Name") or ""
        except Exception as e:
            errors += 1
            log("error", f"  could not read clip properties: {e}")
            continue

        if not clip_path:
            skipped_match += 1
            continue

        sidecar, sc_path = _match_sidecar_to_clip(clip_path)
        if not sidecar:
            skipped_match += 1
            continue

        cur = sidecar["current"]
        new_name = _new_display_name(cur.get("source", ""), os.path.basename(clip_path), cur)

        # Flag before renaming: detecting a promotion falls back to
        # comparing the clip's current (pre-rename) name.
        if FLAG_CLIPS:
            try:
                status, reason, changed = _flag_clip(
                    item, cur, clip_path, clip_name, new_name, stale_cache, sc_path)
            except Exception as e:
                flag_errors += 1
                log("warning", f"  could not flag {clip_name!r}: {e}")
            else:
                status_counts[status] += 1
                if changed and status != STATUS_WATCHED:
                    detail = f" ({reason})" if reason else ""
                    log("info", f"  [{_STATUS_FLAGS[status]} flag] {clip_name}{detail}")

        if not new_name:
            skipped_match += 1
            continue

        if clip_name == new_name:
            skipped_idempotent += 1
            continue

        try:
            item.SetClipProperty("Clip Name", new_name)
        except Exception as e:
            errors += 1
            log("error", f"  SetClipProperty raised for {clip_name!r}: {e}")
            continue

        # SetClipProperty's bool return is unreliable in recent Resolve
        # versions (often False even on success), so verify by reading
        # the property back.
        try:
            actual = item.GetClipProperty("Clip Name") or ""
        except Exception:
            actual = ""

        if actual == new_name:
            renamed += 1
            log("info", f"  {clip_name}  ->  {new_name}")
        else:
            errors += 1
            log("error", f"  failed: {clip_name!r} -> {new_name!r} "
                f"(name still reads as {actual!r})")

    log("info", "")
    log("info", f"Renamed:            {renamed}")
    log("info", f"Already up to date: {skipped_idempotent}")
    log("info", f"No sidecar match:   {skipped_match}")
    log("info", f"Errors:             {errors}")
    if FLAG_CLIPS:
        log("info", f"Flagged watched ({FLAG_WATCHED}):   {status_counts[STATUS_WATCHED]}")
        log("info", f"Flagged promoted ({FLAG_PROMOTED}):   {status_counts[STATUS_PROMOTED]}")
        log("info", f"Flagged stale ({FLAG_STALE}):      {status_counts[STATUS_STALE]}")
        if flag_errors:
            log("warning", f"Flag errors:        {flag_errors}")
    stats = _empty_stats(ok=True, errors=errors)
    stats.update(status_counts)
    stats.update({
        "renamed": renamed,
        "idempotent": skipped_idempotent,
        "no_match": skipped_match,
        "flag_errors": flag_errors,
    })
    return stats


def main():
    # Force line-buffered streams so streaming parents (e.g. LVM's GUI worker)
    # see progress immediately rather than after the subprocess exits.
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except (AttributeError, ValueError):
        pass

    if dvr_script is None:
        fallback = _default_modules_path()
        print("DaVinciResolveScript module not found.", file=sys.stderr)
        print("Looked in: " + (fallback or "<unknown platform>"), file=sys.stderr)
        print("Looked exists: " + str(bool(fallback and os.path.isdir(fallback))),
              file=sys.stderr)
        print("Python: " + sys.executable, file=sys.stderr)
        print("", file=sys.stderr)
        print("If you are running this from Workspace -> Scripts -> Edit:",
              file=sys.stderr)
        print(" 1. Preferences -> System -> General -> 'External scripting "
              "using' = Local; restart Resolve.", file=sys.stderr)
        print(" 2. Confirm the modules folder above exists. If Resolve is "
              "installed in a non-default location, copy DaVinciResolveScript.py "
              "from your install's Developer/Scripting/Modules folder into the "
              "ProgramData path above (or set PYTHONPATH).", file=sys.stderr)
        return 1

    stats = rename_clips(dvr_script.scriptapp("Resolve"))
    if not stats["ok"]:
        return 1
    return 0 if stats["errors"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
