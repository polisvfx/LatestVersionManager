"""LVM Restore Versions — DaVinci Resolve companion script.

Walks the current project's media pool and renames each clip's display
name to the source version recorded in the LVM sidecar
(``.latest_history*.json``) found next to the clip on disk.

The on-disk file is not touched — only the clip's display name in
Resolve's media pool. Re-running is idempotent.

Install (Windows):
    %APPDATA%\\Blackmagic Design\\DaVinci Resolve\\Support\\Fusion\\Scripts\\Edit\\

Install (macOS):
    ~/Library/Application Support/Blackmagic Design/DaVinci Resolve/Fusion/Scripts/Edit/

Install (Linux):
    ~/.local/share/DaVinciResolve/Fusion/Scripts/Edit/

Run from Workspace -> Scripts -> Edit -> lvm_restore_versions.

Compatible with the script API exposed by DaVinci Resolve 17+.
"""

import glob
import json
import os
import re
import sys


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


def _new_display_name(source_path, clip_basename):
    """Compose the new display name for *clip_basename* using *source_path*.

    For a single file: returns the source filename verbatim
    (e.g. ``hero_comp_v003.mov``). For a sequence frame, preserves the
    frame suffix and extension from the clip and uses the source's stem.
    """
    source_basename = os.path.basename(source_path) if source_path else ""
    if not source_basename:
        return ""

    frame_match = _FRAME_EXT_RE.search(clip_basename)
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

    pattern = os.path.join(clip_dir, ".latest_history*.json")
    for sc_path in sorted(glob.glob(pattern)):
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

    Returns a stats dict: ``{"renamed", "idempotent", "no_match", "errors",
    "ok"}``. ``ok`` is True when the rename completed (even with zero matches);
    False only when we couldn't talk to a project at all.
    """
    if log is None:
        log = _default_log

    if not resolve:
        log("error", "Could not connect to DaVinci Resolve.")
        return {"renamed": 0, "idempotent": 0, "no_match": 0, "errors": 1, "ok": False}

    project = resolve.GetProjectManager().GetCurrentProject()
    if not project:
        log("error", "No project is open.")
        return {"renamed": 0, "idempotent": 0, "no_match": 0, "errors": 1, "ok": False}

    media_pool = project.GetMediaPool()
    root = media_pool.GetRootFolder()

    renamed = 0
    skipped_match = 0
    skipped_idempotent = 0
    errors = 0
    processed = 0
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

        sidecar, _ = _match_sidecar_to_clip(clip_path)
        if not sidecar:
            skipped_match += 1
            continue

        cur = sidecar["current"]
        new_name = _new_display_name(cur.get("source", ""), os.path.basename(clip_path))
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
    return {
        "renamed": renamed,
        "idempotent": skipped_idempotent,
        "no_match": skipped_match,
        "errors": errors,
        "ok": True,
    }


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
