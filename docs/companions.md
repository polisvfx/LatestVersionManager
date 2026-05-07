# NLE Companion Scripts

LVM promotes a versioned source (e.g. `SH0010_comp_v003.mov`) to a "latest"
file (`SH0010_comp_latest.mov`) so DaVinci Resolve, Adobe Premiere, and other
tools always read from a stable filename. The trade-off: an editor importing
`SH0010_comp_latest.mov` sees `_latest` in the project panel and loses track
of which actual version is on disk.

The companion scripts under `companions/` fix this by reading the LVM
sidecar (`.latest_history*.json`) next to the imported clip and renaming the
**clip's display name** in the NLE to the source version
(e.g. `SH0010_comp_v003.mov`). The on-disk file is never touched — LVM
keeps owning it.

Re-running is idempotent. Promoting a new version and re-running picks up
the new version automatically.

---

## DaVinci Resolve

`companions/resolve/lvm_restore_versions.py`

### Install

Copy the script into Resolve's user Scripts folder:

| OS | Path |
| --- | --- |
| Windows | `%APPDATA%\Blackmagic Design\DaVinci Resolve\Support\Fusion\Scripts\Edit\` |
| macOS | `~/Library/Application Support/Blackmagic Design/DaVinci Resolve/Fusion/Scripts/Edit/` |
| Linux | `~/.local/share/DaVinciResolve/Fusion/Scripts/Edit/` |

### Run

In Resolve: **Workspace → Scripts → Edit → lvm_restore_versions**.

A summary prints to the Resolve console (Workspace → Console).

### Troubleshooting: "DaVinciResolveScript module not found"

Resolve gates the scripting module behind a preference. If the script
prints that error from inside Resolve:

**Preferences → System → General → External scripting using → Local**
(or **Local + Network**). Restart Resolve after changing it.

The same error from a terminal launch (`python lvm_restore_versions.py`)
is expected — this script is meant to run from inside Resolve. See the
v1.5 roadmap below for LVM-driven external launching (Studio only).

### Free vs Studio

This script runs inside Resolve and works in both the free and Studio
editions. External Python launching from another process is Studio-only —
that's a v1.5 LVM feature, not required here.

---

## Adobe Premiere Pro

`companions/premiere/lvm_restore_versions.jsx`

### Install

No install required for one-off use; you can keep the `.jsx` anywhere on
disk and pick it via the file picker.

For convenience you can drop it into Premiere's Scripts folder (the menu
will still ask you to pick a file — Premiere doesn't auto-list scripts):

| OS | Path |
| --- | --- |
| Windows | `C:\Program Files\Adobe\Adobe Premiere Pro <version>\Scripts\` |
| macOS | `/Applications/Adobe Premiere Pro <version>/Scripts/` |

### Run

In Premiere: **File → Scripts → Run Script File** → pick the `.jsx`.

A summary appears in an `alert()` dialog.

### ExtendScript vs UXP

This script uses ExtendScript, which Adobe supports through **September
2026**. A UXP panel port is on the roadmap (v2 — see `docs/companions.md`
roadmap section once added) and will not change the on-disk sidecar
contract this script depends on.

---

## How matching works

Each clip's directory may hold many sidecars when multiple sources share
a `latest_target`:

```
A001C011_..._comp_latest.mov
A001C019_..._comp_latest.mov
B001C001_..._comp_latest.mov
.latest_history.json                   <- first source kept the default name
.latest_history_A001C019_....json      <- subsequent sources got namespaced
.latest_history_B001C001_....json
```

For each clip the script:

1. Globs `.latest_history*.json` in the clip's directory.
2. Reads each sidecar's `current` entry. The preferred match key is
   `current.latest_basename` (added by LVM at promote time — the on-disk
   stem like `A001C011_..._comp_latest`).
3. Falls back to deriving the stem from `current.source` for older
   sidecars written before the field existed.
4. Renames the clip to `basename(current.source)` for single files, or
   keeps the clip's frame number and extension and uses the source's stem
   for sequences.

Clips without a matching sidecar are left alone.

---

## Custom rename templates

If a watched source uses a non-default `file_rename_template` (anything
other than the default `{source_basename}_latest`), the
`current.latest_basename` field added by recent LVM versions still
captures the actual on-disk stem — matching works without configuration.

For sidecars written before LVM added `latest_basename`, the fallback
assumes the default `_latest` suffix. Custom templates with sidecars from
that older era won't match — re-promote the source once with the current
LVM to regenerate the sidecar with `latest_basename`.

---

## Roadmap

- **v1.5** — LVM "Sync Names" toolbar button. Drives Resolve Studio via
  external Python; signals a Premiere CEP panel via a file-based trigger.
- **v2** — Premiere CEP panel ported to UXP for long-term Adobe support.
