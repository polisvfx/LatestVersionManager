# NLE Companion Scripts

LVM promotes a versioned source (e.g. `SH0010_comp_v003.mov`) to a "latest"
file whose name is determined by your project's `file_rename_template` —
typically `SH0010_comp_latest.mov`, but it could be `SH0010_comp_v999.mov`,
`SH0010_comp_final.mov`, or anything else you configured. Whatever the
template produces, an editor importing that file sees the template-named
clip in the NLE's project panel and loses track of which actual version
is on disk.

The companion scripts under `companions/` fix this by reading the LVM
sidecar (`.latest_history*.json`) next to the imported clip and renaming
the **clip's display name** in the NLE to the **source filename recorded
in the sidecar** — independent of whatever rename template produced the
on-disk file (e.g. `SH0010_comp_v003.mov`). The on-disk file is never
touched — LVM keeps owning it.

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
2. Reads each sidecar's `current` entry. The match key is
   `current.latest_basename` — the actual on-disk stem LVM wrote
   (whatever your `file_rename_template` produced). The script does **not**
   look for the literal string `_latest`; it uses whatever name the sidecar
   recorded.
3. **Fallback for old sidecars** (no `latest_basename` field): derives a
   stem from `current.source` assuming the default `{source_basename}_latest`
   template. Custom templates with old sidecars won't match — re-promoting
   once with current LVM regenerates the sidecar with `latest_basename` and
   matching works from then on.
4. Renames the clip to `basename(current.source)` for single files, or
   keeps the clip's frame number and extension and uses the source's stem
   for sequences.

Clips without a matching sidecar are left alone.

---

## Custom rename templates

The script is template-agnostic for sidecars written by current LVM. The
`current.latest_basename` field captures the on-disk stem produced by your
`file_rename_template` verbatim — `_latest`, `_v999`, `_final`,
`_approved`, anything. The script never looks for the literal `_latest`
string.

The only constraint is the legacy fallback: sidecars written by older LVM
versions (before `latest_basename` existed) only match when the default
`{source_basename}_latest` template was in use. Re-promote each source
once with current LVM to upgrade its sidecar; from then on every template
works.

---

## Run from inside LVM (DaVinci Resolve Studio only)

LVM can launch the Resolve companion script for you. Open Resolve, then in
LVM go to **Tools → Sync Names in NLE → DaVinci Resolve**. Output appears
in the LVM log dock (View → Log).

LVM shells out to a Python subprocess with the standard
`RESOLVE_SCRIPT_API` / `RESOLVE_SCRIPT_LIB` / `PYTHONPATH` env vars set,
then runs the same script you'd run from Workspace → Scripts. The menu
entry is disabled when Resolve's scripting modules aren't detected on
this machine.

**Free DaVinci Resolve doesn't support external scripting** — Free users
still have the in-NLE path (Workspace → Scripts → Edit →
lvm_restore_versions), which is fully featured.

## Roadmap

- **v1.5 (in progress)** — Premiere side: a CEP panel that listens for
  LVM triggers via a file watcher, so LVM's "Sync Names" menu can drive
  Premiere too.
- **v2** — Premiere CEP panel ported to UXP for long-term Adobe support.
