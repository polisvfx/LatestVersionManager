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

> ## NLE-driven sync requirements
>
> **DaVinci Resolve:** the LVM-driven sync (status-bar button, Tools menu,
> auto-sync after promote) needs DaVinci Resolve **Studio (paid)** — these
> entry points stay disabled on the Free edition. Free users get exactly
> the same renaming from inside Resolve via Workspace → Scripts → Edit →
> lvm_restore_versions; see [§ DaVinci Resolve](#davinci-resolve).
>
> **Adobe Premiere:** the LVM-driven sync needs the **CEP panel**
> installed under your Adobe CEP extensions folder. Once the panel is
> running, the LVM **Sync Premiere** button works in any licensed
> Premiere Pro install — there is no Free/paid split. The standalone
> `.jsx` (File → Scripts → Run Script File) also works without the panel
> for one-shot renames.

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

This in-Resolve script (Workspace → Scripts) runs in **both Free and
Studio** — fully featured, no upgrade needed. Setting `External scripting
using → Local` in Preferences is enough.

What's **Studio-only** is the LVM-driven path (status-bar button, Tools
menu, auto-sync). Blackmagic restricts external Python scripting to
Studio licenses, so LVM cannot spawn a subprocess against Free Resolve.
Free users keep using the in-Resolve script entry above; functionality
is identical, you just trigger it from inside Resolve instead of from
LVM's window.

---

## Adobe Premiere Pro

Two ways to run the rename inside Premiere — pick whichever fits your workflow.

### Option 1: standalone `.jsx` (one-shot)

`companions/premiere/lvm_restore_versions.jsx`

No install required. In Premiere: **File → Scripts → Run Script File** →
pick the `.jsx`. A summary appears in an `alert()` dialog.

For convenience you can drop the file into Premiere's Scripts folder
(the menu still asks you to pick a file — Premiere doesn't auto-list
scripts):

| OS | Path |
| --- | --- |
| Windows | `C:\Program Files\Adobe\Adobe Premiere Pro <version>\Scripts\` |
| macOS | `/Applications/Adobe Premiere Pro <version>/Scripts/` |

### Option 2: CEP panel (recommended for LVM-driven sync)

`companions/premiere/lvm_panel/`

A small docked panel that watches for trigger files written by LVM and
runs the rename automatically. Once installed, LVM's status-bar **Sync
Premiere** button and the **Auto-sync after promote → Premiere**
checkbox both work.

#### Install — one click from inside LVM (recommended)

1. Open LVM → **File → Project Settings** → **NLE Companion Scripts**.
2. Find the **Premiere** row and click **Install panel...**.

That's it. LVM copies the panel into Adobe's CEP extensions folder for
the current user *and* enables `PlayerDebugMode` for every CSXS version
Premiere might use (9, 10, 11, 12), so you don't need to touch the
registry or Terminal yourself. Both writes go to the user-scoped
locations on Windows and macOS — no admin password is needed.

Restart Premiere if it was already open, then open **Window → Extensions
→ LVM Sync Versions** to dock the panel.

To remove later, click **Uninstall panel** from the same dialog.

#### Install — manual (if you prefer)

Copy `companions/premiere/lvm_panel/` to:

| OS | Path |
| --- | --- |
| Windows | `%APPDATA%\Adobe\CEP\extensions\com.polisvfx.lvm.panel\` |
| macOS | `~/Library/Application Support/Adobe/CEP/extensions/com.polisvfx.lvm.panel/` |

The folder must contain `CSXS/manifest.xml` directly — i.e. the install
path is `…\com.polisvfx.lvm.panel\CSXS\manifest.xml`, not nested inside
another `lvm_panel` folder.

Then enable unsigned extensions (one-time, required because the panel
is not Adobe-signed):

| OS | What to do |
| --- | --- |
| Windows | Registry: set `HKEY_CURRENT_USER\Software\Adobe\CSXS.<N>\PlayerDebugMode` (string `"1"`) where `<N>` is your CEP version (`11` for Premiere 2024+, `10` for 2022/2023). |
| macOS | Terminal: `defaults write com.adobe.CSXS.11 PlayerDebugMode 1` (replace `11` with your CEP version). |

Then restart Premiere and dock via **Window → Extensions → LVM Sync
Versions**.

#### Use

The panel writes a heartbeat file every ~10 s — that's how LVM's status
bar knows it's connected. When LVM's **Sync Premiere** button is clicked
(or auto-sync fires after a promote), LVM drops a JSON trigger file
into `%APPDATA%\LVM\triggers\` (or platform equivalent); the panel
picks it up within ~1 s, runs the rename, and deletes the trigger.

The panel has its own **Sync Versions Now** button for manual runs
without involving LVM.

#### Free vs paid Adobe Premiere

There is no Free/Studio split for Premiere — the CEP panel and the
.jsx work in any licensed Premiere Pro install (CC subscription).

### ExtendScript vs UXP

Both options use ExtendScript, which Adobe supports through **September
2026**. A UXP port of the panel is on the roadmap and will not change
the on-disk sidecar contract these scripts depend on.

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

## Run from inside LVM

LVM exposes the rename as two status-bar buttons (right-hand side):
**Sync Resolve** and **Sync Premiere**. Each one is enabled when its
target NLE is reachable — greyed out otherwise, with a tooltip
explaining what's missing.

The same actions are available from **Tools → Sync Names in NLE**, and
each NLE has its own **auto-sync after promote** checkbox in
**Project Settings → NLE Companion Scripts**.

### DaVinci Resolve (Studio only)

> **Studio-only**: LVM-driven sync needs external Python scripting,
> which Blackmagic gates behind a paid Studio license. Free Resolve
> users get exactly the same renaming via the in-Resolve script —
> the only difference is whether you click inside Resolve or inside
> LVM.

LVM imports `DaVinciResolveScript` directly into its own Python at
runtime via ctypes, so renames run **in-process** and stream output
to LVM's log dock (View → Log). No subprocess, no env-var setup, no
Python interpreter required on the user's PATH.

### Adobe Premiere

LVM writes a JSON **trigger file** to `%APPDATA%\LVM\triggers\` (or
platform equivalent). The installed CEP panel polls that directory,
runs the rename inside Premiere, and deletes the trigger. The panel
must be installed and Premiere must be running with the panel
docked — see the install instructions above.

LVM detects an alive panel via a heartbeat file the panel writes every
~10 s; the **Sync Premiere** status-bar button stays disabled until
the heartbeat is fresh. Auto-sync after promote also gates on the
heartbeat (and logs a skip line if the panel isn't running).

## Roadmap

- **v2** — Port the Premiere CEP panel to UXP for long-term Adobe
  support. ExtendScript / CEP are deprecated by Adobe in September 2026;
  UXP keeps the same on-disk sidecar contract so this should be a
  mechanical port for the host script and a more involved one for the
  panel manifest + UI.
