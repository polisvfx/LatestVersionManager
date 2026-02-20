# Latest Version Manager (LVM)

A desktop tool for managing versioned renders and media in VFX and post-production pipelines.

In a typical compositing or grading workflow, artists iterate through numbered versions of their renders (`hero_comp_v001`, `hero_comp_v002`, `hero_comp_v003`...) while downstream tools like DaVinci Resolve or Premiere sometimes read from a single "latest" folder or file. LVM helps keeping track - it lets you pick which version is live, copies or links the files into the target location, and keeps a full history of what was promoted and when.


<img width="1919" height="989" alt="Screenshot 2026-02-16 232233" src="https://github.com/user-attachments/assets/03e9952e-198c-48e4-a645-3ec862697565" />
<img width="1153" height="786" alt="Screenshot 2026-02-16 232045" src="https://github.com/user-attachments/assets/f1d7146e-161b-4538-8a00-500153c13636" />
<img width="815" height="801" alt="Screenshot 2026-02-16 232057" src="https://github.com/user-attachments/assets/c532c403-0f57-487a-85cc-61183f83cbc0" />
<img width="1418" height="1158" alt="Screenshot 2026-02-16 232548" src="https://github.com/user-attachments/assets/5c312f5e-e832-4adf-bc24-ea480da9649d" />
<img width="835" height="515" alt="Screenshot 2026-02-16 233813" src="https://github.com/user-attachments/assets/0618d082-62f3-40d7-97f1-bd92c51ae5b8" />


## What It Does

**Discover versioned content automatically.** Point LVM at a directory tree and it walks through it looking for version-numbered folders and image sequences. It understands common VFX naming conventions, frame numbering (`.1001.exr`, `_1001.exr`), and a wide range of media formats (EXR, DPX, TIFF, MOV, MXF, and more).

**Promote any version to "latest" with one click.** Select a source and a version, and LVM copies (or symlinks/hardlinks) all the files into the target directory your tools are reading from. File names are cleaned up automatically - version tags are stripped so the downstream tool sees a stable, predictable path regardless of which version is active.

**Track frame ranges and timecodes.** LVM detects frame ranges in image sequences and reads embedded timecodes. When you switch versions, it warns you if the frame range or timecode has changed - a common gotcha when swapping renders mid-project.

**Keep a full promotion history.** Every promotion is logged with the version, timestamp, frame range, and who made the change. You can review the history for any source to see exactly what was live and when - useful for debugging or rolling back.

**Verify integrity at any time.** The verify command checks that the files in every "latest" folder actually match what's expected - catching silent corruption, accidental deletions, or manual edits that went sideways.

**Batch promote everything at once.** The "promote all" feature scans every source in your project and promotes each one to its highest available version in a single operation. Sources already on the latest version are skipped automatically.

**Watch for new versions in real time.** The file watcher monitors your source directories and alerts you through the GUI when new versions appear, so you don't have to manually rescan.

**Organize sources into groups.** For larger projects with many shots or sequences, sources can be grouped and managed together. Path templates with tokens like `{source_name}`, `{group}`, and `{task}` automate where files come from and where they go.

**Filter with whitelists and blacklists.** Control which folders get picked up during discovery by including or excluding keywords - handy for ignoring WIP renders, test outputs, or denoise passes you don't want in the pipeline.

## How to Use It

LVM works as both a **GUI application** and a **command-line tool**.

### GUI

```bash
python app.py
```
Theres also a launch-script for each OS (start_lvm.bat/.sh/.command)

The GUI provides a full project management interface - add sources, browse versions, promote with a progress bar, view history, configure settings, and monitor directories for changes.

### CLI

```bash
# Set up a new project
python main.py setup --name "MyShow" --dir ./myshow

# Discover versioned content in a directory
python main.py discover ./renders --depth 4

# Scan configured sources and list available versions
python main.py scan myproject.json

# Check what's currently promoted
python main.py status myproject.json

# Promote a specific version
python main.py promote myproject.json hero_comp v003

# Promote everything to the latest version
python main.py promote-all myproject.json -y

# Preview what would happen without copying anything
python main.py promote myproject.json hero_comp v003 --dry-run

# View promotion history
python main.py history myproject.json hero_comp

# Verify file integrity across all sources
python main.py verify myproject.json
```

## Installation

### Pre-built Binaries

Download the latest release for your platform from the [Releases](../../releases) page:

| Platform | Download | Notes |
|---|---|---|
| **Windows** | `LatestVersionManager-windows.zip` | Extract and run `LatestVersionManager.exe` |
| **macOS** | `LatestVersionManager-macos.dmg` | Mount the DMG and drag to Applications |
| **Linux** | `LatestVersionManager-linux.zip` | Extract and run `./LatestVersionManager` |

#### macOS: "Cannot verify" warning

When opening the app for the first time on macOS, you may see:

> *"Apple could not verify LatestVersionManager.app is free of malware that may harm your Mac or compromise your privacy."*

This happens because the app is not notarized with an Apple Developer certificate. To fix this, open Terminal and run:

```bash
xattr -cr /Applications/LatestVersionManager.app
```

Then open the app normally. You only need to do this once.

### From Source

Requires **Python 3.12+**.

```bash
pip install -r requirements.txt
```

Dependencies: PySide6 (Qt6 GUI), watchdog (file system monitoring), fileseq (frame sequence handling).

## Link Modes

LVM supports three ways of placing files in the target directory:

- **Copy** - duplicates the files (safest, works everywhere)
- **Symlink** - creates symbolic links (fast, saves disk space, may require admin on Windows)
- **Hardlink** - creates hard links (fast, saves disk space, same-volume only)

The tool detects what's available on your system and falls back gracefully.
