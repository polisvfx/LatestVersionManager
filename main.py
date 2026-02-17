#!/usr/bin/env python3
"""
Latest Version Manager - CLI entry point.

Usage:
    python main.py setup                    Set up a new project
    python main.py discover <directory>     Scan a directory for versioned content
    python main.py init                     Create an example config
    python main.py scan <config>            Scan all sources for versions
    python main.py status <config>          Show current status of all sources
    python main.py promote <config> <source_name> <version>   Promote a version
    python main.py promote-all <config>     Promote all sources to their highest version
    python main.py history <config> <source_name>             Show history
    python main.py verify <config>          Verify integrity of all sources
    python main.py validate <config>        Validate config file
"""

import sys
import os
import re
import json
import logging
import argparse

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.lvm.config import load_config, create_example_config, create_project
from src.lvm.scanner import VersionScanner
from src.lvm.promoter import Promoter, generate_report
from src.lvm.models import WatchedSource
from src.lvm.discovery import discover, format_discovery_report
from src.lvm.timecode import populate_timecodes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _human_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def cmd_setup(args):
    """Set up a new project."""
    project_name = args.name
    project_dir = args.dir or os.getcwd()

    whitelist = [kw.strip() for kw in args.whitelist.split(",")] if args.whitelist else []
    blacklist = [kw.strip() for kw in args.blacklist.split(",")] if args.blacklist else []
    task_tokens = [t.strip() for t in args.tasks.split(",")] if args.tasks else []

    print(f"Setting up project: {project_name}")
    print(f"Project directory: {os.path.abspath(project_dir)}")
    if whitelist:
        print(f"Whitelist: {', '.join(whitelist)}")
    if blacklist:
        print(f"Blacklist: {', '.join(blacklist)}")
    if task_tokens:
        print(f"Task tokens: {', '.join(task_tokens)}")

    output = create_project(
        project_name=project_name,
        project_dir=project_dir,
        name_whitelist=whitelist,
        name_blacklist=blacklist,
        task_tokens=task_tokens,
        output_filename=os.path.basename(args.output) if args.output else None,
    )

    print(f"\nProject file created: {output}")
    print("\nNext steps:")
    print("  1. Use 'discover' to scan for existing versioned content")
    print("  2. Edit project settings (GUI: File > Project Settings)")
    print("  3. Use 'scan' to verify your configured sources")


def cmd_discover(args):
    """Discover versioned content in a directory tree."""
    root_dir = args.directory
    if not os.path.isdir(root_dir):
        print(f"Directory not found: {root_dir}")
        sys.exit(1)

    whitelist = [kw.strip() for kw in args.whitelist.split(",")] if args.whitelist else None
    blacklist = [kw.strip() for kw in args.blacklist.split(",")] if args.blacklist else None

    print(f"Scanning for versioned content in: {os.path.abspath(root_dir)}")
    print(f"Max depth: {args.depth}")
    if args.extensions:
        print(f"Extensions filter: {args.extensions}")
    if whitelist:
        print(f"Whitelist: {', '.join(whitelist)}")
    if blacklist:
        print(f"Blacklist: {', '.join(blacklist)}")
    print()

    extensions = None
    if args.extensions:
        extensions = [e if e.startswith(".") else f".{e}" for e in args.extensions]

    results = discover(
        root_dir=root_dir,
        max_depth=args.depth,
        extensions=extensions,
        whitelist=whitelist,
        blacklist=blacklist,
    )

    # Populate timecodes (lazy-loaded after discovery scan)
    for result in results:
        populate_timecodes(result.versions_found)

    report = format_discovery_report(results, root_dir)
    print(report)


def cmd_init(args):
    """Create an example config file."""
    path = create_example_config(args.output)
    print(f"Created example config: {path}")
    print("Edit this file to match your project structure, then use 'scan' to test it.")


def cmd_scan(args):
    """Scan all watched sources and list detected versions."""
    config = load_config(args.config)
    print(f"Project: {config.project_name}\n")

    for source in config.watched_sources:
        print(f"--- {source.name} ---")
        print(f"  Source: {source.source_dir}")
        print(f"  Pattern: {source.version_pattern}")

        scanner = VersionScanner(source, config.task_tokens)
        versions = scanner.scan()

        if not versions:
            print("  No versions found.\n")
            continue

        # Populate timecodes based on project setting
        if config.timecode_mode != "never":
            populate_timecodes(versions)

        # Check current version
        promoter = Promoter(source, config.task_tokens)
        current = promoter.get_current_version()
        current_ver = current.version if current else None

        for v in versions:
            marker = " <-- CURRENT" if v.version_string == current_ver else ""
            frames = f"  frames: {v.frame_range}" if v.frame_range else ""
            tc = f"  TC: {v.start_timecode}" if v.start_timecode else ""
            print(
                f"  {v.version_string}  |  {v.file_count} files  |  "
                f"{v.total_size_human}{frames}{tc}{marker}"
            )
        print()


def cmd_status(args):
    """Show current status of all sources."""
    config = load_config(args.config)
    print(f"Project: {config.project_name}\n")

    for source in config.watched_sources:
        promoter = Promoter(source, config.task_tokens)
        current = promoter.get_current_version()
        integrity = promoter.verify()

        status = current.version if current else "NOT SET"
        icon = "OK" if integrity["valid"] else "WARNING"

        print(f"  [{icon}] {source.name}: {status}")
        if not integrity["valid"]:
            print(f"         {integrity['message']}")

        if current:
            print(f"         Set by {current.set_by} at {current.set_at}")
            if current.frame_range:
                print(f"         Frames: {current.frame_range} ({current.frame_count} files)")
            if current.start_timecode:
                print(f"         Timecode: {current.start_timecode}")
    print()


def cmd_promote(args):
    """Promote a specific version."""
    config = load_config(args.config)

    # Find the named source
    source = _find_source(config, args.source_name)
    if not source:
        print(f"Source '{args.source_name}' not found in config.")
        print(f"Available: {', '.join(s.name for s in config.watched_sources)}")
        sys.exit(1)

    # Find the requested version
    scanner = VersionScanner(source, config.task_tokens)
    versions = scanner.scan()
    if config.timecode_mode != "never":
        populate_timecodes(versions)

    target_version = None
    for v in versions:
        if v.version_string == args.version or str(v.version_number) == args.version:
            target_version = v
            break

    if not target_version:
        print(f"Version '{args.version}' not found for source '{source.name}'.")
        print(f"Available: {', '.join(v.version_string for v in versions)}")
        sys.exit(1)

    promoter = Promoter(source, config.task_tokens)
    current_entry = promoter.get_current_version()

    # Dry run mode
    if args.dry_run:
        preview = promoter.dry_run(target_version)
        print(f"\nDry Run — {preview['total_files']} files, {preview['link_mode']} mode")
        print(f"Target: {preview['target_dir']}\n")
        for item in preview['file_map']:
            src_name = os.path.basename(item['source'])
            print(f"  {src_name}  ->  {item['target_name']}")
        print(f"\nTotal: {_human_size(preview['total_size_bytes'])}")

        # Frame range mismatch warning
        if current_entry and current_entry.frame_range and target_version.frame_range:
            if current_entry.frame_range != target_version.frame_range:
                print(f"\n  ** Frame range changed: {current_entry.frame_range} -> {target_version.frame_range}")
            if current_entry.frame_count != target_version.frame_count:
                print(f"  ** Frame count changed: {current_entry.frame_count} -> {target_version.frame_count}")
        return

    # Normal promote — show info
    print(f"Promoting {source.name} -> {target_version.version_string}")
    print(f"  Source: {target_version.source_path}")
    print(f"  Target: {source.latest_target}")
    print(f"  Files:  {target_version.file_count} ({target_version.total_size_human})")
    if target_version.frame_range:
        print(f"  Frames: {target_version.frame_range}")
    if target_version.start_timecode:
        print(f"  TC:     {target_version.start_timecode}")

    # Warn if timecode changed from current
    if (current_entry and current_entry.start_timecode and target_version.start_timecode
            and current_entry.start_timecode != target_version.start_timecode):
        print(f"  ** Timecode changed: {current_entry.start_timecode} -> {target_version.start_timecode}")

    # Frame range mismatch warning
    if current_entry and current_entry.frame_range and target_version.frame_range:
        if current_entry.frame_range != target_version.frame_range:
            print(f"  ** Frame range changed: {current_entry.frame_range} -> {target_version.frame_range}")
        if current_entry.frame_count != target_version.frame_count:
            print(f"  ** Frame count changed: {current_entry.frame_count} -> {target_version.frame_count}")

    if not args.yes:
        confirm = input("\nProceed? [y/N] ").strip().lower()
        if confirm != "y":
            print("Cancelled.")
            return

    # Do it
    def progress(current, total, filename):
        pct = int(current / total * 100)
        print(f"\r  Copying: {current}/{total} ({pct}%) - {filename}", end="", flush=True)

    entry = promoter.promote(target_version, progress_callback=progress)
    print(f"\n\nDone. {source.name} is now at {entry.version}")

    # Write report if requested
    if args.report:
        report = generate_report(entry, source)
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"Report written to: {args.report}")


def cmd_promote_all(args):
    """Promote all sources to their highest version."""
    config = load_config(args.config)
    print(f"Project: {config.project_name}\n")

    promote_list = []
    skipped = []
    already_current = []

    for source in config.watched_sources:
        if not source.latest_target:
            skipped.append(f"{source.name} (no latest target)")
            continue

        scanner = VersionScanner(source, config.task_tokens)
        versions = scanner.scan()
        if not versions:
            skipped.append(f"{source.name} (no versions found)")
            continue

        highest = versions[-1]
        promoter = Promoter(source, config.task_tokens)
        current = promoter.get_current_version()

        if not args.force and current and current.version == highest.version_string:
            integrity = promoter.verify()
            if integrity["valid"]:
                already_current.append(f"{source.name} (already on {highest.version_string})")
                continue

        promote_list.append((source, highest, promoter))

    if not promote_list:
        print("Nothing to promote.")
        if already_current:
            print(f"\nAlready current ({len(already_current)}):")
            for s in already_current:
                print(f"  {s}")
        if skipped:
            print(f"\nSkipped ({len(skipped)}):")
            for s in skipped:
                print(f"  {s}")
        return

    # Show plan
    print(f"Will promote {len(promote_list)} source(s):\n")
    for source, version, _ in promote_list:
        print(f"  {source.name}: {version.version_string} ({version.total_size_human})")

    if already_current:
        print(f"\nAlready current ({len(already_current)}):")
        for s in already_current:
            print(f"  {s}")
    if skipped:
        print(f"\nSkipped ({len(skipped)}):")
        for s in skipped:
            print(f"  {s}")

    # Dry run — just show the plan
    if args.dry_run:
        print("\n(dry run — no files were copied)")
        return

    if not args.yes:
        confirm = input("\nProceed? [y/N] ").strip().lower()
        if confirm != "y":
            print("Cancelled.")
            return

    # Execute promotions
    reports = []
    for i, (source, version, promoter) in enumerate(promote_list):
        print(f"\n[{i+1}/{len(promote_list)}] Promoting {source.name} -> {version.version_string}...")
        try:
            entry = promoter.promote(version)
            print(f"  Done.")
            reports.append(generate_report(entry, source))
        except Exception as e:
            print(f"  FAILED: {e}")

    print(f"\nBatch promotion complete: {len(reports)}/{len(promote_list)} succeeded.")

    # Write report if requested
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(reports, f, indent=2, ensure_ascii=False)
        print(f"Report written to: {args.report}")


def cmd_history(args):
    """Show promotion history for a source."""
    config = load_config(args.config)

    source = _find_source(config, args.source_name)
    if not source:
        print(f"Source '{args.source_name}' not found.")
        sys.exit(1)

    promoter = Promoter(source, config.task_tokens)
    history = promoter.get_history()

    if not history:
        print(f"No promotion history for '{source.name}'.")
        return

    print(f"Promotion history for: {source.name}\n")
    for i, entry in enumerate(history):
        marker = " <-- CURRENT" if i == 0 else ""
        frames = f"  ({entry.frame_range})" if entry.frame_range else ""
        tc = f"  TC: {entry.start_timecode}" if entry.start_timecode else ""
        print(f"  {entry.set_at}  |  {entry.version}  |  by {entry.set_by}{frames}{tc}{marker}")


def cmd_verify(args):
    """Verify integrity of all latest targets."""
    config = load_config(args.config)
    print(f"Verifying: {config.project_name}\n")

    all_ok = True
    for source in config.watched_sources:
        promoter = Promoter(source, config.task_tokens)
        result = promoter.verify()
        icon = "OK" if result["valid"] else "!!"
        print(f"  [{icon}] {source.name}: {result['message']}")
        if not result["valid"]:
            all_ok = False

    if all_ok:
        print("\nAll sources verified OK.")
    else:
        print("\nSome sources have issues - check above.")


def cmd_validate(args):
    """Validate a project config file."""
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"INVALID: Failed to load config: {e}")
        sys.exit(1)

    issues = []
    warnings = []

    # Check project name
    if not config.project_name or config.project_name == "Untitled":
        warnings.append("Project name is default/empty")

    # Check each source
    for source in config.watched_sources:
        if not os.path.isdir(source.source_dir):
            issues.append(f"{source.name}: source_dir does not exist: {source.source_dir}")
        if source.latest_target and not os.path.isdir(source.latest_target):
            warnings.append(f"{source.name}: latest_target does not exist yet: {source.latest_target}")
        if not source.file_extensions:
            warnings.append(f"{source.name}: no file extensions configured")
        if not source.version_pattern:
            issues.append(f"{source.name}: no version pattern configured")

    # Check templates
    if config.latest_path_template and "{" in config.latest_path_template:
        tokens_found = re.findall(r"\{(\w+)\}", config.latest_path_template)
        known = {"project_root", "group_root", "source_name", "source_basename",
                 "source_fullname", "source_filename", "source_dir", "group"}
        unknown = set(tokens_found) - known
        if unknown:
            warnings.append(f"Unknown tokens in latest_path_template: {unknown}")

    # Check groups
    for source in config.watched_sources:
        if source.group and source.group not in config.groups:
            warnings.append(f"{source.name}: assigned to group '{source.group}' which is not defined")

    # Report
    if issues:
        print("ERRORS:")
        for issue in issues:
            print(f"  [!!] {issue}")
    if warnings:
        print("WARNINGS:")
        for w in warnings:
            print(f"  [?]  {w}")
    if not issues and not warnings:
        print(f"Config OK: {config.project_name} ({len(config.watched_sources)} sources)")
    elif not issues:
        print(f"\nConfig valid with {len(warnings)} warning(s)")
    else:
        print(f"\nConfig has {len(issues)} error(s)")
        sys.exit(1)


def _find_source(config, name: str) -> WatchedSource:
    """Find a watched source by name (case-insensitive)."""
    name_lower = name.lower()
    for s in config.watched_sources:
        if s.name.lower() == name_lower:
            return s
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Latest Version Manager - manage versioned file sequences for VFX",
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # setup
    p_setup = subparsers.add_parser("setup", help="Set up a new LVM project")
    p_setup.add_argument("--name", required=True, help="Project name")
    p_setup.add_argument("--dir", default=None, help="Project directory (default: current dir)")
    p_setup.add_argument("--whitelist", default="", help="Comma-separated keywords to include (e.g. 'comp,grade')")
    p_setup.add_argument("--blacklist", default="", help="Comma-separated keywords to exclude (e.g. 'denoise,wip')")
    p_setup.add_argument("--tasks", default="", help="Comma-separated task names (e.g. 'comp,grade,dmp,comp_%%%')")
    p_setup.add_argument("-o", "--output", default=None, help="Output filename (default: lvm_project.json)")

    # discover
    p_discover = subparsers.add_parser("discover", help="Scan a directory for versioned content")
    p_discover.add_argument("directory", help="Root directory to scan")
    p_discover.add_argument("--depth", type=int, default=4, help="Max directory depth (default: 4)")
    p_discover.add_argument("--extensions", nargs="*", help="File extensions to look for (e.g. .exr .dpx)")
    p_discover.add_argument("--whitelist", default="", help="Comma-separated keywords to include")
    p_discover.add_argument("--blacklist", default="", help="Comma-separated keywords to exclude")

    # init
    p_init = subparsers.add_parser("init", help="Create an example config file")
    p_init.add_argument("-o", "--output", default="lvm_project.json", help="Output path")

    # scan
    p_scan = subparsers.add_parser("scan", help="Scan sources for versions")
    p_scan.add_argument("config", help="Path to project config JSON")

    # status
    p_status = subparsers.add_parser("status", help="Show current status")
    p_status.add_argument("config", help="Path to project config JSON")

    # promote
    p_promote = subparsers.add_parser("promote", help="Promote a version to latest")
    p_promote.add_argument("config", help="Path to project config JSON")
    p_promote.add_argument("source_name", help="Name of the watched source")
    p_promote.add_argument("version", help="Version to promote (e.g. v003 or 3)")
    p_promote.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    p_promote.add_argument("--dry-run", action="store_true", help="Preview file operations without copying")
    p_promote.add_argument("--report", help="Write promotion report to file (JSON)")

    # promote-all
    p_promote_all = subparsers.add_parser("promote-all", help="Promote all sources to highest version")
    p_promote_all.add_argument("config", help="Path to project config JSON")
    p_promote_all.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    p_promote_all.add_argument("--force", action="store_true", help="Include sources already on highest version")
    p_promote_all.add_argument("--dry-run", action="store_true", help="Preview without promoting")
    p_promote_all.add_argument("--report", help="Write promotion report to file (JSON)")

    # history
    p_history = subparsers.add_parser("history", help="Show promotion history")
    p_history.add_argument("config", help="Path to project config JSON")
    p_history.add_argument("source_name", help="Name of the watched source")

    # verify
    p_verify = subparsers.add_parser("verify", help="Verify integrity of latest targets")
    p_verify.add_argument("config", help="Path to project config JSON")

    # validate
    p_validate = subparsers.add_parser("validate", help="Validate project config file")
    p_validate.add_argument("config", help="Path to project config JSON")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "setup": cmd_setup,
        "discover": cmd_discover,
        "init": cmd_init,
        "scan": cmd_scan,
        "status": cmd_status,
        "promote": cmd_promote,
        "promote-all": cmd_promote_all,
        "history": cmd_history,
        "verify": cmd_verify,
        "validate": cmd_validate,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
