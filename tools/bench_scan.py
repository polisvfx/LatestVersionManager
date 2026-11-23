"""Benchmark scan / verify / dry run / promote on generated test media.

Generates S sources x V versions x F frames (folder per version, like
``shot1_v003/shot1_v003.1001.exr``) under a root directory, then times the
operations LVM runs against them. Point ``--root`` at a NAS share to measure
network cost; the default is a local temp dir.

    python tools/bench_scan.py                          # small local run
    python tools/bench_scan.py --root \\\\nas\\scratch\\lvm_bench --frames 2000
    python tools/bench_scan.py --profile bench_prof     # + cProfile per phase

Phases (each source in turn, single-threaded, so the numbers are per-call
cost rather than ThreadPool throughput):

    scan-cold      VersionScanner.scan() with no previous results
    scan-warm      scan(previous=...) — the incremental rescan
    promote-copy   promote the newest version (copy mode, empty target)
    verify         Promoter.verify() of what was just promoted
    dry-run        Promoter.dry_run() of the previous version
    repromote      promote the previous version over the newest (swap path)
    promote-link   promote the newest version in hardlink mode (own target)

Nothing outside the root is touched. The generated tree is deleted afterwards
unless --keep is given (reuse it with --reuse).
"""

from __future__ import annotations

import argparse
import cProfile
import io
import os
import pstats
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lvm.models import WatchedSource  # noqa: E402
from lvm.promoter import Promoter, PromotionError  # noqa: E402
from lvm.scanner import VersionScanner  # noqa: E402

TREE_NAME = "lvm_bench"


def generate(root: Path, sources: int, versions: int, frames: int, size: int) -> None:
    payload = b"\0" * size
    for s in range(1, sources + 1):
        for v in range(1, versions + 1):
            vdir = root / f"src{s}" / "renders" / f"shot{s}_v{v:03d}"
            vdir.mkdir(parents=True, exist_ok=True)
            for f in range(1001, 1001 + frames):
                (vdir / f"shot{s}_v{v:03d}.{f}.exr").write_bytes(payload)


def make_sources(root: Path, sources: int, link_mode: str = "copy",
                 target_name: str = "online") -> list[WatchedSource]:
    return [
        WatchedSource(
            name=f"shot{s}",
            source_dir=str(root / f"src{s}" / "renders"),
            latest_target=str(root / f"src{s}" / target_name),
            version_pattern="_v{version}",
            file_extensions=[".exr"],
            link_mode=link_mode,
        )
        for s in range(1, sources + 1)
    ]


class Bench:
    def __init__(self, profile_dir: Path | None):
        self.profile_dir = profile_dir
        self.results: list[tuple[str, float, str]] = []

    def run(self, phase: str, fn, note=""):
        """Time *fn*. *note* is a string or a callable evaluated afterwards."""
        prof = cProfile.Profile() if self.profile_dir else None
        start = time.perf_counter()
        if prof:
            prof.enable()
        try:
            out = fn()
            text = note() if callable(note) else note
        except PromotionError as e:
            out = None
            text = f"failed: {str(e).splitlines()[0]}"
        finally:
            if prof:
                prof.disable()
        elapsed = time.perf_counter() - start
        self.results.append((phase, elapsed, text))
        if prof:
            self.profile_dir.mkdir(parents=True, exist_ok=True)
            prof.dump_stats(str(self.profile_dir / f"{phase}.prof"))
            buf = io.StringIO()
            pstats.Stats(prof, stream=buf).sort_stats("cumulative").print_stats(12)
            (self.profile_dir / f"{phase}.txt").write_text(buf.getvalue(), encoding="utf-8")
        return out

    def report(self, files_per_source: int, sources: int) -> str:
        lines = [f"{'phase':<14} {'total s':>9} {'per source':>11}  note",
                 "-" * 60]
        for phase, secs, note in self.results:
            lines.append(f"{phase:<14} {secs:9.3f} {secs / max(sources, 1):11.4f}  {note}")
        lines.append(f"({sources} source(s), {files_per_source} frame(s) per version)")
        return "\n".join(lines)


def run(args) -> Bench:
    base = Path(args.root) if args.root else Path(tempfile.mkdtemp(prefix="lvm_bench_"))
    root = base / TREE_NAME
    if not args.reuse:
        if root.exists():
            shutil.rmtree(root)
        t = time.perf_counter()
        generate(root, args.sources, args.versions, args.frames, args.size)
        print(f"Generated {args.sources}x{args.versions}x{args.frames} files in "
              f"{time.perf_counter() - t:.2f} s under {root}")
    elif not root.exists():
        raise SystemExit(f"--reuse: {root} does not exist")

    bench = Bench(Path(args.profile) if args.profile else None)
    sources = make_sources(root, args.sources)
    scanners = [VersionScanner(s) for s in sources]

    cold = bench.run("scan-cold", lambda: [sc.scan() for sc in scanners])
    bench.run("scan-warm", lambda: [sc.scan(previous=prev) for sc, prev in zip(scanners, cold)],
              note=lambda: f"reused {sum(sc.last_reused for sc in scanners)} folder(s)")

    promoters = [Promoter(s) for s in sources]
    newest = [vs[-1] for vs in cold]
    previous = [vs[-2] if len(vs) > 1 else vs[-1] for vs in cold]

    def promote_all(proms, versions):
        for p, v in zip(proms, versions):
            p.promote(v, user="bench")
        return True

    def swap_note():
        return f"last swap {max(p.last_swap_seconds or 0 for p in promoters):.3f} s"

    bench.run("promote-copy", lambda: promote_all(promoters, newest), note=swap_note)
    bench.run("verify", lambda: [p.verify() for p in promoters])
    bench.run("dry-run", lambda: [p.dry_run(v) for p, v in zip(promoters, previous)])
    bench.run("repromote", lambda: promote_all(promoters, previous), note=swap_note)

    link_promoters = [Promoter(s) for s in make_sources(root, args.sources, "hardlink", "online_link")]
    bench.run("promote-link", lambda: promote_all(link_promoters, newest))

    if not args.keep:
        shutil.rmtree(root, ignore_errors=True)
        if not args.root:
            shutil.rmtree(base, ignore_errors=True)
    else:
        print(f"Kept {root}")
    return bench


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", help="directory to create the test tree in (default: temp dir)")
    ap.add_argument("--sources", type=int, default=4)
    ap.add_argument("--versions", type=int, default=5)
    ap.add_argument("--frames", type=int, default=200)
    ap.add_argument("--size", type=int, default=0, help="bytes per frame (default 0)")
    ap.add_argument("--profile", metavar="DIR", help="write cProfile .prof/.txt per phase to DIR")
    ap.add_argument("--keep", action="store_true", help="keep the generated tree")
    ap.add_argument("--reuse", action="store_true", help="reuse a tree kept by --keep")
    args = ap.parse_args(argv)
    if args.versions < 1 or args.sources < 1 or args.frames < 1:
        ap.error("--sources, --versions and --frames must be at least 1")

    bench = run(args)
    print(bench.report(args.frames, args.sources))
    return 0


if __name__ == "__main__":
    sys.exit(main())
