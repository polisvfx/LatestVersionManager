"""Tests for the History Timeline dialog.

The pure model builder (`build_timeline_model`) is tested without Qt — it's the
piece doing all the date math and filtering. The dialog instantiation test runs
behind an offscreen QApplication when PySide6 is available, and is skipped
otherwise so the suite still passes in headless CI without Qt installed.
"""

import os
import sys
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.models import HistoryEntry, WatchedSource, ProjectConfig


def _make_project(n_sources: int, m_entries: int, base_dir: Path) -> ProjectConfig:
    """Build a fake project with N sources × M history sidecars on disk."""
    cfg = ProjectConfig(project_name="TimelineTest")
    cfg.groups = {"comp": {"color": "#aabbcc"}, "grade": {"color": "#ccbbaa"}}
    base_time = datetime(2025, 1, 1, 12, 0, 0)

    for i in range(n_sources):
        target = base_dir / f"latest_{i:03d}"
        target.mkdir(parents=True, exist_ok=True)
        source = WatchedSource(
            name=f"shot{i:03d}_comp",
            source_dir=str(base_dir / f"src_{i:03d}"),
            latest_target=str(target),
            group="comp" if i % 2 == 0 else "grade",
        )
        cfg.watched_sources.append(source)

        # Build sidecar JSON directly (newest first, like HistoryManager.save).
        history_entries = []
        for k in range(m_entries):
            t = base_time + timedelta(days=i, hours=k * 6)
            entry = HistoryEntry(
                version=f"v{k+1:03d}",
                source=str(base_dir / f"src_{i:03d}" / f"src_v{k+1:03d}"),
                set_by="tester",
                set_at=t.isoformat(timespec="seconds"),
                frame_range=f"1001-{1000 + (k + 1) * 10}",
                frame_count=(k + 1) * 10,
                file_count=(k + 1) * 10,
            )
            history_entries.append(entry)
        # Newest first
        history_entries.reverse()
        sidecar = target / ".latest_history.json"
        sidecar.write_text(json.dumps({
            "current": history_entries[0].to_dict() if history_entries else None,
            "history": [e.to_dict() for e in history_entries],
        }), encoding="utf-8")

    return cfg


def _loaded_for(cfg: ProjectConfig):
    """Helper: simulate what HistoryLoadWorker yields."""
    from lvm.history import HistoryManager
    loaded = []
    for s in cfg.watched_sources:
        if not s.latest_target:
            loaded.append((s, []))
            continue
        path = os.path.join(s.latest_target, s.history_filename)
        if not os.path.exists(path):
            loaded.append((s, []))
            continue
        loaded.append((s, HistoryManager(path).get_history()))
    return loaded


class BuildTimelineModelTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def _import_builder(self):
        # Imported lazily so the no-PySide6 environment can still load the test
        # module — the import will skip via the dialog test below.
        from app.dialogs.history_timeline import build_timeline_model
        return build_timeline_model

    def test_bar_count_matches_history_entries(self):
        build = self._import_builder()
        cfg = _make_project(n_sources=5, m_entries=4, base_dir=self.tmp)
        loaded = _loaded_for(cfg)
        model = build(loaded, now=datetime(2026, 1, 1))

        self.assertEqual(len(model["rows"]), 5)
        total_bars = sum(len(r["bars"]) for r in model["rows"])
        self.assertEqual(total_bars, 5 * 4)
        # Every row gets a chronological set, each bar's start < end.
        for row in model["rows"]:
            for bar in row["bars"]:
                self.assertLess(bar["start"], bar["end"])
            for a, b in zip(row["bars"], row["bars"][1:]):
                self.assertLessEqual(a["start"], b["start"])
                # And consecutive bars should chain end == next.start.
                self.assertEqual(a["end"], b["start"])

    def test_name_filter(self):
        build = self._import_builder()
        cfg = _make_project(n_sources=3, m_entries=2, base_dir=self.tmp)
        loaded = _loaded_for(cfg)
        model = build(loaded, name_filter="shot001")
        self.assertEqual(len(model["rows"]), 1)
        self.assertEqual(model["rows"][0]["source"].name, "shot001_comp")

    def test_group_filter(self):
        build = self._import_builder()
        cfg = _make_project(n_sources=4, m_entries=2, base_dir=self.tmp)
        loaded = _loaded_for(cfg)
        # In _make_project, even indices → "comp", odd → "grade".
        model = build(loaded, group_filter="grade")
        names = {r["source"].name for r in model["rows"]}
        self.assertEqual(names, {"shot001_comp", "shot003_comp"})

    def test_date_range_filter(self):
        build = self._import_builder()
        cfg = _make_project(n_sources=3, m_entries=3, base_dir=self.tmp)
        loaded = _loaded_for(cfg)
        # Only keep bars whose start falls inside Jan 2025 days 1-2.
        model = build(
            loaded,
            date_from=datetime(2025, 1, 1),
            date_to=datetime(2025, 1, 2, 23, 59, 59),
        )
        for row in model["rows"]:
            for bar in row["bars"]:
                self.assertGreaterEqual(bar["start"], datetime(2025, 1, 1))
                self.assertLessEqual(bar["start"], datetime(2025, 1, 2, 23, 59, 59))

    def test_handles_missing_sidecars(self):
        build = self._import_builder()
        cfg = _make_project(n_sources=2, m_entries=2, base_dir=self.tmp)
        # Add an extra source with no sidecar — must not blow up.
        cfg.watched_sources.append(WatchedSource(
            name="no_history", source_dir=str(self.tmp / "x"),
            latest_target=str(self.tmp / "empty_target"),
        ))
        loaded = _loaded_for(cfg)
        model = build(loaded, now=datetime(2026, 1, 1))
        self.assertEqual(len(model["rows"]), 3)
        # The extra source produces zero bars.
        extra = [r for r in model["rows"] if r["source"].name == "no_history"][0]
        self.assertEqual(extra["bars"], [])


class DialogSmokeTest(unittest.TestCase):
    """Construct the dialog under an offscreen QApplication.

    Skipped when PySide6 isn't importable so this file still runs in
    minimal CI environments.
    """

    @classmethod
    def setUpClass(cls):
        try:
            os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
            from PySide6.QtWidgets import QApplication
        except ImportError:
            raise unittest.SkipTest("PySide6 not available")
        cls._app = QApplication.instance() or QApplication([])

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def test_dialog_loads_without_error(self):
        from app.dialogs.history_timeline import HistoryTimelineDialog, build_timeline_model

        cfg = _make_project(n_sources=3, m_entries=3, base_dir=self.tmp)
        dlg = HistoryTimelineDialog(cfg)
        try:
            # Drive the loaded data through synchronously instead of starting
            # the worker (we don't want a thread race in a unit test).
            dlg._loaded = _loaded_for(cfg)
            dlg._rerender()

            # The scene should contain at least one bar item per history entry.
            from PySide6.QtWidgets import QGraphicsRectItem
            from app.dialogs.history_timeline import _BarItem
            bars = [i for i in dlg.scene.items() if isinstance(i, _BarItem)]
            self.assertEqual(len(bars), 3 * 3)

            # And the source_activated signal fires when a bar is clicked.
            received = []
            dlg.source_activated.connect(lambda name: received.append(name))
            bars[0]._on_click(bars[0]._source_name)
            self.assertEqual(received, [bars[0]._source_name])
        finally:
            dlg.deleteLater()


if __name__ == "__main__":
    unittest.main()
