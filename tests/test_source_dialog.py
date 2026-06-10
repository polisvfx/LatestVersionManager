"""
Tests for SourceDialog — verifies that editing a source through the dialog
preserves fields the dialog does not manage (group, sample_filename,
manual_versions, hooks, rename template, ...). Regression test for the bug
where get_source() rebuilt the WatchedSource from scratch and silently wiped
those fields.

GUI test (imports PySide6) — run locally, not part of the portable CI suite.
"""

import sys
import unittest
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "src"))

from PySide6.QtWidgets import QApplication

# A QApplication is required before any widget can be instantiated.
_app = QApplication.instance() or QApplication(sys.argv)

from app import SourceDialog
from src.lvm.models import ProjectConfig, WatchedSource


def _make_full_source() -> WatchedSource:
    """A source with every dialog-unmanaged field set to a non-default value."""
    return WatchedSource(
        name="HeroComp",
        source_dir="/projects/show/shots/sh010/renders",
        version_pattern="_v{version}",
        file_extensions=[".exr", ".mov"],
        latest_target="/projects/show/shots/sh010/online",
        file_rename_template="{source_name}_latest",
        history_filename=".latest_history_hero.json",
        link_mode="hardlink",
        sample_filename="sh010_comp_v001.1001.exr",
        group="Shots",
        date_format="YYMMDD",
        override_version_pattern=True,
        override_date_format=True,
        override_file_extensions=True,
        override_latest_target=True,
        override_file_rename=True,
        override_link_mode=True,
        block_incomplete_sequences=False,
        override_block_incomplete=True,
        pre_promote_cmd="echo pre {version}",
        post_promote_cmd="echo post {version}",
        override_pre_promote_cmd=True,
        override_post_promote_cmd=True,
        added_at="2026-01-15T10:00:00",
        manual_versions=[{"version_string": "v099", "source_path": "/tmp/v099"}],
    )


class TestSourceDialogEditPreservesFields(unittest.TestCase):
    """get_source() on an edit must not wipe unmanaged fields."""

    def setUp(self):
        self.config = ProjectConfig(project_name="Show")
        self.original = _make_full_source()
        self.dialog = SourceDialog(source=self.original, project_config=self.config)

    def test_unmanaged_fields_survive_unchanged_accept(self):
        result = self.dialog.get_source()
        self.assertEqual(result.group, "Shots")
        self.assertEqual(result.sample_filename, "sh010_comp_v001.1001.exr")
        self.assertEqual(result.file_rename_template, "{source_name}_latest")
        self.assertEqual(result.history_filename, ".latest_history_hero.json")
        self.assertEqual(result.pre_promote_cmd, "echo pre {version}")
        self.assertEqual(result.post_promote_cmd, "echo post {version}")
        self.assertTrue(result.override_file_rename)
        self.assertTrue(result.override_pre_promote_cmd)
        self.assertTrue(result.override_post_promote_cmd)
        self.assertFalse(result.block_incomplete_sequences)
        self.assertTrue(result.override_block_incomplete)
        self.assertEqual(result.added_at, "2026-01-15T10:00:00")
        self.assertEqual(
            result.manual_versions,
            [{"version_string": "v099", "source_path": "/tmp/v099"}],
        )

    def test_managed_fields_round_trip(self):
        result = self.dialog.get_source()
        self.assertEqual(result.name, "HeroComp")
        self.assertEqual(result.source_dir, "/projects/show/shots/sh010/renders")
        self.assertEqual(result.version_pattern, "_v{version}")
        self.assertEqual(result.file_extensions, [".exr", ".mov"])
        self.assertEqual(result.latest_target, "/projects/show/shots/sh010/online")
        self.assertEqual(result.link_mode, "hardlink")
        self.assertEqual(result.date_format, "YYMMDD")
        self.assertTrue(result.override_version_pattern)
        self.assertTrue(result.override_date_format)
        self.assertTrue(result.override_file_extensions)
        self.assertTrue(result.override_latest_target)
        self.assertTrue(result.override_link_mode)

    def test_edited_field_applies_without_touching_others(self):
        self.dialog.name_edit.setText("HeroCompRenamed")
        result = self.dialog.get_source()
        self.assertEqual(result.name, "HeroCompRenamed")
        self.assertEqual(result.group, "Shots")
        self.assertEqual(result.manual_versions, self.original.manual_versions)

    def test_result_is_a_copy_not_the_original(self):
        result = self.dialog.get_source()
        self.assertIsNot(result, self.original)
        result.manual_versions.append({"version_string": "v100"})
        self.assertEqual(len(self.original.manual_versions), 1)


class TestSourceDialogNewSource(unittest.TestCase):
    """The add-source path (no original) keeps producing clean defaults."""

    def test_new_source_defaults(self):
        dlg = SourceDialog(source=None, project_config=ProjectConfig(project_name="Show"))
        dlg.name_edit.setText("NewSource")
        dlg.source_dir_edit.setText("/projects/show/new")
        result = dlg.get_source()
        self.assertEqual(result.name, "NewSource")
        self.assertEqual(result.source_dir, "/projects/show/new")
        self.assertEqual(result.group, "")
        self.assertEqual(result.manual_versions, [])
        self.assertEqual(result.file_rename_template, "")
        self.assertTrue(result.block_incomplete_sequences)
        self.assertFalse(result.override_block_incomplete)


if __name__ == "__main__":
    unittest.main()
