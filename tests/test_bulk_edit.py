"""
Tests for lvm.config.apply_bulk_edits — the Qt-free core of the GUI's
"Edit N Sources..." bulk edit.

Portable: no Qt imports, temp-free, runs in CI.
"""

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lvm.config import apply_bulk_edits, apply_project_defaults, BULK_EDITABLE_FIELDS
from lvm.models import ProjectConfig, WatchedSource


def _make_config() -> ProjectConfig:
    config = ProjectConfig(
        project_name="Show",
        default_version_pattern="_v{version}",
        default_file_extensions=[".exr"],
        default_link_mode="copy",
        default_date_format="",
    )
    for name in ("A", "B", "C"):
        config.watched_sources.append(WatchedSource(
            name=name,
            source_dir=f"/renders/{name}",
            link_mode="copy",
            file_extensions=[".exr"],
            group="Old" if name == "A" else "",
            manual_versions=[{"version_string": "v009"}] if name == "A" else [],
        ))
    return config


class TestApplyBulkEdits(unittest.TestCase):

    def test_override_sets_flag_and_value(self):
        config = _make_config()
        n = apply_bulk_edits(config, ["A", "B"], {
            "link_mode": {"action": "override", "value": "hardlink"},
        })
        self.assertEqual(n, 2)
        a, b, c = config.watched_sources
        self.assertEqual(a.link_mode, "hardlink")
        self.assertTrue(a.override_link_mode)
        self.assertEqual(b.link_mode, "hardlink")
        self.assertTrue(b.override_link_mode)
        # C untouched
        self.assertEqual(c.link_mode, "copy")
        self.assertFalse(c.override_link_mode)

    def test_inherit_clears_flag_and_defaults_refill(self):
        config = _make_config()
        a = config.watched_sources[0]
        a.override_link_mode = True
        a.link_mode = "symlink"

        n = apply_bulk_edits(config, ["A"], {
            "link_mode": {"action": "inherit"},
        })
        self.assertEqual(n, 1)
        self.assertFalse(a.override_link_mode)
        # Caller contract: run apply_project_defaults to refill
        apply_project_defaults(config)
        self.assertEqual(a.link_mode, "copy")

    def test_group_set_and_clear(self):
        config = _make_config()
        n = apply_bulk_edits(config, ["A", "B", "C"], {
            "group": {"action": "set", "value": "Exteriors"},
        })
        self.assertEqual(n, 3)
        self.assertTrue(all(s.group == "Exteriors" for s in config.watched_sources))

        apply_bulk_edits(config, ["B"], {"group": {"action": "set", "value": ""}})
        self.assertEqual(config.watched_sources[1].group, "")
        self.assertEqual(config.watched_sources[0].group, "Exteriors")

    def test_list_values_not_shared_between_sources(self):
        config = _make_config()
        apply_bulk_edits(config, ["A", "B"], {
            "file_extensions": {"action": "override", "value": [".exr", ".mov"]},
        })
        a, b = config.watched_sources[0], config.watched_sources[1]
        self.assertEqual(a.file_extensions, b.file_extensions)
        self.assertIsNot(a.file_extensions, b.file_extensions)
        a.file_extensions.append(".dpx")
        self.assertNotIn(".dpx", b.file_extensions)

    def test_untouched_fields_stay_untouched(self):
        config = _make_config()
        a = config.watched_sources[0]
        apply_bulk_edits(config, ["A"], {
            "date_format": {"action": "override", "value": "YYMMDD"},
        })
        self.assertEqual(a.group, "Old")
        self.assertEqual(a.manual_versions, [{"version_string": "v009"}])
        self.assertEqual(a.source_dir, "/renders/A")
        self.assertEqual(a.name, "A")
        self.assertEqual(a.date_format, "YYMMDD")
        self.assertTrue(a.override_date_format)

    def test_block_incomplete_override(self):
        config = _make_config()
        apply_bulk_edits(config, ["A", "C"], {
            "block_incomplete_sequences": {"action": "override", "value": False},
        })
        a, b, c = config.watched_sources
        self.assertFalse(a.block_incomplete_sequences)
        self.assertTrue(a.override_block_incomplete)
        self.assertFalse(c.block_incomplete_sequences)
        self.assertTrue(b.block_incomplete_sequences)  # untouched

    def test_unknown_source_names_ignored(self):
        config = _make_config()
        n = apply_bulk_edits(config, ["Nope", "AlsoNope"], {
            "link_mode": {"action": "override", "value": "symlink"},
        })
        self.assertEqual(n, 0)
        self.assertTrue(all(s.link_mode == "copy" for s in config.watched_sources))

    def test_unknown_field_ignored(self):
        config = _make_config()
        n = apply_bulk_edits(config, ["A"], {
            "latest_target": {"action": "override", "value": "/evil"},
            "no_such_field": {"action": "set", "value": 1},
        })
        self.assertEqual(n, 0)
        self.assertEqual(config.watched_sources[0].latest_target, "")

    def test_every_advertised_field_is_real(self):
        """BULK_EDITABLE_FIELDS must name actual WatchedSource attributes."""
        ws = WatchedSource(name="x", source_dir="/tmp")
        for field_name, flag in BULK_EDITABLE_FIELDS.items():
            self.assertTrue(hasattr(ws, field_name), field_name)
            if flag is not None:
                self.assertTrue(hasattr(ws, flag), flag)

    def test_mixed_actions_single_call(self):
        config = _make_config()
        b = config.watched_sources[1]
        b.override_version_pattern = True
        b.version_pattern = "_take{version}"

        n = apply_bulk_edits(config, ["B"], {
            "group": {"action": "set", "value": "G"},
            "version_pattern": {"action": "inherit"},
            "link_mode": {"action": "override", "value": "symlink"},
        })
        self.assertEqual(n, 1)
        self.assertEqual(b.group, "G")
        self.assertFalse(b.override_version_pattern)
        self.assertTrue(b.override_link_mode)
        self.assertEqual(b.link_mode, "symlink")


if __name__ == "__main__":
    unittest.main()
