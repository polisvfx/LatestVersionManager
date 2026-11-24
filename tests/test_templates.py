"""Unit tests for lvm.templates — save, load, list shadowing protection, apply."""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from lvm import templates
from lvm.models import ProjectConfig


class TestTemplates(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lvm_tpl_"))
        self.user_dir = self.tmp / "user_templates"
        self.proj_dir = self.tmp / "project"
        self.proj_dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_save_load_apply_round_trip_and_reset(self):
        abs_group_root = str((self.tmp / "machine_specific").resolve())
        cfg = ProjectConfig(
            project_name="ShowA",
            default_link_mode="hardlink",
            default_version_pattern=".v{version}",
            nle_rename_custom_enabled=True,
            nle_rename_custom_template="{source_title}_edit",
            nle_resolve_metadata_field="Description",
            groups={"comp": {"color": "#112233", "root_dir": abs_group_root}},
        )
        with mock.patch.object(templates, "get_user_templates_dir", return_value=self.user_dir):
            saved_path = templates.save_template(cfg, "Studio Default", location="user")
            self.assertTrue(Path(saved_path).is_file())
            data = templates.load_template(saved_path)

        # Absolute group root_dir must be stripped when saving a template
        self.assertEqual(data["groups"]["comp"].get("root_dir", ""), "")

        # Apply to another project config and verify all NLE fields round-trip
        target = ProjectConfig(project_name="ShowB")
        templates.apply_template(target, data)
        self.assertEqual(target.default_link_mode, "hardlink")
        self.assertTrue(target.nle_rename_custom_enabled)
        self.assertEqual(target.nle_rename_custom_template, "{source_title}_edit")
        self.assertEqual(target.nle_resolve_metadata_field, "Description")

        # Applying a fresh minimal template resets custom fields back to defaults
        templates.apply_template(target, {"default_link_mode": "copy"})
        self.assertEqual(target.default_link_mode, "copy")
        self.assertFalse(target.nle_rename_custom_enabled)

    def test_project_template_cannot_shadow_user_template(self):
        cfg_user = ProjectConfig(project_name="UserP", default_link_mode="hardlink")
        cfg_proj = ProjectConfig(project_name="ProjP", default_link_mode="symlink")
        with mock.patch.object(templates, "get_user_templates_dir", return_value=self.user_dir):
            templates.save_template(cfg_user, "Shared", location="user")
            templates.save_template(cfg_proj, "Shared", location="project", project_dir=str(self.proj_dir))
            found = templates.list_templates(str(self.proj_dir))
        # Both are returned distinctly with the user's personal template sorted first
        names_locs = [(t["name"], t["location"]) for t in found]
        self.assertEqual(names_locs, [("Shared", "user"), ("Shared", "project")])


if __name__ == "__main__":
    unittest.main()
