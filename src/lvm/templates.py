"""
Config Templates / Presets - save and load project configuration presets.

Templates store project-level defaults without watched_sources,
allowing users to quickly set up new projects with proven configurations.

Templates can be stored per-user (global) or per-project (local).
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

from .models import ProjectConfig

logger = logging.getLogger(__name__)


def get_user_templates_dir() -> Path:
    """Return the user-global templates directory."""
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        base = Path.home() / ".config"
    d = base / "lvm" / "templates"
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_project_templates_dir(project_dir: str) -> Path:
    """Return the project-local templates directory."""
    d = Path(project_dir) / ".lvm" / "templates"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _sanitize_template_name(name: str) -> str:
    """Convert a template name to a safe filename."""
    safe = name.strip().replace(" ", "_")
    safe = re.sub(r"[^\w\-]", "", safe)
    return safe.lower() or "template"


def save_template(
    config: ProjectConfig,
    template_name: str,
    location: str = "user",
    project_dir: Optional[str] = None,
) -> str:
    """Save a project config as a template (without watched_sources).

    Args:
        config: The project config to save as template.
        template_name: Human-readable name for the template.
        location: "user" for global, "project" for project-local.
        project_dir: Required when location="project".

    Returns:
        Path to the saved template file.
    """
    d = config.to_dict()
    # Remove instance-specific data
    d.pop("watched_sources", None)
    d.pop("discovery_search_history", None)
    d.pop("naming_configured", None)
    d.pop("default_naming_rule", None)
    d.pop("project_root", None)
    # Store the template name
    d["template_name"] = template_name

    filename = f"{_sanitize_template_name(template_name)}.json"

    if location == "project" and project_dir:
        templates_dir = get_project_templates_dir(project_dir)
    else:
        templates_dir = get_user_templates_dir()

    filepath = templates_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=2, ensure_ascii=False)
    logger.info(f"Template saved: {filepath}")
    return str(filepath)


def list_templates(project_dir: Optional[str] = None) -> list[dict]:
    """List all available templates from both user and project directories.

    Project-local templates take precedence over user-global ones with
    the same name.

    Returns:
        List of dicts with 'name', 'path', 'project_name', 'location' keys.
    """
    templates_by_name: dict[str, dict] = {}

    # User-global templates first (lower priority)
    user_dir = get_user_templates_dir()
    for f in sorted(user_dir.glob("*.json")):
        info = _read_template_info(f, "user")
        if info:
            templates_by_name[info["name"]] = info

    # Project-local templates override user-global
    if project_dir:
        proj_dir = Path(project_dir) / ".lvm" / "templates"
        if proj_dir.exists():
            for f in sorted(proj_dir.glob("*.json")):
                info = _read_template_info(f, "project")
                if info:
                    templates_by_name[info["name"]] = info

    return sorted(templates_by_name.values(), key=lambda t: t["name"])


def _read_template_info(filepath: Path, location: str) -> Optional[dict]:
    """Read template metadata from a JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return {
            "name": data.get("template_name", filepath.stem),
            "path": str(filepath),
            "project_name": data.get("project_name", ""),
            "location": location,
        }
    except (json.JSONDecodeError, OSError):
        return None


def load_template(template_path: str) -> dict:
    """Load a template and return its data dict."""
    with open(template_path, "r", encoding="utf-8") as f:
        return json.load(f)


def apply_template(config: ProjectConfig, template_data: dict):
    """Apply template settings to a config, overwriting project-level defaults."""
    field_map = {
        "base_path_template": "base_path_template",
        "latest_path_template": "latest_path_template",
        "default_version_pattern": "default_version_pattern",
        "default_file_extensions": "default_file_extensions",
        "default_file_rename_template": "default_file_rename_template",
        "default_link_mode": "default_link_mode",
        "default_date_format": "default_date_format",
        "name_whitelist": "name_whitelist",
        "name_blacklist": "name_blacklist",
        "task_tokens": "task_tokens",
        "groups": "groups",
        "timecode_mode": "timecode_mode",
        "block_incomplete_sequences": "block_incomplete_sequences",
        "pre_promote_cmd": "pre_promote_cmd",
        "post_promote_cmd": "post_promote_cmd",
    }
    for key, attr in field_map.items():
        if key in template_data:
            setattr(config, attr, template_data[key])
