"""
Config Templates / Presets - save and load project configuration presets.

Templates store project-level defaults without watched_sources,
allowing users to quickly set up new projects with proven configurations.

Templates can be stored per-user (global) or per-project (local).
"""

import copy
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

from .fileutil import atomic_write_json
from .models import ProjectConfig

logger = logging.getLogger(__name__)

# Project-level fields managed by templates. Every key here is persisted
# explicitly on save and reset to ProjectConfig defaults if absent on apply.
_TEMPLATE_FIELDS: tuple[str, ...] = (
    "base_path_template",
    "latest_path_template",
    "default_version_pattern",
    "default_file_extensions",
    "default_file_rename_template",
    "default_link_mode",
    "default_date_format",
    "name_whitelist",
    "name_blacklist",
    "task_tokens",
    "groups",
    "timecode_mode",
    "block_incomplete_sequences",
    "pre_promote_cmd",
    "post_promote_cmd",
    "watch_mode",
    "watch_poll_interval",
    "skip_resolve",
    "source_list_columns",
    "nle_auto_sync_resolve",
    "nle_auto_sync_premiere",
    "nle_rename_include_version",
    "nle_rename_include_frame",
    "nle_rename_include_extension",
    "nle_rename_custom_enabled",
    "nle_rename_custom_template",
    "nle_resolve_metadata_field",
    "discover_and_promote_enabled",
    "auto_promote_notifications",
    "thumbnails_enabled",
)


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

    Explicitly writes all template-managed fields (including those equal to
    defaults) so applying the template reliably resets target configs, and
    strips machine-specific absolute ``root_dir`` paths from groups.
    """
    d = config.to_dict()
    for field in _TEMPLATE_FIELDS:
        d[field] = copy.deepcopy(getattr(config, field))

    # Strip machine-specific absolute root_dir paths from group definitions
    if isinstance(d.get("groups"), dict):
        cleaned_groups = {}
        for gname, props in d["groups"].items():
            if isinstance(props, dict):
                gcopy = dict(props)
                root_dir = gcopy.get("root_dir", "")
                if root_dir and Path(root_dir).is_absolute():
                    gcopy["root_dir"] = ""
                cleaned_groups[gname] = gcopy
        d["groups"] = cleaned_groups

    # Remove instance-specific data
    d.pop("watched_sources", None)
    d.pop("discovery_search_history", None)
    d.pop("naming_configured", None)
    d.pop("default_naming_rule", None)
    d.pop("project_root", None)
    d["template_name"] = template_name

    filename = f"{_sanitize_template_name(template_name)}.json"

    if location == "project" and project_dir:
        templates_dir = get_project_templates_dir(project_dir)
    else:
        templates_dir = get_user_templates_dir()

    filepath = templates_dir / filename
    atomic_write_json(filepath, d)
    logger.info(f"Template saved: {filepath}")
    return str(filepath)


def list_templates(project_dir: Optional[str] = None) -> list[dict]:
    """List available templates from both user and project directories.

    User-global and project-local templates are kept distinct (keyed by
    ``(location, name)``) so a shared/untrusted project folder cannot silently
    shadow a user's personal template with the same name.
    """
    results: list[dict] = []

    try:
        user_dir = get_user_templates_dir()
        for f in sorted(user_dir.glob("*.json")):
            info = _read_template_info(f, "user")
            if info:
                results.append(info)
    except OSError:
        pass

    if project_dir:
        proj_dir = Path(project_dir) / ".lvm" / "templates"
        try:
            if proj_dir.exists():
                for f in sorted(proj_dir.glob("*.json")):
                    info = _read_template_info(f, "project")
                    if info:
                        results.append(info)
        except OSError:
            pass

    return sorted(results, key=lambda t: (t["name"].lower(), 0 if t["location"] == "user" else 1))


def _read_template_info(filepath: Path, location: str) -> Optional[dict]:
    """Read template metadata from a JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return None
        return {
            "name": str(data.get("template_name") or filepath.stem),
            "path": str(filepath),
            "project_name": str(data.get("project_name") or ""),
            "location": location,
            "has_hooks": bool(
                (data.get("pre_promote_cmd") or "").strip()
                or (data.get("post_promote_cmd") or "").strip()
            ),
        }
    except (json.JSONDecodeError, OSError, TypeError):
        return None


def load_template(template_path: str) -> dict:
    """Load a template and return its data dict."""
    with open(template_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Template file {template_path} must contain a JSON object.")
    return data


def apply_template(config: ProjectConfig, template_data: dict):
    """Apply template settings to *config*, resetting omitted fields to defaults
    and deep-copying mutable values."""
    if not isinstance(template_data, dict):
        return
    defaults = ProjectConfig(project_name="")
    for field in _TEMPLATE_FIELDS:
        if field in template_data and template_data[field] is not None:
            setattr(config, field, copy.deepcopy(template_data[field]))
        else:
            setattr(config, field, copy.deepcopy(getattr(defaults, field)))
