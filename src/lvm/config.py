"""
Config Manager - handles loading and saving project configuration.

All paths in a project file are stored relative to the project file's location.
At load time, project_dir is computed and stored on ProjectConfig so that
other modules can resolve relative paths to absolute ones.
"""

import json
import logging
import re
from pathlib import Path

from .models import ProjectConfig, make_relative
from .task_tokens import derive_source_tokens

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_NAME = "lvm_project.json"

# Regex matching {group} plus an optional trailing divider (/ \ _ - .)
_GROUP_TOKEN_RE = re.compile(r"\{group\}([/\\_.\-])?")


def _expand_group_token(template: str, group_name: str) -> str:
    """Replace {group} in a template with the group name.

    If the source has no group, the token AND any single trailing divider
    character (/ \\ _ - .) are removed entirely so the path stays clean.
    """
    if "{group}" not in template:
        return template
    if group_name:
        return template.replace("{group}", group_name)
    # No group — remove token + trailing divider
    return _GROUP_TOKEN_RE.sub("", template)


def _resolve_group_root(config: "ProjectConfig", group_name: str) -> str:
    """Return the absolute root directory for a group.

    Falls back to project_dir when the group has no root_dir set or
    the source has no group.
    """
    fallback = config.effective_project_root or ""
    if not group_name:
        return fallback
    props = config.groups.get(group_name, {})
    root = props.get("root_dir", "")
    return root if root else fallback


def apply_project_defaults(config: ProjectConfig):
    """Apply project-level defaults to sources that don't have overrides.

    For any source field where the override flag is False, copy the
    project-level default value into the source's field.
    """
    for source in config.watched_sources:
        if not source.override_version_pattern:
            source.version_pattern = config.default_version_pattern
        if not source.override_file_extensions:
            source.file_extensions = list(config.default_file_extensions)
        if not source.override_latest_target and config.latest_path_template:
            # Resolve latest target from template using source context
            # Derive tokens from actual filename if available, falling back to source name
            token_input = source.sample_filename or source.name
            tokens = derive_source_tokens(token_input, config.task_tokens)
            tpl = config.latest_path_template
            tpl = tpl.replace("{project_root}", config.effective_project_root)
            tpl = tpl.replace("{group_root}", _resolve_group_root(config, source.group))
            tpl = tpl.replace("{source_name}", tokens["source_name"])
            tpl = tpl.replace("{source_basename}", tokens["source_basename"])
            tpl = tpl.replace("{source_fullname}", tokens["source_fullname"])
            tpl = tpl.replace("{source_filename}", tokens["source_filename"])
            tpl = tpl.replace("{source_dir}", source.source_dir)
            tpl = _expand_group_token(tpl, source.group)
            # Relative paths resolve from the source directory
            resolved = Path(tpl)
            if not resolved.is_absolute() and source.source_dir:
                resolved = Path(source.source_dir) / resolved
            elif not resolved.is_absolute() and config.project_dir:
                resolved = Path(config.project_dir) / resolved
            source.latest_target = str(resolved.resolve())
        if not source.override_file_rename:
            source.file_rename_template = config.default_file_rename_template
        if not source.override_link_mode:
            source.link_mode = config.default_link_mode


def load_config(config_path: str) -> ProjectConfig:
    """Load a project config from a JSON file.

    Sets config.project_dir to the directory containing the config file
    so that relative paths can be resolved at runtime.
    """
    path = Path(config_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    config = ProjectConfig.from_dict(data)
    config.project_dir = str(path.parent)

    # Resolve relative project_root to absolute
    if config.project_root and not Path(config.project_root).is_absolute():
        config.project_root = str((path.parent / config.project_root).resolve())

    # Resolve relative source paths to absolute for runtime use
    for source in config.watched_sources:
        if source.source_dir and not Path(source.source_dir).is_absolute():
            source.source_dir = str((path.parent / source.source_dir).resolve())
        if source.latest_target and not Path(source.latest_target).is_absolute():
            source.latest_target = str((path.parent / source.latest_target).resolve())

    # Resolve relative group root_dir paths to absolute
    for props in config.groups.values():
        rd = props.get("root_dir", "")
        if rd and not Path(rd).is_absolute():
            props["root_dir"] = str((path.parent / rd).resolve())

    # Apply project defaults to sources without overrides
    apply_project_defaults(config)

    logger.info(f"Loaded project '{config.project_name}' with {len(config.watched_sources)} sources")
    return config


def save_config(config: ProjectConfig, config_path: str):
    """Save a project config to a JSON file.

    Converts absolute source/target paths to relative (relative to the
    config file location) before writing.
    """
    path = Path(config_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    project_dir = str(path.parent)

    # Build serializable dict with relative paths
    data = config.to_dict()
    for source_data in data.get("watched_sources", []):
        sd = source_data.get("source_dir", "")
        if sd and Path(sd).is_absolute():
            source_data["source_dir"] = make_relative(sd, project_dir)
        lt = source_data.get("latest_target", "")
        if lt and Path(lt).is_absolute():
            source_data["latest_target"] = make_relative(lt, project_dir)

    # Convert project_root to relative
    pr = data.get("project_root", "")
    if pr and Path(pr).is_absolute():
        data["project_root"] = make_relative(pr, project_dir)

    # Convert group root_dir paths to relative
    for grp_props in data.get("groups", {}).values():
        rd = grp_props.get("root_dir", "")
        if rd and Path(rd).is_absolute():
            grp_props["root_dir"] = make_relative(rd, project_dir)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Keep project_dir in sync
    config.project_dir = project_dir
    logger.info(f"Saved project config to {path}")


def _sanitize_filename(name: str) -> str:
    """Convert a project name to a safe filename component.

    Replaces spaces with underscores, strips non-alphanumeric characters
    (except _ and -), and lowercases the result.
    """
    import re as _re
    safe = name.strip().replace(" ", "_")
    safe = _re.sub(r"[^\w\-]", "", safe)
    return safe.lower() or "project"


def create_project(
    project_name: str,
    project_dir: str,
    name_whitelist: list = None,
    name_blacklist: list = None,
    task_tokens: list = None,
    output_filename: str = None,
    project_root: str = "",
    save_dir: str = "",
) -> str:
    """Create a new project file and return its path.

    The filename defaults to ``<project_name>_lvm.json`` derived from the
    project name. Pass *output_filename* explicitly to override.

    Args:
        project_name: Display name for the project.
        project_dir: Directory where the project file will be created
            (used as both project_root and save location when separate
            paths are not provided).
        name_whitelist: Keywords to include in discovery (e.g. ["comp", "grade"]).
        name_blacklist: Keywords to exclude from discovery (e.g. ["denoise", "wip"]).
        output_filename: Name of the project file (default: derived from project_name).
        project_root: Explicit project root for ``{project_root}`` token.
            Defaults to *save_dir* (or *project_dir*).
        save_dir: Directory where the JSON file is saved. Defaults to
            *project_dir*.

    Returns:
        Absolute path to the created project file.
    """
    if output_filename is None:
        output_filename = f"{_sanitize_filename(project_name)}_lvm.json"

    # Determine save location and project root
    save_dir = str(Path(save_dir or project_dir).resolve())
    project_root = str(Path(project_root or project_dir).resolve())
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    # Only store project_root when it differs from the save location
    store_root = project_root if project_root != save_dir else ""

    config = ProjectConfig(
        project_name=project_name,
        name_whitelist=name_whitelist or [],
        name_blacklist=name_blacklist or [],
        task_tokens=task_tokens or [],
        project_root=store_root,
    )

    output_path = str(Path(save_dir) / output_filename)
    save_config(config, output_path)
    return output_path


def create_example_config(output_path: str = None) -> str:
    """Create an example configuration file and return its path."""
    if output_path is None:
        output_path = DEFAULT_CONFIG_NAME

    example = {
        "project_name": "My Commercial Project",
        "latest_path_template": "../online",
        "default_version_pattern": "_v{version}",
        "name_whitelist": ["comp", "grade"],
        "name_blacklist": ["wip", "temp"],
        "watched_sources": [
            {
                "name": "Hero Comp",
                "source_dir": "shots/hero/renders",
                "version_pattern": "hero_comp_v{version}",
                "file_extensions": [".exr"],
                "latest_target": "shots/hero/online",
                "override_version_pattern": True,
                "override_file_extensions": True,
                "override_latest_target": True,
            },
            {
                "name": "BG Plate Grade",
                "source_dir": "shots/bg/renders",
            },
        ],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(example, f, indent=2, ensure_ascii=False)

    return output_path
