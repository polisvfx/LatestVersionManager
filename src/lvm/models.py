"""
Data models for the Latest Version Manager.
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

# Default file extensions including video formats
DEFAULT_FILE_EXTENSIONS = [".exr", ".dpx", ".tiff", ".tif", ".png", ".jpg", ".mov", ".mxf", ".mp4"]


def resolve_path(template: str, tokens: dict, project_dir: str = "") -> str:
    """Expand token placeholders in a path template and resolve relative to project_dir.

    Tokens use curly braces: {shot}, {sequence}, {version}, {task}, {source_dir}, etc.
    {project_root} is automatically set to project_dir if not provided in tokens.

    Returns an absolute path string.
    """
    all_tokens = dict(tokens)
    if "project_root" not in all_tokens:
        all_tokens["project_root"] = project_dir

    # Expand known tokens, leave unknown ones intact
    result = template
    for key, value in all_tokens.items():
        result = result.replace("{" + key + "}", str(value))

    # Make absolute relative to project_dir
    path = Path(result)
    if not path.is_absolute() and project_dir:
        path = Path(project_dir) / path

    return str(path)


def make_relative(path: str, project_dir: str) -> str:
    """Convert an absolute path to a relative path based on project_dir.

    If the path is already relative or cannot be made relative, return as-is.
    """
    try:
        return os.path.relpath(path, project_dir).replace("\\", "/")
    except ValueError:
        # Different drive on Windows
        return path.replace("\\", "/")


@dataclass
class VersionInfo:
    """Represents a detected version in a watched folder."""
    version_string: str          # e.g. "v003"
    version_number: int          # e.g. 3
    source_path: str             # Full path to the version folder or file
    frame_range: Optional[str] = None   # e.g. "1001-1120"
    frame_count: int = 0
    file_count: int = 0
    total_size_bytes: int = 0
    start_timecode: Optional[str] = None  # e.g. "01:00:00:00"

    @property
    def total_size_human(self) -> str:
        """Return human-readable file size."""
        size = self.total_size_bytes
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"


@dataclass
class HistoryEntry:
    """A single entry in the promotion history."""
    version: str
    source: str
    set_by: str
    set_at: str
    frame_range: Optional[str] = None
    frame_count: int = 0
    file_count: int = 0
    start_timecode: Optional[str] = None
    source_mtime: Optional[float] = None   # max mtime of source files at promotion time
    target_mtime: Optional[float] = None   # max mtime of target files right after promotion

    def to_dict(self) -> dict:
        d = {
            "version": self.version,
            "source": self.source,
            "set_by": self.set_by,
            "set_at": self.set_at,
            "frame_range": self.frame_range,
            "frame_count": self.frame_count,
            "file_count": self.file_count,
            "start_timecode": self.start_timecode,
        }
        if self.source_mtime is not None:
            d["source_mtime"] = self.source_mtime
        if self.target_mtime is not None:
            d["target_mtime"] = self.target_mtime
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "HistoryEntry":
        return cls(
            version=data["version"],
            source=data["source"],
            set_by=data.get("set_by", "unknown"),
            set_at=data.get("set_at", ""),
            frame_range=data.get("frame_range"),
            frame_count=data.get("frame_count", 0),
            file_count=data.get("file_count", 0),
            start_timecode=data.get("start_timecode"),
            source_mtime=data.get("source_mtime"),
            target_mtime=data.get("target_mtime"),
        )

    @classmethod
    def from_version_info(cls, version_info: "VersionInfo", user: str) -> "HistoryEntry":
        return cls(
            version=version_info.version_string,
            source=version_info.source_path,
            set_by=user,
            set_at=datetime.now().isoformat(timespec="seconds"),
            frame_range=version_info.frame_range,
            frame_count=version_info.frame_count,
            file_count=version_info.file_count,
            start_timecode=version_info.start_timecode,
        )


@dataclass
class WatchedSource:
    """Configuration for a single watched folder/source."""
    name: str
    source_dir: str
    version_pattern: str = "_v{version}"
    file_extensions: list = field(default_factory=lambda: list(DEFAULT_FILE_EXTENSIONS))
    latest_target: str = ""
    file_rename_template: str = ""  # e.g. "{source_name}_latest" → name.1001.exr
    history_filename: str = ".latest_history.json"
    link_mode: str = "copy"  # "copy", "symlink", or "hardlink"
    sample_filename: str = ""  # Representative filename for token derivation
    group: str = ""  # Group name (must match a key in ProjectConfig.groups)
    # Override flags — when False, the source inherits the project-wide default
    override_version_pattern: bool = False
    override_file_extensions: bool = False
    override_latest_target: bool = False
    override_file_rename: bool = False
    override_link_mode: bool = False

    @property
    def use_symlinks(self) -> bool:
        """Backward-compatible property."""
        return self.link_mode == "symlink"

    @property
    def has_overrides(self) -> bool:
        """True if this source overrides latest target or link mode settings."""
        return self.override_latest_target or self.override_link_mode

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "source_dir": self.source_dir,
            "version_pattern": self.version_pattern,
            "file_extensions": self.file_extensions,
            "latest_target": self.latest_target,
            "history_filename": self.history_filename,
            "link_mode": self.link_mode,
        }
        if self.file_rename_template:
            d["file_rename_template"] = self.file_rename_template
        if self.sample_filename:
            d["sample_filename"] = self.sample_filename
        if self.group:
            d["group"] = self.group
        # Only serialize override flags when True (compact JSON)
        if self.override_version_pattern:
            d["override_version_pattern"] = True
        if self.override_file_extensions:
            d["override_file_extensions"] = True
        if self.override_latest_target:
            d["override_latest_target"] = True
        if self.override_file_rename:
            d["override_file_rename"] = True
        if self.override_link_mode:
            d["override_link_mode"] = True
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "WatchedSource":
        # Backward compat: old configs have "use_symlinks" bool
        if "link_mode" in data:
            link_mode = data["link_mode"]
        elif data.get("use_symlinks", False):
            link_mode = "symlink"
        else:
            link_mode = "copy"
        # Backward compat: old override flag name
        override_link = data.get("override_link_mode",
                                  data.get("override_use_symlinks", False))
        return cls(
            name=data["name"],
            source_dir=data["source_dir"],
            version_pattern=data.get("version_pattern", "_v{version}"),
            file_extensions=data.get("file_extensions", list(DEFAULT_FILE_EXTENSIONS)),
            latest_target=data.get("latest_target", ""),
            file_rename_template=data.get("file_rename_template", ""),
            history_filename=data.get("history_filename", ".latest_history.json"),
            link_mode=link_mode,
            sample_filename=data.get("sample_filename", ""),
            group=data.get("group", ""),
            override_version_pattern=data.get("override_version_pattern", False),
            override_file_extensions=data.get("override_file_extensions", False),
            override_latest_target=data.get("override_latest_target", False),
            override_file_rename=data.get("override_file_rename", False),
            override_link_mode=override_link,
        )


@dataclass
class ProjectConfig:
    """Top-level project configuration."""
    project_name: str
    watched_sources: list = field(default_factory=list)
    # Path templates with token support
    base_path_template: str = ""
    latest_path_template: str = ""
    # Project-wide defaults
    default_version_pattern: str = "_v{version}"
    default_file_extensions: list = field(default_factory=lambda: list(DEFAULT_FILE_EXTENSIONS))
    default_file_rename_template: str = "{source_basename}_latest"  # tokens: {source_name}, {source_basename}
    default_link_mode: str = "copy"  # "copy", "symlink", or "hardlink"
    # Discovery filters
    name_whitelist: list = field(default_factory=list)
    name_blacklist: list = field(default_factory=list)
    # Task token system
    task_tokens: list = field(default_factory=list)   # e.g. ["comp", "grade", "comp_%%%"]
    # Groups — maps group name to properties dict (currently: {"color": "#rrggbb"})
    groups: dict = field(default_factory=dict)
    # Naming defaults (set during first ingest)
    default_naming_rule: str = ""     # e.g. "source_name", "parent:1", "source_basename"
    naming_configured: bool = False   # True after user has chosen naming convention
    # Timecode extraction mode: "always", "lazy", "never"
    timecode_mode: str = "lazy"
    # Discovery UI state
    discovery_search_history: list = field(default_factory=list)  # recent search paths
    # Explicit project root — when set, overrides project_dir for {project_root} token
    project_root: str = ""
    # Runtime only — not serialized, set by config loader
    project_dir: str = field(default="", repr=False)

    @property
    def effective_project_root(self) -> str:
        """Return the project root for token resolution.

        Uses the explicit project_root if set, otherwise falls back to
        project_dir (the directory containing the JSON config file).
        """
        return self.project_root or self.project_dir

    def to_dict(self) -> dict:
        d = {
            "project_name": self.project_name,
            "watched_sources": [s.to_dict() for s in self.watched_sources],
        }
        if self.base_path_template:
            d["base_path_template"] = self.base_path_template
        if self.latest_path_template:
            d["latest_path_template"] = self.latest_path_template
        if self.default_version_pattern != "_v{version}":
            d["default_version_pattern"] = self.default_version_pattern
        if self.default_file_extensions != DEFAULT_FILE_EXTENSIONS:
            d["default_file_extensions"] = self.default_file_extensions
        if self.default_file_rename_template and self.default_file_rename_template != "{source_basename}_latest":
            d["default_file_rename_template"] = self.default_file_rename_template
        if self.default_link_mode != "copy":
            d["default_link_mode"] = self.default_link_mode
        if self.name_whitelist:
            d["name_whitelist"] = self.name_whitelist
        if self.name_blacklist:
            d["name_blacklist"] = self.name_blacklist
        if self.groups:
            d["groups"] = {k: dict(v) for k, v in self.groups.items()}
        if self.task_tokens:
            d["task_tokens"] = self.task_tokens
        if self.default_naming_rule:
            d["default_naming_rule"] = self.default_naming_rule
        if self.naming_configured:
            d["naming_configured"] = True
        if self.timecode_mode != "lazy":
            d["timecode_mode"] = self.timecode_mode
        if self.discovery_search_history:
            d["discovery_search_history"] = self.discovery_search_history
        if self.project_root:
            d["project_root"] = self.project_root
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "ProjectConfig":
        sources = [WatchedSource.from_dict(s) for s in data.get("watched_sources", [])]
        return cls(
            project_name=data.get("project_name", "Untitled"),
            watched_sources=sources,
            base_path_template=data.get("base_path_template", ""),
            latest_path_template=data.get("latest_path_template", ""),
            default_version_pattern=data.get("default_version_pattern", "_v{version}"),
            default_file_extensions=data.get("default_file_extensions", list(DEFAULT_FILE_EXTENSIONS)),
            default_file_rename_template=data.get("default_file_rename_template", "{source_basename}_latest"),
            default_link_mode=data.get("default_link_mode",
                                        "symlink" if data.get("default_use_symlinks", False) else "copy"),
            name_whitelist=data.get("name_whitelist", []),
            name_blacklist=data.get("name_blacklist", []),
            groups=data.get("groups", {}),
            task_tokens=data.get("task_tokens", []),
            default_naming_rule=data.get("default_naming_rule", ""),
            naming_configured=data.get("naming_configured", False),
            timecode_mode=data.get("timecode_mode", "lazy"),
            discovery_search_history=data.get("discovery_search_history", []),
            project_root=data.get("project_root", ""),
        )


@dataclass
class DiscoveryResult:
    """A discovered versioned location from a directory scan."""
    path: str
    name: str
    versions_found: list = field(default_factory=list)  # list of VersionInfo
    suggested_pattern: str = ""
    suggested_extensions: list = field(default_factory=list)
    sample_filename: str = ""  # representative filename from first version
