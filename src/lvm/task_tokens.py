"""
Task Token System — parsing, matching, and stripping pipeline task names
from versioned filenames.

Task tokens are common pipeline task identifiers (comp, grade, dmp, etc.)
that can be stripped from filenames to produce cleaner source names.

Supports counted wildcards: each % matches exactly one non-divider character.
  comp_%% matches comp_mp, comp_ab (exactly 2 chars)
  comp_%%% matches comp_mpo, comp_abc (exactly 3 chars)
Tokens are bounded by dividers (_, -, .) to avoid partial matches.
"""

import re
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Characters that act as word boundaries for task tokens
DIVIDERS = "_.-"
DIVIDER_RE = r"[_.\-]"

# Version pattern: _v01, .v002, -v0051, _V004
VERSION_RE = re.compile(r"[._\-]v(\d+)", re.IGNORECASE)

# Frame number + extension at end of filename: name.1001.exr, name_1001.exr
FRAME_EXT_RE = re.compile(r"[._](\d{3,8})\.\w+$")

# Date patterns in filenames: 6-digit (DDMMYY/YYMMDD) or 8-digit (DDMMYYYY/YYYYMMDD)
# Bounded by dividers or string start/end to avoid matching frame numbers or reel IDs
DATE_RE = re.compile(r"(?:^|(?<=[._\-]))(\d{6}|\d{8})(?=[._\-]|$)")

# Valid date format identifiers
VALID_DATE_FORMATS = ("DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD")


def validate_date_string(digits: str, date_format: str) -> bool:
    """Check if a digit string forms a plausible date for the given format.

    Performs basic range checks on day (1-31), month (1-12), and year components.

    Args:
        digits: Raw digit string, e.g. "260224" or "20240226".
        date_format: One of "DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD".

    Returns:
        True if the digits form a plausible date.
    """
    try:
        if date_format == "DDMMYY" and len(digits) == 6:
            dd, mm = int(digits[:2]), int(digits[2:4])
        elif date_format == "YYMMDD" and len(digits) == 6:
            mm, dd = int(digits[2:4]), int(digits[4:6])
        elif date_format == "DDMMYYYY" and len(digits) == 8:
            dd, mm, yyyy = int(digits[:2]), int(digits[2:4]), int(digits[4:8])
            if yyyy < 1900 or yyyy > 2099:
                return False
        elif date_format == "YYYYMMDD" and len(digits) == 8:
            yyyy, mm, dd = int(digits[:4]), int(digits[4:6]), int(digits[6:8])
            if yyyy < 1900 or yyyy > 2099:
                return False
        else:
            return False
    except (ValueError, IndexError):
        return False

    return 1 <= dd <= 31 and 1 <= mm <= 12


def parse_date_to_sortable(date_str: str, date_format: str) -> int:
    """Convert a date string to a YYYYMMDD integer for chronological sorting.

    Args:
        date_str: Raw date digits, e.g. "260224" or "20240226".
        date_format: One of "DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD".

    Returns:
        Integer in YYYYMMDD format, e.g. 20240226.  Returns 0 on failure.
    """
    if not validate_date_string(date_str, date_format):
        return 0

    if date_format == "DDMMYY":
        dd, mm, yy = date_str[:2], date_str[2:4], date_str[4:6]
        yyyy = f"20{yy}" if int(yy) < 70 else f"19{yy}"
        return int(f"{yyyy}{mm}{dd}")
    elif date_format == "YYMMDD":
        yy, mm, dd = date_str[:2], date_str[2:4], date_str[4:6]
        yyyy = f"20{yy}" if int(yy) < 70 else f"19{yy}"
        return int(f"{yyyy}{mm}{dd}")
    elif date_format == "DDMMYYYY":
        dd, mm, yyyy = date_str[:2], date_str[2:4], date_str[4:8]
        return int(f"{yyyy}{mm}{dd}")
    elif date_format == "YYYYMMDD":
        return int(date_str)
    return 0


def format_date_display(date_str: str, date_format: str) -> str:
    """Format a raw date string into a human-readable display string.

    Args:
        date_str: Raw date digits, e.g. "260224".
        date_format: One of "DDMMYY", "YYMMDD", "DDMMYYYY", "YYYYMMDD".

    Returns:
        Formatted string like "26-02-24" or the raw string on failure.
    """
    if date_format == "DDMMYY" and len(date_str) == 6:
        return f"{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}"
    elif date_format == "YYMMDD" and len(date_str) == 6:
        return f"{date_str[4:6]}-{date_str[2:4]}-{date_str[:2]}"
    elif date_format == "DDMMYYYY" and len(date_str) == 8:
        return f"{date_str[:2]}-{date_str[2:4]}-{date_str[4:8]}"
    elif date_format == "YYYYMMDD" and len(date_str) == 8:
        return f"{date_str[6:8]}-{date_str[4:6]}-{date_str[:4]}"
    return date_str


def strip_date(name: str, date_format: str = "") -> str:
    """Remove a validated date pattern from a name string.

    Only strips when date_format is set and matched digits validate as a date.
    This prevents false positives on camera reel IDs or other numeric sequences.

    Example: '260224_shotname' with DDMMYY -> 'shotname'
    Example: 'shotname_260224' with DDMMYY -> 'shotname'

    Args:
        name: The string to strip date from.
        date_format: Date format identifier. Empty string disables stripping.

    Returns:
        Name with date removed (or unchanged if no valid date found).
    """
    if not date_format:
        return name

    for m in DATE_RE.finditer(name):
        digits = m.group(1)
        if validate_date_string(digits, date_format):
            # Remove the matched date and clean up dividers
            start = m.start(1)
            end = m.end(1)
            # Also consume an adjacent divider
            if start > 0 and name[start - 1] in DIVIDERS:
                start -= 1
            elif end < len(name) and name[end] in DIVIDERS:
                end += 1
            result = name[:start] + name[end:]
            # Clean up double dividers and leading/trailing dividers
            result = re.sub(r"[_.\-]{2,}", lambda mv: mv.group()[0], result)
            result = result.strip(DIVIDERS)
            return result

    return name


# Module-level cache for compiled task patterns (avoids recompilation)
_task_pattern_cache: dict[str, re.Pattern] = {}


def compile_task_pattern(task_token: str) -> re.Pattern:
    """Convert a task token (possibly with % wildcards) into a bounded regex.

    Results are cached module-level to avoid recompilation across calls.

    Rules:
    - The token must be bounded by dividers or string start/end.
    - Each '%' matches exactly one non-divider character.
      e.g. 'comp_%%' matches 'comp_mp' (2 chars), 'comp_%%%' matches 'comp_mpo' (3 chars).
    - 'comp' matches 'comp' bounded by dividers, not inside 'compositor'.

    Returns:
        Compiled regex pattern.
    """
    if task_token in _task_pattern_cache:
        return _task_pattern_cache[task_token]

    # Build pattern by walking through the token character by character
    pattern_body = ""
    i = 0
    while i < len(task_token):
        if task_token[i] == "%":
            # Count consecutive % characters
            count = 0
            while i < len(task_token) and task_token[i] == "%":
                count += 1
                i += 1
            # Each % = exactly one non-divider character
            pattern_body += rf"[^_.\-]{{{count}}}"
        else:
            pattern_body += re.escape(task_token[i])
            i += 1

    # Bounded by divider or start/end of string
    full_pattern = (
        r"(?:(?<=[_.\-])|(?:^))"
        + pattern_body
        + r"(?=[_.\-]|$)"
    )
    compiled = re.compile(full_pattern, re.IGNORECASE)
    _task_pattern_cache[task_token] = compiled
    return compiled


def find_task_tokens(name: str, task_patterns: list[str]) -> list[dict]:
    """Find all matching task tokens in a name string.

    Args:
        name: The string to search (typically a filename stem without version/frames).
        task_patterns: List of task token patterns (e.g. ["comp", "grade", "comp_%%%"]).

    Returns:
        List of dicts with keys: token, match, start, end. Sorted by start position.
    """
    results = []
    for token in task_patterns:
        pattern = compile_task_pattern(token)
        for m in pattern.finditer(name):
            results.append({
                "token": token,
                "match": m.group(),
                "start": m.start(),
                "end": m.end(),
            })
    # Sort by start position
    results.sort(key=lambda r: r["start"])
    return results


def strip_task_tokens(name: str, task_patterns: list[str]) -> str:
    """Remove all task token matches (and their adjacent dividers) from a name.

    Example: 'hero_comp' with task 'comp' -> 'hero'
    The divider between the preceding segment and the task token is also removed.

    Args:
        name: The string to strip from (should already have version removed).
        task_patterns: List of task token patterns.

    Returns:
        Name with task tokens removed. Trailing/leading dividers cleaned up.
    """
    if not task_patterns:
        return name

    matches = find_task_tokens(name, task_patterns)
    if not matches:
        return name

    result = name
    # Process matches in reverse order to preserve indices
    for m in reversed(matches):
        start = m["start"]
        end = m["end"]
        # Guard against indices beyond current result length after prior removals
        start = min(start, len(result))
        end = min(end, len(result))
        # Also consume the preceding divider if present
        if start > 0 and start <= len(result) and result[start - 1] in DIVIDERS:
            start -= 1
        # Or if no preceding divider, consume trailing divider
        elif end < len(result) and result[end] in DIVIDERS:
            end += 1
        result = result[:start] + result[end:]

    # Clean up any remaining double/trailing/leading dividers
    result = re.sub(r"[_.\-]{2,}", lambda m: m.group()[0], result)
    result = result.strip(DIVIDERS)
    return result


def strip_version(name: str) -> str:
    """Remove version pattern from a name string.

    Example: 'hero_comp_v003' -> 'hero_comp'

    Args:
        name: The string to strip version from.

    Returns:
        Name with version removed, double dividers cleaned up.
    """
    result = VERSION_RE.sub("", name, count=1)
    # Clean up double dividers
    result = re.sub(r"[_.\-]{2,}", lambda m: m.group()[0], result)
    result = result.strip(DIVIDERS)
    return result


def strip_frame_and_ext(filename: str) -> str:
    """Remove frame number and extension from a filename.

    Example: 'hero_comp_v003.1001.exr' -> 'hero_comp_v003'
    Example: 'hero_comp_v003.mov'      -> 'hero_comp_v003'

    Args:
        filename: Full filename including extension.

    Returns:
        Filename stem without frame numbers or extension.
    """
    match = FRAME_EXT_RE.search(filename)
    if match:
        return filename[:match.start()]

    # No frame number — just strip extension
    p = Path(filename)
    return p.stem


def derive_source_tokens(
    source_path_or_name: str,
    task_patterns: list[str] = None,
    date_format: str = "",
) -> dict[str, str]:
    """Compute all source name tokens from a filename or path.

    Given a path like hero_comp_v001.1001.exr and task_patterns=["comp"], returns:
    {
        "source_filename": "hero_comp_v001.1001.exr",
        "source_fullname": "hero_comp_v001",
        "source_name": "hero_comp",
        "source_basename": "hero",
    }

    Args:
        source_path_or_name: A filename or full path. Only the filename part is used.
        task_patterns: List of task token patterns for basename derivation.
        date_format: Date format for stripping dates from basename. Empty disables.

    Returns:
        Dict with keys: source_filename, source_fullname, source_name, source_basename.
    """
    if task_patterns is None:
        task_patterns = []

    p = Path(source_path_or_name)
    filename = p.name  # e.g. hero_comp_v001.1001.exr

    # source_fullname: strip frame number and extension
    fullname = strip_frame_and_ext(filename)

    # source_name: strip version from fullname
    source_name = strip_version(fullname)

    # Strip date before task tokens so date doesn't affect basename
    name_no_date = strip_date(source_name, date_format)

    # source_basename: strip task tokens from source_name (with date removed)
    source_basename = strip_task_tokens(name_no_date, task_patterns)

    # Guard: if basename is empty, fall back to source_name
    if not source_basename:
        source_basename = source_name

    return {
        "source_filename": filename,
        "source_fullname": fullname,
        "source_name": source_name,
        "source_basename": source_basename,
    }


def compute_source_name(
    discovery_result,
    naming_rule: str,
    task_patterns: list[str] = None,
) -> str:
    """Apply a naming rule to produce a WatchedSource name from a DiscoveryResult.

    Args:
        discovery_result: A DiscoveryResult object with .path and .sample_filename.
        naming_rule: One of "parent:N" (N=0 immediate, 1=grandparent, ...),
                     "source_basename", "source_name", "source_fullname".
        task_patterns: List of task token patterns for basename derivation.

    Returns:
        The computed source name string.
    """
    if task_patterns is None:
        task_patterns = []

    p = Path(discovery_result.path)

    # Handle parent directory naming rules
    if naming_rule.startswith("parent:"):
        try:
            depth = int(naming_rule.split(":")[1])
        except (IndexError, ValueError):
            depth = 0
        parts = list(p.parts)
        if depth < len(parts):
            return parts[-(depth + 1)]
        return parts[0] if parts else discovery_result.name

    # Handle file-based naming rules
    sample = getattr(discovery_result, "sample_filename", "") or ""
    if not sample:
        # Fall back to the discovery result name (parent folder name)
        sample = discovery_result.name

    tokens = derive_source_tokens(sample, task_patterns)

    if naming_rule == "source_basename":
        return tokens["source_basename"]
    elif naming_rule == "source_name":
        return tokens["source_name"]
    elif naming_rule == "source_fullname":
        return tokens["source_fullname"]

    # Default: use source_name
    return tokens["source_name"]


def get_naming_options(
    discovery_result,
    task_patterns: list[str] = None,
    max_parent_depth: int = 3,
) -> list[dict]:
    """Generate all available naming options for a DiscoveryResult.

    Used by the NamingRuleDialog to show the user what options are available.

    Args:
        discovery_result: A DiscoveryResult with .path and .sample_filename.
        task_patterns: Task token patterns for basename derivation.
        max_parent_depth: How many parent levels to offer.

    Returns:
        List of dicts with keys: rule, label, preview.
    """
    if task_patterns is None:
        task_patterns = []

    options = []
    p = Path(discovery_result.path)
    parts = list(p.parts)

    # Parent directory options (skip drive letters on Windows)
    for depth in range(min(max_parent_depth, len(parts))):
        idx = -(depth + 1)
        dirname = parts[idx]
        # Skip drive letters like C:\
        if len(dirname) <= 3 and ":" in dirname:
            continue
        options.append({
            "rule": f"parent:{depth}",
            "label": f"Parent directory: \"{dirname}\"" + (f"  (depth {depth})" if depth > 0 else ""),
            "preview": dirname,
        })

    # File-based options (need sample_filename)
    sample = getattr(discovery_result, "sample_filename", "") or ""
    if sample:
        tokens = derive_source_tokens(sample, task_patterns)

        options.append({
            "rule": "source_name",
            "label": f"Source name: \"{tokens['source_name']}\"  (no version/frames)",
            "preview": tokens["source_name"],
        })

        if task_patterns and tokens["source_basename"] != tokens["source_name"]:
            options.append({
                "rule": "source_basename",
                "label": f"Base name: \"{tokens['source_basename']}\"  (no version/frames/tasks)",
                "preview": tokens["source_basename"],
            })

        if tokens["source_fullname"] != tokens["source_name"]:
            options.append({
                "rule": "source_fullname",
                "label": f"Full name: \"{tokens['source_fullname']}\"  (includes version)",
                "preview": tokens["source_fullname"],
            })

    return options
