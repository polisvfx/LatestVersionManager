"""
Scan result cache for fast project startup.

Caches VersionInfo lists per source to avoid expensive directory
scanning on every project load.  The cache is stored as JSON in
.lvm_cache/scan_cache.json next to the project file.
"""

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Optional

from .models import VersionInfo, WatchedSource

logger = logging.getLogger(__name__)

CACHE_VERSION = 1  # Bump when cache format changes
CACHE_FILENAME = "scan_cache.json"


def _source_fingerprint(source: WatchedSource) -> str:
    """Compute a fingerprint for the source fields that affect scan results.

    If any of these fields change, cached versions for this source are
    invalid and must be re-scanned.
    """
    key_data = json.dumps({
        "source_dir": source.source_dir,
        "version_pattern": source.version_pattern,
        "file_extensions": sorted(source.file_extensions),
        "sample_filename": source.sample_filename,
        "date_format": source.date_format,
    }, sort_keys=True)
    return hashlib.md5(key_data.encode()).hexdigest()


def cache_path_for_project(config_path: str) -> Path:
    """Return the cache file path for a given project config file."""
    return Path(config_path).parent / ".lvm_cache" / CACHE_FILENAME


def load_cache(
    config_path: str,
    sources: list[WatchedSource],
) -> dict[str, list[VersionInfo]]:
    """Load cached scan results, returning only entries with valid fingerprints.

    Returns a dict mapping source name to list[VersionInfo].
    Sources whose fingerprint doesn't match (config changed) or
    that aren't in the cache are simply omitted from the result.
    """
    cp = cache_path_for_project(config_path)
    if not cp.exists():
        return {}

    try:
        with open(cp, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to read scan cache: %s", e)
        return {}

    if data.get("cache_version") != CACHE_VERSION:
        logger.info("Scan cache version mismatch, ignoring cache")
        return {}

    cached_sources = data.get("sources", {})
    result: dict[str, list[VersionInfo]] = {}

    for source in sources:
        entry = cached_sources.get(source.name)
        if not entry:
            continue
        expected_fp = _source_fingerprint(source)
        if entry.get("fingerprint") != expected_fp:
            logger.info("Cache fingerprint mismatch for '%s', will rescan", source.name)
            continue
        try:
            versions = [VersionInfo.from_dict(v) for v in entry.get("versions", [])]
            result[source.name] = versions
        except (KeyError, TypeError) as e:
            logger.warning("Failed to deserialize cache for '%s': %s", source.name, e)
            continue

    logger.info("Loaded scan cache: %d/%d sources cached", len(result), len(sources))
    return result


def save_cache(
    config_path: str,
    sources: list[WatchedSource],
    versions_cache: dict[str, list[VersionInfo]],
) -> None:
    """Save scan results to the cache file.

    Uses atomic write (write to temp, then rename) to prevent corruption.
    """
    cp = cache_path_for_project(config_path)
    cp.parent.mkdir(parents=True, exist_ok=True)

    sources_data = {}
    for source in sources:
        versions = versions_cache.get(source.name, [])
        sources_data[source.name] = {
            "fingerprint": _source_fingerprint(source),
            "cached_at": time.time(),
            "versions": [v.to_dict() for v in versions],
        }

    data = {
        "cache_version": CACHE_VERSION,
        "saved_at": time.time(),
        "sources": sources_data,
    }

    tmp_path = cp.with_suffix(".json.tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1, ensure_ascii=False)
        tmp_path.replace(cp)
        logger.info("Scan cache saved: %d sources", len(sources_data))
    except OSError as e:
        logger.warning("Failed to save scan cache: %s", e)
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def clear_cache(config_path: str) -> None:
    """Delete the cache file for a project."""
    cp = cache_path_for_project(config_path)
    if cp.exists():
        try:
            cp.unlink()
            logger.info("Scan cache cleared")
        except OSError as e:
            logger.warning("Failed to clear scan cache: %s", e)
