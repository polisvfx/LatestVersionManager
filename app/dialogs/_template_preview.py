"""Shared template token helpers used across Settings and Latest Path dialogs."""

import re

_TOKEN_NAME_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def unknown_tokens(text: str, allowed: set[str]) -> list[str]:
    """Return any {token} names referenced in *text* that are not in *allowed*."""
    seen: list[str] = []
    for m in _TOKEN_NAME_RE.finditer(text or ""):
        name = m.group(1)
        if name not in allowed and name not in seen:
            seen.append(name)
    return seen


def wrap_path_for_qt(path_text: str) -> str:
    """Insert zero-width spaces after path separators so Qt can wrap long paths."""
    return path_text.replace("/", "/\u200b").replace("\\", "\\\u200b")
