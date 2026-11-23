"""
Pre/post-promote hook execution.

Hooks are shell commands that run before and after file promotion.
Environment variables are passed to give hooks context about the operation.
"""

__all__ = [
    "HookError",
    "run_hook",
    "run_pre_promote_hook",
    "run_post_promote_hook",
    "quote_shell_token",
]

import logging
import os
import re
import shlex
import subprocess
import sys

from .models import WatchedSource, VersionInfo

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"\{(\w+)\}")
_WIN_UNSAFE_RE = re.compile(r'[\s&|<>^()%!"]')


def _subprocess_kwargs() -> dict:
    """Return platform-specific kwargs to suppress console windows on Windows."""
    kwargs = {}
    if sys.platform == "win32":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = subprocess.SW_HIDE
        kwargs["startupinfo"] = si
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    return kwargs


class HookError(Exception):
    """Raised when a pre-promote hook fails (non-zero exit)."""
    pass


def quote_shell_token(value: str, *, platform_name: str | None = None) -> str:
    """Quote a token value safely for shell interpolation.

    On POSIX uses ``shlex.quote``; on Windows escapes ``cmd.exe`` metacharacters
    and wraps in double quotes when needed so paths/names containing spaces or
    ``&``, ``|``, ``<``, ``>``, ``^``, ``%`` cannot inject commands.
    """
    plat = platform_name or sys.platform
    if plat == "win32":
        # Strip NUL / newlines which can split batch commands
        cleaned = value.replace("\x00", "").replace("\r", "").replace("\n", " ")
        if not cleaned:
            return '""'
        if not _WIN_UNSAFE_RE.search(cleaned):
            return cleaned
        escaped = cleaned.replace('"', '""').replace("%", "%%")
        return f'"{escaped}"'
    cleaned = value.replace("\x00", "")
    return shlex.quote(cleaned)


def _build_hook_env(
    source: WatchedSource,
    version: VersionInfo,
    user: str,
    project_name: str,
) -> dict:
    """Build environment variables for hook execution."""
    env = os.environ.copy()
    env["LVM_SOURCE_NAME"] = source.name
    env["LVM_VERSION"] = version.version_string
    env["LVM_SOURCE_DIR"] = version.source_path
    env["LVM_TARGET_DIR"] = source.latest_target or ""
    env["LVM_LINK_MODE"] = source.link_mode
    env["LVM_USER"] = user
    env["LVM_PROJECT_NAME"] = project_name
    if version.frame_range:
        env["LVM_FRAME_RANGE"] = version.frame_range
    env["LVM_FILE_COUNT"] = str(version.file_count)
    return env


def _build_hook_tokens(
    source: WatchedSource,
    version: VersionInfo,
    user: str,
    project_name: str,
) -> dict:
    """Build {token} -> value map for hook command substitution.

    Lowercase tokens mirror the LVM_* env vars so users can write either
    ``echo {source_name}`` or ``echo $LVM_SOURCE_NAME`` / ``%LVM_SOURCE_NAME%``.
    """
    return {
        "source_name":  source.name,
        "version":      version.version_string,
        "source_dir":   version.source_path,
        "target_dir":   source.latest_target or "",
        "link_mode":    source.link_mode,
        "user":         user,
        "project_name": project_name,
        "frame_range":  version.frame_range or "",
        "file_count":   str(version.file_count),
    }


def _substitute_tokens(cmd: str, tokens: dict, *, quote: bool = True) -> str:
    """Single-pass replacement of ``{token}`` occurrences in *cmd*.

    Unknown tokens are left untouched so users notice typos rather than getting
    silent empties. Single-pass substitution prevents a value containing
    ``{other_token}`` from being expanded a second time. When *quote* is True,
    values are shell-quoted unless the user already wrapped the placeholder in
    matching quotes (in which case inner characters are still escaped).
    """
    if not cmd or "{" not in cmd:
        return cmd

    def _repl(m: re.Match) -> str:
        key = m.group(1)
        if key not in tokens:
            return m.group(0)
        val = tokens[key]
        if not quote:
            return val
        start, end = m.span()
        prev_ch = cmd[start - 1] if start > 0 else ""
        next_ch = cmd[end] if end < len(cmd) else ""
        if prev_ch == '"' and next_ch == '"':
            # Already inside double quotes: escape internal quotes/metachars
            cleaned = val.replace("\x00", "").replace("\r", "").replace("\n", " ")
            if sys.platform == "win32":
                return cleaned.replace('"', '""').replace("%", "%%")
            return (cleaned.replace("\\", "\\\\")
                           .replace('"', '\\"')
                           .replace("$", "\\$")
                           .replace("`", "\\`"))
        if prev_ch == "'" and next_ch == "'" and sys.platform != "win32":
            return val.replace("\x00", "").replace("'", "'\"'\"'")
        return quote_shell_token(val)

    return _TOKEN_RE.sub(_repl, cmd)


def run_hook(
    cmd: str,
    env: dict,
    label: str = "hook",
    timeout: int = 300,
) -> tuple[int, str, str]:
    """Run a shell command and return (returncode, stdout, stderr).

    Args:
        cmd: Shell command to execute.
        env: Environment variables dict.
        label: Human-readable label for logging.
        timeout: Maximum seconds before killing the process.

    Returns:
        Tuple of (returncode, stdout, stderr).

    Raises:
        HookError: On timeout or OS-level execution failure.
    """
    if not cmd.strip():
        return 0, "", ""

    logger.info(f"Running {label}: {cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            **_subprocess_kwargs(),
        )
        if result.stdout:
            logger.info(f"{label} stdout: {result.stdout.rstrip()}")
        if result.stderr:
            logger.warning(f"{label} stderr: {result.stderr.rstrip()}")
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        raise HookError(f"{label} timed out after {timeout}s")
    except OSError as e:
        raise HookError(f"{label} failed to execute: {e}")


def run_pre_promote_hook(
    source: WatchedSource,
    version: VersionInfo,
    user: str,
    project_name: str,
) -> tuple[int, str, str]:
    """Run the pre-promote hook. Raises HookError on failure."""
    if not source.pre_promote_cmd:
        return 0, "", ""
    env = _build_hook_env(source, version, user, project_name)
    cmd = _substitute_tokens(
        source.pre_promote_cmd,
        _build_hook_tokens(source, version, user, project_name),
    )
    rc, stdout, stderr = run_hook(cmd, env, "pre-promote hook")
    if rc != 0:
        raise HookError(
            f"Pre-promote hook exited with code {rc}.\n"
            f"Command: {source.pre_promote_cmd}\n"
            f"Stderr: {stderr.strip()}"
        )
    return rc, stdout, stderr


def run_post_promote_hook(
    source: WatchedSource,
    version: VersionInfo,
    user: str,
    project_name: str,
) -> tuple[int, str, str]:
    """Run the post-promote hook. Logs but does not block on failure."""
    if not source.post_promote_cmd:
        return 0, "", ""
    env = _build_hook_env(source, version, user, project_name)
    cmd = _substitute_tokens(
        source.post_promote_cmd,
        _build_hook_tokens(source, version, user, project_name),
    )
    try:
        rc, stdout, stderr = run_hook(cmd, env, "post-promote hook")
        if rc != 0:
            logger.warning(
                "Post-promote hook exited with code %d (command: %s)",
                rc, source.post_promote_cmd,
            )
        return rc, stdout, stderr
    except HookError as e:
        logger.error(f"Post-promote hook failed: {e}")
        return -1, "", str(e)
