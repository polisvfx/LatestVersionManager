"""
Pre/post-promote hook execution.

Hooks are shell commands that run before and after file promotion.
Environment variables are passed to give hooks context about the operation.
"""

import logging
import os
import subprocess
import sys
from typing import Optional

from .models import WatchedSource, VersionInfo

logger = logging.getLogger(__name__)


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
    rc, stdout, stderr = run_hook(source.pre_promote_cmd, env, "pre-promote hook")
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
    try:
        return run_hook(source.post_promote_cmd, env, "post-promote hook")
    except HookError as e:
        logger.error(f"Post-promote hook failed: {e}")
        return -1, "", str(e)
