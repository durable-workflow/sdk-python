"""Contract tests for ``scripts/check-public-boundary.sh``.

The scanner is the public/private boundary prevention gate for this public
repo. These tests pin the behavior so that future edits to the scanner do not
silently drop a denied pattern.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCANNER = REPO_ROOT / "scripts" / "check-public-boundary.sh"

_LOCAL_IDENTITY_EMAIL = "build@" + "durable-workflow" + ".local"


def _git(cwd: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    merged_env.setdefault("GIT_CONFIG_GLOBAL", "/dev/null")
    merged_env.setdefault("GIT_CONFIG_SYSTEM", "/dev/null")
    merged_env.setdefault("HOME", str(cwd))
    if env:
        merged_env.update(env)
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=merged_env,
        check=True,
        capture_output=True,
        text=True,
    )


def _init_repo(cwd: Path) -> None:
    _git(cwd, "init", "-q", "-b", "main")
    _git(cwd, "config", "user.name", "Durable Workflow Test")
    _git(cwd, "config", "user.email", "test@example.invalid")


def _commit(cwd: Path, name: str, body: str, *, author_email: str = "test@example.invalid") -> str:
    (cwd / name).write_text(body, encoding="utf-8")
    _git(cwd, "add", name)
    _git(
        cwd,
        "commit",
        "-q",
        "-m",
        f"Add {name}",
        env={
            "GIT_AUTHOR_NAME": "Durable Workflow Test",
            "GIT_AUTHOR_EMAIL": author_email,
            "GIT_COMMITTER_NAME": "Durable Workflow Test",
            "GIT_COMMITTER_EMAIL": author_email,
        },
    )
    return _git(cwd, "rev-parse", "HEAD").stdout.strip()


def _run_scanner(cwd: Path, rev_range: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PUBLIC_BOUNDARY_GIT_RANGE"] = rev_range
    return subprocess.run(
        ["bash", str(SCANNER)],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def scratch_repo(tmp_path: Path) -> Path:
    # Copy the scanner into the scratch repo so the scanner's relative "." root is stable.
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    scripts_dir = repo_dir / "scripts"
    scripts_dir.mkdir()
    shutil.copy2(SCANNER, scripts_dir / SCANNER.name)
    _init_repo(repo_dir)
    _git(repo_dir, "add", "scripts/check-public-boundary.sh")
    _git(repo_dir, "commit", "-q", "-m", "Seed scanner")
    return repo_dir


class TestPublicBoundaryScanner:
    def test_clean_commit_passes(self, scratch_repo: Path) -> None:
        _commit(scratch_repo, "hello.txt", "hello world\n")
        result = _run_scanner(scratch_repo, "-1 HEAD")
        assert result.returncode == 0, result.stderr

    def test_local_identity_commit_is_rejected(self, scratch_repo: Path) -> None:
        sha = _commit(
            scratch_repo,
            "hello.txt",
            "hello world\n",
            author_email=_LOCAL_IDENTITY_EMAIL,
        )
        result = _run_scanner(scratch_repo, "-1 HEAD")
        assert result.returncode == 1, (result.stdout, result.stderr)
        assert "forbidden commit metadata" in result.stderr
        assert sha[:12] in result.stderr
