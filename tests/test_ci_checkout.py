from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECKOUT_SCRIPT = ROOT / "scripts" / "ci" / "checkout-public-repository.py"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


@pytest.mark.parametrize(
    "runner_server_url",
    ["https://github.com", "https://ci.example.test"],
    ids=["github", "alternate-runner"],
)
@pytest.mark.parametrize(
    ("repository", "public_url"),
    [
        ("cli", "https://github.com/durable-workflow/cli.git"),
        ("server", "https://github.com/durable-workflow/server.git"),
    ],
)
def test_public_checkout_uses_github_authority_on_every_runner(
    tmp_path: Path,
    runner_server_url: str,
    repository: str,
    public_url: str,
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    git_capture = tmp_path / "git-arguments"
    fake_git = bin_dir / "git"
    fake_git.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$GIT_CAPTURE"\n')
    fake_git.chmod(0o755)

    environment = os.environ.copy()
    environment.update(
        {
            "GITHUB_SERVER_URL": runner_server_url,
            "GIT_CAPTURE": str(git_capture),
            "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
        }
    )
    destination = tmp_path / repository

    subprocess.run(
        [sys.executable, str(CHECKOUT_SCRIPT), repository, str(destination)],
        check=True,
        env=environment,
    )

    assert git_capture.read_text().splitlines() == [
        "-c",
        "credential.helper=",
        "clone",
        "--depth=1",
        "--no-tags",
        public_url,
        str(destination),
    ]


def test_ci_workflow_uses_portable_public_checkouts() -> None:
    workflow = CI_WORKFLOW.read_text()

    assert "checkout-public-repository.py server server" in workflow
    public_repository_inputs = [
        line.strip() for line in workflow.splitlines() if line.strip().startswith("repository: durable-workflow/")
    ]
    assert public_repository_inputs == ["repository: durable-workflow/.github"]


@pytest.mark.parametrize("commit", ["main", "short", "A" * 40, "--upload-pack=command", "0" * 40 + ";command"])
def test_candidate_checkout_rejects_non_sha_before_running_git(tmp_path: Path, commit: str) -> None:
    capture = tmp_path / "called"
    fake_git = tmp_path / "git"
    fake_git.write_text('#!/bin/sh\ntouch "$GIT_CAPTURE"\n')
    fake_git.chmod(0o755)
    environment = {**os.environ, "PATH": str(tmp_path), "GIT_CAPTURE": str(capture)}
    result = subprocess.run(
        [sys.executable, str(CHECKOUT_SCRIPT), "server", str(tmp_path / "server"), "--commit", commit],
        env=environment, capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert not capture.exists()


@pytest.mark.parametrize("matches", [True, False])
def test_candidate_checkout_verifies_the_requested_public_commit(tmp_path: Path, matches: bool) -> None:
    commit = "a" * 40
    resolved = commit if matches else "b" * 40
    capture = tmp_path / "git-calls"
    fake_git = tmp_path / "git"
    fake_git.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$GIT_CAPTURE"\n'
        'case "$*" in *rev-parse*) printf "%s\\n" "$RESOLVED_COMMIT" ;; esac\n'
    )
    fake_git.chmod(0o755)
    environment = {
        **os.environ, "PATH": str(tmp_path), "GIT_CAPTURE": str(capture), "RESOLVED_COMMIT": resolved,
    }
    result = subprocess.run(
        [sys.executable, str(CHECKOUT_SCRIPT), "server", str(tmp_path / "server"), "--commit", commit],
        env=environment, capture_output=True, text=True,
    )
    assert (result.returncode == 0) is matches
    calls = capture.read_text().splitlines()
    assert "https://github.com/durable-workflow/server.git" in calls[0]
    assert any(f"fetch --depth=1 origin {commit}" in call for call in calls)
    assert all("credential.helper=" in call for call in calls)
    if matches:
        assert f"Integration server source commit: {commit}" in result.stdout
