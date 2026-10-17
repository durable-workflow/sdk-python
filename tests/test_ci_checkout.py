from __future__ import annotations

import os
import runpy
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
        ("workflow", "https://github.com/durable-workflow/workflow.git"),
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
        "--",
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


@pytest.mark.parametrize("destination_name", ["--bare", "--mirror"])
def test_public_checkout_keeps_option_like_destinations_literal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    destination_name: str,
) -> None:
    source = tmp_path / "source"
    subprocess.run(["git", "init", "--quiet", "--initial-branch=main", str(source)], check=True)
    subprocess.run(
        [
            "git", "-C", str(source),
            "-c", "user.name=Checkout test", "-c", "user.email=checkout@example.invalid",
            "commit", "--quiet", "--allow-empty", "-m", "Synthetic checkout fixture",
        ],
        check=True,
    )
    expected_commit = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
    ).stdout.strip()

    checkout_script = runpy.run_path(str(CHECKOUT_SCRIPT))
    monkeypatch.setitem(checkout_script["PUBLIC_REPOSITORIES"], "workflow", source.as_uri())
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")
    monkeypatch.chdir(tmp_path)
    checkout_script["checkout"]("workflow", Path(destination_name))

    destination = tmp_path / destination_name
    assert (destination / ".git").is_dir()
    actual = subprocess.run(
        ["git", "-C", str(destination), "rev-parse", "HEAD", "--is-bare-repository"],
        check=True, capture_output=True, text=True,
    ).stdout.splitlines()
    assert actual == [expected_commit, "false"]


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
@pytest.mark.parametrize("repository", ["server", "workflow"])
def test_candidate_checkout_verifies_the_requested_public_commit(
    tmp_path: Path, matches: bool, repository: str,
) -> None:
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
        [sys.executable, str(CHECKOUT_SCRIPT), repository, str(tmp_path / repository), "--commit", commit],
        env=environment, capture_output=True, text=True,
    )
    assert (result.returncode == 0) is matches
    calls = capture.read_text().splitlines()
    assert f"https://github.com/durable-workflow/{repository}.git" in calls[0]
    assert any(f"fetch --depth=1 origin {commit}" in call for call in calls)
    assert all("credential.helper=" in call for call in calls)
    if matches:
        assert f"Integration {repository} source commit: {commit}" in result.stdout
