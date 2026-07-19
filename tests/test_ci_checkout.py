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

    assert "checkout-public-repository.py cli cli" in workflow
    assert "checkout-public-repository.py server server" in workflow
    assert "repository: durable-workflow/" not in workflow
