"""Contract for .github/workflows/publish.yml (#240).

Only the publish job can mint a PyPI OIDC token, and it runs no project code:
after the central policy's credential-free checkout it downloads the build
job's artifact, checks its digest, extracts it and uploads it. Post-publish
verification runs in a later job without id-token, and the source GitHub
Release is created by a minimal job that runs only the preinstalled GitHub
CLI.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PUBLISH_WORKFLOW = ROOT / ".github" / "workflows" / "publish.yml"

PYPI_PUBLISH_ACTION = "pypa/gh-action-pypi-publish@"
DOWNLOAD_ARTIFACT_ACTION = "actions/download-artifact@"
CHECKOUT_ACTION = "actions/checkout@"

# Commands that would run project code, a build backend or an installer.
PROJECT_CODE = re.compile(
    r"(^|[\s;&|(])(python3?|pip3?|pipx|uv|uvx|twine|hatch|poetry|tox|nox|make)(\s|$)"
    r"|python3?\s+-m\s+(build|pip)|\bscripts/|\bsetup\.py\b|\bpyproject\.toml\b",
)


def workflow() -> dict[str, Any]:
    document = yaml.safe_load(PUBLISH_WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def jobs() -> dict[str, dict[str, Any]]:
    value = workflow()["jobs"]
    assert isinstance(value, dict)
    return value


def needs(job: dict[str, Any]) -> list[str]:
    value = job.get("needs", [])
    return [value] if isinstance(value, str) else list(value)


def uses(job: dict[str, Any]) -> list[str]:
    return [step["uses"] for step in job.get("steps", []) if "uses" in step]


def runs(job: dict[str, Any]) -> list[str]:
    return [step["run"] for step in job.get("steps", []) if "run" in step]


def test_the_workflow_grants_no_token_by_default() -> None:
    assert workflow()["permissions"] == {"contents": "read"}


def test_only_the_publish_job_can_mint_an_oidc_token() -> None:
    minting = {name for name, job in jobs().items() if (job.get("permissions") or {}).get("id-token") == "write"}
    assert minting == {"publish"}
    assert "id-token" not in workflow()["permissions"]


def test_the_publish_job_has_exactly_the_permissions_publication_needs() -> None:
    publish = jobs()["publish"]
    assert publish["permissions"] == {"actions": "read", "id-token": "write"}
    assert publish["environment"] == "pypi"
    assert needs(publish) == ["build"]


def test_the_publish_job_only_downloads_checks_and_uploads() -> None:
    publish = jobs()["publish"]
    actions = uses(publish)
    assert [action.split("@")[0] + "@" for action in actions] == [
        CHECKOUT_ACTION, DOWNLOAD_ARTIFACT_ACTION, PYPI_PUBLISH_ACTION,
    ]
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", action) for action in actions)
    # The central action policy's fixed preamble for a privileged artifact
    # consumer: a credential-free checkout that nothing in the job executes.
    assert publish["steps"][0] == {"uses": actions[0], "with": {
        "fetch-depth": 0, "persist-credentials": False, "ref": "${{ github.sha }}",
    }}
    # The artifact is downloaded by its recorded id, with digest enforcement.
    download = next(step for step in publish["steps"] if step.get("uses", "").startswith(DOWNLOAD_ARTIFACT_ACTION))
    assert download["with"]["digest-mismatch"] == "error"
    assert download["with"]["artifact-ids"] == "${{ needs.build.outputs.artifact-id }}"
    scripts = runs(publish)
    assert len(scripts) == 2
    for script in scripts:
        assert PROJECT_CODE.search(script) is None, script
    assert "sha256sum" in scripts[0] and "EXPECTED_ARTIFACT_DIGEST" in scripts[0]
    assert scripts[1].split() == ["mkdir", "dist", "tar", "-xf", "isolated-python-dist/dist-handoff.tar", "-C", "dist"]


@pytest.mark.parametrize("script", [
    "pip install .", "python -m build", "python scripts/check_release_metadata.py", "pip install --dry-run x",
    "twine upload dist/*", "uv build", "cd x && python3 -m pip install -e .",
])
def test_the_project_code_pattern_catches_installs_and_builds(script: str) -> None:
    assert PROJECT_CODE.search(script) is not None


def test_the_pypi_publish_action_runs_only_in_the_publish_job() -> None:
    for name, job in jobs().items():
        if name != "publish":
            assert not any(action.startswith(PYPI_PUBLISH_ACTION) for action in uses(job)), name
            assert job.get("environment") is None, name


def test_post_publish_verification_runs_after_publication_without_oidc() -> None:
    verify = jobs()["verify"]
    assert verify["permissions"] == {"contents": "read"}
    assert set(needs(verify)) == {"build", "publish"}
    checkout = next(step for step in verify["steps"] if step.get("uses", "").startswith(CHECKOUT_ACTION))
    assert checkout["with"]["persist-credentials"] is False
    scripts = runs(verify)
    # The same checks, in the same order: exact PyPI JSON, then the project
    # surface and its supported installs.
    assert len(scripts) == 2
    assert "scripts/check_release_metadata.py" in scripts[0] and "--pypi-version" in scripts[0]
    assert "scripts/check_pypi_project_surface.py" in scripts[1]
    for script in scripts:
        assert "--attempts 30" in script and "--interval-seconds 10" in script


def test_the_github_release_follows_verification_and_runs_no_third_party_code() -> None:
    release = jobs()["github-release"]
    assert release["permissions"] == {"contents": "write"}
    assert set(needs(release)) == {"build", "verify"}
    # Privileged, so it runs only where publication itself may run.
    assert release["if"] == jobs()["publish"]["if"]
    assert uses(release) == []
    scripts = runs(release)
    assert len(scripts) == 1
    commands = {line.split()[0] for line in scripts[0].splitlines() if line.strip()}
    assert commands <= {"if", "gh", "arguments=(--verify-tag", "arguments+=(--prerelease)", "fi"}
    assert PROJECT_CODE.search(scripts[0]) is None
    assert "gh release create" in scripts[0] and "--verify-tag" in scripts[0]


def test_the_build_job_holds_no_publication_credential() -> None:
    build = jobs()["build"]
    assert "permissions" not in build
    assert not any(action.startswith(PYPI_PUBLISH_ACTION) for action in uses(build))
