from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / ".github" / "workflows"
DOCS_CHECKS = WORKFLOW_ROOT / "docs-pr.yml"
DOCS_DEPLOYMENT = WORKFLOW_ROOT / "docs.yml"

DOCS_PATHS = [
    "src/**",
    "docs/**",
    "mkdocs.yml",
    "pyproject.toml",
    "scripts/check-docs-analytics.py",
    "scripts/check-docs-layout.py",
    ".github/workflows/docs.yml",
    ".github/workflows/docs-pr.yml",
]
CHECKOUT_ACTION = "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803"
SETUP_PYTHON_ACTION = "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1"
UPLOAD_PAGES_ACTION = "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9"
DEPLOY_PAGES_ACTION = "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128"


def load_workflow(path: Path) -> dict[str, Any]:
    document = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(document, dict), f"{path.name} must contain a workflow mapping"
    return document


def write_permissions(value: object) -> set[str]:
    if isinstance(value, str):
        return {"*"} if value == "write-all" else set()
    if not isinstance(value, dict):
        return set()
    return {str(name) for name, access in value.items() if access == "write"}


def action_references(job: dict[str, Any]) -> list[str]:
    steps = job.get("steps")
    assert isinstance(steps, list)
    return [step["uses"] for step in steps if isinstance(step, dict) and "uses" in step]


def run_commands(job: dict[str, Any]) -> str:
    steps = job.get("steps")
    assert isinstance(steps, list)
    return "\n".join(str(step["run"]) for step in steps if isinstance(step, dict) and "run" in step)


def assert_actions_are_pinned(references: list[str]) -> None:
    for reference in references:
        assert re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", reference), f"action is not commit-pinned: {reference}"


def test_pull_request_workflows_do_not_grant_write_permissions() -> None:
    checked: list[str] = []
    for path in sorted(WORKFLOW_ROOT.glob("*.y*ml")):
        workflow = load_workflow(path)
        triggers = workflow.get("on")
        assert isinstance(triggers, dict), f"{path.name} must declare explicit triggers"
        if "pull_request" not in triggers:
            continue

        checked.append(path.name)
        assert not write_permissions(workflow.get("permissions")), (
            f"{path.name} grants workflow-level write permission to pull requests"
        )
        jobs = workflow.get("jobs")
        assert isinstance(jobs, dict) and jobs, f"{path.name} must declare jobs"
        for job_name, job in jobs.items():
            assert isinstance(job, dict)
            assert not write_permissions(job.get("permissions")), (
                f"{path.name} job {job_name} grants write permission to pull requests"
            )

    assert "docs-pr.yml" in checked


def test_docs_pull_request_checks_are_read_only_and_complete() -> None:
    workflow = load_workflow(DOCS_CHECKS)
    assert workflow["on"] == {
        "pull_request": {
            "branches": ["main"],
            "paths": DOCS_PATHS,
        }
    }
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["jobs"]) == {"validate"}

    validate = workflow["jobs"]["validate"]
    references = action_references(validate)
    assert references == [CHECKOUT_ACTION, SETUP_PYTHON_ACTION]
    assert_actions_are_pinned(references)
    commands = run_commands(validate)
    assert "mkdocs build --strict" in commands
    assert "python scripts/check-docs-analytics.py site" in commands
    assert "python scripts/check-docs-layout.py site" in commands
    assert "upload-pages-artifact" not in DOCS_CHECKS.read_text(encoding="utf-8")
    assert "deploy-pages" not in DOCS_CHECKS.read_text(encoding="utf-8")


def test_pages_deployment_is_push_only_and_has_exact_authority() -> None:
    workflow = load_workflow(DOCS_DEPLOYMENT)
    assert workflow["on"] == {
        "push": {
            "branches": ["main"],
            "paths": DOCS_PATHS,
        }
    }
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["jobs"]) == {"build", "deploy"}

    build = workflow["jobs"]["build"]
    assert "permissions" not in build
    build_references = action_references(build)
    assert build_references == [CHECKOUT_ACTION, SETUP_PYTHON_ACTION, UPLOAD_PAGES_ACTION]
    assert_actions_are_pinned(build_references)
    commands = run_commands(build)
    assert "mkdocs build --strict" in commands
    assert "python scripts/check-docs-analytics.py site" in commands
    assert "python scripts/check-docs-layout.py site" in commands

    deploy = workflow["jobs"]["deploy"]
    assert deploy["needs"] == "build"
    assert deploy["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "pages": "write",
    }
    deploy_references = action_references(deploy)
    assert deploy_references == [DEPLOY_PAGES_ACTION]
    assert_actions_are_pinned(deploy_references)

    source = DOCS_DEPLOYMENT.read_text(encoding="utf-8")
    assert "actions/download-artifact@" not in source
    assert (REPO_ROOT / "docs" / "CNAME").read_text(encoding="utf-8").strip() == "python.durable-workflow.com"
    assert "site_url: https://python.durable-workflow.com/" in (REPO_ROOT / "mkdocs.yml").read_text(encoding="utf-8")
