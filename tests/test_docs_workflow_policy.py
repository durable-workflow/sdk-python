from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / ".github" / "workflows"
DOCS_CHECKS = WORKFLOW_ROOT / "docs-pr.yml"
DOCS_DEPLOYMENT = WORKFLOW_ROOT / "docs.yml"
DOCS_VISUAL = WORKFLOW_ROOT / "docs-visual.yml"

DOCS_PATHS = [
    "src/**",
    "docs/**",
    "overrides/**",
    "mkdocs.yml",
    "pyproject.toml",
    "scripts/ci/classify_docs_visual_changes.py",
    "scripts/ci/test-classify-docs-visual-changes.py",
    "scripts/api_reference_release.py",
    "scripts/check_api_reference_install.py",
    "scripts/check-docs-analytics.py",
    "scripts/check-docs-layout.py",
    "scripts/mkdocs_hooks.py",
    ".github/workflows/docs.yml",
    ".github/workflows/docs-pr.yml",
    ".github/workflows/docs-visual.yml",
]
CHECKOUT_ACTION = "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803"
SETUP_PYTHON_ACTION = "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1"
UPLOAD_PAGES_ACTION = "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9"
DEPLOY_PAGES_ACTION = "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128"
SETUP_NODE_ACTION = "actions/setup-node@249970729cb0ef3589644e2896645e5dc5ba9c38"
UPLOAD_ARTIFACT_ACTION = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
VISUAL_WORKFLOW = "./.github/workflows/docs-visual.yml"


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
    assert set(workflow["jobs"]) == {"validate", "visual-evidence"}

    validate = workflow["jobs"]["validate"]
    references = action_references(validate)
    assert references == [CHECKOUT_ACTION, SETUP_PYTHON_ACTION]
    assert_actions_are_pinned(references)
    commands = run_commands(validate)
    assert "python scripts/ci/test-classify-docs-visual-changes.py" in commands
    assert "mkdocs build --strict" in commands
    assert "python scripts/check_api_reference_install.py --site site" in commands
    assert "python scripts/check-docs-analytics.py site" in commands
    assert "python scripts/check-docs-layout.py site" in commands
    assert "upload-pages-artifact" not in DOCS_CHECKS.read_text(encoding="utf-8")
    assert "deploy-pages" not in DOCS_CHECKS.read_text(encoding="utf-8")

    visual = workflow["jobs"]["visual-evidence"]
    assert visual["uses"] == VISUAL_WORKFLOW
    assert visual["with"] == {"source_base_sha": "${{ github.event.pull_request.base.sha }}"}
    assert visual["permissions"] == {"contents": "read"}


def test_pages_deployment_requires_main_or_published_release_authority() -> None:
    workflow = load_workflow(DOCS_DEPLOYMENT)
    assert workflow["on"]["push"] == {
        "branches": ["main"],
        "paths": DOCS_PATHS,
    }
    call_inputs = workflow["on"]["workflow_call"]["inputs"]
    assert set(call_inputs) == {"published_release", "source_base_sha", "source_ref"}
    assert call_inputs["published_release"]["default"] == "false"
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["jobs"]) == {"build", "visual-evidence", "deploy"}

    build = workflow["jobs"]["build"]
    assert "permissions" not in build
    build_references = action_references(build)
    assert build_references == [CHECKOUT_ACTION, SETUP_PYTHON_ACTION, UPLOAD_PAGES_ACTION]
    assert_actions_are_pinned(build_references)
    commands = run_commands(build)
    assert "python scripts/ci/test-classify-docs-visual-changes.py" in commands
    assert "mkdocs build --strict" in commands
    assert "python scripts/check_api_reference_install.py --site site" in commands
    assert "python scripts/check_api_reference_install.py --site site --install" in commands
    assert "python scripts/check-docs-analytics.py site" in commands
    assert "python scripts/check-docs-layout.py site" in commands
    assert commands.count("mkdocs build --strict") == 2
    assert commands.index("python scripts/check-docs-layout.py site") < commands.index('sed -i "s/__CLOUDFLARE')
    assert commands.index('sed -i "s/__CLOUDFLARE') < commands.index(
        "python scripts/check-docs-analytics.py site --require-token"
    )
    build_steps = build["steps"]
    public_install = next(
        index
        for index, step in enumerate(build_steps)
        if step.get("name") == "Verify the rendered command against public PyPI"
    )
    pages_upload = next(
        index for index, step in enumerate(build_steps) if step.get("name") == "Upload GitHub Pages artifact"
    )
    assert public_install < pages_upload
    assert build_steps[public_install]["if"] == "${{ github.server_url == 'https://github.com' }}"
    assert build_steps[0]["with"]["ref"] == "${{ inputs.source_ref || github.sha }}"

    visual = workflow["jobs"]["visual-evidence"]
    assert visual["uses"] == VISUAL_WORKFLOW
    assert visual["with"] == {
        "source_base_sha": "${{ inputs.source_base_sha || github.event.before }}",
        "source_ref": "${{ inputs.source_ref || github.sha }}",
    }
    assert visual["permissions"] == {"contents": "read"}

    deploy = workflow["jobs"]["deploy"]
    assert deploy["needs"] == ["build", "visual-evidence"]
    assert deploy["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "pages": "write",
    }
    assert "inputs.published_release" in deploy["if"]
    deploy_references = action_references(deploy)
    assert deploy_references == [DEPLOY_PAGES_ACTION]
    assert_actions_are_pinned(deploy_references)

    source = DOCS_DEPLOYMENT.read_text(encoding="utf-8")
    assert "actions/download-artifact@" not in source
    assert (REPO_ROOT / "docs" / "CNAME").read_text(encoding="utf-8").strip() == "python.durable-workflow.com"
    mkdocs = yaml.load((REPO_ROOT / "mkdocs.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert mkdocs["site_url"] == "https://python.durable-workflow.com/"
    assert mkdocs["theme"]["font"] == "false"


def test_documentation_typography_does_not_require_external_font_hosts() -> None:
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text(encoding="utf-8"))
    assert config["theme"]["font"] is False


def test_visual_evidence_workflow_uses_the_interaction_classifier_and_exact_viewports() -> None:
    workflow = load_workflow(DOCS_VISUAL)
    assert set(workflow["on"]) == {"workflow_call"}
    inputs = workflow["on"]["workflow_call"]["inputs"]
    assert set(inputs) == {"source_base_sha", "source_ref"}
    assert inputs["source_base_sha"]["required"] == "false"
    assert inputs["source_base_sha"]["type"] == "string"
    assert inputs["source_base_sha"]["description"]
    assert inputs["source_ref"]["required"] == "false"
    assert inputs["source_ref"]["type"] == "string"
    assert inputs["source_ref"]["description"]
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["jobs"]) == {"visual-evidence"}

    visual = workflow["jobs"]["visual-evidence"]
    assert visual["if"] == "github.api_url == 'https://api.github.com'"
    references = action_references(visual)
    assert references == [
        CHECKOUT_ACTION,
        CHECKOUT_ACTION,
        SETUP_PYTHON_ACTION,
        SETUP_NODE_ACTION,
        UPLOAD_ARTIFACT_ACTION,
    ]
    assert_actions_are_pinned(references)

    commands = run_commands(visual)
    assert visual["steps"][0]["with"]["ref"] == "${{ inputs.source_ref || github.sha }}"
    assert "candidate/scripts/ci/classify_docs_visual_changes.py" in commands
    assert "visual-controller/scripts/visual_evidence.py validate" in commands
    assert "1440x900 768x1024 390x844 640x360" in commands
    assert "capture default" in commands
    assert "capture navigation-open" in commands
    assert "capture search-open" in commands
    assert "capture search-populated" in commands
    assert "?q=workflow" in commands
    assert '--click "$search_selector"' in commands
    assert "--click \".md-header__button[for='__drawer']\"" in commands
    assert "--state-scope responsive" in commands
    assert "capture_args+=(--full-page)" in commands
    assert "python-docs-visual-classification" in commands
    assert "visual-review/classification.json" in commands
    assert 'if [ "$NAVIGATION_REQUIRED" = true ] && [ "$width" -lt 960 ]' in commands
    assert "separately qualified navigation-open matrix" not in commands
    assert "--changed-file documentation.css" in commands
    assert "--changed-file search.html" in commands
    assert "source-repository durable-workflow/sdk-python" in commands
    assert "git -C candidate rev-parse HEAD" in commands
    assert "steps.classify.outputs.source_revision" in DOCS_VISUAL.read_text(encoding="utf-8")
