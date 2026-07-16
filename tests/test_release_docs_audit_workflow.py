import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def read_repo_file(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def workflow_job_block(workflow: str, job_name: str) -> str:
    match = re.search(rf"(?ms)^  {re.escape(job_name)}:\n.*?(?=^  [A-Za-z0-9_-]+:\n|\Z)", workflow)
    assert match is not None, f"{job_name} job is missing from publish workflow"
    return match.group(0)


def test_publish_workflow_runs_docs_audit_in_recoverable_job() -> None:
    workflow = read_repo_file(".github/workflows/publish.yml")
    auditor = read_repo_file("scripts/ci/check-docs-release-audit.sh")
    publish_job = workflow_job_block(workflow, "publish")
    docs_audit_job = workflow_job_block(workflow, "verify-docs-release-audit")

    for expected in [
        "Verify live docs release audit after PyPI publish",
        "DOCS_RELEASE_AUDIT_ARTIFACT: sdk-python",
        "DOCS_RELEASE_AUDIT_VERSION: ${{ needs.build.outputs.release_tag }}",
        "DOCS_RELEASE_AUDIT_EVIDENCE: docs-release-audit-evidence.json",
        "DOCS_RELEASE_AUDIT_HANDOFF: docs-release-audit-handoff.json",
        "scripts/ci/check-docs-release-audit.sh",
        "Upload docs release audit evidence",
        "docs-release-audit-evidence.json",
        "docs-release-audit-handoff.json",
    ]:
        assert expected in workflow

    assert "permissions:\n  contents: read" in workflow
    assert "contents: write" not in docs_audit_job
    assert "pypa/gh-action-pypi-publish@release/v1" in publish_job
    assert "Verify live docs release audit after PyPI publish" not in publish_job
    assert "Upload docs release audit evidence" not in publish_job
    assert "needs: [build, publish]" in docs_audit_job
    assert "durable-workflow.release.docs-release-audit-evidence" in auditor
    assert "durable-workflow.release.docs-artifact-tuple-handoff" in auditor
    assert "DOCS_RELEASE_AUDIT_HANDOFF" in auditor
    assert "schema: 'durable-workflow.docs.refresh-request'" in auditor
    assert "repository: 'durable-workflow.github.io'" in auditor
    assert "refresh_command: 'npm run refresh:public-artifact-versions'" in auditor
    assert "refresh_files: refreshFiles" in auditor
    assert "observed_artifact_versions: versions" in auditor
    assert "docs_artifact_tuple_handoff: handoff" in auditor

    publish_offset = workflow.index("Publish to PyPI")
    docs_audit_job_offset = workflow.index("verify-docs-release-audit:")
    docs_audit_offset = docs_audit_job.index("Verify live docs release audit after PyPI publish")
    upload_offset = docs_audit_job.index("Upload docs release audit evidence")

    assert publish_offset < docs_audit_job_offset
    assert docs_audit_offset < upload_offset
