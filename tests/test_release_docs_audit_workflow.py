import json
import os
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
AUDITOR = REPO_ROOT / "scripts/ci/check-docs-release-audit.sh"


def read_repo_file(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def workflow_job_block(workflow: str, job_name: str) -> str:
    match = re.search(rf"(?ms)^  {re.escape(job_name)}:\n.*?(?=^  [A-Za-z0-9_-]+:\n|\Z)", workflow)
    assert match is not None, f"{job_name} job is missing from publish workflow"
    return match.group(0)


def run_audit(
    tmp_path: Path,
    *,
    expected_version: str,
    live_version: str | None,
    enforcement: str,
) -> tuple[subprocess.CompletedProcess[str], dict[str, object], dict[str, object] | None]:
    evidence_path = tmp_path / "evidence.json"
    handoff_path = tmp_path / "handoff.json"
    if live_version is None:
        audit_url = (tmp_path / "missing-audit.json").as_uri()
    else:
        audit_path = tmp_path / "docs-page-release-audit.json"
        audit_path.write_text(
            json.dumps(
                {
                    "schema": "durable-workflow.docs.page-release-audit",
                    "artifact_versions": {"sdk-python": live_version},
                }
            ),
            encoding="utf-8",
        )
        audit_url = audit_path.as_uri()

    env = {
        **os.environ,
        "DOCS_RELEASE_AUDIT_ARTIFACT": "sdk-python",
        "DOCS_RELEASE_AUDIT_VERSION": expected_version,
        "DOCS_RELEASE_AUDIT_URL": audit_url,
        "DOCS_RELEASE_AUDIT_ATTEMPTS": "1",
        "DOCS_RELEASE_AUDIT_RETRY_SLEEP": "0",
        "DOCS_RELEASE_AUDIT_EVIDENCE": str(evidence_path),
        "DOCS_RELEASE_AUDIT_HANDOFF": str(handoff_path),
        "DOCS_RELEASE_AUDIT_ENFORCEMENT": enforcement,
    }
    result = subprocess.run(
        ["sh", str(AUDITOR)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    handoff = json.loads(handoff_path.read_text(encoding="utf-8")) if handoff_path.exists() else None
    return result, evidence, handoff


def test_publish_workflow_separates_publication_authority_from_docs_freshness() -> None:
    workflow = read_repo_file(".github/workflows/publish.yml")
    build_job = workflow_job_block(workflow, "build")
    publish_job = workflow_job_block(workflow, "publish")
    docs_audit_job = workflow_job_block(workflow, "verify-docs-release-audit")

    assert '[[ ! "$REQUESTED_TAG" =~ ^[0-9]+\\.[0-9]+\\.[0-9]+' in build_job
    assert 'if [ "$REQUESTED_TAG" != "$package_version" ]' in build_job
    assert 'if [ "$tag_commit" != "$head_commit" ]' in build_job
    assert "pypa/gh-action-pypi-publish@release/v1" in publish_job
    assert 'gh release create "$RELEASE_TAG"' in publish_job
    assert "DOCS_RELEASE_AUDIT_ENFORCEMENT: advisory" in docs_audit_job
    assert "DOCS_RELEASE_AUDIT_EVIDENCE: docs-release-audit-evidence.json" in docs_audit_job
    assert "DOCS_RELEASE_AUDIT_HANDOFF: docs-release-audit-handoff.json" in docs_audit_job
    assert "needs: [build, publish]" in docs_audit_job
    assert "contents: write" not in docs_audit_job
    assert "if: always()" in docs_audit_job
    assert docs_audit_job.index("scripts/ci/check-docs-release-audit.sh") < docs_audit_job.index(
        "actions/upload-artifact@v4"
    )


def test_stale_tuple_is_actionable_without_reversing_publication(tmp_path: Path) -> None:
    result, evidence, handoff = run_audit(
        tmp_path,
        expected_version="0.4.101",
        live_version="0.4.99",
        enforcement="advisory",
    )

    assert result.returncode == 0
    assert "::warning" in result.stderr
    assert evidence["schema"] == "durable-workflow.release.docs-release-audit-evidence"
    assert evidence["outcome"] == "stale"
    assert evidence["lifecycle"] == "docs_freshness"
    assert evidence["publication_outcome_impact"] == "none"
    assert evidence["expected_version"] == "0.4.101"
    assert evidence["actual_version"] == "0.4.99"
    assert handoff is not None
    assert handoff["schema"] == "durable-workflow.release.docs-artifact-tuple-handoff"
    assert handoff["lifecycle"] == "docs_freshness"
    assert handoff["priority"] == "low"
    assert handoff["stale_artifact"] == {
        "name": "sdk-python",
        "expected_version": "0.4.101",
        "live_version": "0.4.99",
    }
    assert "priority:P3" in handoff["ready_item"]["labels"]
    guard = handoff["release_status_guard"]
    assert guard["stable_default_docs_line"] == "1.x"
    assert guard["prerelease_docs_line"] == "2.0"
    assert guard["no_default_docs_cutover"] is True


def test_stale_tuple_remains_enforceable_outside_publication(tmp_path: Path) -> None:
    result, evidence, handoff = run_audit(
        tmp_path,
        expected_version="0.4.101",
        live_version="0.4.99",
        enforcement="required",
    )

    assert result.returncode != 0
    assert evidence["outcome"] == "stale"
    assert handoff is not None


def test_unavailable_tuple_retains_non_authoritative_evidence(tmp_path: Path) -> None:
    result, evidence, handoff = run_audit(
        tmp_path,
        expected_version="0.4.101",
        live_version=None,
        enforcement="advisory",
    )

    assert result.returncode == 0
    assert evidence["outcome"] == "unavailable"
    assert evidence["lifecycle"] == "docs_freshness"
    assert evidence["publication_outcome_impact"] == "none"
    assert handoff is None


def test_current_tuple_passes_without_a_refresh_handoff(tmp_path: Path) -> None:
    result, evidence, handoff = run_audit(
        tmp_path,
        expected_version="0.4.101",
        live_version="0.4.101",
        enforcement="advisory",
    )

    assert result.returncode == 0
    assert evidence["outcome"] == "pass"
    assert evidence["actual_version"] == "0.4.101"
    assert handoff is None
