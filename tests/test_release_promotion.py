from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tarfile
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

SCRIPT = Path(__file__).parents[1] / ".github/scripts/promote_release.py"
spec = importlib.util.spec_from_file_location("promote_release", SCRIPT)
assert spec is not None
assert spec.loader is not None
promotion = importlib.util.module_from_spec(spec)
spec.loader.exec_module(promotion)
VERSION = "2.17.2"
APPROVAL_PATH = f".github/release-candidates/{VERSION}.json"
FILES = {f"durable_workflow-{VERSION}-py3-none-any.whl": b"reviewed wheel",
         f"durable_workflow-{VERSION}.tar.gz": b"reviewed sdist"}


def git(root: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *arguments], text=True).strip()


def commit(root: Path) -> str:
    git(root, "add", ".")
    git(root, "-c", "user.name=Release test", "-c", "user.email=release@example.invalid",
                  "commit", "--quiet", "-m", "fixture")
    return git(root, "rev-parse", "HEAD")


def write_tar(path: Path, entries: list[tuple[str, bytes, bytes, int]]) -> None:
    with tarfile.open(path, "w") as archive:
        for name, data, kind, mode in entries:
            member = tarfile.TarInfo(name)
            member.type = kind
            member.mode = mode
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def candidate(tmp_path: Path) -> tuple[Path, dict, Path]:
    root = tmp_path / "source"
    root.mkdir()
    git(root, "init", "--quiet")
    (root / "pyproject.toml").write_text(f'[project]\nversion = "{VERSION}"\n')
    source = commit(root)
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    archive = incoming / "dist-handoff.tar"
    write_tar(archive, [("./", b"", tarfile.DIRTYPE, 0o755)] + [
        ("./" + name, data, tarfile.REGTYPE, 0o644) for name, data in FILES.items()
    ])
    approval = {"schema": "sdk-python.reviewed-release/v1", "version": VERSION,
                "source_commit": source, "source_tree": git(root, "rev-parse", "HEAD^{tree}"),
                "run_id": 12345, "run_attempt": 1, "artifact_id": 67890,
                "artifact_sha256": digest(archive),
                "files": {name: hashlib.sha256(data).hexdigest() for name, data in FILES.items()}}
    path = root / APPROVAL_PATH
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(approval))
    commit(root)
    return root, approval, incoming


def test_promotes_the_exact_reviewed_bytes_after_an_approval_only_commit(candidate, tmp_path: Path) -> None:
    root, approval, incoming = candidate
    checked = promotion.load_approval(root, VERSION)
    assert checked == approval
    dist = tmp_path / "dist"
    promotion.extract_reviewed(checked, incoming, dist)
    assert {path.name: path.read_bytes() for path in dist.iterdir()} == FILES


@pytest.mark.parametrize("stale", [False, True])
@pytest.mark.skipif(sys.version_info < (3, 11), reason="publication uses Python 3.12's builtin tomllib")
def test_build_only_source_resolution_requires_the_event_commit(candidate, tmp_path: Path, stale: bool) -> None:
    root, _, _ = candidate
    workflow = yaml.safe_load((SCRIPT.parents[1] / "workflows/publish.yml").read_text())
    step = next(step for step in workflow["jobs"]["build"]["steps"] if step.get("id") == "release_source")
    output = tmp_path / "output"
    environment = {**os.environ, "PUBLISH_REQUESTED": "false", "REQUESTED_TAG": "release-branch",
                   "REQUESTED_COMMIT": "", "GITHUB_OUTPUT": str(output),
                   "GITHUB_SHA": "0" * 40 if stale else git(root, "rev-parse", "HEAD"),
                   "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]}
    result = subprocess.run(["bash", "-c", step["run"]], cwd=root, env=environment, capture_output=True)
    if stale:
        assert result.returncode != 0
        assert not output.exists()
    else:
        assert result.returncode == 0, result.stderr
        assert "publish=false\n" in output.read_text()


def test_refuses_source_changes_after_the_reviewed_build(candidate) -> None:
    root, _, _ = candidate
    (root / "pyproject.toml").write_text("changed after review")
    commit(root)
    with pytest.raises(ValueError, match="beyond its approval"):
        promotion.load_approval(root, VERSION)


def test_refuses_an_unrelated_source_tree(candidate) -> None:
    root, approval, _ = candidate
    approval["source_tree"] = "0" * 40
    (root / APPROVAL_PATH).write_text(json.dumps(approval))
    commit(root)
    with pytest.raises(ValueError, match="source tree"):
        promotion.load_approval(root, VERSION)


def test_refuses_duplicate_approval_members(candidate) -> None:
    root, approval, _ = candidate
    (root / APPROVAL_PATH).write_text('{"version":"2.17.2",' + json.dumps(approval)[1:])
    commit(root)
    with pytest.raises(ValueError, match="duplicate member"):
        promotion.load_approval(root, VERSION)


@pytest.mark.parametrize("field,value", [
    ("schema", "other"), ("version", "2.17.1"), ("source_commit", "main"),
    ("run_id", True), ("run_attempt", 0), ("artifact_id", "67890"),
    ("artifact_sha256", "sha256:" + "0" * 64), ("files", {"other.whl": "0" * 64}),
    ("unknown", "refuse"),
])
def test_refuses_malformed_or_open_approval_descriptors(candidate, field, value) -> None:
    root, approval, _ = candidate
    approval[field] = value
    (root / APPROVAL_PATH).write_text(json.dumps(approval))
    commit(root)
    with pytest.raises(ValueError):
        promotion.load_approval(root, VERSION)


def authority(approval: dict) -> tuple[dict, dict]:
    repository = {"id": 98765, "full_name": "durable-workflow/sdk-python"}
    run = {"id": approval["run_id"], "run_attempt": 1, "head_sha": approval["source_commit"],
           "event": "workflow_dispatch", "path": ".github/workflows/publish.yml", "status": "completed",
           "conclusion": "success", "repository": repository, "head_repository": repository}
    artifact = {"id": approval["artifact_id"], "name": "dist-handoff.tar", "expired": False,
                "digest": "sha256:" + approval["artifact_sha256"], "size_in_bytes": 10240,
                "workflow_run": {"id": approval["run_id"], "head_sha": approval["source_commit"],
                                 "repository_id": 98765, "head_repository_id": 98765}}
    return run, artifact


def test_requires_the_successful_same_repository_build_and_exact_artifact(candidate) -> None:
    _, approval, _ = candidate
    run, artifact = authority(approval)
    promotion.verify_authority(approval, run, artifact, 98765)


@pytest.mark.parametrize("kind,field,value", [
    ("run", "id", 555), ("run", "run_attempt", 2), ("run", "head_sha", "0" * 40),
    ("run", "event", "pull_request"), ("run", "path", ".github/workflows/other.yml"),
    ("run", "status", "in_progress"), ("run", "conclusion", "failure"),
    ("run", "head_repository", {"id": 98765, "full_name": "fork/sdk-python"}),
    ("artifact", "id", 555), ("artifact", "name", "other.tar"), ("artifact", "expired", True),
    ("artifact", "digest", "sha256:" + "0" * 64), ("artifact", "size_in_bytes", True),
    ("artifact", "workflow_run", {"id": 555}),
])
def test_refuses_stale_forked_or_substituted_build_authority(candidate, kind, field, value) -> None:
    _, approval, _ = candidate
    run, artifact = authority(approval)
    selected = run if kind == "run" else artifact
    selected[field] = value
    with pytest.raises(ValueError, match="authority mismatch"):
        promotion.verify_authority(approval, run, artifact, 98765)


def test_refuses_changed_outer_bytes_before_creating_output(candidate, tmp_path: Path) -> None:
    _, approval, incoming = candidate
    with (incoming / "dist-handoff.tar").open("ab") as output:
        output.write(b"tampered")
    dist = tmp_path / "dist"
    with pytest.raises(ValueError, match="reviewed bytes"):
        promotion.extract_reviewed(approval, incoming, dist)
    assert not dist.exists()


@pytest.mark.parametrize("name,kind,mode", [
    ("../escape", tarfile.REGTYPE, 0o644), ("/absolute", tarfile.REGTYPE, 0o644),
    ("extra.json", tarfile.REGTYPE, 0o644), (next(iter(FILES)), tarfile.SYMTYPE, 0o644),
    (next(iter(FILES)), tarfile.LNKTYPE, 0o644), (next(iter(FILES)), tarfile.FIFOTYPE, 0o644),
    (next(iter(FILES)), tarfile.REGTYPE, 0o755),
])
def test_refuses_unsafe_or_unexpected_members_even_with_a_matching_outer_hash(
    candidate, tmp_path: Path, name, kind, mode,
) -> None:
    _, approval, incoming = candidate
    archive = incoming / "dist-handoff.tar"
    write_tar(archive, [(name, b"unsafe", kind, mode)])
    approval["artifact_sha256"] = digest(archive)
    dist = tmp_path / "dist"
    with pytest.raises(ValueError):
        promotion.extract_reviewed(approval, incoming, dist)
    assert not dist.exists()


@pytest.mark.parametrize("fault", ["duplicate", "missing", "changed_inner", "extra_download"])
def test_refuses_partial_or_substituted_distributions(candidate, tmp_path: Path, fault: str) -> None:
    _, original, incoming = candidate
    approval = deepcopy(original)
    entries = [(name, data, tarfile.REGTYPE, 0o644) for name, data in FILES.items()]
    if fault == "duplicate":
        entries.append(entries[0])
    elif fault == "missing":
        entries.pop()
    elif fault == "changed_inner":
        entries[0] = (entries[0][0], b"changed", tarfile.REGTYPE, 0o644)
    else:
        (incoming / "extra").write_bytes(b"extra")
    archive = incoming / "dist-handoff.tar"
    write_tar(archive, entries)
    approval["artifact_sha256"] = digest(archive)
    dist = tmp_path / "dist"
    with pytest.raises(ValueError):
        promotion.extract_reviewed(approval, incoming, dist)
    assert not dist.exists()


def test_refuses_a_symlink_in_place_of_the_download(candidate, tmp_path: Path) -> None:
    _, approval, incoming = candidate
    archive = incoming / "dist-handoff.tar"
    archive.rename(incoming / "real")
    archive.symlink_to("real")
    with pytest.raises(ValueError):
        promotion.extract_reviewed(approval, incoming, tmp_path / "dist")


def test_cannot_overwrite_existing_output(candidate, tmp_path: Path) -> None:
    _, approval, incoming = candidate
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "retain").write_text("keep")
    with pytest.raises(FileExistsError):
        promotion.extract_reviewed(approval, incoming, dist)
    assert (dist / "retain").read_text() == "keep"


@pytest.mark.parametrize("fault", [
    None, "changed_wheel", "changed_sdist", "extra", "hidden", "symlink", "missing", "directory", "empty_digest",
])
def test_final_handoff_rechecks_reviewed_bytes_after_candidate_execution(tmp_path: Path, fault: str | None) -> None:
    workflow = yaml.safe_load((SCRIPT.parents[1] / "workflows/publish.yml").read_text())
    step = next(step for step in workflow["jobs"]["build"]["steps"]
                if step.get("name") == "Package the privileged PyPI handoff as one immutable file")
    dist = tmp_path / "dist"
    dist.mkdir()
    for name, data in FILES.items():
        (dist / name).write_bytes(data)
    wheel = next(name for name in FILES if name.endswith(".whl"))
    sdist = next(name for name in FILES if name.endswith(".tar.gz"))
    environment = {**os.environ, "PROMOTE_REVIEWED": "true", "RELEASE_VERSION": VERSION,
                   "REVIEWED_WHEEL_SHA256": hashlib.sha256(FILES[wheel]).hexdigest(),
                   "REVIEWED_SDIST_SHA256": hashlib.sha256(FILES[sdist]).hexdigest()}
    if fault in ("changed_wheel", "changed_sdist"):
        target = wheel if fault == "changed_wheel" else sdist
        (dist / target).write_bytes(b"changed after smoke")
    elif fault in ("extra", "hidden"):
        (dist / ("extra" if fault == "extra" else ".extra")).write_bytes(b"unexpected")
    elif fault in ("symlink", "missing", "directory"):
        (dist / wheel).unlink()
        if fault == "symlink":
            (tmp_path / "external").write_bytes(FILES[wheel])
            (dist / wheel).symlink_to(tmp_path / "external")
        elif fault == "directory":
            (dist / wheel).mkdir()
    elif fault == "empty_digest":
        environment["REVIEWED_WHEEL_SHA256"] = ""
    result = subprocess.run(["bash", "-c", step["run"]], cwd=tmp_path, env=environment, capture_output=True)
    archive = tmp_path / "dist-handoff.tar"
    if fault is not None:
        assert result.returncode != 0
        assert not archive.exists()
    else:
        assert result.returncode == 0, result.stderr
        with tarfile.open(archive) as handle:
            promoted = {Path(member.name).name: handle.extractfile(member).read()
                        for member in handle if member.isreg()}
        assert promoted == FILES
