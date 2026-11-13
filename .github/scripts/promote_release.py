#!/usr/bin/env python3
"""Promote the exact reviewed build-only distributions before PyPI upload."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tarfile
from pathlib import Path, PurePosixPath

REPOSITORY = "durable-workflow/sdk-python"
MAX_FILE_BYTES = 64 * 1024 * 1024
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"[0-9a-f]{64}")
KEYS = {"schema", "version", "source_commit", "source_tree", "run_id", "run_attempt",
        "artifact_id", "artifact_sha256", "files"}


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("release approval contains a duplicate member")
        result[key] = value
    return result


def load_approval(root: Path, version: str) -> dict:
    if re.fullmatch(r"\d+\.\d+\.\d+", version) is None:
        raise ValueError("release version must be exact stable SemVer")
    name = f".github/release-candidates/{version}.json"
    path = root / name
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        raise ValueError("release approval must be a bounded regular file")
    approval = json.loads(path.read_text(), object_pairs_hook=unique_object)
    if (not isinstance(approval, dict) or set(approval) != KEYS
            or approval["schema"] != "sdk-python.reviewed-release/v1" or approval["version"] != version):
        raise ValueError("release approval schema or version mismatch")
    for key in ("source_commit", "source_tree"):
        if not isinstance(approval[key], str) or SHA.fullmatch(approval[key]) is None:
            raise ValueError("release approval requires exact source identities")
    for key in ("run_id", "run_attempt", "artifact_id"):
        if type(approval[key]) is not int or approval[key] <= 0:
            raise ValueError("release approval requires positive numeric producer identities")
    files = approval["files"]
    expected = {f"durable_workflow-{version}-py3-none-any.whl", f"durable_workflow-{version}.tar.gz"}
    if not isinstance(files, dict) or set(files) != expected:
        raise ValueError("release approval must name exactly the wheel and sdist")
    for value in (approval["artifact_sha256"], *files.values()):
        if not isinstance(value, str) or DIGEST.fullmatch(value) is None:
            raise ValueError("release approval requires exact artifact digests")
    source = approval["source_commit"]
    subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", source, "HEAD"], check=True)
    if git(root, "rev-parse", f"{source}^{{tree}}") != approval["source_tree"]:
        raise ValueError("reviewed source tree mismatch")
    # The approval cannot be embedded in its own reviewed source commit.
    # Every executable/configuration/package change must precede the build.
    if git(root, "diff", "--no-renames", "--name-status", source, "HEAD") != f"A\t{name}":
        raise ValueError("publication source differs from reviewed source beyond its approval file")
    return approval


def verify_authority(approval: dict, run: dict, artifact: dict, repository_id: int) -> None:
    repository = {"id": repository_id, "full_name": REPOSITORY}
    if (run.get("id") != approval["run_id"] or run.get("run_attempt") != approval["run_attempt"]
            or run.get("head_sha") != approval["source_commit"] or run.get("event") != "workflow_dispatch"
            or run.get("path") != ".github/workflows/publish.yml" or run.get("status") != "completed"
            or run.get("conclusion") != "success"
            or any(run.get(key, {}).get(field) != value
                   for key in ("repository", "head_repository") for field, value in repository.items())):
        raise ValueError("reviewed build run authority mismatch")
    producer = artifact.get("workflow_run", {})
    if (artifact.get("id") != approval["artifact_id"] or artifact.get("name") != "dist-handoff.tar"
            or artifact.get("expired") is not False
            or artifact.get("digest") != "sha256:" + approval["artifact_sha256"]
            or type(artifact.get("size_in_bytes")) is not int
            or not 0 < artifact["size_in_bytes"] <= 2 * MAX_FILE_BYTES + 65536
            or producer.get("id") != approval["run_id"]
            or producer.get("head_sha") != approval["source_commit"]
            or producer.get("repository_id") != repository_id
            or producer.get("head_repository_id") != repository_id):
        raise ValueError("reviewed artifact authority mismatch")


def api(path: str) -> dict:
    result = subprocess.check_output(["gh", "api", f"repos/{REPOSITORY}/{path}"])
    if len(result) > 1048576:
        raise ValueError("producer metadata exceeds its bound")
    return json.loads(result)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1048576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_reviewed(approval: dict, incoming: Path, dist: Path) -> None:
    archive = incoming / "dist-handoff.tar"
    if (incoming.is_symlink() or not incoming.is_dir() or set(incoming.iterdir()) != {archive}
            or archive.is_symlink() or not archive.is_file()
            or archive.stat().st_size > 2 * MAX_FILE_BYTES + 65536
            or file_sha256(archive) != approval["artifact_sha256"]):
        raise ValueError("downloaded producer artifact differs from reviewed bytes")
    contents = {}
    with tarfile.open(archive, mode="r:") as handle:
        for member in handle:
            if member.name in (".", "./") and member.isdir() and member.size == 0:
                continue
            name = PurePosixPath(member.name)
            if (name.is_absolute() or len(name.parts) != 1 or "\\" in member.name
                    or name.name not in approval["files"] or name.name in contents
                    or not member.isreg() or member.mode != 0o644 or not 0 < member.size <= MAX_FILE_BYTES):
                raise ValueError("producer artifact has unexpected, duplicate or unsafe members")
            stream = handle.extractfile(member)
            if stream is None:
                raise ValueError("producer artifact member cannot be read")
            data = stream.read(MAX_FILE_BYTES + 1)
            if len(data) != member.size or hashlib.sha256(data).hexdigest() != approval["files"][name.name]:
                raise ValueError("distribution differs from reviewed bytes")
            contents[name.name] = data
    if set(contents) != set(approval["files"]):
        raise ValueError("producer artifact does not contain both reviewed distributions")
    # Validate everything before creating output; never extract archive paths.
    dist.mkdir()
    for name, data in sorted(contents.items()):
        descriptor = os.open(dist / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("resolve", "extract"))
    parser.add_argument("--version", required=True)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--incoming", type=Path, default=Path(".release-candidate"))
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    args = parser.parse_args()
    approval = load_approval(args.root, args.version)
    if args.operation == "resolve":
        verify_authority(approval, api(f"actions/runs/{approval['run_id']}"),
                         api(f"actions/artifacts/{approval['artifact_id']}"), int(os.environ["GITHUB_REPOSITORY_ID"]))
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write(f"run-id={approval['run_id']}\nartifact-id={approval['artifact_id']}\n")
    else:
        extract_reviewed(approval, args.incoming, args.dist)


if __name__ == "__main__":
    main()
