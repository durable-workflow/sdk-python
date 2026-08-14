from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = REPO_ROOT / "scripts" / "ci" / "validate-release-docs-source.py"
VERSION = "2.0.0-rc.31"


def git(repo: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def commit(repo: Path, message: str) -> str:
    git(repo, "add", ".")
    git(
        repo,
        "-c",
        "user.name=Release Docs Test",
        "-c",
        "user.email=release-docs@example.invalid",
        "commit",
        "-m",
        message,
    )
    return git(repo, "rev-parse", "HEAD")


def release_repo(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "release-source"
    repo.mkdir()
    git(repo, "init", "--initial-branch=main")
    (repo / "README.md").write_text("parent\n", encoding="utf-8")
    parent = commit(repo, "Create parent")
    (repo / "pyproject.toml").write_text(
        "\n".join(
            [
                "[project]",
                'name = "durable-workflow"',
                f'version = "{VERSION}"',
                "",
                "[tool.durable-workflow]",
                f'product-train = "{VERSION}"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    source = commit(repo, "Prepare release")
    git(repo, "tag", VERSION, source)
    return repo, source, parent


def run_validator(
    repo: Path,
    *,
    source: str,
    parent: str,
    version: str = VERSION,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "python",
            str(VALIDATOR),
            "--repo-root",
            str(repo),
            "--source-sha",
            source,
            "--parent-sha",
            parent,
            "--release-version",
            version,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_exact_release_source_tuple_is_accepted(tmp_path: Path) -> None:
    repo, source, parent = release_repo(tmp_path)

    result = run_validator(repo, source=source, parent=parent)

    assert result.returncode == 0
    assert f"Validated release docs source {VERSION}@{source}" in result.stdout


def test_stale_source_identity_fails_closed(tmp_path: Path) -> None:
    repo, source, parent = release_repo(tmp_path)
    (repo / "README.md").write_text("newer main state\n", encoding="utf-8")
    observed = commit(repo, "Advance main")

    result = run_validator(repo, source=source, parent=parent)

    assert result.returncode == 1
    assert f"checkout is {observed}, expected {source}" in result.stderr


def test_mismatched_parent_identity_fails_closed(tmp_path: Path) -> None:
    repo, source, _ = release_repo(tmp_path)
    unrelated_parent = "f" * 40

    result = run_validator(repo, source=source, parent=unrelated_parent)

    assert result.returncode == 1
    assert f"expected {unrelated_parent}" in result.stderr


def test_mismatched_public_version_identity_fails_closed(tmp_path: Path) -> None:
    repo, source, parent = release_repo(tmp_path)

    result = run_validator(repo, source=source, parent=parent, version="2.0.0-rc.26")

    assert result.returncode == 1
    assert "does not match project.version" in result.stderr
