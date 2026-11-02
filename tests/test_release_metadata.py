from __future__ import annotations

import io
import json
import sys
import tarfile
import zipfile
from dataclasses import replace
from pathlib import Path
from types import ModuleType

import pytest
from scripts import check_release_metadata
from scripts.check_release_metadata import ReleaseMetadataError, SourceMetadata
from scripts.release_compatibility import (
    CompatibilityContractError,
    declared_runtime_protocol_version,
    validate_server_release_baselines,
)

from durable_workflow.client import PROTOCOL_VERSION

REPO_ROOT = Path(__file__).resolve().parents[1]


def source_metadata() -> SourceMetadata:
    return SourceMetadata(
        commit="a" * 40,
        name="durable-workflow",
        version="2.0.1",
        registry_version="2.0.1",
        server_version="2.0.0",
        worker_protocol_version="1.19",
        summary="Python SDK for the Durable Workflow 2.0 platform",
        classifiers=("Programming Language :: Python :: 3",),
        readme=(
            "# Durable Workflow\n\n"
            "Build replay-safe Python workflows with explicit activities and durable execution semantics.\n"
        ),
    )


def test_worker_release_identity_matches_supported_server_and_protocol() -> None:
    manifest = check_release_metadata._load_toml_parser().loads(
        (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    project = manifest["project"]
    release = manifest["tool"]["durable-workflow"]

    assert check_release_metadata.VERSION_PATTERN.fullmatch(project["version"]) is not None
    assert release["product-train"] == project["version"]
    assert release["registry-version"] == project["version"].replace("-rc.", "rc")
    assert check_release_metadata.VERSION_PATTERN.fullmatch(release["supported-server-versions"]) is not None
    assert release["worker-protocol-version"] == PROTOCOL_VERSION == "1.20"
    assert release["durable-selection"] is True
    assert release["durable-selection-minimum-worker-protocol-version"] == "1.19"
    baselines = validate_server_release_baselines(
        release["supported-server-versions"], release["supported-server-release-baselines"]
    )
    assert baselines == {"2": release["supported-server-versions"], "3": "3.0.0"}
    record = (REPO_ROOT / "src/durable_workflow/server_compatibility.json").read_bytes()
    assert record == (json.dumps({
        "supported-server-versions": release["supported-server-versions"],
        "supported-server-release-baselines": baselines,
    }, indent=2) + "\n").encode()

    assert declared_runtime_protocol_version(
        (REPO_ROOT / "src" / "durable_workflow" / "client.py").read_text(encoding="utf-8")
    ) == "1.20"


@pytest.mark.parametrize("baselines", [
    None, {}, [], "3.0.0", {2: "2.8.0"}, {"02": "2.8.0"},
    {"0": "0.1.0"}, {"2": "2.8.0", "3": 3},
    {"2": "2.8.0", "3": "3.0.0-rc.1"},
    {"2": "2.8.0", "3": "3.00.0"},
    {"2": "2.8.0", "4": "3.0.0"},
    {"2": "2.9.0", "3": "3.0.0"}, {"3": "3.0.0"},
])
def test_server_baselines_refuse_malformed_or_contradictory_maps(baselines: object) -> None:
    with pytest.raises(CompatibilityContractError):
        validate_server_release_baselines("2.8.0", baselines)


def test_server_baselines_do_not_imply_unlisted_major_support() -> None:
    baselines = validate_server_release_baselines("2.8.0", {"2": "2.8.0", "3": "3.0.0"})
    assert "4" not in baselines


COMPATIBILITY_RECORD = b'{"supported-server-versions":"2.0.0"}\n'


def write_distributions(
    dist: Path,
    source: SourceMetadata,
    *,
    wheel_record: bytes | None,
    sdist_record: bytes | None,
) -> None:
    headers = (
        f"Name: {source.name}\nVersion: {source.registry_version}\nSummary: {source.summary}\n"
        "Description-Content-Type: text/markdown\n"
        + "".join(f"Classifier: {value}\n" for value in source.classifiers)
        + "\n" + source.readme
    ).encode()
    with zipfile.ZipFile(dist / "sdk.whl", "w") as archive:
        archive.writestr("sdk.dist-info/METADATA", headers)
        if wheel_record is not None:
            archive.writestr("durable_workflow/server_compatibility.json", wheel_record)
    members = {"sdk/PKG-INFO": headers, "sdk/README.md": source.readme.encode()}
    if sdist_record is not None:
        members["sdk/src/durable_workflow/server_compatibility.json"] = sdist_record
    with tarfile.open(dist / "sdk.tar.gz", "w:gz") as archive:
        for path, content in members.items():
            member = tarfile.TarInfo(path)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))


@pytest.mark.parametrize(("wheel_record", "sdist_record", "refusal"), [
    (COMPATIBILITY_RECORD, COMPATIBILITY_RECORD, None),
    (b"{}\n", COMPATIBILITY_RECORD, "wheel Server compatibility record differs"),
    (COMPATIBILITY_RECORD, b"{}\n", "sdist Server compatibility record differs"),
    (None, COMPATIBILITY_RECORD, "wheel Server compatibility record differs"),
    (COMPATIBILITY_RECORD, None, "sdist must contain one Server compatibility record"),
])
def test_built_packages_must_retain_exact_server_compatibility(
    tmp_path: Path,
    wheel_record: bytes | None,
    sdist_record: bytes | None,
    refusal: str | None,
) -> None:
    source = replace(source_metadata(), server_compatibility=COMPATIBILITY_RECORD)
    write_distributions(tmp_path, source, wheel_record=wheel_record, sdist_record=sdist_record)
    if refusal is None:
        check_release_metadata.verify_built_distributions(tmp_path, source)
    else:
        with pytest.raises(ReleaseMetadataError, match=refusal):
            check_release_metadata.verify_built_distributions(tmp_path, source)


@pytest.mark.parametrize(("wheel_record", "sdist_record", "refusal"), [
    (None, None, None),
    (COMPATIBILITY_RECORD, None, "wheel Server compatibility record is absent from exact source"),
    (None, COMPATIBILITY_RECORD, "sdist Server compatibility record is absent from exact source"),
])
def test_legacy_source_packages_must_not_carry_unverified_server_compatibility(
    tmp_path: Path,
    wheel_record: bytes | None,
    sdist_record: bytes | None,
    refusal: str | None,
) -> None:
    source = source_metadata()
    assert source.server_compatibility is None
    write_distributions(tmp_path, source, wheel_record=wheel_record, sdist_record=sdist_record)
    if refusal is None:
        check_release_metadata.verify_built_distributions(tmp_path, source)
    else:
        with pytest.raises(ReleaseMetadataError, match=refusal):
            check_release_metadata.verify_built_distributions(tmp_path, source)


EXACT_SOURCE_RECORD = (
    json.dumps({
        "supported-server-versions": "2.8.0",
        "supported-server-release-baselines": {"2": "2.8.0", "3": "3.0.0"},
    }, indent=2) + "\n"
).encode()


@pytest.mark.parametrize(("baselines", "record", "refusal"), [
    (None, None, None),
    ('{ "2" = "2.8.0", "3" = "3.0.0" }', EXACT_SOURCE_RECORD, None),
    (None, EXACT_SOURCE_RECORD, "record is present without supported-server-release-baselines"),
    ('{ "2" = "2.8.0", "3" = "3.0.0" }', None, "requires the packaged Server compatibility record"),
    ('{ "2" = "2.8.0", "3" = "3.0.0" }', EXACT_SOURCE_RECORD.replace(b"3.0.0", b"3.1.0"),
     "packaged Server compatibility record differs from pyproject.toml"),
    ('{ "2" = "2.8.0", "3" = "3.0.0" }', EXACT_SOURCE_RECORD.rstrip(b"\n"),
     "packaged Server compatibility record differs from pyproject.toml"),
    ('{ "2" = "2.8.0" }', EXACT_SOURCE_RECORD, "packaged Server compatibility record differs from pyproject.toml"),
    ('{ "2" = "2.8.0", "3" = "3.0.0-rc.1" }', EXACT_SOURCE_RECORD, "stable release of its keyed major"),
    ('{ "3" = "3.0.0" }', EXACT_SOURCE_RECORD, "contradict or omit the legacy baseline"),
    ('{ "2" = "2.9.0", "3" = "3.0.0" }', EXACT_SOURCE_RECORD, "contradict or omit the legacy baseline"),
    ("{}", EXACT_SOURCE_RECORD, "non-empty major-to-release map"),
    ('"3.0.0"', EXACT_SOURCE_RECORD, "non-empty major-to-release map"),
])
def test_release_metadata_loader_binds_server_compatibility_to_exact_source(
    monkeypatch: pytest.MonkeyPatch,
    baselines: str | None,
    record: bytes | None,
    refusal: str | None,
) -> None:
    commit = "c" * 40
    baseline_line = "" if baselines is None else f"supported-server-release-baselines = {baselines}\n"
    pyproject = f"""
[project]
name = "durable-workflow"
version = "2.12.4"
description = "Python SDK for Durable Workflow 2.0"
readme = "README.md"
classifiers = ["Programming Language :: Python :: 3"]

[tool.durable-workflow]
product-train = "2.12.4"
registry-version = "2.12.4"
supported-server-versions = "2.8.0"
{baseline_line}worker-protocol-version = "1.20"
""".encode()
    record_path = "src/durable_workflow/server_compatibility.json"

    def git(*arguments: str) -> bytes:
        if arguments[0] == "rev-parse":
            return f"{commit}\n".encode()
        if arguments[0] == "ls-tree":
            assert arguments[1:] == ("--name-only", commit, "--", record_path)
            return b"" if record is None else f"{record_path}\n".encode()
        assert arguments[0] == "show"
        if arguments[1] == f"{commit}:pyproject.toml":
            return pyproject
        if arguments[1] == f"{commit}:README.md":
            return b"# Durable Workflow\n\nBuild durable Python workflows.\n"
        if arguments[1] == f"{commit}:src/durable_workflow/client.py":
            return b'PROTOCOL_VERSION = "1.20"\n'
        assert arguments[1] == f"{commit}:{record_path}"
        assert record is not None
        return record

    monkeypatch.setattr(check_release_metadata, "_git", git)

    if refusal is not None:
        with pytest.raises(ReleaseMetadataError, match=refusal):
            check_release_metadata.load_source_metadata("HEAD")
        return
    source = check_release_metadata.load_source_metadata("HEAD")
    assert source.server_compatibility == record


def pypi_json(source: SourceMetadata, **overrides: object) -> bytes:
    info: dict[str, object] = {
        "name": source.name,
        "version": source.registry_version,
        "summary": source.summary,
        "classifiers": list(source.classifiers),
        "description": source.readme,
        "description_content_type": "text/markdown",
    }
    info.update(overrides)
    return json.dumps({"info": info}).encode()


def test_release_metadata_loader_uses_tomli_without_stdlib_tomllib(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback = ModuleType("tomli")
    imports: list[str] = []

    def import_module(name: str) -> ModuleType:
        imports.append(name)
        if name == "tomllib":
            raise ModuleNotFoundError("No module named 'tomllib'", name="tomllib")
        assert name == "tomli"
        return fallback

    monkeypatch.setattr(check_release_metadata.importlib, "import_module", import_module)

    assert check_release_metadata._load_toml_parser() is fallback
    assert imports == ["tomllib", "tomli"]


@pytest.mark.parametrize(
    ("version", "server_version"),
    [("2.0.1", "2.0.0"), ("2.1.0", "2.4.0")],
)
def test_release_metadata_loader_accepts_the_authorized_stable_identity(
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    server_version: str,
) -> None:
    commit = "b" * 40
    pyproject = f"""
[project]
name = "durable-workflow"
version = "{version}"
description = "Python SDK for Durable Workflow 2.0"
readme = "README.md"
classifiers = ["Programming Language :: Python :: 3"]

[tool.durable-workflow]
product-train = "{version}"
registry-version = "{version}"
supported-server-versions = "{server_version}"
worker-protocol-version = "1.19"
""".encode()
    readme = b"# Durable Workflow\n\nBuild durable Python workflows.\n"
    runtime = b'PROTOCOL_VERSION = "1.19"\n'

    def git(*arguments: str) -> bytes:
        if arguments[0] == "rev-parse":
            return f"{commit}\n".encode()
        if arguments[0] == "ls-tree":
            return b""
        assert arguments[0] == "show"
        if arguments[1].endswith(":pyproject.toml"):
            return pyproject
        if arguments[1].endswith(":README.md"):
            return readme
        assert arguments[1].endswith(":src/durable_workflow/client.py")
        return runtime

    monkeypatch.setattr(check_release_metadata, "_git", git)

    source = check_release_metadata.load_source_metadata(version)
    assert source.version == version
    assert source.registry_version == version
    assert source.server_version == server_version


def test_normal_project_page_is_retained_as_rendered_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    source = source_metadata()
    marker = "Build replay-safe Python workflows with explicit activities and durable execution semantics."

    def request(url: str, accept: str) -> bytes:
        if accept == "application/json":
            return pypi_json(source)
        assert url.endswith(f"/{source.registry_version}/")
        return f"<html><body><h1>{source.registry_version}</h1><p>{marker}</p></body></html>".encode()

    monkeypatch.setattr(check_release_metadata, "_request_bytes", request)

    audit = check_release_metadata.verify_pypi(source, source.version)

    assert audit.outcome == "match"


def test_200_client_challenge_is_advisory_after_exact_json_matches(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = source_metadata()
    challenge = b"""<!doctype html>
    <html><head><title>Client Challenge</title></head>
    <body><p>JavaScript is disabled</p><p>Checking your browser...</p></body></html>
    """

    def request(url: str, accept: str) -> bytes:
        del url
        return pypi_json(source) if accept == "application/json" else challenge

    monkeypatch.setattr(check_release_metadata, "_request_bytes", request)
    monkeypatch.setattr(check_release_metadata, "load_source_metadata", lambda _source_ref: source)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_release_metadata.py",
            "--source-ref",
            source.commit,
            "--pypi-version",
            source.version,
        ],
    )

    assert check_release_metadata.main() == 0

    output = capsys.readouterr()
    assert "exact PyPI JSON matches" in output.out
    assert "::warning title=PyPI rendered-page audit::" in output.err
    assert "exact PyPI JSON remains authoritative" in output.err


def test_exact_json_metadata_mismatch_remains_release_blocking(monkeypatch: pytest.MonkeyPatch) -> None:
    source = source_metadata()

    def request(url: str, accept: str) -> bytes:
        del url
        assert accept == "application/json"
        return pypi_json(source, version="2.0.0rc21")

    monkeypatch.setattr(check_release_metadata, "_request_bytes", request)

    with pytest.raises(ReleaseMetadataError, match="exact PyPI JSON field version"):
        check_release_metadata.verify_pypi(source, source.version)


def test_exact_json_metadata_mismatch_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    attempts: list[str] = []
    sleeps: list[float] = []

    def verify(_source: SourceMetadata, _version: str) -> check_release_metadata.ProjectPageAudit:
        attempts.append("verify")
        raise ReleaseMetadataError("exact PyPI JSON field summary differs from the source commit")

    monkeypatch.setattr(check_release_metadata, "load_source_metadata", lambda _: source)
    monkeypatch.setattr(check_release_metadata, "verify_pypi", verify)
    monkeypatch.setattr(check_release_metadata.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_release_metadata.py",
            "--source-ref",
            source.commit,
            "--pypi-version",
            source.version,
            "--attempts",
            "30",
            "--interval-seconds",
            "10",
        ],
    )

    with pytest.raises(ReleaseMetadataError, match="exact PyPI JSON field summary differs"):
        check_release_metadata.main()

    assert attempts == ["verify"]
    assert sleeps == []
