from __future__ import annotations

import base64
import hashlib
import json
from types import SimpleNamespace

import pytest
from scripts.ci.activity_acceptance_output import Output

from tests.test_published_activity_acceptance import runner


@pytest.mark.parametrize("artifact_source", ["candidate", "registry"])
def test_closed_retry_download_uses_the_same_approved_bytes_and_complete_final_lock(
    tmp_path, monkeypatch, artifact_source,
):
    release = runner.release
    data = b"inert candidate marker; never installed"
    digest = hashlib.sha256(data).hexdigest()
    approval = {"version": release.VERSION, "files": {release.WHEEL: digest, release.SDIST: "b" * 64}}
    dependencies = b"anyio==1 --hash=sha256:" + b"c" * 64 + b"\n"
    captured = {runner.REQUIREMENTS: dependencies, "release.json": json.dumps(approval).encode()}
    for filename in (runner.VERIFIER_FILENAME, runner.SCENARIOS["retry-cap"][0], "conftest.py"):
        captured[filename] = b"fixed inert source"
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / release.WHEEL).write_bytes(data)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT", tmp_path / "output")
    monkeypatch.setattr(runner, "SCENARIO", "retry-cap")
    monkeypatch.setattr(runner, "ARTIFACT_SOURCE", artifact_source)
    requests = []

    def call(*arguments, **kwargs):
        if arguments[0] == "git":
            return SimpleNamespace(stdout=json.dumps(approval))
        assert arguments[1:5] == ("-I", "-m", "pip", "download")
        assert "--require-hashes" in arguments
        assert "--only-binary=:all:" in arguments
        assert kwargs["env"]["PIP_INDEX_URL"] == "https://pypi.org/simple"
        selected_lock = (runner.OUTPUT / arguments[-1]).read_bytes()
        requests.append(selected_lock)
        wheels = runner.OUTPUT / "wheels"
        (wheels / "dependency.whl").write_bytes(b"inert dependency marker")
        if artifact_source == "registry":
            (wheels / release.WHEEL).write_bytes(data)
        return SimpleNamespace(stdout="fixed download result", stderr="")

    monkeypatch.setattr(runner, "call", call)
    files = Output(runner.OUTPUT)
    try:
        result = runner.download_wheels({"sdk_version": release.VERSION}, captured, files)
        assert len(requests) == 1
        assert (b"durable-workflow" in requests[0]) is (artifact_source == "registry")
        installed_lock = base64.b64decode(result["files"][runner.REQUIREMENTS]["base64"])
        assert installed_lock == release.lock(dependencies, approval)[0]
        assert result["wheel_sha256"] == digest
        assert base64.b64decode(result["wheels"][release.WHEEL]["base64"]) == data
        assert (runner.OUTPUT / "wheels" / release.WHEEL).read_bytes() == data
    finally:
        files.close()


@pytest.mark.parametrize("scenario", ["activity-identity", "timer-redrive"])
def test_candidate_mode_cannot_expand_an_existing_published_case(monkeypatch, scenario):
    monkeypatch.setattr(runner, "SCENARIO", scenario)
    monkeypatch.setattr(runner, "ARTIFACT_SOURCE", "candidate")
    monkeypatch.setattr(runner, "call", lambda *a, **k: pytest.fail("refuse before Git, download or Docker"))
    with pytest.raises(ValueError, match="fixed retry-cap case"):
        runner.download_wheels({}, {}, None)
