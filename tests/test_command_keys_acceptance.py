"""Keep incomplete transport evidence from becoming an installed acceptance PASS."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

SOURCE = Path(__file__).parents[1] / "scripts/conformance/command_keys/response_loss.py"
SPEC = importlib.util.spec_from_file_location("command_keys_acceptance", SOURCE)
assert SPEC is not None and SPEC.loader is not None
adapter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(adapter)
CONTROLLER_SPEC = importlib.util.spec_from_file_location(
    "command_keys_controller", SOURCE.with_name("run_response_loss.py")
)
assert CONTROLLER_SPEC is not None and CONTROLLER_SPEC.loader is not None
controller = importlib.util.module_from_spec(CONTROLLER_SPEC)
CONTROLLER_SPEC.loader.exec_module(controller)


@pytest.mark.parametrize("kind", ["signal", "update"])
def test_explicit_key_exercises_unicode_character_not_byte_limit(kind):
    key = adapter.explicit_key(kind)
    assert len(key) == 255
    assert len(key.encode()) > 255
    assert key != adapter.explicit_key(kind)


def record(**changes):
    return {
        "method": "POST",
        "path": "/api/workflows/synthetic/update/value",
        "request_body_bytes": 100,
        "request_body_sha256": "a" * 64,
        "upstream_status": 200,
        "upstream_complete": True,
        "upstream_body_bytes": 50,
        "upstream_body_sha256": "b" * 64,
        "dropped": True,
        **changes,
    }


def test_complete_loss_and_retry_agree_with_independent_sender():
    records = [record(), record(dropped=False)]
    observed = adapter.loss_records(SimpleNamespace(records=records), records[0]["path"], 2)
    adapter.compare_requests(observed, [{"sha256": "a" * 64, "bytes": 100}] * 2)


@pytest.mark.parametrize(
    "changes",
    [
        {"dropped": False},
        {"upstream_complete": False},
        {"upstream_status": 502},
        {"upstream_body_bytes": 0},
        {"upstream_body_sha256": ""},
    ],
)
def test_incomplete_or_unplanned_loss_refuses(changes):
    item = record(**changes)
    with pytest.raises(AssertionError):
        adapter.loss_records(SimpleNamespace(records=[item]), item["path"], 1)


@pytest.mark.parametrize(
    "requests",
    [
        [],
        [{"sha256": "a" * 64, "bytes": 99}],
        [{"sha256": "c" * 64, "bytes": 100}],
        [{"sha256": "a" * 64, "bytes": 100}, {"sha256": "c" * 64, "bytes": 100}],
    ],
)
def test_truncated_or_changed_request_comparison_refuses(requests):
    with pytest.raises(AssertionError):
        adapter.compare_requests([record()], requests)


@pytest.mark.parametrize(
    "change",
    [
        {"request_id": "different"},
        {"command_id": "different"},
        {"retained": False},
        {"intent_verified": False},
        {"run_id": "different"},
    ],
)
def test_retained_identity_and_intent_are_required(change):
    original = {"request_id": "synthetic-key", "command_id": "command-1", "run_id": "run-1"}
    retry = {**original, "retained": True, "intent_verified": True, **change}
    with pytest.raises(AssertionError):
        adapter.retained(original, retry)


def test_installed_file_substitution_and_source_overlay_refuse(tmp_path, monkeypatch):
    wheel = tmp_path / "synthetic.whl"
    root = tmp_path / "installed"
    names = ["durable_workflow/__init__.py"] + [f"durable_workflow/file{i}.py" for i in range(45)]
    with ZipFile(wheel, "w") as archive:
        for name in names:
            archive.writestr(name, b"# synthetic fixture\n")
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"# synthetic fixture\n")
    monkeypatch.setattr(adapter, "WHEEL_SHA256", hashlib.sha256(wheel.read_bytes()).hexdigest())
    distribution = SimpleNamespace(
        version=adapter.SDK_VERSION, read_text=lambda _: None, locate_file=lambda name: root / name
    )
    monkeypatch.setattr(adapter.importlib.metadata, "distribution", lambda _: distribution)
    import durable_workflow

    monkeypatch.setattr(durable_workflow, "__file__", str(root / names[0]))
    assert adapter.installed_identity(wheel)["runtime_files_verified"] == 46
    (root / names[-1]).write_bytes(b"# substituted\n")
    with pytest.raises(ValueError, match="differs"):
        adapter.installed_identity(wheel)
    (root / names[-1]).write_bytes(b"# synthetic fixture\n")
    monkeypatch.setattr(durable_workflow, "__file__", str(tmp_path / "overlay/__init__.py"))
    with pytest.raises(ValueError, match="outside"):
        adapter.installed_identity(wheel)


@pytest.mark.parametrize(
    "volume,message",
    [
        (False, "error: no such object: synthetic-owned"),
        (True, "Error response from daemon: get synthetic-owned: no such volume"),
    ],
)
def test_cleanup_recognizes_only_missing_owned_objects(volume, message):
    result = SimpleNamespace(returncode=1, stderr=message + "\n")
    assert controller.missing_object(result, "synthetic-owned", volume=volume)
    assert not controller.missing_object(result, "someone-elses-object", volume=volume)
    result.stderr = "Cannot connect to the Docker daemon"
    assert not controller.missing_object(result, "synthetic-owned", volume=volume)
    result.stderr = message
    result.returncode = 0
    assert not controller.missing_object(result, "synthetic-owned", volume=volume)


@pytest.mark.parametrize(
    "field,value",
    [
        ("Architecture", "arm64"),
        ("Os", "windows"),
        ("RepoDigests", []),
        ("Config", {"Labels": {"org.opencontainers.image.revision": "unqualified-source"}}),
    ],
)
def test_unqualified_local_image_refuses(field, value):
    image = {
        "Architecture": "amd64",
        "Os": "linux",
        "Id": "synthetic-config-id",
        "RepoDigests": ["ghcr.io/durable-workflow/server@" + controller.EXPECTED_SERVER["index"]],
        "Config": {"Labels": {"org.opencontainers.image.revision": controller.EXPECTED_SERVER["source"]}},
    }
    assert controller.image_identity([image])["revision"] == controller.EXPECTED_SERVER["source"]
    with pytest.raises(ValueError, match="differs"):
        controller.image_identity([{**image, field: value}])
