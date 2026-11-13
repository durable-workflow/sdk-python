"""Substituted files cannot become executable authority or certify a result."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1] / "scripts/conformance/command_keys"


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


guards = module("capture_controls", ROOT / "input_guards.py")
bootstrap = module("bootstrap_controls", ROOT / "consumer_bootstrap.py")
controller = module("controller_controls", ROOT / "run_response_loss.py")
ROLES = ["adapter", "proxy", "wheel", "bootstrap", "requirements"]


@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("substitution", ["symlink", "rename-identical", "rewrite", "hardlink"])
def test_all_input_roles_reject_symlinks_hardlinks_and_path_or_inode_changes(tmp_path, role, substitution):
    path = tmp_path / "input"
    path.write_bytes(b"original input")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if substitution == "symlink":
        alias = tmp_path / "alias"
        alias.symlink_to(path)
        with pytest.raises(ValueError):
            guards.CapturedInput(alias, role, maximum=1024, expected=digest)
    elif substitution == "hardlink":
        import os

        os.link(path, tmp_path / "second-link")
        with pytest.raises(ValueError):
            guards.CapturedInput(path, role, maximum=1024, expected=digest)
    else:
        with guards.CapturedInput(path, role, maximum=1024, expected=digest) as captured:
            if substitution == "rename-identical":
                path.rename(tmp_path / "previous")
                path.write_bytes(b"original input")
            else:
                path.write_bytes(b"substituted input")
            assert captured.data == b"original input"
            with pytest.raises(ValueError):
                captured.recheck()


def test_parent_directory_rename_refuses_even_identical_replacement(tmp_path):
    parent = tmp_path / "owned"
    parent.mkdir(mode=0o700)
    path = parent / "input"
    path.write_bytes(b"original")
    with guards.CapturedInput(path, "adapter", maximum=100) as captured:
        parent.rename(tmp_path / "renamed")
        parent.mkdir(mode=0o700)
        path.write_bytes(b"original")
        with pytest.raises(ValueError, match="parent was replaced"):
            captured.recheck()


def test_substituted_adapter_cannot_self_certify_its_report(tmp_path):
    path = tmp_path / "adapter.py"
    path.write_text("def attest(): return {'complete': False}\n")
    with guards.CapturedInput(path, "adapter", maximum=4096) as captured:
        forged = {"complete": True, "adapter_sha256": captured.sha256}
        path.rename(tmp_path / "original.py")
        path.write_text("def attest(): return " + repr(forged) + "\n")
        # The predecessor's hash-then-path-load and self-reported comparison
        # accepts this synthetic forgery. The corrected execution sees only
        # the captured original and its post-call identity check refuses.
        predecessor = runpy.run_path(str(path))["attest"]()
        assert predecessor["complete"] and predecessor["adapter_sha256"] == captured.sha256
        executed = bootstrap.execute_source(captured.data, "captured_adapter_control")
        assert executed.attest()["complete"] is False
        with pytest.raises(ValueError):
            captured.recheck()


@pytest.mark.parametrize("role", ["adapter", "proxy", "wheel", "bootstrap", "requirements"])
def test_bundle_byte_substitution_refuses_before_execution(role):
    sources = {
        key: {"base64": base64.b64encode(b"original").decode(), "sha256": hashlib.sha256(b"original").hexdigest()}
        for key in bootstrap.ROLES
    }
    sources[role]["base64"] = base64.b64encode(b"substituted").decode()
    with pytest.raises(ValueError, match="bytes differ"):
        bootstrap.decode_inputs({"sources": sources})


def test_no_host_interpreter_path_can_be_supplied(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_response_loss.py",
            "--runner-commit",
            "a" * 40,
            "--wheel",
            "wheel",
            "--proxy",
            "proxy",
            "--output",
            "new-output",
            "--python",
            "/arbitrary/program",
        ],
    )
    with pytest.raises(SystemExit):
        controller.parse_args()
    assert "unrecognized arguments: --python /arbitrary/program" in capsys.readouterr().err


@pytest.mark.parametrize(
    "changes",
    [
        {"Architecture": "arm64"},
        {"RepoDigests": ["python@sha256:" + "0" * 64]},
        {"Config": {"Env": ["PYTHON_VERSION=unqualified"]}},
    ],
)
def test_unqualified_interpreter_image_refuses(changes):
    image = {
        "Architecture": "amd64",
        "Os": "linux",
        "Id": "qualified-image",
        "RepoDigests": ["python@" + controller.PYTHON_INDEX],
        "Config": {"Env": ["PYTHON_VERSION=" + controller.PYTHON_VERSION]},
    }
    assert controller.interpreter_identity([image])["version"] == controller.PYTHON_VERSION
    with pytest.raises(ValueError, match="fixed published Python"):
        controller.interpreter_identity([{**image, **changes}])


def test_inventory_catches_installed_dependency_or_interpreter_mutation(tmp_path):
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin/python").write_bytes(b"trusted interpreter fixture")
    before = bootstrap.inventory(tmp_path)
    (tmp_path / "bin/python").write_bytes(b"substituted interpreter fixture")
    assert bootstrap.inventory(tmp_path) != before


def test_requirement_lock_covers_only_exact_wheel_hashes():
    lines = (ROOT / "consumer-requirements.txt").read_text().splitlines()
    assert len(lines) == 9
    assert all("==" in line and " --hash=sha256:" in line for line in lines)
    assert controller.WHEEL_SHA256 in "\n".join(lines)
    # This JSON framing round trip retains bytes rather than an executable path.
    original = b"compiled from captured bytes\n"
    bundle = {
        "sources": {
            role: {"base64": base64.b64encode(original).decode(), "sha256": hashlib.sha256(original).hexdigest()}
            for role in bootstrap.ROLES
        }
    }
    assert bootstrap.decode_inputs(json.loads(json.dumps(bundle)))["adapter"] == original
