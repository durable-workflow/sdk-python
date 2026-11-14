from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1] / "scripts/ci"


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + ".py"))
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runtime = load("activity_acceptance_runtime")
files = load("activity_acceptance_output")


def test_malformed_create_stdout_does_not_discard_the_full_owned_cid():
    identity, agrees, candidates = runtime.created_identity("malformed stdout", "a" * 64 + "\n")
    assert identity == "a" * 64
    assert agrees is False
    assert candidates == ["a" * 64]


def test_disagreeing_creation_channels_preserve_both_candidates_without_admitting_start():
    identity, agrees, candidates = runtime.created_identity("a" * 64, "b" * 64)
    assert identity == "b" * 64
    assert agrees is False
    assert candidates == ["a" * 64, "b" * 64]


def test_private_output_refuses_a_preexisting_result_symlink(tmp_path):
    output = files.Output(tmp_path / "result")
    target = tmp_path / "foreign"
    target.write_text("preserved")
    (output.path / "link.json").symlink_to(target)
    try:
        with pytest.raises(FileExistsError):
            output.write("link.json", "replacement")
        assert target.read_text() == "preserved"
    finally:
        output.close()


def test_terminal_receipt_uses_the_reserved_descriptor_after_directory_replacement(tmp_path):
    output = files.Output(tmp_path / "result")
    original = tmp_path / "original"
    output.path.rename(original)
    output.path.mkdir()
    try:
        report = {"complete": True}
        output.finish(report)
        assert report["complete"] is False
        assert json.loads((original / "result.json").read_text())["output_failure"] == "identity_changed"
        assert not (output.path / "result.json").exists()
    finally:
        output.close()


def test_replaced_result_cannot_become_a_successful_terminal_receipt(tmp_path):
    output = files.Output(tmp_path / "result")
    (output.path / "result.json").rename(output.path / "retained.json")
    (output.path / "result.json").write_text("foreign")
    try:
        report = {"complete": True}
        output.finish(report)
        assert report["complete"] is False
        original = os.pread(output.files["result.json"], 4096, 0)
        assert json.loads(original)["complete"] is False
        assert (output.path / "result.json").read_text() == "foreign"
    finally:
        output.close()
