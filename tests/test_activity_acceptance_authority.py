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


def test_oversized_report_retains_bounded_nonpass_and_exact_cleanup_ids(tmp_path):
    output = files.Output(tmp_path / "result")
    cleanup = {"a" * 64: {"absent": True, "oom_killed": False}, "remaining-owned-labels": {"absent": True}}
    report = {"complete": True, "scenario": "pagination", "runtime_id": "a" * 64,
              "consumer_id": "b" * 64, "network_id": "c" * 64, "cleanup": cleanup,
              "large_inventory": "x" * (files.RESULT_LIMIT + 1)}
    try:
        output.finish(report)
        data = (output.path / "result.json").read_bytes()
        receipt = json.loads(data)
        assert len(data) <= files.RESULT_LIMIT
        assert receipt["complete"] is report["complete"] is False
        assert receipt["output_failure"] == "terminal_result_exceeded_bound"
        assert receipt["runtime_id"] == "a" * 64
        assert receipt["consumer_id"] == "b" * 64
        assert receipt["network_id"] == "c" * 64
        assert receipt["cleanup"] == cleanup
        assert receipt["oversized_report_bytes"] > files.RESULT_LIMIT
    finally:
        output.close()


def test_oversized_unbounded_cleanup_is_explicitly_unqualified_and_cannot_overwrite_foreign_path(tmp_path):
    output = files.Output(tmp_path / "result")
    original = tmp_path / "retained"
    output.path.rename(original)
    output.path.mkdir()
    (output.path / "result.json").write_text("foreign")
    report = {"complete": True, "cleanup": {"untrusted": "x" * (files.RESULT_LIMIT * 2)}}
    try:
        output.finish(report)
        data = (original / "result.json").read_bytes()
        receipt = json.loads(data)
        assert len(data) <= files.RESULT_LIMIT
        assert receipt["complete"] is False
        assert receipt["output_boundary_failure"] == "identity_changed"
        assert receipt["cleanup"]["retained"] is False
        assert (output.path / "result.json").read_text() == "foreign"
    finally:
        output.close()
