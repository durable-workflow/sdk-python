from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from tests.test_activity_acceptance_consumer import consumer
from tests.test_published_activity_acceptance import runner

ROOT = Path(__file__).parents[1]


def test_retry_effect_is_disabled_in_ordinary_ci_and_candidate_access_is_read_only():
    configuration = yaml.load((ROOT / ".github/workflows/ci.yml").read_text(), Loader=yaml.BaseLoader)
    inputs = configuration["on"]["workflow_dispatch"]["inputs"]
    assert inputs["activity_retry_cap"]["default"] == "false"
    assert inputs["retry_cap_artifact_source"]["options"] == ["candidate", "registry"]
    job = configuration["jobs"]["activity-retry-cap"]
    assert job["if"] == "github.event_name == 'workflow_dispatch' && inputs.activity_retry_cap"
    assert job["permissions"] == {"contents": "read", "actions": "read"}
    resolver = next(step for step in job["steps"] if step.get("id") == "retry-candidate")
    assert resolver["if"] == "inputs.retry_cap_artifact_source == 'candidate'"
    assert resolver["env"] == {"GH_TOKEN": "${{ github.token }}"}
    assert resolver["run"].endswith(" --version " + runner.release.VERSION)
    selected = json.loads((ROOT / "tests/fixtures/published-retry-cap.json").read_text())
    assert selected["sdk_version"] == runner.release.VERSION


def test_retry_case_has_closed_packet_membership_and_cannot_borrow_identity_source():
    packet = {
        "scenario": "retry-cap", "sdk_version": "2.18.0", "wheel_name": "sdk.whl", "wheel_sha256": "a" * 64,
        "wheels": {"sdk.whl": {"sha256": "a" * 64}},
        "files": dict.fromkeys((
            "requirements.txt", "verify_activity_install.py", "conftest.py", "test_activity_retry_cap.py",
        ), {}),
    }
    consumer.validate_setup(packet)
    packet["files"]["test_activity_identity.py"] = packet["files"].pop("test_activity_retry_cap.py")
    with pytest.raises(ValueError, match="membership differs"):
        consumer.validate_setup(packet)


def test_retry_result_cannot_borrow_another_cases_passing_junit(monkeypatch):
    monkeypatch.setattr(runner, "SCENARIO", "retry-cap")
    accepted = runner.SCENARIOS["retry-cap"][1]
    runner.verify_case_result({"complete": True, "junit": f'<testsuite><testcase name="{accepted}"/></testsuite>'})
    other = runner.SCENARIOS["activity-identity"][1]
    with pytest.raises(ValueError, match="inventory differs"):
        runner.verify_case_result({"complete": True, "junit": f'<testsuite><testcase name="{other}"/></testsuite>'})
