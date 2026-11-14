from __future__ import annotations

from copy import deepcopy

import pytest
from scripts.ci import activity_acceptance_release as release

from tests.integration import test_activity_retry_cap as cell


def observations(mode):
    delays = cell.CASES[mode]
    count = len(delays) + 1
    clock = 1_000_000_000
    callbacks = [{"attempt": 1, "at_ns": clock}]
    for attempt, delay in enumerate(delays, start=2):
        clock += delay * 1_000_000_000
        callbacks.append({"attempt": attempt, "at_ns": clock})
    policy = cell.selected_policy(mode)
    commands = [{
        "type": "record_local_activity", "activity_type": cell.ACTIVITY, "execution_mode": "local",
        "retry_policy": policy, "outcome": "completed", "attempts": [
            {"attempt_number": attempt, "outcome": "failed", "backoff_seconds": delay}
            for attempt, delay in enumerate(delays, start=1)
        ] + [{"attempt_number": count, "outcome": "completed"}],
    }, {"type": "complete_workflow"}]
    payload = {"activity_type": cell.ACTIVITY, "execution_mode": "local", "activity": {"retry_policy": policy}}
    history = [{"event_type": "ActivityScheduled", "payload": deepcopy(payload)}]
    for attempt in range(1, count + 1):
        history.append({"event_type": "ActivityStarted", "payload": deepcopy(payload)})
        if attempt < count:
            history.append({"event_type": "ActivityRetryScheduled", "payload": {
                **deepcopy(payload), "retry_after_attempt": attempt, "retry_backoff_seconds": delays[attempt - 1],
            }})
    history.append({"event_type": "ActivityCompleted", "payload": deepcopy(payload)})
    return callbacks, commands, history


@pytest.mark.parametrize("mode", cell.CASES)
def test_exact_policy_attempt_history_and_monotonic_observations_pass_without_mutation(mode):
    values = observations(mode)
    original = deepcopy(values)
    cell.verify_retry(mode, *values)
    assert values == original


@pytest.mark.parametrize("fault", ["actual_wait", "tail_report", "attempt_order", "extra_command", "policy", "history"])
def test_incomplete_or_invented_retry_evidence_refuses(fault):
    callbacks, commands, history = observations("short-list")
    if fault == "actual_wait":
        callbacks[-1]["at_ns"] = callbacks[-2]["at_ns"]
    elif fault == "tail_report":
        commands[0]["attempts"][-2]["backoff_seconds"] = 0
    elif fault == "attempt_order":
        callbacks[-1]["attempt"] = 1
    elif fault == "extra_command":
        commands.append({"type": "start_timer"})
    elif fault == "policy":
        commands[0]["retry_policy"] = {"max_attempts": 5, "backoff_seconds": [1, 2, 0, 0]}
    else:
        next(event for event in history if event["event_type"] == "ActivityRetryScheduled")["payload"][
            "retry_backoff_seconds"
        ] = 0
    with pytest.raises(AssertionError):
        cell.verify_retry("short-list", callbacks, commands, history)


def approval(digest="a" * 64):
    return {"version": release.VERSION, "files": {release.WHEEL: digest, release.SDIST: "b" * 64}}


def test_effective_lock_adds_only_the_approved_sdk_to_the_fixed_dependencies():
    result, digest = release.lock(b"pytest==1 --hash=sha256:" + b"c" * 64 + b"\n", approval())
    assert result.endswith(f"durable-workflow==2.18.0 --hash=sha256:{digest}\n".encode())
    assert result.count(b"durable-workflow") == 1


@pytest.mark.parametrize("fault", ["version", "digest", "member", "sdk_in_dependencies"])
def test_unapproved_release_or_dependency_override_refuses(fault):
    record = approval()
    dependencies = b"pytest==1 --hash=sha256:" + b"c" * 64
    if fault == "version":
        record["version"] = "2.17.2"
    elif fault == "digest":
        record["files"][release.WHEEL] = "wrong"
    elif fault == "member":
        record["files"]["foreign.whl"] = "d" * 64
    else:
        dependencies += b"\ndurable-workflow==2.17.2 --hash=sha256:" + b"d" * 64
    with pytest.raises(ValueError):
        release.lock(dependencies, record)


def test_candidate_consumption_binds_bytes_and_refuses_replacement_or_symlink(tmp_path):
    import hashlib

    dist = tmp_path / "dist"
    dist.mkdir()
    wheel = dist / release.WHEEL
    data = b"fixed inert candidate bytes"
    wheel.write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    assert release.candidate_wheel(tmp_path, digest) == data
    wheel.write_bytes(b"replacement")
    with pytest.raises(ValueError, match="differs from the approved"):
        release.candidate_wheel(tmp_path, digest)
    wheel.unlink()
    foreign = tmp_path / "foreign"
    foreign.write_bytes(data)
    wheel.symlink_to(foreign)
    with pytest.raises(OSError):
        release.candidate_wheel(tmp_path, digest)
