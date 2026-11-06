from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from itertools import permutations
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import ActivityFailed, NonDeterministicReplayError, TypedValueError
from durable_workflow.replay_clock_math import carbon_from_milliseconds, carbon_milliseconds
from durable_workflow.workflow import WorkflowContext, query_state, replay
from tests.business_clock_workflows import clock_text
from tests.test_business_replay_clock import result

START = "2026-01-01T00:00:00.000000+00:00"
FIRST = "2026-01-01T00:00:03.123100+00:00"
TIED = "2026-01-01T00:00:03.123400+00:00"
REFERENCE = Path(__file__).parent / "fixtures" / "replay_clock_carbon_reference.json"
VECTORS = json.loads(REFERENCE.read_text())["vectors"]


def test_retained_carbon_reference_identity_is_exact() -> None:
    assert hashlib.sha256(REFERENCE.read_bytes()).hexdigest() == (
        "09a9051249a6dc4c0e3c177d26f058b393d563cf9f42b120c48f5d86371ea38f"
    )
    reference = json.loads(REFERENCE.read_text())
    assert (reference["php"], reference["carbon"], reference["source"], reference["timestamp_source_sha256"]) == (
        "8.4.27", "3.15.1", "ce0e5636fae7d1e99856eb70cae4c719a284aa08",
        "57b2a235656f8edad7c333087a64e45491adaaf1b01ac9c40e53185b73db356d",
    )


@pytest.mark.parametrize("vector", VECTORS, ids=lambda vector: vector["input"])
def test_actual_carbon_rounding_and_reconstruction_vectors(vector: dict[str, Any]) -> None:
    time = datetime.fromisoformat(vector["input"].replace("Z", "+00:00"))
    observed = carbon_milliseconds(time)
    assert observed == vector["milliseconds"]
    assert carbon_from_milliseconds(vector["milliseconds"]).isoformat(timespec="microseconds").replace(
        "+00:00", "Z",
    ) == vector["reconstructed"]


@pytest.mark.parametrize("sign", [-1, 1])
def test_unproved_float_microsecond_range_stays_unavailable(sign: int) -> None:
    time = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=sign * ((1 << 53) + 1))
    assert carbon_milliseconds(time) is None


def group_path(base: int, size: int, index: int, *, nested: bool = False) -> list[dict[str, Any]]:
    def entry(start: int, count: int, position: int) -> dict[str, Any]:
        return {"parallel_group_id": f"parallel-activities:{start}:{count}", "parallel_group_kind": "activity",
                "parallel_group_base_sequence": start, "parallel_group_size": count, "parallel_group_index": position}

    path = [entry(base, size, index)]
    if nested and index:
        path.append(entry(base + 1, size - 1, index - 1))
    return path


def history(
    times: list[Any] | None = None, *, failures: set[int] | None = None, order: tuple[int, ...] | None = None,
    nested: bool = False,
) -> list[dict[str, Any]]:
    times = [FIRST, TIED] if times is None else times
    failures = failures or set()
    size = len(times)
    rows: list[dict[str, Any]] = [{"event_type": "WorkflowStarted", "recorded_at": START, "payload": {}}]
    terminals = []
    for index, time in enumerate(times):
        path = group_path(1, size, index, nested=nested)
        payload = {"sequence": index + 1, "activity_execution_id": f"group-execution-{index}",
                   "activity_type": f"group.{index}", **path[-1], "parallel_group_path": path}
        rows.append({"event_type": "ActivityScheduled", "recorded_at": START, "payload": deepcopy(payload)})
        if index in failures:
            payload["message"] = f"failure-{index}"
        else:
            payload["result"] = serializer.envelope(f"value-{index}")
        terminals.append({"event_type": "ActivityFailed" if index in failures else "ActivityCompleted",
                          "recorded_at": time, "payload": payload})
    rows.extend(terminals[index] for index in (order if order is not None else range(size)))
    return rows


@workflow.defn(name="tests.business-clock-parallel-probe")
class ParallelProbe:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        self.ctx = ctx
        commands = [ctx.schedule_activity(f"group.{index}", []) for index in range(self.size)]
        if self.nested:
            commands = [commands[0], commands[1:]]
        self.failure = None
        try:
            self.values = yield commands
        except ActivityFailed as error:
            self.values = None
            self.failure = (error.activity_type, str(error))
        self.time = clock_text(ctx)
        return {"values": self.values, "failure": self.failure, "clock": self.time,
                "repeated": clock_text(ctx) == self.time, "legacy": ctx.now().isoformat(timespec="microseconds")}

    size = 2
    nested = False

    @workflow.query("clock")
    def clock(self) -> str | None:
        return clock_text(self.ctx)

    @workflow.query("body-clock")
    def body_clock(self) -> str | None:
        return self.time


@pytest.mark.parametrize("order", [(0, 1), (1, 0)])
def test_success_uses_authored_tie_order_and_full_precision_not_arrival_or_history_max(order: tuple[int, ...]) -> None:
    rows = history(order=order)
    rows.append({"event_type": "WorkflowTaskCompleted", "recorded_at": "2099-01-01T00:00:00Z", "payload": {}})
    original = deepcopy(rows)
    expected = {"values": ["value-0", "value-1"], "failure": None, "clock": FIRST,
                "repeated": True, "legacy": START}
    assert result(rows, ParallelProbe) == expected
    assert result(rows, ParallelProbe) == expected
    assert rows == original


@pytest.mark.parametrize("times,expected", [
    ([FIRST, "2026-01-01T00:00:03.123500Z"], "2026-01-01T00:00:03.123500+00:00"),
    ([TIED, FIRST], TIED),
    (["1969-12-31T23:59:59.999500Z", "1969-12-31T23:59:59.999501Z"],
     "1969-12-31T23:59:59.999501+00:00"),
])
def test_success_uses_carbon_millisecond_comparison_without_replacing_full_precision(
    times: list[str], expected: str,
) -> None:
    assert result(history(times), ParallelProbe)["clock"] == expected


@pytest.mark.parametrize("order", [(0, 1), (1, 0)])
def test_caught_failure_ignores_successful_sibling_time_and_reconstructs_carbon_milliseconds(
    order: tuple[int, ...],
) -> None:
    value = result(history(["1969-12-31T23:59:59.999500Z", "2099-01-01T00:00:00Z"],
                           failures={0}, order=order), ParallelProbe)
    assert value == {"values": None, "failure": ("group.0", "failure-0"),
                     "clock": "1969-12-31T23:59:59.999000+00:00", "repeated": True, "legacy": START}


@pytest.mark.parametrize("times,expected", [
    ([FIRST, TIED], "2026-01-01T00:00:03.123000+00:00"),
    ([TIED, FIRST], "2026-01-01T00:00:03.123000+00:00"),
    ([FIRST, "2026-01-01T00:00:01Z"], None),
])
def test_multiple_failures_preserve_legacy_winner_and_refuse_a_different_core_winner(
    times: list[str], expected: str | None,
) -> None:
    value = result(history(times, failures={0, 1}, order=(1, 0)), ParallelProbe)
    assert value["failure"] == ("group.0", "failure-0")
    assert value["clock"] == expected


@pytest.mark.parametrize("order", list(permutations(range(3))))
def test_nested_all_activity_paths_follow_authored_order(order: tuple[int, ...]) -> None:
    class Nested(ParallelProbe):
        size = 3
        nested = True

    value = result(history([FIRST, TIED, "2026-01-01T00:00:02Z"], order=order, nested=True), Nested)
    assert value["values"] == ["value-0", ["value-1", "value-2"]]
    assert value["clock"] == FIRST


@pytest.mark.parametrize("bad", [None, False, 17, "", "bad", "2026-02-30T00:00:00Z", "2026-01-01T24:00:00Z",
                                 "2026-01-01T00:00:00.1234567Z", "9999-01-01T00:00:00Z"])
def test_missing_invalid_or_unproved_required_time_does_not_borrow_a_sibling(bad: Any) -> None:
    assert result(history([FIRST, bad]), ParallelProbe)["clock"] is None


def test_literal_core_time_precedence_and_actual_created_at_fallback_are_family_specific() -> None:
    rows = history()
    rows[-2]["payload"]["reused_recorded_at"] = "2099-01-01T00:00:00Z"
    rows[-2]["created_at"] = "2098-01-01T00:00:00Z"
    assert result(rows, ParallelProbe)["clock"] == FIRST
    rows[-2]["recorded_at"] = None
    rows[-2]["created_at"] = FIRST
    assert result(rows, ParallelProbe)["clock"] == FIRST
    rows[-2]["recorded_at"] = "bad"
    assert result(rows, ParallelProbe)["clock"] is None


@pytest.mark.parametrize("query", ["clock", "body-clock"])
@pytest.mark.parametrize("failed", [False, True])
def test_query_body_and_callback_cannot_borrow_the_executor_group_aggregate(query: str, failed: bool) -> None:
    rows = history(failures={0} if failed else set())
    assert result(rows, ParallelProbe)["clock"] is not None
    assert query_state(ParallelProbe, rows, [], query, payload_codec="avro") is None


@pytest.mark.parametrize("field,value", [("activity_execution_id", "foreign"), ("activity_execution_id", None),
    ("sequence", "1"), ("sequence", True), ("workflow_sequence", "1"),
    ("parallel_group_base_sequence", "1"), ("parallel_group_index", True)])
def test_unproved_opening_keeps_the_legacy_outcome_without_certifying_time(field: str, value: Any) -> None:
    rows = history()
    rows[1]["payload"][field] = value
    actual = result(rows, ParallelProbe)
    assert actual["values"] == ["value-0", "value-1"]
    assert actual["clock"] is None


def test_duplicate_numeric_claim_and_unindexed_same_execution_cannot_evade_group_qualification() -> None:
    for sequence in ("1", "not-a-counter"):
        rows = history()
        duplicate = deepcopy(rows[1])
        duplicate["payload"]["sequence"] = sequence
        rows.insert(1, duplicate)
        assert result(rows, ParallelProbe)["clock"] is None


def test_absent_lifecycle_and_duplicate_starts_preserve_values_without_proving_time() -> None:
    rows = history()
    rows.pop(1)
    assert result(rows, ParallelProbe)["clock"] is None
    rows = history()
    started = deepcopy(rows[1])
    started["event_type"] = "ActivityStarted"
    rows[2:2] = [started, deepcopy(started)]
    assert result(rows, ParallelProbe)["clock"] is None


def test_original_activity_type_mismatch_remains_a_replay_error() -> None:
    rows = history()
    for row in rows[1:]:
        if row["payload"]["sequence"] == 1:
            row["payload"]["activity_type"] = "different"
    with pytest.raises(NonDeterministicReplayError):
        result(rows, ParallelProbe)


@pytest.mark.parametrize("kind", ["ActivityFailed", "ActivityTimedOut", "ActivityCancelled"])
def test_each_persisted_failure_kind_keeps_its_exception_and_group_reconstruction(kind: str) -> None:
    rows = history(failures={0})
    rows[-2]["event_type"] = kind
    value = result(rows, ParallelProbe)
    assert value["failure"] == ("group.0", "failure-0")
    assert value["clock"] == "2026-01-01T00:00:03.123000+00:00"


def test_failed_member_needs_its_time_but_does_not_require_a_successful_sibling_time() -> None:
    value = result(history([FIRST, None], failures={0}), ParallelProbe)
    assert value["clock"] == "2026-01-01T00:00:03.123000+00:00"
    assert result(history([FIRST, None], failures={0, 1}), ParallelProbe)["clock"] is None


def test_none_result_remains_a_success_and_does_not_become_a_selected_failure() -> None:
    rows = history()
    rows[-2]["payload"]["result"] = serializer.envelope(None)
    value = result(rows, ParallelProbe)
    assert value["values"] == [None, "value-1"]
    assert value["failure"] is None
    assert value["clock"] == FIRST


@pytest.mark.parametrize("empty_path", [False, True])
def test_complete_flat_metadata_also_binds_without_a_nested_path(empty_path: bool) -> None:
    rows = history()
    for row in rows[1:]:
        if empty_path:
            row["payload"]["parallel_group_path"] = []
        else:
            row["payload"].pop("parallel_group_path")
    assert result(rows, ParallelProbe)["clock"] == FIRST


def test_unrepresented_or_conflicting_time_alias_stays_unavailable() -> None:
    rows = history()
    rows[-1]["timestamp"] = "2099-01-01T00:00:00Z"
    assert result(rows, ParallelProbe)["clock"] is None
    rows[-1]["timestamp"] = rows[-1].pop("recorded_at")
    assert result(rows, ParallelProbe)["clock"] is None  # No model-field carrier to qualify.


def test_empty_group_preserves_a_proved_boundary_and_a_later_group_can_move_backwards() -> None:
    class Multiple:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.schedule_activity("group.0", [])
            before = clock_text(ctx)
            yield []
            after_empty = clock_text(ctx)
            values = yield [ctx.schedule_activity("group.0", []), ctx.schedule_activity("group.1", [])]
            return before, after_empty, clock_text(ctx), values

    rows = history(["2026-01-01T00:00:08Z"])
    for row in rows[1:]:
        for field in _metadata_fields():
            row["payload"].pop(field)
    later = history()
    for row in later[1:]:
        row["payload"]["sequence"] += 1
        row["payload"]["activity_execution_id"] += "-later"
        path = group_path(2, 2, row["payload"]["sequence"] - 2)
        row["payload"].update(**path[-1], parallel_group_path=path)
    rows.extend(later[1:])
    assert result(rows, Multiple) == (
        "2026-01-01T00:00:08.000000+00:00", "2026-01-01T00:00:08.000000+00:00", FIRST,
        ["value-0", "value-1"],
    )


def _metadata_fields() -> tuple[str, ...]:
    return ("parallel_group_id", "parallel_group_kind", "parallel_group_base_sequence", "parallel_group_size",
            "parallel_group_index", "parallel_group_path")


def test_pending_and_unmarked_legacy_groups_do_not_receive_a_proved_clock() -> None:
    rows = history()
    rows.pop()
    pending = replay(ParallelProbe, rows, [], payload_codec="avro")
    try:
        assert pending.commands and all(getattr(command, "activity_type", None) for command in pending.commands)
    finally:
        pending.close()
    rows = history()
    for row in rows[1:]:
        for field in _metadata_fields():
            row["payload"].pop(field)
    assert result(rows, ParallelProbe)["clock"] is None


def test_mixed_and_recorded_local_groups_remain_explicitly_unqualified() -> None:
    class Mixed:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            values = yield [ctx.schedule_activity("group.0", []), ctx.start_timer(1)]
            return values, clock_text(ctx)

    rows = history()
    for row in rows[1:]:
        payload = row["payload"]
        payload["parallel_group_id"] = "parallel-calls:1:2"
        payload["parallel_group_kind"] = "mixed"
        for entry in payload["parallel_group_path"]:
            entry.update(parallel_group_id="parallel-calls:1:2", parallel_group_kind="mixed")
        if payload["sequence"] == 2:
            row["event_type"] = "TimerScheduled" if row["event_type"] == "ActivityScheduled" else "TimerFired"
            for field in ("activity_type", "activity_execution_id", "result"):
                payload.pop(field, None)
            payload.update(timer_kind="durable_timer", timer_id="group-timer", delay_seconds=1)
    assert result(rows, Mixed) == (["value-0", None], None)

    class Local:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            values = yield [ctx.local_activity("prepared.first", []), ctx.local_activity("prepared.second", [])]
            return values, clock_text(ctx)

    fixture = json.loads((REFERENCE.parent / "replay_regressions/prepared-local-group-cold-results.json").read_text())
    assert result(fixture["history"], Local) == (["fast-value", "fast-value"], None)


def test_typed_adapter_failure_cannot_claim_a_successful_group_clock() -> None:
    class Typed:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield [ctx.schedule_activity("group.0", [], result_type=int), ctx.schedule_activity("group.1", [])]
            except TypedValueError:
                return clock_text(ctx)
            raise AssertionError("the original typed-value error was not delivered")

    assert result(history(), Typed) is None


def test_a_later_independent_scalar_boundary_recovers_query_authority_after_a_group() -> None:
    class Later(ParallelProbe):
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            self.ctx = ctx
            yield [ctx.schedule_activity("group.0", []), ctx.schedule_activity("group.1", [])]
            group_time = clock_text(ctx)
            yield ctx.schedule_activity("after", [])
            self.time = clock_text(ctx)
            return group_time, self.time

    rows = history()
    later = {"sequence": 3, "activity_execution_id": "after-group", "activity_type": "after"}
    rows.extend([
        {"event_type": "ActivityScheduled", "payload": dict(later)},
        {"event_type": "ActivityCompleted", "recorded_at": "2026-01-01T00:00:02Z",
         "payload": {**later, "result": serializer.envelope("after")}},
    ])
    assert result(rows, Later) == (FIRST, "2026-01-01T00:00:02.000000+00:00")
    assert query_state(Later, rows, [], "clock", payload_codec="avro") == "2026-01-01T00:00:02.000000+00:00"


def test_optional_started_events_do_not_contribute_their_times() -> None:
    rows = history()
    starts = []
    for row in rows[1:3]:
        started = deepcopy(row)
        started.update(event_type="ActivityStarted", recorded_at="2098-01-01T00:00:00Z")
        starts.append(started)
    rows[3:3] = starts
    assert result(rows, ParallelProbe)["clock"] == FIRST


@pytest.mark.parametrize("field,value", [("parallel_group_mode", "unknown"),
    ("selection_member_key", "other"), ("parallel_group_base_sequence", "1")])
def test_unqualified_mode_selection_alias_and_raw_path_counter_cannot_be_overwritten_by_a_later_valid_row(
    field: str, value: Any,
) -> None:
    rows = history()
    if field == "parallel_group_base_sequence":
        rows[1]["payload"]["parallel_group_path"][0][field] = value
    else:
        rows[1]["payload"][field] = value
    observed = result(rows, ParallelProbe)
    assert observed["values"] == ["value-0", "value-1"]
    assert observed["clock"] is None
