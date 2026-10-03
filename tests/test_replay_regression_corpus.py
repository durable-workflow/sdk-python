from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import Replayer, Worker, serializer, workflow
from durable_workflow.client import Client, WorkflowStreamAppendItem
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled, WorkflowPayloadDecodeError
from durable_workflow.workflow import (
    LocalActivityExecutionAborted,
    WorkflowContext,
    commands_to_server_commands,
    query_state,
)
from tests.test_golden_history_replay import (
    GoldenSagaCompensationWorkflow,
    GoldenSignalWaitWorkflow,
    GoldenSingleActivityWorkflow,
    GoldenTimeoutWaitWorkflow,
    GoldenVersionMarkerWorkflow,
)
from tests.test_update_signal_condition_replay import (
    PostConditionReceiversWorkflow,
    UpdateSignalConditionTimerWorkflow,
)

FIXTURE_SCHEMA = "durable-workflow.replay-regression/v1"
FIXTURE_DIR = Path(__file__).parent / "fixtures" / "replay_regressions"


@workflow.defn(name="tests.replay.parallel-metadata-producer")
class ParallelMetadataProducerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (
            yield [
                ctx.schedule_activity("golden.activity-one", []),
                ctx.start_child_workflow("golden.child", []),
                ctx.start_timer(1),
            ]
        )


@workflow.defn(name="tests.replay.parallel-result-binding")
class ParallelResultBindingWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        results = yield [
            ctx.schedule_activity("position-first", []),
            ctx.schedule_activity("position-second", []),
        ]
        return {"results": results}


@workflow.defn(name="tests.replay.cold-replacement-satisfied-condition")
class ColdReplacementSatisfiedConditionWorkflow:
    def __init__(self) -> None:
        self.approved_by: str | None = None

    @workflow.signal("approve")
    def approve(self, approver: str) -> None:
        self.approved_by = approver

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        before_condition = yield ctx.schedule_activity("before-condition", [])
        yield ctx.wait_condition(
            lambda: self.approved_by is not None,
            key="approved",
        )
        after_condition = yield ctx.schedule_activity("after-condition", [])
        return {
            "status": "completed",
            "before_condition": before_condition,
            "approved_by": self.approved_by,
            "after_condition": after_condition,
        }


@workflow.defn(name="tests.replay.selection-await-marker")
class SelectionAwaitMarkerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected = yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        return {"winner": selected.key, "winner_value": selected.result()}


@workflow.defn(name="tests.replay.selection-missing-nested-opening")
class SelectionMissingNestedOpeningWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected = yield ctx.select(
            {
                "nested": [
                    ctx.schedule_activity("nested-first", []),
                    ctx.schedule_activity("nested-second", []),
                ],
                "winner": ctx.schedule_activity("winner", []),
            }
        )
        yield ctx.schedule_activity("post-winner", [])
        return selected.result()


@workflow.defn(name="tests.replay.selection-cancellation-query")
class SelectionCancellationQueryWorkflow:
    def __init__(self) -> None:
        self.state = "before-cancel"

    @workflow.query("state")
    def current_state(self) -> str:
        return self.state

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected = yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        yield selected.handles["slow"].cancel()
        self.state = "after-cancel"
        return self.state


@workflow.defn(name="tests.replay.nested-parallel-path")
class NestedParallelPathWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (
            yield [
                ctx.schedule_activity("path-first", []),
                [
                    ctx.start_child_workflow("path-child", []),
                    ctx.start_timer(1),
                ],
            ]
        )


@workflow.defn(name="tests.replay.workflow-stream-author")
class WorkflowStreamAuthorWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.append_workflow_stream(
            "output",
            [WorkflowStreamAppendItem(payload={"message": "once"})],
        )
        yield ctx.close_workflow_stream("output")
        return "done"


@workflow.defn(name="tests.replay.recorded-side-effect")
class RecordedSideEffectWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        def unexpected_execution() -> int:
            raise AssertionError("recorded side effect callable ran during replay")

        return (yield ctx.side_effect(unexpected_execution))


@workflow.defn(name="tests.replay.paged-recorded-side-effects")
class PagedRecordedSideEffectsWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        def unexpected_execution() -> int:
            raise AssertionError("recorded side effect callable ran during replay")

        first = yield ctx.side_effect(unexpected_execution)
        second = yield ctx.side_effect(unexpected_execution)
        return first + second


@workflow.defn(name="tests.replay.message-stream-consumer")
class MessageStreamConsumerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        messages = yield from ctx.message_stream("orders").receive(2)
        return [
            {
                "message_id": message.message_id,
                "position": message.position,
                "arguments": message.arguments,
            }
            for message in messages
        ]


@workflow.defn(name="tests.replay.workflow-memo-author")
class WorkflowMemoAuthorWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.upsert_memo(
            {
                "binary": b"same",
                "double": 7.0,
                "invalid_binary": b"\xff\x00",
                "long": 7,
                "nested": {"beta": 2, "alpha": 1},
                "text": "same",
            }
        )
        return "memo-replayed"


@workflow.defn(name="tests.replay.yielded-continue-after-metadata")
class YieldedContinueAfterMetadataWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.upsert_search_attributes({"stage": "continued"})
        yield ctx.upsert_memo({"added": "from-upsert", "overwritten": "after"})
        yield ctx.continue_as_new("successor")


@workflow.defn(name="tests.replay.local-activity-cold-result")
class LocalActivityColdResultWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield ctx.local_activity("golden.local", []))


@workflow.defn(name="tests.replay.prepared-local-cold-results")
class PreparedLocalColdResultsWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        first = yield ctx.local_activity("prepared.first", [])
        second = yield ctx.local_activity("prepared.second", [])
        return [first, second]


@workflow.defn(name="tests.replay.prepared-local-group-cold-results")
class PreparedLocalGroupColdResultsWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield [ctx.local_activity("prepared.first", []), ctx.local_activity("prepared.second", [])])


@workflow.defn(name="tests.replay.cooperative-reopened-condition-cleanup")
class CooperativeReopenedConditionCleanupWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.wait_condition(lambda: False, key="forward-wait")
        except WorkflowCancelled as exc:
            with ctx.cancellation_shield():
                yield ctx.start_timer(1)
            return exc.request_id
        return "not cancelled"


@workflow.defn(name="tests.replay.child-policy-author")
class ChildPolicyAuthorWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield ctx.start_child_workflow("child", []))


WORKFLOWS = [
    ChildPolicyAuthorWorkflow,
    ColdReplacementSatisfiedConditionWorkflow,
    CooperativeReopenedConditionCleanupWorkflow,
    GoldenSagaCompensationWorkflow,
    GoldenSignalWaitWorkflow,
    GoldenSingleActivityWorkflow,
    GoldenTimeoutWaitWorkflow,
    GoldenVersionMarkerWorkflow,
    LocalActivityColdResultWorkflow,
    PreparedLocalColdResultsWorkflow,
    PreparedLocalGroupColdResultsWorkflow,
    MessageStreamConsumerWorkflow,
    NestedParallelPathWorkflow,
    ParallelMetadataProducerWorkflow,
    ParallelResultBindingWorkflow,
    PagedRecordedSideEffectsWorkflow,
    PostConditionReceiversWorkflow,
    RecordedSideEffectWorkflow,
    SelectionAwaitMarkerWorkflow,
    SelectionCancellationQueryWorkflow,
    SelectionMissingNestedOpeningWorkflow,
    UpdateSignalConditionTimerWorkflow,
    WorkflowStreamAuthorWorkflow,
    WorkflowMemoAuthorWorkflow,
    YieldedContinueAfterMetadataWorkflow,
]
WORKFLOW_TYPES = {str(getattr(workflow, "__workflow_name__", workflow.__name__)): workflow for workflow in WORKFLOWS}


def _fixture_paths() -> list[Path]:
    return sorted(FIXTURE_DIR.glob("*.json"))


def _decode_envelopes(value: Any) -> Any:
    if isinstance(value, Mapping):
        if "codec" in value and "blob" in value:
            return _decode_envelopes(serializer.decode_envelope(dict(value)))
        return {str(key): _decode_envelopes(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode_envelopes(item) for item in value]
    return value


def _command_documents(commands: Sequence[Any]) -> list[dict[str, Any]]:
    server_commands = commands_to_server_commands(
        commands,
        "regression-corpus",
        payload_codec=serializer.AVRO_CODEC,
        size_warning=None,
    )
    documents: list[dict[str, Any]] = []
    for command, server_command in zip(commands, server_commands, strict=True):
        normalized = _decode_envelopes(server_command)
        assert isinstance(normalized, dict)
        normalized["command_type"] = type(command).__name__
        documents.append(normalized)
    return documents


def _assert_matches(expected: Any, actual: Any, context: str) -> None:
    if isinstance(expected, Mapping):
        assert isinstance(actual, Mapping), f"{context} expected an object, observed {type(actual).__name__}"
        for key, value in expected.items():
            assert key in actual, f"{context} is missing {key!r}"
            _assert_matches(value, actual[key], f"{context}.{key}")
        return

    if isinstance(expected, Sequence) and not isinstance(expected, str | bytes):
        assert isinstance(actual, Sequence) and not isinstance(actual, str | bytes), (
            f"{context} expected an array, observed {type(actual).__name__}"
        )
        assert len(expected) == len(actual), f"{context} expected {len(expected)} entries, observed {len(actual)}"
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual, strict=True)):
            _assert_matches(expected_item, actual_item, f"{context}[{index}]")
        return

    assert actual == expected, f"{context} expected {expected!r}, observed {actual!r}"


def _execute_fixture(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    assert fixture.get("fixture_schema") == FIXTURE_SCHEMA
    assert "python" in fixture.get("bindings", [])

    workflow = fixture.get("workflow")
    assert isinstance(workflow, dict)
    workflow_type = workflow.get("type")
    assert isinstance(workflow_type, str) and workflow_type
    assert workflow_type in WORKFLOW_TYPES, (
        f"replay fixture workflow {workflow_type!r} has no Python implementation; "
        "register its reproducer workflow in WORKFLOWS"
    )
    start_input = workflow.get("input")
    assert start_input is None or isinstance(start_input, list)
    payload_codec = workflow.get("payload_codec")

    history = fixture.get("history", [])
    assert isinstance(history, list)
    if "history" in fixture:
        assert history

    query = workflow.get("query")
    query_result: Any = None
    if query is None:
        outcome = Replayer(workflows=[WORKFLOW_TYPES[workflow_type]]).replay(
            history,
            start_input,
            workflow_type=workflow_type,
            payload_codec=("json" if payload_codec is None else payload_codec),
        )
        commands = _command_documents(outcome.commands)
        message_stream_cursors = outcome.message_stream_cursors
        message_stream_waits = outcome.message_stream_waits
    else:
        assert isinstance(query, dict)
        query_name = query.get("name")
        query_args = query.get("arguments", [])
        assert isinstance(query_name, str) and query_name
        assert isinstance(query_args, list)
        query_result = query_state(
            WORKFLOW_TYPES[workflow_type],
            history,
            [] if start_input is None else start_input,
            query_name,
            query_args,
            payload_codec=("json" if payload_codec is None else payload_codec),
        )
        commands = []
        message_stream_cursors = []
        message_stream_waits = []

    declared_commands = fixture.get("command_sequence")
    if declared_commands is not None:
        _assert_matches(
            declared_commands,
            commands,
            f"{fixture.get('id', '<unnamed>')}.command_sequence",
        )

    expected = fixture.get("expected")
    assert isinstance(expected, dict) and expected
    observed: dict[str, Any] = {
        "command_sequence": commands,
        "message_stream_cursors": message_stream_cursors,
        "message_stream_waits": message_stream_waits,
    }
    if query is not None:
        observed["query_result"] = query_result
    if len(commands) == 1:
        observed.update(commands[0])
    _assert_matches(expected, observed, f"{fixture.get('id', '<unnamed>')}.expected")
    return commands


@pytest.mark.parametrize(
    "path",
    _fixture_paths(),
    ids=lambda path: path.name,
)
def test_checked_in_replay_regression_corpus_uses_official_replayer(
    path: Path,
) -> None:
    fixture = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(fixture, dict)

    workflow = fixture.get("workflow")
    assert isinstance(workflow, dict)
    expected = fixture.get("expected")
    assert isinstance(expected, dict) and expected
    expected_error = expected.get("error")
    expected_replay_error = fixture.get("expected_replay_error")
    if isinstance(expected_replay_error, dict):
        message = expected_replay_error.get("message_contains")
        assert isinstance(message, str) and message
        assert expected.get("command_sequence") == []
        if expected_replay_error.get("type") == "LocalActivityExecutionAborted":
            with pytest.raises(LocalActivityExecutionAborted, match=message):
                _execute_fixture(fixture)
            return
        assert expected_replay_error.get("type") == "NonDeterministicReplayError"
        workflow_sequence = expected_replay_error.get("workflow_sequence")
        assert isinstance(workflow_sequence, int)
        with pytest.raises(NonDeterministicReplayError, match=message) as captured:
            _execute_fixture(fixture)
        assert captured.value.workflow_sequence == workflow_sequence
        return
    if isinstance(expected_error, str):
        with pytest.raises(
            (ValueError, WorkflowPayloadDecodeError),
            match=expected_error,
        ):
            _execute_fixture(fixture)

        return
    if workflow.get("payload_codec") != serializer.AVRO_CODEC:
        with pytest.raises(WorkflowPayloadDecodeError, match="unsupported_payload_codec"):
            _execute_fixture(fixture)

        return

    _execute_fixture(fixture)


@pytest.mark.asyncio
async def test_worker_replays_recorded_side_effects_across_history_pages() -> None:
    fixture = json.loads((FIXTURE_DIR / "paged-recorded-side-effects.json").read_text(encoding="utf-8"))
    history = fixture["history"]
    client = AsyncMock(spec=Client)
    client.workflow_task_history.return_value = {
        "history_events": history[1:],
        "next_history_page_token": None,
    }
    worker = Worker(client, task_queue="paged-history", workflows=[PagedRecordedSideEffectsWorkflow])

    commands = await worker._run_workflow_task_core({
        "task_id": "task-paged-side-effects",
        "workflow_id": "workflow-paged-side-effects",
        "run_id": "run-paged-side-effects",
        "workflow_type": fixture["workflow"]["type"],
        "workflow_task_attempt": 1,
        "history_events": history[:1],
        "next_history_page_token": "second-page",
        "arguments": serializer.encode([], codec="avro"),
        "payload_codec": "avro",
    })

    client.workflow_task_history.assert_awaited_once_with(
        task_id="task-paged-side-effects",
        next_history_page_token="second-page",
        lease_owner=worker.worker_id,
        workflow_task_attempt=1,
    )
    client.complete_workflow_task.assert_awaited_once()
    assert commands is not None
    assert [command["type"] for command in commands] == ["complete_workflow"]
    assert serializer.decode_envelope(commands[0]["result"]) == 198


@pytest.mark.parametrize(
    "fixture",
    [
        {
            "fixture_schema": FIXTURE_SCHEMA,
            "id": "history-format-contract",
            "protocol_version": "1.0",
            "bindings": ["python"],
            "workflow": {
                "type": "golden.single-activity",
                "input": ["Ada"],
                "payload_codec": serializer.AVRO_CODEC,
            },
            "history": [
                {
                    "event_type": "ActivityCompleted",
                    "payload": {"result": serializer.encode("hello Ada", codec=serializer.AVRO_CODEC)},
                }
            ],
            "expected": {"command_sequence": [{"type": "complete_workflow", "result": "hello Ada"}]},
        },
        {
            "fixture_schema": FIXTURE_SCHEMA,
            "id": "command-sequence-format-contract",
            "protocol_version": "1.0",
            "bindings": ["python"],
            "workflow": {
                "type": "golden.single-activity",
                "input": ["Ada"],
                "payload_codec": serializer.AVRO_CODEC,
            },
            "command_sequence": [
                {
                    "type": "schedule_activity",
                    "activity_type": "golden.greet",
                    "arguments": ["Ada"],
                }
            ],
            "expected": {"command_type": "ScheduleActivity"},
        },
    ],
    ids=lambda fixture: str(fixture["id"]),
)
def test_replay_regression_formats_execute_through_official_replayer(
    fixture: dict[str, Any],
) -> None:
    _execute_fixture(fixture)


def test_impossible_event_and_command_fixture_is_rejected() -> None:
    fixture = {
        "fixture_schema": FIXTURE_SCHEMA,
        "id": "impossible-event-command",
        "protocol_version": "1.0",
        "bindings": ["python"],
        "workflow": {
            "type": "golden.single-activity",
            "input": ["Ada"],
            "payload_codec": serializer.AVRO_CODEC,
        },
        "history": [
            {
                "event_type": "ImpossibleEvent",
                "payload": {},
            }
        ],
        "command_sequence": [{"type": "impossible_command"}],
        "expected": {
            "command_sequence": [{"type": "impossible_command"}],
        },
    }

    with pytest.raises(AssertionError, match=r"command_sequence\[0\]\.type"):
        _execute_fixture(fixture)
