"""Opt-in typed values at the activity boundary (#184, slice 1)."""

from __future__ import annotations

import base64
import json
import pickle
from dataclasses import asdict, dataclass, field
from enum import Enum, IntEnum
from pathlib import Path
from typing import Any, Literal, Optional
from unittest.mock import AsyncMock

import pytest

from durable_workflow import NonRetryableError, TypedValueError, activity, serializer, workflow
from durable_workflow._typed_values import from_payload_value, to_payload_value, validate_annotation
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, WorkflowContext, replay

try:
    from enum import StrEnum
except ImportError:  # Python 3.10
    class StrEnum(str, Enum):  # type: ignore[no-redef]
        pass

FIXTURES = Path(__file__).parent / "fixtures"


class Status(Enum):
    OPEN = "open"
    SHIPPED = "shipped"


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


class Channel(StrEnum):
    WEB = "web"
    STORE = "store"


@dataclass
class Item:
    sku: str
    qty: int = 1


@dataclass
class Shipment:
    order_id: str
    status: Status
    items: list[Item]
    priority: Priority = Priority.LOW
    channel: Channel = Channel.WEB
    note: Optional[str] = None  # noqa: UP045 - typing.Optional is part of the contract
    weights: dict[str, float] = field(default_factory=dict)


SHIPMENT = Shipment("o-1", Status.SHIPPED, [Item("a", 2), Item("b")], Priority.HIGH, Channel.STORE, "gift",
                    {"a": 1.5})
NEUTRAL = {
    "order_id": "o-1", "status": "shipped", "items": [{"sku": "a", "qty": 2}, {"sku": "b", "qty": 1}],
    "priority": 2, "channel": "store", "note": "gift", "weights": {"a": 1.5},
}


def by_hand(value: Shipment) -> dict[str, Any]:
    """What a user passes today: dataclasses.asdict() with each Enum's .value."""
    plain = asdict(value)
    plain.update(status=value.status.value, priority=value.priority.value, channel=value.channel.value)
    return plain


# Conversion.
def test_a_typed_value_round_trips_through_the_neutral_wire_format() -> None:
    assert to_payload_value(SHIPMENT) == NEUTRAL
    decoded = from_payload_value(serializer.decode(serializer.encode(to_payload_value(SHIPMENT)), codec="avro"), Shipment)
    assert decoded == SHIPMENT
    assert type(decoded.priority) is Priority and type(decoded.channel) is Channel


def test_automatic_encoding_gives_the_bytes_of_asdict_and_value() -> None:
    assert serializer.encode(to_payload_value(SHIPMENT)) == serializer.encode(by_hand(SHIPMENT))


def test_a_newer_payload_with_an_extra_field_decodes() -> None:
    newer = {**NEUTRAL, "added_in_v2": True, "items": [{"sku": "a", "qty": 2, "weight_kg": 3.0}]}
    decoded = from_payload_value(newer, Shipment)
    assert decoded.items == [Item("a", 2)]
    assert not hasattr(decoded, "added_in_v2")


def test_an_older_payload_missing_optional_fields_takes_defaults() -> None:
    older = {"order_id": "o-1", "status": "open", "items": [{"sku": "a"}]}
    assert from_payload_value(older, Shipment) == Shipment("o-1", Status.OPEN, [Item("a")])


def test_optional_accepts_null_and_scalars_keep_their_kinds() -> None:
    assert from_payload_value({**NEUTRAL, "note": None}, Shipment).note is None
    assert from_payload_value(3, float) == 3.0 and type(from_payload_value(3, float)) is float
    assert from_payload_value(None, Optional[Item]) is None  # noqa: UP045
    assert from_payload_value([{"sku": "x"}], list[Item]) == [Item("x")]
    assert from_payload_value({"k": "web"}, dict[str, Channel]) == {"k": Channel.WEB}


@pytest.mark.parametrize(("payload", "path", "expected", "detail"), [
    ({**NEUTRAL, "items": [{"qty": 2}]}, "shipment.items[0].sku", "str", "the field is missing"),
    ({k: v for k, v in NEUTRAL.items() if k != "status"}, "shipment.status", "Status", "the field is missing"),
    ({**NEUTRAL, "status": "lost-in-transit"}, "shipment.status", "Status", "the string is not a member"),
    ({**NEUTRAL, "priority": True}, "shipment.priority", "Priority", "the boolean is not a member"),
    ({**NEUTRAL, "items": "secret-sku"}, "shipment.items", "list[", "got string"),
    ({**NEUTRAL, "weights": {"a": "heavy"}}, "shipment.weights['a']", "float", "got string"),
    ({**NEUTRAL, "order_id": None}, "shipment.order_id", "str", "got null"),
    ({**NEUTRAL, "items": [{"sku": "a", "qty": True}]}, "shipment.items[0].qty", "int", "got boolean"),
    ("secret-order", "shipment", "Shipment", "got string"),
])
def test_a_value_that_cannot_be_converted_names_the_path_never_the_value(
    payload: Any, path: str, expected: str, detail: str,
) -> None:
    with pytest.raises(TypedValueError) as raised:
        from_payload_value(payload, Shipment, "shipment")
    assert raised.value.path == path
    assert raised.value.expected.startswith(expected)
    assert raised.value.detail == detail
    for secret in ("secret", "lost-in-transit", "heavy"):
        assert secret not in str(raised.value)


@dataclass
class Nested:
    inner: Item


@pytest.mark.parametrize("annotation", [
    int | str, Optional[int | str], dict[int, str], Literal["a"], tuple[int, ...], set[str], Path,  # noqa: UP045
])
def test_ambiguous_or_unsupported_annotations_are_refused_up_front(annotation: Any) -> None:
    with pytest.raises(TypeError):
        validate_annotation(annotation, "value")


def test_an_enum_with_non_scalar_values_is_refused() -> None:
    class Pair(Enum):
        A = (1, 2)

    with pytest.raises(TypeError, match="str, int or float values"):
        validate_annotation(Pair, "value")


def test_supported_annotations_validate() -> None:
    for annotation in (Shipment, Nested, list[Item], dict[str, Item], Optional[Shipment], Any, int, bytes):  # noqa: UP045
        validate_annotation(annotation, "value")


# Typed activities.
@activity.defn(name="tests.typed.ship", typed=True)
def ship(shipment: Shipment, channel: Channel, *extra: Item) -> Shipment:
    return Shipment(shipment.order_id, Status.SHIPPED, shipment.items + list(extra), channel=channel)


@activity.defn(name="tests.typed.untyped")
def untyped(value: Any) -> Shipment:
    return SHIPMENT


def test_a_typed_activity_decodes_its_arguments_and_returns_the_neutral_value() -> None:
    result = ship({"order_id": "o-9", "status": "open", "items": []}, "store", {"sku": "z"})
    assert result == {
        "order_id": "o-9", "status": "shipped", "items": [{"sku": "z", "qty": 1}], "priority": 1,
        "channel": "store", "note": None, "weights": {},
    }


def test_an_untyped_activity_keeps_receiving_plain_values() -> None:
    assert untyped({"order_id": "o-1"}) is SHIPMENT


def test_a_typed_activity_argument_mismatch_is_non_retryable() -> None:
    with pytest.raises(TypedValueError) as raised:
        ship({"order_id": "o-9", "status": "open"}, "store")
    assert raised.value.path == "shipment.items"
    assert isinstance(raised.value, NonRetryableError) and isinstance(raised.value, TypeError)
    with pytest.raises(TypedValueError, match=r"extra\[0\]\.sku"):
        ship({"order_id": "o-9", "status": "open", "items": []}, "web", {"qty": 1})


def test_a_typed_activity_with_an_unsupported_annotation_fails_when_declared() -> None:
    with pytest.raises(TypeError, match="does not support|do not support"):
        @activity.defn(name="tests.typed.bad", typed=True)
        def bad(value: set[str]) -> None:
            return None


def test_a_typed_activity_and_its_error_pickle_by_reference() -> None:
    # Supervised callbacks pickle the activity callable by reference.
    assert pickle.loads(pickle.dumps(ship)) is ship
    error = TypedValueError("shipment.items", "list[Item]", "got string")
    restored = pickle.loads(pickle.dumps(error))
    assert (restored.path, restored.expected, restored.detail, str(restored)) == (
        error.path, error.expected, error.detail, str(error))


def _worker_and_client(fn: Any) -> tuple[Any, AsyncMock]:
    from durable_workflow.client import Client
    from durable_workflow.worker import Worker

    client = AsyncMock(spec=Client)
    client.complete_activity_task = AsyncMock(return_value={"outcome": "completed"})
    client.fail_activity_task = AsyncMock(return_value={"outcome": "failed"})
    return Worker(client, task_queue="q1", workflows=[], activities=[fn]), client


def _activity_task(activity_type: str, arguments: list[Any]) -> dict[str, Any]:
    return {"task_id": "t1", "activity_attempt_id": "a1", "activity_type": activity_type,
            "arguments": serializer.encode(arguments, codec="avro"), "payload_codec": "avro"}


@pytest.mark.asyncio
async def test_a_worker_fails_a_typed_activity_mismatch_without_retries() -> None:
    worker, client = _worker_and_client(ship)
    status = await worker._run_activity_task(_activity_task("tests.typed.ship", [{"order_id": "o-9", "status": "open"},
                                                                                 "web"]))
    assert status == "failed_non_retryable"
    failure = client.fail_activity_task.await_args.kwargs
    assert failure["non_retryable"] is True and failure["failure_type"] == "TypedValueError"
    assert failure["message"] == "shipment.items: expected list[Item]; the field is missing"
    client.complete_activity_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_worker_completes_a_dataclass_result_as_its_neutral_value() -> None:
    worker, client = _worker_and_client(untyped)
    assert await worker._run_activity_task(_activity_task("tests.typed.untyped", [None])) == "completed"
    assert client.complete_activity_task.await_args.kwargs["result"] == NEUTRAL


# Workflow side.
def test_scheduled_dataclass_arguments_are_sent_as_their_neutral_value() -> None:
    command = ScheduleActivity("tests.typed.ship", [SHIPMENT, Channel.STORE, Status.OPEN])
    assert command.arguments == [NEUTRAL, "store", "open"]
    server = command.to_server_command("queue")
    assert server["arguments"] == serializer.envelope([by_hand(SHIPMENT), "store", "open"])


def test_result_type_is_local_only_and_validated() -> None:
    plain = ScheduleActivity("x", [])
    typed = ScheduleActivity("x", [], result_type=Shipment)
    assert plain == typed
    assert plain.to_server_command("queue") == typed.to_server_command("queue")
    assert "result_type" not in json.dumps(typed.to_server_command("queue"))
    with pytest.raises(TypeError):
        ScheduleActivity("x", [], result_type=int | str)


# The interop codec fixture pins the neutral bytes PHP and Rust read.
INTEROP = FIXTURES / "codec_regressions" / "typed-dataclass-enum-neutral-map.json"


@dataclass
class InteropItem:
    sku: str
    qty: int


@dataclass
class InteropOrder:
    order_id: str
    status: Status
    priority: Priority
    items: list[InteropItem]
    note: Optional[str] = None  # noqa: UP045


INTEROP_ORDER = InteropOrder("o-1", Status.SHIPPED, Priority.HIGH, [InteropItem("a", 2)])


def test_interop_fixture_pins_the_bytes_of_asdict_and_value() -> None:
    wire = json.loads(INTEROP.read_text(encoding="utf-8"))["framing"]["wire_base64"]
    by_hand_value = {**asdict(INTEROP_ORDER), "status": "shipped", "priority": 2}
    assert serializer.encode(to_payload_value(INTEROP_ORDER)) == wire
    assert serializer.encode(by_hand_value) == wire
    assert base64.b64decode(wire)[:2] == b"\xc3\x01"
    assert from_payload_value(serializer.decode(wire, codec="avro"), InteropOrder) == INTEROP_ORDER


# Replay: the corpus fixture was recorded by an untyped workflow.
def summary(first_status: str, skus: list[str], second_qty: int, count: int) -> dict[str, Any]:
    return {"first_status": first_status, "skus": skus, "second_qty": second_qty, "count": count}


@workflow.defn(name="tests.replay.typed-activity-results")
class TypedActivityResultsWorkflow:
    """Opts in to result_type for a single call and a parallel group."""

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        first = yield ctx.schedule_activity("typed.lookup", ["o-1"], result_type=Shipment)
        second, count = yield [
            ctx.schedule_activity("typed.lookup", ["o-2"], result_type=Shipment),
            ctx.schedule_activity("typed.count", [], result_type=int),
        ]
        return summary(first.status.value, [item.sku for item in first.items],
                       sum(item.qty for item in second.items), count)


@workflow.defn(name="tests.replay.untyped-activity-results")
class UntypedActivityResultsWorkflow:
    """The workflow that recorded the corpus history, reading plain maps."""

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        first = yield ctx.schedule_activity("typed.lookup", ["o-1"])
        second, count = yield [ctx.schedule_activity("typed.lookup", ["o-2"]), ctx.schedule_activity("typed.count", [])]
        return summary(first["status"], [item["sku"] for item in first["items"]],
                       sum(item.get("qty", 1) for item in second["items"]), count)


CORPUS = FIXTURES / "replay_regressions" / "typed-activity-results-opt-in.json"


def corpus_history() -> list[dict[str, Any]]:
    return json.loads(CORPUS.read_text(encoding="utf-8"))["history"]


def test_an_untyped_history_replays_unchanged_with_result_type() -> None:
    fixture = json.loads(CORPUS.read_text(encoding="utf-8"))
    typed = replay(TypedActivityResultsWorkflow, corpus_history(), [], payload_codec="avro")
    untyped_outcome = replay(UntypedActivityResultsWorkflow, corpus_history(), [], payload_codec="avro")
    assert list(typed.commands) == list(untyped_outcome.commands) == [CompleteWorkflow(fixture["expected"]["result"])]


@workflow.defn(name="tests.typed.catches-mismatch")
class CatchesMismatchWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.schedule_activity("typed.lookup", ["o-1"], result_type=list[Item])
        except TypedValueError as error:
            return {"path": error.path, "expected": error.expected}
        return "decoded"


def test_a_result_type_mismatch_is_raised_at_the_yield() -> None:
    outcome = replay(CatchesMismatchWorkflow, corpus_history()[:3], [], payload_codec="avro")
    assert list(outcome.commands) == [CompleteWorkflow({"path": "activity result", "expected": "list[Item]"})]


@workflow.defn(name="tests.typed.parallel-mismatch")
class ParallelMismatchWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        first = yield ctx.schedule_activity("typed.lookup", ["o-1"])
        try:
            yield [ctx.schedule_activity("typed.lookup", ["o-2"], result_type=Shipment),
                   ctx.schedule_activity("typed.count", [], result_type=str)]
        except TypedValueError as error:
            return {"first": first["order_id"], "path": error.path, "detail": error.detail}
        return "decoded"


def test_a_parallel_member_mismatch_is_raised_at_the_group_yield() -> None:
    outcome = replay(ParallelMismatchWorkflow, corpus_history(), [], payload_codec="avro")
    assert list(outcome.commands) == [
        CompleteWorkflow({"first": "o-1", "path": "activity result", "detail": "got integer"}),
    ]


def _selection_history() -> list[dict[str, Any]]:
    from tests.test_durable_selection import _activity_completed, _activity_scheduled, _winner_marker

    return [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", {"sku": "fast-sku", "unknown": True}),
        _winner_marker(),
        _activity_completed(0, "slow", {"sku": "slow-sku", "qty": 4}),
    ]


@workflow.defn(name="tests.typed.selection")
class TypedSelectionWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected = yield ctx.select({
            "slow": ctx.schedule_activity("slow-activity", [], result_type=Item),
            "fast": ctx.schedule_activity("fast-activity", [], result_type=Item),
        })
        slow = yield selected.handles["slow"].await_result()
        return {"winner": selected.key, "item": asdict(selected.result()), "slow": asdict(slow)}


@workflow.defn(name="tests.typed.selection-mismatch")
class MismatchedSelectionWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected = yield ctx.select({
            "slow": ctx.schedule_activity("slow-activity", [], result_type=int),
            "fast": ctx.schedule_activity("fast-activity", [], result_type=int),
        })
        assert isinstance(selected.failure, TypedValueError)
        try:
            yield selected.handles["slow"].await_result()
        except TypedValueError as error:
            return {"winner": selected.key, "winner_detail": selected.failure.detail, "slow_detail": error.detail}
        return "decoded"


def test_selection_members_are_decoded_to_their_result_type() -> None:
    (command,) = replay(TypedSelectionWorkflow, _selection_history(), [], payload_codec="avro").commands
    assert command == CompleteWorkflow({
        "winner": "fast", "item": {"sku": "fast-sku", "qty": 1}, "slow": {"sku": "slow-sku", "qty": 4},
    })


def test_a_selection_mismatch_is_the_winner_failure_and_raised_at_a_handle_await() -> None:
    (command,) = replay(MismatchedSelectionWorkflow, _selection_history(), [], payload_codec="avro").commands
    assert command == CompleteWorkflow({"winner": "fast", "winner_detail": "got object", "slow_detail": "got object"})
