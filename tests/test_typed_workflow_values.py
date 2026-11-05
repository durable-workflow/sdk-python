"""Opt-in typed values at the workflow, child and client boundaries (#184, slice 2)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import TypedValueError, serializer, workflow
from durable_workflow.external_storage import LocalFilesystemExternalStorage
from durable_workflow.workflow import (
    CompleteUpdate,
    CompleteWorkflow,
    ContinueAsNew,
    FailWorkflow,
    StartChildWorkflow,
    WorkflowContext,
    apply_update,
    commands_to_server_commands,
    query_state,
    replay,
)

CORPUS = Path(__file__).parent / "fixtures" / "replay_regressions" / "typed-workflow-values-opt-in.json"


class Tier(Enum):
    BASIC = "basic"
    GOLD = "gold"


@dataclass
class Customer:
    name: str
    tier: Tier = Tier.BASIC
    tags: list[str] = field(default_factory=list)


@dataclass
class Approval:
    approver: str
    note: str | None = None


ADA = Customer("Ada", Tier.GOLD, ["vip"])
ADA_NEUTRAL = {"name": "Ada", "tier": "gold", "tags": ["vip"]}


@workflow.defn(name="tests.replay.typed-terminal-result")
class TypedTerminalResult:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield ctx.schedule_activity("tests.typed.echo", [], result_type=Customer))


@workflow.defn(name="tests.replay.typed-workflow-values", typed=True)
class TypedOnboarding:
    """Typed run, signal, update and query arguments, and a typed child result."""

    def __init__(self) -> None:
        self.customer: Customer | None = None
        self.approval: Approval | None = None

    @workflow.signal("approve")
    def approve(self, approval: Approval) -> None:
        self.approval = approval

    @workflow.update("retier")
    def retier(self, tier: Tier) -> str:
        assert self.customer is not None
        self.customer.tier = tier
        return tier.value

    @workflow.query("describe")
    def describe(self, field_name: str) -> Any:
        assert self.customer is not None
        return getattr(self.customer, field_name)

    def run(self, ctx: WorkflowContext, customer: Customer):  # type: ignore[no-untyped-def]
        self.customer = customer
        account = yield ctx.start_child_workflow("tests.accounts.open", [customer], result_type=Customer)
        yield ctx.wait_condition(lambda: self.approval is not None, key="approval")
        assert self.approval is not None
        return {"name": account.name, "tier": account.tier.value, "approver": self.approval.approver,
                "note": self.approval.note}


@workflow.defn(name="tests.replay.untyped-workflow-values")
class UntypedOnboarding:
    """The workflow that recorded the corpus history, reading plain maps."""

    def __init__(self) -> None:
        self.approval: dict[str, Any] | None = None

    @workflow.signal("approve")
    def approve(self, approval: Approval) -> None:
        # Annotated but untyped: it must still receive the plain map.
        self.approval = approval  # type: ignore[assignment]

    def run(self, ctx: WorkflowContext, customer: Customer):  # type: ignore[no-untyped-def]
        account = yield ctx.start_child_workflow("tests.accounts.open", [customer])
        yield ctx.wait_condition(lambda: self.approval is not None, key="approval")
        assert self.approval is not None
        return {"name": account["name"], "tier": account["tier"], "approver": self.approval["approver"],
                "note": self.approval.get("note")}


def corpus() -> dict[str, Any]:
    return json.loads(CORPUS.read_text(encoding="utf-8"))


def test_an_untyped_history_replays_unchanged_with_typed_true() -> None:
    fixture = corpus()
    typed = replay(TypedOnboarding, fixture["history"], fixture["workflow"]["input"], payload_codec="avro")
    untyped = replay(UntypedOnboarding, fixture["history"], fixture["workflow"]["input"], payload_codec="avro")
    expected = [CompleteWorkflow(fixture["expected"]["result"])]
    assert list(typed.commands) == list(untyped.commands) == expected


def test_typed_query_and_update_arguments_are_decoded() -> None:
    fixture = corpus()
    history, start = fixture["history"], fixture["workflow"]["input"]
    assert query_state(TypedOnboarding, history, start, "describe", ["tier"], payload_codec="avro") == Tier.GOLD
    accepted = {"event_type": "UpdateAccepted", "payload": {
        "update_id": "update-1", "update_name": "retier",
        "arguments": serializer.encode(["basic"], codec="avro"), "payload_codec": "avro"}}
    result = apply_update(TypedOnboarding, [*history[:-2], accepted], start, "update-1", payload_codec="avro")
    assert result == CompleteUpdate("update-1", "basic")


def test_a_typed_run_argument_mismatch_fails_the_workflow_without_the_value() -> None:
    (command,) = replay(TypedOnboarding, [{"event_type": "WorkflowStarted", "recorded_at": "2026-11-04T00:00:00+00:00",
                                           "payload": {}}], [{"tier": "gold", "secret": "s3cr3t"}],
                        payload_codec="avro").commands
    assert isinstance(command, FailWorkflow)
    assert command.exception_type == "TypedValueError"
    assert "customer.name" in command.message and "s3cr3t" not in command.message


def test_typed_workflow_methods_keep_their_signatures_and_registries() -> None:
    import inspect

    assert list(inspect.signature(TypedOnboarding.run).parameters) == ["self", "ctx", "customer"]
    assert TypedOnboarding.__workflow_signals__ == {"approve": "approve"}  # type: ignore[attr-defined]
    assert TypedOnboarding.__workflow_updates__ == {"retier": "retier"}  # type: ignore[attr-defined]


def test_an_unsupported_handler_annotation_fails_when_the_workflow_is_declared() -> None:
    with pytest.raises(TypeError):
        @workflow.defn(name="tests.typed.bad-signal", typed=True)
        class BadSignal:
            @workflow.signal("bad")
            def bad(self, value: set[str]) -> None:
                return None

            def run(self, ctx: WorkflowContext) -> None:
                return None


def test_child_arguments_and_terminal_values_travel_as_neutral_bytes() -> None:
    child = StartChildWorkflow("tests.accounts.open", [ADA])
    assert child.arguments == [ADA_NEUTRAL]
    assert child.to_server_command("q")["arguments"] == serializer.envelope([asdict(ADA) | {"tier": "gold"}])
    assert child == StartChildWorkflow("tests.accounts.open", [ADA], result_type=Customer)
    with pytest.raises(TypeError):
        StartChildWorkflow("tests.accounts.open", [], result_type=int | str)
    completed = CompleteWorkflow(ADA).to_server_command("q")
    assert completed["result"] == serializer.envelope(ADA_NEUTRAL)
    assert CompleteUpdate("u", Tier.GOLD).to_server_command("q")["result"] == serializer.envelope("gold")
    assert ContinueAsNew(arguments=[ADA]).to_server_command("q")["arguments"] == serializer.envelope([ADA_NEUTRAL])
    # The command keeps the authored value; only the wire form is neutral.
    assert CompleteWorkflow(ADA).result is ADA


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize(("command", "key", "neutral"), [
    (CompleteWorkflow(ADA), "result", ADA_NEUTRAL),
    (CompleteUpdate("typed-update", {"customer": ADA, "tier": Tier.GOLD}),
     "result", {"customer": ADA_NEUTRAL, "tier": "gold"}),
    (ContinueAsNew(arguments=[ADA, Tier.GOLD]), "arguments", [ADA_NEUTRAL, "gold"]),
    (CompleteWorkflow(ADA_NEUTRAL), "result", ADA_NEUTRAL),
    (CompleteUpdate("plain-update", None), "result", None),
    (ContinueAsNew(arguments=[ADA_NEUTRAL, "gold"]), "arguments", [ADA_NEUTRAL, "gold"]),
])
def test_worker_batch_preserves_typed_terminal_wire_contract(
    tmp_path: Path, external: bool, command: Any, key: str, neutral: Any,
) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path) if external else None
    options = {"size_warning": None, "external_storage": storage,
               "external_storage_threshold_bytes": 1 if external else None}
    direct = command.to_server_command("q", **options)
    authored = command.arguments if isinstance(command, ContinueAsNew) else command.result
    before = CompleteWorkflow({"plain": [True, 0, b"bytes"]})
    after = CompleteUpdate("tail", [None, 1.5])
    batch = commands_to_server_commands([before, command, after], "q", **options)
    assert len(batch) == 3
    assert serializer.decode_envelope(batch[0]["result"], external_storage=storage) == before.result
    assert serializer.decode_envelope(batch[2]["result"], external_storage=storage) == after.result
    assert serializer.decode_envelope(batch[1][key], external_storage=storage) == neutral
    assert {k: v for k, v in batch[1].items() if k != key} == {k: v for k, v in direct.items() if k != key}
    if external:
        assert "external_storage" in batch[1][key]
    else:
        assert batch[1] == direct
    assert (command.arguments if isinstance(command, ContinueAsNew) else command.result) is authored


def test_a_child_result_type_mismatch_is_raised_at_the_yield() -> None:
    @workflow.defn(name="tests.typed.child-mismatch")
    class ChildMismatch:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_child_workflow("tests.accounts.open", [], result_type=list[Customer])
            except TypedValueError as error:
                return {"path": error.path, "detail": error.detail}
            return "decoded"

    history = [event for event in corpus()["history"] if event["event_type"] in {
        "WorkflowStarted", "ChildWorkflowScheduled", "ChildRunStarted", "ChildRunCompleted"}]
    (command,) = replay(ChildMismatch, history, [], payload_codec="avro").commands
    assert command == CompleteWorkflow({"path": "child workflow result", "detail": "got object"})


@pytest.mark.asyncio
async def test_the_client_decodes_a_result_type_and_sends_neutral_arguments() -> None:
    from durable_workflow.client import Client, WorkflowExecution, WorkflowHandle

    client = Client("http://server.invalid", token="t")
    client.describe_workflow = AsyncMock(return_value=WorkflowExecution(  # type: ignore[method-assign]
        workflow_id="wf", run_id="run", workflow_type="x", status="completed", payload_codec="avro"))
    client.get_history = AsyncMock(return_value={"events": [  # type: ignore[method-assign]
        {"event_type": "WorkflowCompleted", "payload": {"output": serializer.envelope(ADA_NEUTRAL),
                                                        "payload_codec": "avro"}}]})
    handle = WorkflowHandle(client, "wf", "run")
    assert await handle.result(result_type=Customer, timeout=1) == ADA
    assert await handle.result(timeout=1) == ADA_NEUTRAL
    with pytest.raises(TypedValueError, match="workflow result: expected list"):
        await handle.result(result_type=list[Customer], timeout=1)
    envelope = client._payload_envelope([ADA, Tier.GOLD], kind="workflow_input", workflow_id="wf", task_queue="q")
    assert envelope == serializer.envelope([ADA_NEUTRAL, "gold"])
    await client.aclose()


# A predicate's recorded fingerprint must not change when typed=True is turned
# on (#255 review): zero-arg handlers stay unwrapped, and a wrapped handler is
# fingerprinted as the method it wraps.
@workflow.defn(name="tests.replay.typed-predicate-fingerprint", typed=True)
class TypedPredicate:
    def __init__(self) -> None:
        self.approvals: list[Approval] = []

    @workflow.signal("approve")
    def approve(self, approval: Approval) -> None:
        self.approvals.append(approval)

    @workflow.query("is_done")
    def is_done(self) -> bool:
        return bool(self.approvals)

    @workflow.query("ready")
    def ready(self, minimum: int = 2) -> bool:
        return len(self.approvals) >= minimum

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(self.is_done, key="done")
        yield ctx.wait_condition(self.ready, key="ready")
        return [approval.approver for approval in self.approvals]


def _untyped_predicate_history() -> list[dict[str, Any]]:
    """The history an untyped deploy of TypedPredicate records on this Python.

    Fingerprints hash the predicate's bytecode, which differs between Python
    versions, so they are computed from the unwrapped methods at run time
    rather than stored in a corpus fixture.
    """
    import inspect

    from durable_workflow.workflow import _condition_predicate_fingerprint

    # inspect.unwrap reaches the authored method whether or not typed=True
    # wrapped it, which is what an untyped deploy fingerprints.
    instance = TypedPredicate()
    done = _condition_predicate_fingerprint(inspect.unwrap(TypedPredicate.is_done).__get__(instance))
    ready = _condition_predicate_fingerprint(inspect.unwrap(TypedPredicate.ready).__get__(instance))
    at = "2026-11-04T00:00:0{}+00:00"

    def approve(value: dict[str, Any], sequence: int, second: int) -> dict[str, Any]:
        return {"event_type": "SignalReceived", "recorded_at": at.format(second), "payload": {
            "signal_name": "approve", "value": serializer.envelope([value]), "payload_codec": "avro",
            "workflow_sequence": sequence}}

    def opened(wait_id: str, key: str, sequence: int, fingerprint: str, second: int) -> dict[str, Any]:
        return {"event_type": "ConditionWaitOpened", "recorded_at": at.format(second), "payload": {
            "condition_wait_id": wait_id, "condition_key": key, "sequence": sequence,
            "condition_definition_fingerprint": fingerprint}}

    def satisfied(wait_id: str, key: str, sequence: int, second: int) -> dict[str, Any]:
        return {"event_type": "ConditionWaitSatisfied", "recorded_at": at.format(second), "payload": {
            "condition_wait_id": wait_id, "condition_key": key, "sequence": sequence}}

    return [
        {"event_type": "WorkflowStarted", "recorded_at": at.format(0), "payload": {}},
        opened("done-1", "done", 1, done, 1), approve({"approver": "Grace"}, 1, 2), satisfied("done-1", "done", 1, 2),
        opened("ready-1", "ready", 2, ready, 3), approve({"approver": "Ada", "note": "ok"}, 2, 4),
        satisfied("ready-1", "ready", 2, 4),
    ]


def test_typed_true_keeps_recorded_predicate_fingerprints() -> None:
    from durable_workflow.workflow import _condition_predicate_fingerprint

    instance = TypedPredicate()
    # A zero-arg handler is not wrapped at all.
    assert not hasattr(TypedPredicate.is_done, "__typed_workflow_method__")
    # A wrapped handler keeps the fingerprint of the method it wraps.
    original = TypedPredicate.ready.__typed_workflow_method__  # type: ignore[attr-defined]
    assert _condition_predicate_fingerprint(instance.ready) == _condition_predicate_fingerprint(
        original.__get__(instance))


def test_an_untyped_history_with_predicate_fingerprints_replays_under_typed_true() -> None:
    (command,) = replay(TypedPredicate, _untyped_predicate_history(), [], payload_codec="avro").commands
    assert command == CompleteWorkflow(["Grace", "Ada"])


@pytest.mark.parametrize("kind", [staticmethod, classmethod])
def test_static_and_class_method_handlers_are_refused_when_typed(kind: Any) -> None:
    def approve(*args: Any) -> None:
        return None

    approve.__signal_name__ = "approve"  # type: ignore[attr-defined]
    namespace = {"approve": kind(approve), "run": lambda self, ctx: None}
    with pytest.raises(TypeError, match="handlers must be instance methods"):
        workflow.defn(name=f"tests.typed.{kind.__name__}-handler", typed=True)(type("StaticHandler", (), namespace))
