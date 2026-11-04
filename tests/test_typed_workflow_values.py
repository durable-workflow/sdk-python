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
from durable_workflow.workflow import (
    CompleteUpdate,
    CompleteWorkflow,
    ContinueAsNew,
    FailWorkflow,
    StartChildWorkflow,
    WorkflowContext,
    apply_update,
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
    def approve(self, approval: dict[str, Any]) -> None:
        self.approval = approval

    def run(self, ctx: WorkflowContext, customer: dict[str, Any]):  # type: ignore[no-untyped-def]
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
