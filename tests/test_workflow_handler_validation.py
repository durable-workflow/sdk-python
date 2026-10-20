from __future__ import annotations

from collections.abc import AsyncIterator, Generator
from functools import wraps
from typing import Any

import pytest

from durable_workflow import Client, Worker, workflow
from durable_workflow.errors import InvalidWorkflowDefinition
from durable_workflow.testing import WorkflowEnvironment


def _handler(shape: str) -> Any:
    async def coroutine(self: Any) -> None:
        raise AssertionError("unsupported handler must never run")


    async def async_generator(self: Any) -> AsyncIterator[None]:
        yield None


    def generator(self: Any) -> Generator[None, None, None]:
        yield None

    return {"coroutine": coroutine, "async_generator": async_generator, "generator": generator}[shape]


def _definition(kind: str, handler: Any, inherited: bool) -> type:
    decorated = getattr(workflow, kind)("change")(handler)

    def run(self: Any, ctx: Any) -> str:
        return "done"

    base = type("HandlerWorkflow", (), {"receive": decorated, "run": run})
    definition = type("InheritedHandlerWorkflow", (base,), {}) if inherited else base
    definition.__workflow_name__ = "handler-validation"
    return definition


@pytest.mark.parametrize("kind", ["signal", "query", "update", "update_validator"])
@pytest.mark.parametrize("shape", ["coroutine", "async_generator", "generator"])
@pytest.mark.parametrize("inherited", [False, True])
@pytest.mark.parametrize("entry", ["definition", "worker", "environment", "replayer", "replay"])
def test_unsupported_handlers_are_rejected_before_execution(
    kind: str, shape: str, inherited: bool, entry: str,
) -> None:
    definition = _definition(kind, _handler(shape), inherited)
    with pytest.raises(InvalidWorkflowDefinition, match="handler-validation.*receive.*synchronous"):
        if entry == "definition":
            workflow.defn(name="handler-validation")(definition)
        elif entry == "worker":
            Worker(Client("http://127.0.0.1:1"), task_queue="unused", workflows=[definition])
        elif entry == "environment":
            WorkflowEnvironment().register_workflow(definition)
        elif entry == "replayer":
            workflow.Replayer(workflows=[definition])
        else:
            workflow.replay(definition, [], [])


@pytest.mark.parametrize("shape", ["coroutine", "async_generator", "generator"])
def test_wrapped_handlers_are_validated(shape: str) -> None:
    original = _handler(shape)

    @wraps(original)
    def receive(self: Any) -> Any:
        return original(self)

    with pytest.raises(InvalidWorkflowDefinition, match="handler-validation.*receive"):
        workflow.defn(name="handler-validation")(_definition("signal", receive, False))


@pytest.mark.parametrize("kind", ["signal", "query", "update", "update_validator"])
@pytest.mark.parametrize("shape", ["coroutine", "async_generator", "generator"])
def test_sync_wrapper_cannot_return_an_async_or_generator_result(kind: str, shape: str) -> None:
    original = _handler(shape)

    def receive(self: Any) -> Any:
        return original(self)

    definition = workflow.defn(name="handler-validation")(_definition(kind, receive, False))
    if kind == "signal":
        from durable_workflow import serializer

        history = [{"event_type": "SignalReceived", "payload": {
            "signal_name": "change", "value": serializer.envelope([]), "payload_codec": "avro",
        }}]

        def run(self: Any, ctx: Any) -> Any:
            yield ctx.schedule_activity("tick", [])

        definition.run = run
        with pytest.raises(InvalidWorkflowDefinition, match="unsupported_workflow_handler_result.*receive"):
            workflow.replay(definition, history, [])
    elif kind == "query":
        from durable_workflow.errors import QueryFailed

        with pytest.raises(QueryFailed, match="unsupported_workflow_handler_result.*receive"):
            workflow.query_state(definition, [], [], "change")
    elif kind == "update":
        from durable_workflow import serializer

        result = workflow.apply_update(definition, [{"event_type": "UpdateAccepted", "payload": {
            "update_id": "update-1", "update_name": "change", "value": serializer.envelope([]),
            "payload_codec": "avro",
        }}], [], "update-1")
        assert isinstance(result, workflow.FailUpdate)
        assert "unsupported_workflow_handler_result" in result.message
    else:
        from durable_workflow.errors import UpdateRejected

        definition.__workflow_updates__ = {"change": "receive"}
        with pytest.raises(UpdateRejected, match="unsupported_workflow_handler_result.*receive"):
            workflow.validate_update(definition, [], [], "change", [])


def test_valid_sync_handlers_remain_supported() -> None:
    @workflow.defn(name="sync-handlers")
    class SynchronousWorkflow:
        @workflow.signal("change")
        def receive(self) -> None:
            pass

        @workflow.query("read")
        def read(self) -> str:
            return "value"

        @workflow.update("change")
        def change(self) -> str:
            return "updated"

        @workflow.update_validator("change")
        def validate(self) -> bool:
            return True

        def run(self, ctx: Any) -> str:
            return "done"

    Worker(Client("http://127.0.0.1:1"), task_queue="unused", workflows=[SynchronousWorkflow])
    assert WorkflowEnvironment().execute_workflow(SynchronousWorkflow) == "done"
    assert workflow.Replayer(workflows=[SynchronousWorkflow]).replay([], [], workflow_type="sync-handlers")
