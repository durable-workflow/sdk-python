from __future__ import annotations

from collections.abc import AsyncIterator, Generator
from functools import partial, wraps
from typing import Any

import pytest

from durable_workflow import Client, Worker, workflow
from durable_workflow.testing import WorkflowEnvironment


class CoroutineWorkflow:
    async def run(self, ctx: Any) -> str:
        return "never executed"


class AsyncGeneratorWorkflow:
    async def run(self, ctx: Any) -> AsyncIterator[str]:
        yield "unsupported"


class AsyncCallable:
    async def __call__(self, ctx: Any) -> str:
        return "unsupported"


class CallableWorkflow:
    run = AsyncCallable()


class PartialWorkflow:
    run = staticmethod(partial(CoroutineWorkflow.run, object()))


ASYNC_DEFINITIONS = [CoroutineWorkflow, AsyncGeneratorWorkflow, CallableWorkflow, PartialWorkflow]


@pytest.mark.parametrize("definition", ASYNC_DEFINITIONS)
def test_definition_rejects_async_entrypoints(definition: type) -> None:
    with pytest.raises(ValueError, match="unsupported_workflow_entrypoint.*synchronous"):
        workflow.defn(name="invalid-async")(definition)


@pytest.mark.parametrize("definition", ASYNC_DEFINITIONS)
def test_worker_rejects_undecorated_async_entrypoints_before_network(definition: type) -> None:
    with pytest.raises(ValueError, match=definition.__name__):
        Worker(Client("http://127.0.0.1:1"), task_queue="unused", workflows=[definition])


@pytest.mark.parametrize("definition", ASYNC_DEFINITIONS)
def test_direct_replay_rejects_async_without_creating_a_coroutine(definition: type) -> None:
    with pytest.raises(ValueError, match="unsupported_workflow_entrypoint"):
        workflow.replay(definition, [], [])


def test_test_environment_revalidates_entrypoint_after_decoration() -> None:
    @workflow.defn(name="changed-entrypoint")
    class ChangedWorkflow:
        def run(self, ctx: Any) -> str:
            return "valid"

    ChangedWorkflow.run = CoroutineWorkflow.run  # type: ignore[assignment]
    with pytest.raises(ValueError, match="changed-entrypoint"):
        WorkflowEnvironment().register_workflow(ChangedWorkflow)


def test_wrapped_and_inherited_async_entrypoints_are_rejected() -> None:
    class InheritedWorkflow(CoroutineWorkflow):
        pass

    class WrappedWorkflow:
        @wraps(CoroutineWorkflow.run)
        def run(self, ctx: Any) -> Any:
            return CoroutineWorkflow.run(self, ctx)

    for definition in [InheritedWorkflow, WrappedWorkflow]:
        with pytest.raises(ValueError, match="unsupported_workflow_entrypoint"):
            workflow.replay(definition, [], [])


@pytest.mark.parametrize("generator", [False, True])
def test_runtime_awaitable_result_is_not_completed(generator: bool) -> None:
    class AwaitableResultWorkflow:
        def run(self, ctx: Any) -> Any:
            return CoroutineWorkflow.run(self, ctx)

    class GeneratorAwaitableResultWorkflow:
        def run(self, ctx: Any) -> Generator[Any, Any, Any]:
            if False:
                yield None
            return CoroutineWorkflow.run(self, ctx)

    definition = GeneratorAwaitableResultWorkflow if generator else AwaitableResultWorkflow
    with pytest.raises(ValueError, match="unsupported_workflow_result"):
        workflow.replay(definition, [], [])


def test_synchronous_and_generator_workflows_remain_supported() -> None:
    @workflow.defn(name="valid-sync")
    class SynchronousWorkflow:
        def run(self, ctx: Any) -> str:
            return "done"

    @workflow.defn(name="valid-generator")
    class GeneratorWorkflow:
        def run(self, ctx: Any) -> Generator[Any, Any, str]:
            if False:
                yield None
            return "done"

    Worker(Client("http://127.0.0.1:1"), task_queue="unused", workflows=[SynchronousWorkflow, GeneratorWorkflow])
    environment = WorkflowEnvironment()
    for definition in [SynchronousWorkflow, GeneratorWorkflow]:
        assert environment.execute_workflow(definition) == "done"
        assert workflow.replay(definition, [], []).commands[0].result == "done"  # type: ignore[union-attr]
