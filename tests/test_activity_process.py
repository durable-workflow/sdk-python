from __future__ import annotations

import asyncio
import ctypes
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import activity
from durable_workflow._activity_process import (
    CallbackInvocation,
    CallbackProcessLost,
    SupervisedCallback,
)
from durable_workflow.activity import ActivityInfo
from durable_workflow.errors import NonRetryableError
from durable_workflow.interceptors import ActivityHandler, ActivityInterceptorContext, PassthroughWorkerInterceptor


def invocation(handler: Any, *args: Any, interceptors: tuple[Any, ...] = ()) -> CallbackInvocation:
    return CallbackInvocation(
        handler=handler, args=args,
        info=ActivityInfo("task", "process-test", "attempt", 1, "queue", "owner"),
        task={"task_id": "task", "activity_attempt_id": "attempt"}, interceptors=interceptors,
    )


def typed_result(value: bytes) -> dict[str, Any]:
    return {"bytes": value, "task": activity.context().info.task_id, "nested": [1, None, {"x": True}]}


async def authored_heartbeat() -> bytes:
    await activity.context().heartbeat({"progress": b"typed"})
    return b"result"


def authored_sync_heartbeat() -> bytes:
    return asyncio.run(authored_heartbeat())


class ProcessInterceptor(PassthroughWorkerInterceptor):
    async def execute_activity(self, context: ActivityInterceptorContext, next: ActivityHandler) -> Any:
        assert context.worker_id == "owner"
        assert context.task["activity_attempt_id"] == "attempt"
        return {"intercepted": await next(context)}


class ProcessFailure(NonRetryableError):
    code = 42


def failing() -> None:
    raise ProcessFailure("original failure")


async def blocked_without_python_progress(marker: str) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path(marker).write_text(str(os.getpid()))
    # PyDLL retains the callback interpreter's GIL during the C call. A Python
    # thread or signal callback in this interpreter cannot supervise this work.
    ctypes.PyDLL(None).sleep(60)


def wait_for_release(marker: str) -> None:
    Path(marker).write_text(str(os.getpid()))
    while not Path(marker + ".release").exists():
        time.sleep(0.02)


async def wait_for_file(path: Path) -> None:
    async def ready() -> None:
        while not path.exists():
            await asyncio.sleep(0.02)
    await asyncio.wait_for(ready(), timeout=5.0)


async def wait_for_exit(pid: int) -> None:
    async def gone() -> None:
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            await asyncio.sleep(0.02)
    await asyncio.wait_for(gone(), timeout=7.0)


async def no_heartbeat(details: dict[str, Any] | None) -> None:
    raise AssertionError("callback emitted an unauthored heartbeat")


async def test_spawn_preserves_typed_result_context_and_interceptors() -> None:
    callback = SupervisedCallback(invocation(typed_result, b"\x00\xff", interceptors=(ProcessInterceptor(),)))
    try:
        await callback.start()
        outcome = await callback.result(no_heartbeat)
        assert outcome.failure is None
        assert outcome.value == {"intercepted": {"bytes": b"\x00\xff", "task": "task",
                                                "nested": [1, None, {"x": True}]}}
        assert callback.stopped is True
        assert not Path(callback.directory).exists()
    finally:
        await callback.close()


@pytest.mark.parametrize("handler", [authored_heartbeat, authored_sync_heartbeat])
async def test_only_authored_heartbeat_crosses_to_the_owner(handler: Any) -> None:
    observed: list[Any] = []
    owner_loop = asyncio.get_running_loop()

    async def heartbeat(details: dict[str, Any] | None) -> None:
        assert asyncio.get_running_loop() is owner_loop
        observed.append(details)

    callback = SupervisedCallback(invocation(handler))
    try:
        await callback.start()
        outcome = await callback.result(heartbeat)
        assert outcome.value == b"result"
        assert outcome.failure is None
        assert observed == [{"progress": b"typed"}]
        assert callback.stopped is True
    finally:
        await callback.close()


async def test_spawn_preserves_application_failure_metadata() -> None:
    callback = SupervisedCallback(invocation(failing))
    try:
        await callback.start()
        outcome = await callback.result(no_heartbeat)
        assert outcome.failure is not None
        assert outcome.failure.message == "original failure"
        assert outcome.failure.failure_type == "ProcessFailure"
        assert outcome.failure.failure_class == "tests.test_activity_process.ProcessFailure"
        assert outcome.failure.failure_code == 42
        assert outcome.failure.non_retryable is True
        assert "raise ProcessFailure" in outcome.failure.stack_trace
        assert callback.stopped is True
    finally:
        await callback.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process signals")
async def test_stop_kills_and_joins_a_gil_blocked_callback_without_heartbeats(tmp_path: Path) -> None:
    marker = tmp_path / "callback"
    callback = SupervisedCallback(invocation(blocked_without_python_progress, str(marker)))
    try:
        await callback.start()
        await wait_for_file(marker)
        pid = int(marker.read_text())
        started = time.monotonic()
        await callback.stop()
        assert time.monotonic() - started < 3.0
        assert callback.stopped is True
        await wait_for_exit(pid)
        assert not Path(callback.directory).exists()
    finally:
        await callback.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX process signals")
async def test_owner_sigkill_leaves_supervisor_to_stop_and_reap_callback(tmp_path: Path) -> None:
    marker = tmp_path / "callback"
    supervisor_marker = tmp_path / "supervisor"
    script = tmp_path / "owner.py"
    script.write_text("""import asyncio
import sys
from pathlib import Path
from durable_workflow._activity_process import SupervisedCallback
from tests.test_activity_process import blocked_without_python_progress, invocation
async def run():
    callback = SupervisedCallback(invocation(blocked_without_python_progress, sys.argv[1]))
    await callback.start()
    Path(sys.argv[2]).write_text(str(callback._supervisor.pid) + '\\n' + callback.directory)
    await asyncio.Event().wait()
if __name__ == '__main__':
    asyncio.run(run())
""")
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    owner = subprocess.Popen([sys.executable, str(script), str(marker), str(supervisor_marker)], env=environment)
    try:
        await wait_for_file(marker)
        await wait_for_file(supervisor_marker)
        supervisor_pid, directory = supervisor_marker.read_text().splitlines()
        callback_pid = int(marker.read_text())
        os.kill(owner.pid, signal.SIGKILL)
        await asyncio.to_thread(owner.wait, 5.0)
        assert owner.returncode == -signal.SIGKILL
        await wait_for_exit(callback_pid)
        await wait_for_exit(int(supervisor_pid))
        assert not Path(directory).exists()
    finally:
        if owner.poll() is None:
            owner.kill()
            await asyncio.to_thread(owner.wait, 5.0)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process signals")
async def test_dead_supervisor_with_live_callback_never_proves_stop(tmp_path: Path) -> None:
    marker = tmp_path / "callback"
    callback = SupervisedCallback(invocation(wait_for_release, str(marker)))
    try:
        await callback.start()
        await wait_for_file(marker)
        pid = int(marker.read_text())
        callback._supervisor.kill()
        with pytest.raises((CallbackProcessLost, BrokenPipeError, ConnectionResetError)):
            await callback.stop()
        assert callback.stopped is False
        os.kill(pid, 0)  # A dead supervisor alone left the callback alive.
    finally:
        Path(str(marker) + ".release").touch()
        if marker.exists():
            await wait_for_exit(int(marker.read_text()))
        await callback.close()


def test_nonimportable_handler_is_rejected_before_spawn() -> None:
    def callback() -> None:
        pass
    with pytest.raises(ValueError, match="spawn-compatible.*importable handler"):
        SupervisedCallback(invocation(callback))


async def test_cancellation_during_confirmed_result_join_still_reaps_and_proves_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    callback = SupervisedCallback(invocation(typed_result, b"typed"))
    entering_join = asyncio.Event()
    release_join = asyncio.Event()
    close = SupervisedCallback.close

    async def paused_close(self: SupervisedCallback) -> None:
        entering_join.set()
        await release_join.wait()
        await close(self)

    monkeypatch.setattr(SupervisedCallback, "close", paused_close)
    result: asyncio.Task[Any] | None = None
    try:
        await callback.start()
        result = asyncio.create_task(callback.result(no_heartbeat))
        await asyncio.wait_for(entering_join.wait(), timeout=5)
        result.cancel()
        await asyncio.sleep(0)
        assert not callback.stopped
        release_join.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(result, timeout=7)
        assert callback.stopped and callback._joined and callback._exitcode == 0
    finally:
        release_join.set()
        if result is not None:
            await asyncio.gather(result, return_exceptions=True)
        if not callback.stopped:
            await callback.stop()
