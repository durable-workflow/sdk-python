"""Owned callback processes for the unfinished cooperative worker.

The supervisor never unpickles or invokes the serialized application callback.
Single-byte control messages keep owner disconnect observable even if an
application payload is large or its writer is killed. Attempt-local private
files carry payloads. This module does not grant or report Server authority.
"""

from __future__ import annotations

import asyncio
import inspect
import multiprocessing
import pickle
import selectors
import shutil
import socket
import tempfile
import threading
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any, cast

from .activity import ActivityContext, ActivityInfo, _set_context
from .errors import ActivityCancelled, NonRetryableError
from .interceptors import ActivityInterceptorContext, WorkerInterceptor


class CallbackProcessLost(RuntimeError):
    """The owner could not prove the callback outcome or physical stop."""


@dataclass(frozen=True)
class CallbackInvocation:
    handler: Callable[..., Any]
    args: tuple[Any, ...]
    info: ActivityInfo
    task: dict[str, Any]
    interceptors: tuple[WorkerInterceptor, ...] = ()

    def serialize(self) -> bytes:
        try:
            return pickle.dumps(self, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as error:
            raise ValueError(
                f"cooperative activity {self.info.activity_type!r} requires a spawn-compatible "
                "importable handler, arguments and interceptors"
            ) from error


@dataclass(frozen=True)
class CallbackFailure:
    message: str
    failure_type: str
    failure_class: str
    failure_code: int | None
    stack_trace: str
    non_retryable: bool


@dataclass(frozen=True)
class CallbackOutcome:
    value: Any = None
    failure: CallbackFailure | None = None


def _write_payload(directory: str, name: str, payload: Any) -> None:
    destination = Path(directory, name)
    temporary = destination.with_suffix(".writing")
    with temporary.open("wb") as stream:
        pickle.dump(payload, stream, protocol=pickle.HIGHEST_PROTOCOL)
    temporary.replace(destination)


def _read_payload(directory: str, name: str) -> Any:
    with Path(directory, name).open("rb") as stream:
        return pickle.load(stream)


def _callback(directory: str, channel: socket.socket) -> None:
    # Deserialize only in the process that is permitted to run application code.
    heartbeat_lock = threading.Lock()

    def heartbeat_request(details: dict[str, Any] | None) -> None:
        with heartbeat_lock:
            _write_payload(directory, "heartbeat", details)
            channel.sendall(b"H")
            if channel.recv(1) != b"Y":
                raise ActivityCancelled()

    async def heartbeat(details: dict[str, Any] | None) -> None:
        await asyncio.to_thread(heartbeat_request, details)

    async def invoke(invocation: CallbackInvocation) -> Any:
        _set_context(ActivityContext(info=invocation.info, client=cast(Any, None), heartbeat_callback=heartbeat))
        context = ActivityInterceptorContext(
            worker_id=invocation.info.worker_id, task_queue=invocation.info.task_queue,
            task=invocation.task, activity_type=invocation.info.activity_type, args=invocation.args,
        )

        async def call(ctx: ActivityInterceptorContext) -> Any:
            value = (invocation.handler(*ctx.args) if inspect.iscoroutinefunction(invocation.handler)
                     else await asyncio.to_thread(invocation.handler, *ctx.args))
            return await value if inspect.isawaitable(value) else value

        handler = call
        for interceptor in reversed(invocation.interceptors):
            next_handler = handler

            async def intercepted(
                ctx: ActivityInterceptorContext, *, interceptor: WorkerInterceptor = interceptor,
                next_handler: Callable[[ActivityInterceptorContext], Awaitable[Any]] = next_handler,
            ) -> Any:
                return await interceptor.execute_activity(ctx, next_handler)

            handler = intercepted
        try:
            return await handler(context)
        finally:
            _set_context(None)

    try:
        invocation = pickle.loads(Path(directory, "invocation").read_bytes())
        if not isinstance(invocation, CallbackInvocation):
            raise TypeError("invalid callback invocation")
        channel.sendall(b"B")
        try:
            _write_payload(directory, "outcome", CallbackOutcome(value=asyncio.run(invoke(invocation))))
        except BaseException as error:
            code = getattr(error, "code", None)
            _write_payload(directory, "outcome", CallbackOutcome(failure=CallbackFailure(
                message=str(error), failure_type=type(error).__name__,
                failure_class=f"{type(error).__module__}.{type(error).__qualname__}",
                failure_code=code if isinstance(code, int) and not isinstance(code, bool) else None,
                stack_trace=traceback.format_exc(), non_retryable=isinstance(error, NonRetryableError),
            )))
    finally:
        channel.close()


def _stop_owned_callback(callback: BaseProcess) -> None:
    if callback.is_alive():
        callback.terminate()
        callback.join(timeout=0.5)
    if callback.is_alive():
        callback.kill()
    # If the OS cannot reap it yet, retain the supervisor and ownership. Never
    # send stop evidence before an actual join, even after the owner disappears.
    callback.join()


def _supervise(directory: str, owner: socket.socket) -> None:
    context = multiprocessing.get_context("spawn")
    supervisor_channel, callback_channel = socket.socketpair()
    callback = context.Process(target=_callback, args=(directory, callback_channel))
    callback_started = False
    stopped = False
    try:
        callback.start()
        callback_started = True
        callback_channel.close()
        with selectors.DefaultSelector() as selector:
            selector.register(owner, selectors.EVENT_READ, "owner")
            selector.register(supervisor_channel, selectors.EVENT_READ, "callback")
            while True:
                for key, _ in selector.select(timeout=0.05):
                    message = cast(socket.socket, key.fileobj).recv(1)
                    if key.data == "owner":
                        if message in (b"", b"F"):
                            return
                        if message == b"S":
                            _stop_owned_callback(callback)
                            stopped = True
                            owner.sendall(b"X")
                        elif message in (b"Y", b"N") and not stopped:
                            supervisor_channel.sendall(message)
                    elif message in (b"B", b"H"):
                        owner.sendall(message)
                    elif message == b"":
                        selector.unregister(supervisor_channel)
                if not stopped and callback.exitcode is not None:
                    callback.join()
                    stopped = True
                    owner.sendall(b"D")
    except (BrokenPipeError, ConnectionResetError):
        pass  # Owner disappeared. The finally block still owns and reaps its callback.
    finally:
        if callback_started:
            _stop_owned_callback(callback)
            callback.close()
        callback_channel.close()
        supervisor_channel.close()
        owner.close()
        shutil.rmtree(directory, ignore_errors=True)


class SupervisedCallback:
    """One owner, one supervisor and one isolated application callback.

    The owner must serialize access to result()/stop(). Cancellation of result()
    leaves the process owned until stop() proves shutdown. A killed supervisor
    never supplies stop evidence. The worker integration remains a separate gate.
    """

    def __init__(self, invocation: CallbackInvocation) -> None:
        serialized = invocation.serialize()
        self.directory = tempfile.mkdtemp(prefix="dw-activity-")
        try:
            Path(self.directory, "invocation").write_bytes(serialized)
        except BaseException:
            shutil.rmtree(self.directory, ignore_errors=True)
            raise
        self._owner, self._supervisor_channel = socket.socketpair()
        self._owner.setblocking(False)
        self._supervisor = multiprocessing.get_context("spawn").Process(
            target=_supervise, args=(self.directory, self._supervisor_channel),
        )
        self._started = False
        self._closed = False
        self._joined = False
        self._exitcode: int | None = None
        self.stopped = False

    async def start(self) -> None:
        try:
            # Pass only the private directory and control socket to spawn. The
            # payload is already serialized, and this short start cannot leave
            # a background spawn thread untracked if the owner is cancelled.
            self._supervisor.start()
            self._started = True
            self._supervisor_channel.close()
            message = await asyncio.wait_for(self._receive(), timeout=5.0)
            if message != b"B":
                raise CallbackProcessLost("callback did not prove spawn readiness")
        except BaseException:
            await self.close()
            raise

    async def _receive(self) -> bytes:
        try:
            message = await asyncio.get_running_loop().sock_recv(self._owner, 1)
        except (ConnectionResetError, OSError) as error:
            raise CallbackProcessLost("callback supervisor disconnected") from error
        if not message:
            raise CallbackProcessLost("callback supervisor disconnected without stop evidence")
        return message

    async def _send(self, message: bytes) -> None:
        await asyncio.get_running_loop().sock_sendall(self._owner, message)

    async def result(self, heartbeat: Callable[[dict[str, Any] | None], Awaitable[None]]) -> CallbackOutcome:
        while True:
            message = await self._receive()
            if message == b"H":
                await heartbeat(_read_payload(self.directory, "heartbeat"))
                await self._send(b"Y")
            elif message == b"D":
                outcome = _read_payload(self.directory, "outcome")
                if not isinstance(outcome, CallbackOutcome):
                    raise CallbackProcessLost("callback did not produce a typed outcome")
                await self._finish()
                return outcome
            else:
                raise CallbackProcessLost("callback supervisor returned an unexpected result boundary")

    async def stop(self) -> None:
        if self.stopped:
            return
        async def wait_for_stop() -> None:
            while await self._receive() not in (b"D", b"X"):
                pass
            await self._finish()

        try:
            await self._send(b"S")
            await asyncio.wait_for(wait_for_stop(), timeout=5.0)
        except BaseException:
            await self.close()
            raise

    async def _finish(self) -> None:
        await self._send(b"F")
        await self.close()
        if self._exitcode != 0:
            raise CallbackProcessLost("callback supervisor did not confirm a clean join")
        self.stopped = True

    async def close(self) -> None:
        if self._joined:
            return
        if not self._closed:
            self._closed = True
            self._owner.close()
            self._supervisor_channel.close()
        if self._started:
            await asyncio.to_thread(self._supervisor.join, 5.0)
            if self._supervisor.is_alive():
                raise CallbackProcessLost("callback supervisor has not confirmed shutdown")
            self._exitcode = self._supervisor.exitcode
        else:
            shutil.rmtree(self.directory, ignore_errors=True)
        self._supervisor.close()
        self._joined = True
