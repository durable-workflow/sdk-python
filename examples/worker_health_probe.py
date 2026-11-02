"""Serve worker readiness and liveness probes next to a long-running worker.

Run against a Server:

    DURABLE_WORKFLOW_SERVER_URL=http://localhost:8080 \\
    DURABLE_WORKFLOW_AUTH_TOKEN=dev-token-123 \\
    python examples/worker_health_probe.py

Then probe it:

    curl -i http://127.0.0.1:8081/readyz
    curl -i http://127.0.0.1:8081/livez

The probe server runs on its own thread. ``Worker.status()`` performs no
network I/O and does not need the worker's event loop, so ``/livez`` still
answers, with 503, when that loop is stalled. Bind the probe port only to the
pod or host network your orchestrator uses; the response carries worker
identity and progress, not payloads or credentials.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from durable_workflow import Client, Worker, activity, workflow


@activity.defn(name="greet")
def greet(name: str) -> str:
    return f"hello, {name}"


@workflow.defn(name="greeter")
class GreeterWorkflow:
    def run(self, ctx, name):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("greet", [name])
        return result


def start_probe_server(worker: Worker, host: str, port: int) -> ThreadingHTTPServer:
    class ProbeHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server API
            status = worker.status()
            if self.path == "/readyz":
                healthy = status.ready
            elif self.path == "/livez":
                healthy = status.live
            elif self.path == "/status":
                healthy = True
            else:
                self.send_error(404)
                return
            body = json.dumps(status.as_dict()).encode()
            self.send_response(200 if healthy else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - http.server API
            return

    server = ThreadingHTTPServer((host, port), ProbeHandler)
    threading.Thread(target=server.serve_forever, name="worker-health-probe", daemon=True).start()
    return server


async def main() -> None:
    async with Client(
        os.environ.get("DURABLE_WORKFLOW_SERVER_URL", "http://localhost:8080"),
        token=os.environ.get("DURABLE_WORKFLOW_AUTH_TOKEN"),
        namespace=os.environ.get("DURABLE_WORKFLOW_NAMESPACE", "default"),
    ) as client:
        worker = Worker(
            client,
            task_queue=os.environ.get("DURABLE_WORKFLOW_TASK_QUEUE", "python-workers"),
            workflows=[GreeterWorkflow],
            activities=[greet],
            # Keep this below the orchestrator's termination grace period.
            shutdown_timeout=25.0,
        )
        probe = start_probe_server(
            worker,
            os.environ.get("DW_HEALTH_HOST", "127.0.0.1"),
            int(os.environ.get("DW_HEALTH_PORT", "8081")),
        )
        shutdown_requested = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, shutdown_requested.set)

        run_task = asyncio.create_task(worker.run())
        try:
            done, _ = await asyncio.wait(
                {run_task, asyncio.create_task(shutdown_requested.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            try:
                await worker.stop()
            finally:
                try:
                    await run_task
                finally:
                    probe.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
