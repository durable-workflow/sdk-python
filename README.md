# Durable Workflow (Python SDK)

A Python SDK for the [Durable Workflow server](https://github.com/durable-workflow/server). Speaks the server's language-neutral HTTP/JSON worker protocol — no PHP runtime required.

Status: **Alpha**. Core features implemented: workflows, activities, schedules, signals, timers, child workflows, continue-as-new, side effects, version markers, and worker-applied accepted updates. Client calls for queries and updates exist; Python workflow-side query receiver metadata is available, while server-routed Python query execution and pre-accept update validator routing are still in progress. Full language-neutral protocol support for cross-PHP/Python orchestration is the release goal.

## Install

```bash
pip install durable-workflow
```

Or for development:

```bash
pip install -e '.[dev]'
```

## Quickstart

```python
from durable_workflow import Client, Worker, workflow, activity

@activity.defn(name="greet")
def greet(name: str) -> str:
    return f"hello, {name}"

@workflow.defn(name="greeter")
class GreeterWorkflow:
    def run(self, ctx, name):
        result = yield ctx.schedule_activity("greet", [name])
        return result

async def main():
    client = Client("http://server:8080", token="dev-token-123", namespace="default")
    worker = Worker(
        client,
        task_queue="python-workers",
        workflows=[GreeterWorkflow],
        activities=[greet],
    )
    handle = await client.start_workflow(
        workflow_type="greeter",
        workflow_id="greet-1",
        task_queue="python-workers",
        input=["world"],
    )
    await worker.run_until(workflow_id="greet-1", timeout=30.0)
    result = await client.get_result(handle)
    print(result)  # "hello, world"
```

For a fuller deployable example, see
[`examples/order_processing`](examples/order_processing), which runs a
multi-activity order workflow against a local server with Docker Compose.

## Activity retries and timeouts

Configure per-call activity retries and deadlines from workflow code:

```python
from durable_workflow import ActivityRetryPolicy

result = yield ctx.schedule_activity(
    "charge-card",
    [order],
    retry_policy=ActivityRetryPolicy(
        max_attempts=4,
        initial_interval_seconds=1,
        backoff_coefficient=2,
        maximum_interval_seconds=30,
        non_retryable_error_types=["ValidationError"],
    ),
    start_to_close_timeout=120,
    schedule_to_close_timeout=300,
    heartbeat_timeout=15,
)
```

Child workflow starts use the same retry policy shape and workflow-level
execution/run timeout names:

```python
from durable_workflow import ChildWorkflowRetryPolicy

receipt = yield ctx.start_child_workflow(
    "payment.child",
    [order],
    retry_policy=ChildWorkflowRetryPolicy(
        max_attempts=3,
        initial_interval_seconds=2,
        backoff_coefficient=2,
        non_retryable_error_types=["ValidationError"],
    ),
    execution_timeout_seconds=600,
    run_timeout_seconds=120,
)
```

## Workflow signals, queries, and updates

Signals mutate workflow state during replay:

```python
@workflow.defn(name="approval")
class ApprovalWorkflow:
    def __init__(self) -> None:
        self.approved = False

    @workflow.signal("approve")
    def approve(self, by: str) -> None:
        self.approved = True

    @workflow.query("status")
    def status(self) -> dict:
        return {"approved": self.approved}

    @workflow.update("set_approval")
    def set_approval(self, approved: bool) -> dict:
        self.approved = approved
        return {"approved": self.approved}

    @set_approval.validator
    def validate_set_approval(self, approved: bool) -> None:
        if not isinstance(approved, bool):
            raise ValueError("approved must be boolean")
```

The Python SDK records query and update receiver metadata on workflow classes,
exposes a query-state replay helper, and applies accepted updates on Python
workflow tasks by emitting `complete_update` or `fail_update` back to the
server. Query routing and synchronous pre-accept update validator execution are
still server-side follow-ups; use those paths only with deployments that
advertise support for the target workflow type.

Workers fingerprint registered workflow class definitions and advertise those
fingerprints during registration. Re-registering the same `worker_id` with a
changed class body for an already advertised workflow type raises immediately;
restart the worker process with a new id before serving changed workflow code.

## Features

- **Async-first**: Built on `httpx` and `asyncio`
- **Type-safe**: Full type hints, passes `mypy --strict`
- **Polyglot**: Works alongside PHP workers on the same task queue
- **HTTP/JSON protocol**: No gRPC, no protobuf dependencies
- **Codec envelopes**: Avro payloads by default, with JSON decode compatibility for existing history
- **Payload-size warnings**: Structured warnings before oversized workflow, activity, signal, update, query, or search-attribute payloads reach the server
- **Workflow definition guard**: Worker registration refuses same-id hot reloads when a workflow class definition changed
- **Worker interceptors**: Typed hooks around workflow tasks, activity calls, and query tasks for tracing, logging, and custom metrics
- **Metrics hooks**: Pluggable counters and histograms, with an optional Prometheus adapter

## Payload-size warnings

The SDK logs a structured warning before an encoded payload reaches 80% of the
default 2 MiB server payload limit. Warnings include context such as
`workflow_id`, `activity_name`, `signal_name`, `update_name`, `query_name`,
`payload_size`, `threshold_bytes`, and `limit_bytes` when those fields are
known at the call site.

Tune or disable the warning threshold on the client:

```python
client = Client(
    "https://workflow.example.internal",
    payload_size_limit_bytes=4 * 1024 * 1024,
    payload_size_warning_threshold_percent=75,
)

quiet_client = Client(
    "https://workflow.example.internal",
    payload_size_warnings=False,
)
```

## Authentication

For local servers that use one shared bearer token, pass `token=`:

```python
client = Client("http://server:8080", token="shared-token", namespace="default")
```

For production servers with role-scoped tokens, pass separate credentials for
control-plane calls and worker-plane polling:

```python
client = Client(
    "https://workflow.example.internal",
    control_token="operator-token",
    worker_token="worker-token",
    namespace="orders",
)
```

Create one client per namespace when your deployment issues namespace-scoped
tokens. The SDK sends the configured token as `Authorization: Bearer ...` and
the namespace as `X-Namespace` on every request.

## Metrics

Pass a recorder to `Client(metrics=...)` or `Worker(metrics=...)` to collect request, poll, and task metrics. The SDK ships a no-op default, an `InMemoryMetrics` recorder for tests or custom exporter loops, and `PrometheusMetrics` for deployments that install the optional extra:

```bash
pip install 'durable-workflow[prometheus]'
```

```python
from durable_workflow import Client, PrometheusMetrics

metrics = PrometheusMetrics()
client = Client("http://server:8080", token="dev-token-123", metrics=metrics)
```

Custom recorders implement `increment(name, value=1.0, tags=None)` and `record(name, value, tags=None)`.

## Worker interceptors

Use `Worker(interceptors=[...])` when instrumentation needs the task payload,
result, or exception around worker execution instead of only aggregate counters.
Interceptors run in list order; the first interceptor is the outermost wrapper.

```python
from durable_workflow import (
    ActivityInterceptorContext,
    ActivityHandler,
    PassthroughWorkerInterceptor,
    Worker,
)

class LoggingInterceptor(PassthroughWorkerInterceptor):
    async def execute_activity(
        self,
        context: ActivityInterceptorContext,
        next: ActivityHandler,
    ) -> object:
        print("activity started", context.activity_type)
        try:
            result = await next(context)
        except Exception:
            print("activity failed", context.activity_type)
            raise
        print("activity completed", context.activity_type)
        return result

worker = Worker(
    client,
    task_queue="python-workers",
    workflows=[GreeterWorkflow],
    activities=[greet],
    interceptors=[LoggingInterceptor()],
)
```

## Documentation

Full documentation is available at
[durable-workflow.github.io/docs/2.0/polyglot/python](https://durable-workflow.github.io/docs/2.0/polyglot/python):

- [Python SDK guide](https://durable-workflow.com/docs/2.0/polyglot/python)
- [API reference](https://python.durable-workflow.com/)

## Requirements

- Python ≥ 3.10
- A running [Durable Workflow server](https://github.com/durable-workflow/server)

## Compatibility

SDK version 0.2.x is compatible with servers that advertise these protocol
manifests from `GET /api/cluster/info`:

- `control_plane.version: "2"`
- `control_plane.request_contract.schema: durable-workflow.v2.control-plane-request.contract` version `1`
- `worker_protocol.version: "1.0"`

The top-level server `version` is build identity only. The worker checks these
protocol manifests at startup and fails closed when compatibility is missing,
unknown, or undiscoverable.

## Development

```bash
# Install dev dependencies
pip install -e '.[dev]'

# Run tests
pytest

# Run integration tests (requires Docker)
pytest -m integration

# Type check
mypy src/durable_workflow/

# Lint
ruff check src/ tests/

# Preview the API reference site locally
pip install -e '.[docs]'
mkdocs serve
```

The API reference is published to [python.durable-workflow.com](https://python.durable-workflow.com/) and rebuilt automatically on push to `main`.

## License

MIT
