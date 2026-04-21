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

## Retry policy scopes

Retry and timeout settings are scoped to the layer where you configure them:

- `TransportRetryPolicy` on `Client(...)` retries SDK HTTP requests only. It handles transient connection failures, request timeouts, 5xx responses, and 429 rate limits. It does not retry workflow code, activity code, child workflows, or failed workflow runs.
- `ActivityRetryPolicy` on `ctx.schedule_activity(...)` is recorded into durable history with that activity command. It controls server-side attempts for that one activity execution.
- `ChildWorkflowRetryPolicy` on `ctx.start_child_workflow(...)` is recorded with that child-start command. It controls server-side attempts for that child workflow execution.
- `non_retryable_error_types` belongs to durable activity/child retry policies. `non_retryable=True` on an activity failure bypasses the activity retry budget and surfaces the failure to the workflow.

Timeout names are also layer-specific. `start_to_close_timeout` limits one activity attempt, `schedule_to_start_timeout` limits queue wait before an activity starts, `schedule_to_close_timeout` limits the whole activity execution including retries, and `heartbeat_timeout` limits the gap between activity heartbeats. For child workflows, `execution_timeout_seconds` covers the overall child workflow execution and `run_timeout_seconds` covers one run.

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

Use `yield ctx.wait_condition(lambda: self.approved, key="approved",
timeout=30)` to wait for signal- or update-mutated workflow state without
polling timers by hand. The SDK sends a stable predicate fingerprint with the
durable wait command and rejects replay if history records a different wait
key or predicate fingerprint, so condition changes fail visibly instead of
silently resolving a different wait.

Workers fingerprint registered workflow class definitions and advertise those
fingerprints during registration. Re-registering the same `worker_id` with a
changed class body for an already advertised workflow type raises immediately;
restart the worker process with a new id before serving changed workflow code.

Workers also advertise their local workflow and activity concurrency limits
during registration. Tune `max_concurrent_workflow_tasks` and
`max_concurrent_activity_tasks` on `Worker(...)` to align local semaphores with
the server's task-queue admission and operator visibility surfaces. Use
`Client.list_task_queues()` or `Client.describe_task_queue("orders")` to read
the server-side workflow, activity, and query-task admission status before
tuning those local limits:

```python
queues = await client.list_task_queues()
for queue in queues.task_queues:
    workflow_admission = queue.admission.workflow_tasks if queue.admission else None
    print(queue.name, workflow_admission.status if workflow_admission else "unknown")
```

The workflow and activity admission objects expose both queue-level and
namespace-level server budgets, including active lease caps and per-minute
dispatch-rate limits, so automation can detect whether local worker slots,
queue caps, or namespace caps are constraining throughput.

## Replay captured histories

Use `Replayer` to debug a captured history without connecting to a live server:

```python
from durable_workflow import Replayer

replayer = Replayer(workflows=[ApprovalWorkflow])
outcome = replayer.replay(history_export)

for command in outcome.commands:
    print(command)
```

`history_export` can be the server's event list or a dictionary with an
`events` key. When the history contains a `WorkflowStarted` event, the replayer
infers the workflow type and input from that event; otherwise pass
`workflow_type=` and `start_input=` explicitly. The returned `ReplayOutcome`
contains the commands the workflow would emit next, including determinism
failures surfaced as workflow failure commands.

## Features

- **Async-first**: Built on `httpx` and `asyncio`
- **Type-safe**: Full type hints, passes `mypy --strict`
- **Polyglot**: Works alongside PHP workers on the same task queue
- **HTTP/JSON protocol**: No gRPC, no protobuf dependencies
- **Codec envelopes**: Avro payloads by default, with JSON decode compatibility for existing history
- **Payload-size warnings**: Structured warnings before oversized workflow, activity, schedule, signal, update, query, or search-attribute payloads reach the server
- **Workflow definition guard**: Worker registration refuses same-id hot reloads when a workflow class definition changed
- **Deterministic workflow helpers**: `ctx.now()`, `ctx.random()`, `ctx.uuid4()`, and `ctx.uuid7()` replay from workflow state
- **Worker interceptors**: Typed hooks around workflow tasks, activity calls, and query tasks for tracing, logging, and custom metrics
- **Metrics hooks**: Pluggable counters and histograms, with an optional Prometheus adapter

## Payload-size warnings

The SDK logs a structured warning before an encoded payload reaches 80% of the
default 2 MiB server payload limit. Warnings include context such as
`workflow_id`, `workflow_type`, `activity_name`, `schedule_id`, `signal_name`,
`update_name`, `query_name`, `payload_size`, `threshold_bytes`, and
`limit_bytes` when those fields are known at the call site.

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

## Avro payload type boundaries

The default Avro codec uses a generic JSON wrapper so PHP, Python, and other
workers can exchange the same wire format. It preserves JSON-native values:
`None`, booleans, numbers, strings, lists, and dictionaries with string keys.

Class-carrying values are not encoded with type metadata. Convert pydantic
models, attrs classes, dataclasses, pendulum values, `datetime` / `date` /
`time`, `UUID`, `Decimal`, and plain `Enum` values to explicit dictionaries or
scalars before passing them to the SDK. `IntEnum` and `StrEnum` encode because
they are JSON scalar subclasses, but they decode as `int` and `str`.
`OrderedDict` decodes as a plain `dict`.

Use `to_avro_payload_value(...)` when a rich value should enter durable
history through the default Avro envelope:

```python
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from uuid import UUID

from durable_workflow import Client, to_avro_payload_value


class OrderStatus(Enum):
    PENDING = "pending"


@dataclass
class OrderInput:
    order_id: UUID
    placed_at: datetime
    amount: Decimal
    status: OrderStatus


order = OrderInput(
    order_id=UUID("12345678-1234-5678-1234-567812345678"),
    placed_at=datetime.now(timezone.utc),
    amount=Decimal("10.25"),
    status=OrderStatus.PENDING,
)

client = Client("http://server:8080", token="dev-token-123")
await client.start_workflow(
    "order-workflow",
    task_queue="orders",
    workflow_id="order-123",
    input=[to_avro_payload_value(order)],
)
```

The helper also accepts pydantic-style models with `model_dump(mode="json")`
and attrs-style classes. Rebuild domain objects explicitly inside workflows or
activities, for example `OrderInput(order_id=UUID(data["order_id"]), ...)`.
Adapter output is part of the durable history contract, so changing that shape
is a workflow compatibility change.

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
