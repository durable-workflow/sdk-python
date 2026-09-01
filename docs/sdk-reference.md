# Python SDK reference

This extended guide covers the complete public surface of the Durable Workflow
Python SDK for [Cloud](https://cloud.durable-workflow.com/) and
[self-hosted Server](https://github.com/durable-workflow/server). Start with
the [SDK portal](index.md) for the shortest runnable path.

Status: **Stable 2.0**. Core features include workflows, activities,
schedules, signals, timers, child workflows, continue-as-new, side effects,
version markers, worker-applied accepted updates, replay verification, the
in-process `WorkflowEnvironment` test harness, and invocable activity carriers.

Python workers execute server-routed query tasks after the Server advertises the query-tasks capability through cluster discovery.

## Install

```bash
pip install durable-workflow
```

Use a virtual environment and lock the resolved package version with the rest
of your application dependencies.

Or for development:

```bash
pip install -e '.[dev]'
```

## Quickstart

```python
import asyncio
from uuid import uuid4

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
    workflow_id = f"greet-{uuid4().hex}"
    async with Client(
        "http://server:8080",
        token="dev-token-123",
        namespace="default",
    ) as client:
        worker = Worker(
            client,
            task_queue="python-workers",
            workflows=[GreeterWorkflow],
            activities=[greet],
        )
        handle = await client.start_workflow(
            workflow_type="greeter",
            workflow_id=workflow_id,
            task_queue="python-workers",
            input=["world"],
        )
        await worker.run_until(workflow_id=workflow_id, timeout=30.0)
        result = await client.get_result(handle)
        print(result)  # "hello, world"

if __name__ == "__main__":
    asyncio.run(main())
```

Pass the server or managed-runtime base URL to `Client`; the SDK appends its
own `/api` routes. For example, use `http://server:8080`, not
`http://server:8080/api`. Managed-runtime paths such as
`https://cloud.example/api/runtime/v1/namespaces/acme` are valid as written.

A workflow ID is the durable identity of an instance, not a per-attempt request
ID. Starting the same ID again with the default `reject` duplicate policy raises
the typed `WorkflowAlreadyStarted` exception. Generate a unique ID for each new
instance, as the quickstart does. For an intentionally idempotent start, catch
`WorkflowAlreadyStarted` and reconnect with
`client.get_workflow_handle(workflow_id)`; choose `allow` or
`terminate_existing` only when creating another run or replacing the current
instance is the intended behavior.

For a fuller deployable example, see
[`examples/order_processing`](https://github.com/durable-workflow/sdk-python/tree/main/examples/order_processing), which runs a
multi-activity order workflow against a local server with Docker Compose.

## Schedule visibility and paging

`list_schedules()` returns one typed `ScheduleList` page. Status and workflow
type are exact server-side filters; the visibility query uses the server's
documented equality-predicate grammar. All filters combine with AND semantics.

```python
page = await client.list_schedules(
    status="active",
    workflow_type="orders.rollup",
    query='Region = "eu" AND Priority = 2',
    page_size=25,
)

while page.next_page_token is not None:
    page = await client.list_schedules(
        status="active",
        workflow_type="orders.rollup",
        query='Region = "eu" AND Priority = 2',
        page_size=25,
        next_page_token=page.next_page_token,
    )
```

Continuation tokens are opaque. Reuse them unchanged with the same namespace,
status, workflow type, and query; `None` terminates traversal. Invalid filters
and malformed, mismatched, cross-namespace, or stale tokens raise
`ScheduleListError`, which retains `status`, `reason()`, `field`, `errors`,
`last_safe_cursor`, and the complete server response in `body`.

## Retry policy scopes

Retry and timeout settings are scoped to the layer where you configure them:

- `TransportRetryPolicy` on `Client(...)` retries SDK HTTP requests only. It handles transient connection failures, request timeouts, 5xx responses, and 429 rate limits. It does not retry workflow code, activity code, child workflows, or failed workflow runs.
- `ActivityRetryPolicy` on `ctx.schedule_activity(...)` is recorded into durable history with that activity command. It controls server-side attempts for that one activity execution.
- `ChildWorkflowRetryPolicy` on `ctx.start_child_workflow(...)` is recorded with that child-start command. It controls server-side attempts for that child workflow execution.
- `non_retryable_error_types` belongs to durable activity/child retry policies. `non_retryable=True` on an activity failure bypasses the activity retry budget and surfaces the failure to the workflow.

Timeout names are also layer-specific. `start_to_close_timeout` limits one activity attempt, `schedule_to_start_timeout` limits queue wait before an activity starts, `schedule_to_close_timeout` limits the whole activity execution including retries, and `heartbeat_timeout` limits the gap between activity heartbeats. For child workflows, `execution_timeout_seconds` covers the overall child workflow execution and `run_timeout_seconds` covers one run.

## Activity failure payloads

When replay raises `ActivityFailed`, the top-level attributes expose the
stable cross-language fields: `activity_type`, `failure_category`,
`exception_type`, `message`, `non_retryable`, and `code`. The
`exception_payload` dictionary is filtered to language-neutral keys such as
`type`, `message`, `details`, `details_payload_codec`, and `non_retryable`.
Runtime diagnostics like PHP or Python exception classes, source file paths,
line numbers, and traces are not included by default unless the history event
contains an explicit `diagnostics` or `runtime_diagnostics` envelope.

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

## Deterministic parallel groups

Yield a list to schedule one durable parallel barrier. Lists can nest and mix
activities, child workflows, and timers. The worker flattens only the Server
commands, records a stable full `parallel_group_path` on every leaf, and
returns results in the original nested input shape regardless of terminal
delivery order:

```python
results = yield [
    ctx.schedule_activity("load-profile", [customer_id]),
    [
        ctx.start_child_workflow("quote-shipping", [customer_id]),
        ctx.start_timer(5),
    ],
]
profile, (shipping, _) = results
```

One failed activity or child is thrown at the list-yield point by durable input
position. Already recorded sibling completions remain replayable; late and
exact duplicate terminal deliveries do not change the selected result.

## Saga compensation

`ctx.saga()` registers ordinary activity commands as compensations and runs
them sequentially in reverse registration order after failure or cooperative
cancellation:

```python
def forward(saga):
    flight = yield ctx.schedule_activity("trip.reserve-flight", [])
    saga.add_compensation("trip.cancel-flight", [flight])

    hotel = yield ctx.schedule_activity("trip.reserve-hotel", [])
    saga.add_compensation("trip.cancel-hotel", [hotel])

    ctx.throw_if_cancellation_requested()
    yield ctx.schedule_activity("trip.charge", [])
    return {"status": "booked"}

return (yield from ctx.saga().run(forward))
```

Compensation stops at its first failure. `SagaCompensationFailed` retains the
initiating failure, compensation failure, activity type, and deterministic
registration order as structured fields.

## Nexus service calls

Workflow code can call a registered Nexus service operation through
`WorkflowContext.call_nexus_service(...)`. The worker executes the service
operation through the service-catalog API, records the response or typed
failure as a durable side-effect marker, and resumes replay from that marker
on subsequent workflow tasks.

```python
from durable_workflow import NexusOperationFailed

try:
    result = yield ctx.call_nexus_service(
        "greeter",
        "shared",
        "greet",
        ["Ada"],
        service_sdk_language="workflow-php",
    )
    print(result.service_call_id, result.result)
except NexusOperationFailed as exc:
    print(exc.service_call_id, exc.service_error_type, exc.typed_error_message)
```

The SDK assigns a deterministic idempotency key when one is not provided and
attaches the caller workflow instance id, caller run id, `sdk-python` caller
language, target service language, operation name, request payload,
service-call id, response or failure surface, and optional artifact metadata
to the recorded result.

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

The Python SDK records query and update receiver metadata on workflow classes.
Python workers poll server-routed query tasks, replay workflow state, execute
the declared query handler, and complete or fail each task back to the Server.
The Server must advertise
`worker_protocol.server_capabilities.query_tasks: true` from
`GET /api/cluster/info`; workers advertise `query_tasks` at registration only
after that discovery succeeds. `Client.query_workflow()` checks the same
manifest before sending a query and raises `RuntimeCapabilityUnsupported` or
`RuntimeDiscoveryUnavailable` with remediation when the route cannot be used.

Python workers advertise declared update validators and evaluate them on a
dedicated synchronous validation task before the Server records an accepted
update. Validation replays the authoritative workflow state without committing
commands or invoking the update handler. A validator-bearing worker refuses to
register unless Server discovery advertises the exact pre-accept validation
contract, so `wait_for="accepted"` means the declared validator has approved the
update. Rejections raise `UpdateRejected`; worker loss, timeout, incompatible
workers, and unsupported capability paths raise `UpdateValidationFailed` with
the Server's typed reason and retryability. `wait_for="completed"` additionally
waits for the accepted update handler to reach its terminal outcome.

Malformed signal and query payloads are reported as typed client errors with
the server's documented reason and status preserved:

```python
from durable_workflow import Client, QueryFailed, SignalFailed

client = Client("http://localhost:8080")

try:
    await client.signal_workflow("counter-1", "increment", args=["not-an-int"])
except SignalFailed as exc:
    assert exc.reason == "invalid_signal_arguments"
    assert exc.status == 422
    assert exc.validation_errors is not None

try:
    await client.query_workflow("counter-1", "current-at", args=["not-an-int"])
except QueryFailed as exc:
    assert exc.reason == "invalid_query_arguments"
    assert exc.status == 422
    assert exc.validation_errors is not None
```

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
queue caps, namespace caps, or downstream dispatch budget groups are
constraining throughput.

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

For CI and operator replay gates, the package also installs offline
verification commands:

```bash
durable-workflow-replay-verify tests/fixtures/golden_history \
  --workflows my_app.workflows:all_workflows \
  --output replay-report.json

durable-workflow-replay-verify exported-history-bundles \
  --simulate-bundles \
  --output replay-simulation.json

durable-workflow-history-bundle-verify exported-history-bundles/run-001.json \
  --output integrity-report.json
```

`durable-workflow-replay-verify` emits the same verdict and
`promotion_decision` vocabulary as the platform replay contract. Golden-history
mode replays cross-runtime fixtures against registered workflow classes;
`--simulate-bundles` integrity-checks every exported history bundle in a
directory and reports missing bundle evidence as a blocking result. Because
bundle simulation does not execute workflow code in Python, a clean
integrity-only simulation recommends `review_before_promote` rather than
`safe_to_promote`.

## Python conformance gate

The package includes the Python SDK published-artifact parity contract used by
host conformance runners:

```bash
durable-workflow-python-conformance --manifest --pretty
durable-workflow-python-conformance --host-evidence --pretty
durable-workflow-python-conformance --compose host-evidence.json --pretty > python-conformance-result.json
durable-workflow-python-conformance --evaluate python-conformance-result.json --pretty
```

The evaluator rejects smoke-only evidence. A passing record must include the
official CLI install/start/result path, cold first-user setup, concrete
artifact versions, protocol traces, a no-PHP-assumption audit, and the complete
Python capability table. Host runners can feed their raw published-artifact
observations to `--compose`; omitted parity cells become explicit
`not_covered` entries so the gate reports the remaining scenario or capability
instead of accepting a smoke-only result. The composer accepts canonical
snake_case IDs and runbook-style hyphenated IDs such as `server-up` and
`result-returned`, nested runner tables, resolved artifact/source aliases,
boolean `passed` cells, nested protocol trace planes, and no-PHP audit check
aliases. CLI result-path evidence should come from the actual published
commands that return terminal workflow output: `workflow:start --wait`,
`workflow:describe`, or `workflow:show-run --follow`.

## External payload storage

Large payload transport is automatic when the namespace runtime advertises the
authenticated external-payload capability. `Client` keeps small Avro payloads
inline, uploads larger encoded bytes through the runtime URL, and sends only an
opaque runtime-owned reference. Incoming references are fetched with the same
namespace and role credential, then size and SHA-256 are verified before Avro
decode. The bounded verified-byte cache reduces repeated replay fetches and
never deletes runtime-owned objects.

Managed Cloud applications do not configure a bucket, container, provider SDK,
provider credential, or provider URI parser. The ordinary client configuration
is sufficient for client operations and workers:

```python
from durable_workflow import Client

client = Client(
    "https://runtime.example",
    token=runtime_role_credential,
    namespace="billing",
)
```

The local filesystem, S3, GCS, and Azure Blob drivers remain available only as
explicit self-hosted integrations for runtimes that advertise acceptance of
direct provider references. Selecting one requires passing an
`external_storage` instance yourself; namespace discovery never constructs a
provider driver from the runtime's backing-storage identity. Those adapters
are not the managed Cloud contract and the SDK does not install their provider
libraries.

## Features

- **Async-first**: Built on `httpx` and `asyncio`
- **Type-safe**: Full type hints, passes `mypy --strict`
- **Polyglot**: Works alongside PHP workers on the same task queue
- **HTTP/JSON protocol**: No gRPC, no protobuf dependencies
- **Codec envelopes**: Avro is the sole workflow payload codec; JSON remains the HTTP document transport
- **External payload references**: automatic runtime-mediated upload/fetch with opaque references, typed failures, integrity verification, and a bounded cache; direct provider drivers remain explicit self-hosted integrations
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

The default Avro codec uses the fixed recursive
`durable_workflow.protocol.Value` schema and standard single-object framing.
It preserves `None`, booleans, signed 64-bit integers, finite doubles, bytes,
UTF-8 strings, lists, and dictionaries with string keys as distinct branches.
Unknown schema fingerprints fail with `unsupported_payload_schema`; the codec
never guesses or silently falls back to JSON.

Class-carrying values are not encoded with type metadata. Convert pydantic
models, attrs classes, dataclasses, pendulum values, `datetime` / `date` /
`time`, `UUID`, `Decimal`, and plain `Enum` values to explicit dictionaries or
scalars before passing them to the SDK. `IntEnum` and `StrEnum` encode because
they are Python scalar subclasses selected as Avro `LongValue` and
`StringValue`, but they decode as `int` and `str`.
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

For production servers with role-scoped tokens, keep worker and control
credentials in separate processes. A worker process needs only its worker
credential; the SDK uses it for cluster discovery, registration, polling,
heartbeats, and graceful deregistration:

```python
worker_client = Client(
    "https://workflow.example.internal",
    worker_token="worker-token",
    namespace="orders",
)
worker = Worker(worker_client, task_queue="orders", workflows=[OrderWorkflow])
```

A control process uses only its operator or admin credential:

```python
control_client = Client(
    "https://workflow.example.internal",
    control_token="operator-token",
    namespace="orders",
)
handle = await control_client.start_workflow(
    "order-workflow",
    task_queue="orders",
    workflow_id="order-123",
)
```

Create one client per namespace when your deployment issues namespace-scoped
tokens. The SDK sends the configured token as `Authorization: Bearer ...` and
the namespace as `X-Namespace` on every request. Scoped credentials are never
substituted across roles: `worker_token` authorizes only worker-plane requests,
and `control_token` authorizes only control-plane requests. Cluster discovery is
the explicit exception because the server permits both roles to inspect its
compatibility manifest. A client configured with only the opposite role's token
still fails before transport for actual worker or control operations; use
`token` when one shared credential intentionally authorizes both planes.

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

Full documentation is available at:

- [Python SDK guide](https://durable-workflow.com/docs/2.0/polyglot/python)
- [API reference](https://python.durable-workflow.com/)

## Requirements

- Python ≥ 3.10
- A running [Durable Workflow server](https://github.com/durable-workflow/server)

## Compatibility

Stable `2.x` releases follow semantic versioning. The SDK discovers runtime
capabilities at startup, and the server must advertise these protocol manifests
from `GET /api/cluster/info`:

- `control_plane.version: "2"`
- `control_plane.request_contract.schema: durable-workflow.v2.control-plane-request.contract` version `1`
- `auth_composition_contract.schema: durable-workflow.v2.auth-composition.contract` version `1`
- `worker_protocol.version: >=1.19,<2.0`
- `worker_protocol.external_task_input_contract.schema: durable-workflow.v2.external-task-input.contract` version `1`
- `worker_protocol.external_task_result_contract.schema: durable-workflow.v2.external-task-result.contract` version `1`

The top-level server `version` is build identity only. The worker checks these
protocol manifests at startup and fails closed when compatibility is missing,
unknown, or undiscoverable.

Carriers and support tooling can validate `auth_composition_contract` with
`parse_auth_composition_contract()` before resolving connection, namespace,
token, TLS, profile, and redacted effective-configuration diagnostics.

External task carriers can validate fixture artifacts from
`worker_protocol.external_task_input_contract.fixtures` with
`parse_external_task_input_artifact()` and parse leased task envelopes with
`parse_external_task_input()`.

They can also validate result fixture artifacts from
`worker_protocol.external_task_result_contract.fixtures` with
`parse_external_task_result_artifact()` and parse result envelopes with
`parse_external_task_result()`. The result parser exposes stable carrier
decisions for success, retryability, malformed output, cancellation, deadline
exceeded, handler crash, decode failure, and unsupported payload
codec/reference states without treating stderr as a machine signal.

Invocable activity carriers can use `InvocableActivityHandler` as a reference
adapter for HTTP or serverless runtimes. It accepts the same external-task input
envelope, invokes a registered activity handler, and returns the same
external-task result envelope while rejecting workflow-task inputs:

```python
from durable_workflow import InvocableActivityHandler

adapter = InvocableActivityHandler({"billing.charge-card": charge_card})
result_envelope = await adapter.handle(request_json)
```

Bridge adapters can hand bounded webhook ingress into the server through
`Client.send_webhook_bridge_event()`. The method returns the server's typed
bridge outcome for accepted, duplicate, and rejected events, including
machine-readable HTTP 422 rejection outcomes:

```python
outcome = await client.send_webhook_bridge_event(
    "pagerduty",
    action="signal_workflow",
    idempotency_key="pagerduty-event-3003",
    target={"workflow_id": "wf-remediation-42", "signal_name": "incident_escalated"},
    input={"severity": "critical", "service": "checkout"},
    correlation={"provider": "pagerduty", "event_type": "incident.triggered"},
)

if outcome.accepted:
    print(outcome.workflow_id, outcome.control_plane_outcome)
else:
    print(outcome.outcome, outcome.reason)
```

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
