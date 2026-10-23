# Testing

## Test failures, retries and compensation

This failure/retry behavior and callable child mocks require Python SDK
2.10.0 or later. Earlier packages let activity mock exceptions escape and do
not provide these child callables or retry observations.

Synchronous mock exceptions become terminal history events. The production
replayer then raises `ActivityFailed` or `ChildWorkflowFailed` at the authored
yield, so the workflow's handlers and compensation code run normally:

```python
from durable_workflow import workflow
from durable_workflow.errors import ActivityFailed
from durable_workflow.testing import WorkflowEnvironment
from durable_workflow.workflow import ActivityRetryPolicy


@workflow.defn(name="checkout-test")
class Checkout:
    def run(self, ctx):
        yield ctx.schedule_activity("reserve", [])
        try:
            return (yield ctx.schedule_activity(
                "charge", [], retry_policy=ActivityRetryPolicy(max_attempts=3)
            ))
        except ActivityFailed as error:
            yield ctx.schedule_activity("release", [])
            return {"compensated": True, "failure_type": error.exception_type}


def decline():
    raise ValueError("payment unavailable")


env = WorkflowEnvironment()
env.register_activity_result("reserve", None)
env.register_activity("charge", decline)
env.register_activity_result("release", None)
assert env.execute_workflow(Checkout) == {
    "compensated": True, "failure_type": "ValueError"
}
assert env.mock_calls[1].attempt_count == 3
assert env.mock_calls[1].status == "failed"
```

Each logical call invokes its mock once per attempt. Later workflow replays
reuse its final history event and do not invoke the old mock again. The last
attempt's error reaches the workflow when its retry budget is exhausted.
No configured policy means one attempt, matching the external-worker default.
Explicit `ActivityRetryPolicy`, `ChildWorkflowRetryPolicy` and mapping policies
use their positive integer `max_attempts`. Non-retryable type lists match
either a short error name or its qualified Python class name.

An activity mock raising `NonRetryableError` stops immediately with
`ActivityFailed.non_retryable=True`. Its reported type is the outer
`NonRetryableError`; a supplied cause does not replace that type. Child
exceptions follow the existing workflow failure mapper. `ChildWorkflowFailed`
exposes the child class/message; the published child-history contract does not
carry an activity-style `non_retryable` field. Use the child retry policy's
type exclusions to stop retries for a named child error.

Register a child callable with
`env.register_child_workflow("credit-check", check_credit)`. It receives the
child arguments as positional arguments and can return a value or raise an
ordinary exception. `register_child_workflow_result()` still registers a
canned success. A later child result or callable registration replaces the
previous one. Missing mocks remain actionable `KeyError` fixture errors.

`env.mock_calls` contains `MockCallRecord` objects in logical execution order,
with `kind`, `name`, `run_number`, `attempt_count`, `status` and the last
observed `failure`. These observations span a continue-as-new chain and reset
on the next `execute_workflow()` call. They are test records, separate from
durable history. A later successful attempt keeps the last observed failure
available while setting the call's status to `completed`.

`WorkflowEnvironment(mock_attempt_limit=100)` caps each logical call at 100
actual attempts by default. If more retries remain, the harness raises
`RuntimeError` with the last callback error as its cause and records
`attempt_limit_exceeded`. It does not deliver a fabricated exhausted failure
to the workflow. Raise the cap deliberately for a larger bounded test.

Retry backoff does not sleep or advance a virtual clock. Mocks must be
synchronous callables. They do not test durable queue scheduling, actual
retry delays, timeout/lease enforcement, worker processes, cancellation or
backend recovery; qualify those behaviors with a real Server and workers.

## Mock a Nexus service operation

These Nexus mock registration methods require Python SDK 2.10.0 or later.

Register a result or a synchronous callable by endpoint, service and operation
name. The mock runs once for each authored call, even when later workflow tasks
replay that call. A later registration for the same three names replaces the
previous mock.

```python
from durable_workflow import workflow
from durable_workflow.errors import NexusOperationFailed
from durable_workflow.testing import WorkflowEnvironment


@workflow.defn(name="create-invoice")
class InvoiceWorkflow:
    def run(self, ctx, customer):
        try:
            operation = yield ctx.call_nexus_service(
                "billing", "invoices", "create", [customer]
            )
            return operation.result
        except NexusOperationFailed as error:
            return {"refused": error.service_error_type}


env = WorkflowEnvironment()
env.register_nexus_operation_result(
    "billing", "invoices", "create", {"id": "invoice-1"}
)
assert env.execute_workflow(InvoiceWorkflow, "customer-1") == {"id": "invoice-1"}


def decline(customer):
    raise NexusOperationFailed(
        "Payment declined", service_error_type="PaymentDeclined", reason="insufficient_funds"
    )


env.register_nexus_operation("billing", "invoices", "create", decline)
assert env.execute_workflow(InvoiceWorkflow, "customer-1") == {"refused": "PaymentDeclined"}
```

Callable mocks receive the operation arguments as positional arguments. An
ordinary exception becomes `NexusOperationFailed`, with the original exception
class in `service_error_type` and its message preserved. Raising
`NexusOperationFailed` directly also preserves its reason and typed failure
metadata. Workflow code can catch the failure and yield compensation commands.
An unmocked operation raises an actionable `KeyError` from the harness.

`call_nexus_service()` normally waits for completion and returns a
`NexusOperationResult` whose `result` contains the mocked value.
`start_nexus_operation()` normally waits for acceptance: it returns an accepted,
started operation with a synthetic service-call ID and no completed result.
Explicit acceptance waits and asynchronous modes follow the same distinction.

These mocks run in-process without service endpoints or network requests.
They do not test namespace authorization, delayed remote completion, service
retries, timeouts or cancellation. Use a real Server and service workers for
those checks.

::: durable_workflow.testing
