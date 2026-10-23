# Testing

## Mock a Nexus service operation

This API is available in unreleased SDK source. Published packages gain these
registration methods with the next SDK release.

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
