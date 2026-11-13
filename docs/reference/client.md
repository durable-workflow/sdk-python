# Client

## Read a query result

`Client.query_workflow()` returns the Server's receipt. Read its `result`
member for the decoded value returned by the query handler:

```python
receipt = await client.query_workflow("approval-1", "status")
status = receipt["result"]
print(status["approved"])
```

The receipt also identifies the workflow, run and query. A successful handler
may return `None`; that value appears in `receipt["result"]`. Query failures
raise `QueryFailed`. `WorkflowHandle.query()` and the synchronous client
return the same receipt shape.

## Read an update outcome

`Client.update_workflow()` returns a Server receipt. Its default wait stage is
`accepted`: the handler may still be running when the call returns. Keep the
receipt's `update_id` to read the result later without submitting another update.

```python
from durable_workflow import UpdateStillPending

receipt = await client.update_workflow(
    "order-1", "rename", args=["new name"], wait_for="accepted",
)
update_id = receipt["update_id"]

try:
    result = await client.update_outcome("order-1", update_id, wait_timeout_seconds=30)
except UpdateStillPending as pending:
    update_id = pending.update_id
    print(f"Update {update_id} is still running; read its outcome again later.")
else:
    print(result)  # A completed handler may return None.
```

`update_outcome()` waits for completion by default. A pending update raises
`UpdateStillPending`, carrying the workflow, run, update and request identities.
It never returns pending work as `None`. To inspect the raw receipt without
waiting for completion, call `update_outcome_receipt()`; pass
`wait_for="completed"` to wait there too. Reads follow an update across
continue-as-new and never admit another update.

| Outcome | Result |
| --- | --- |
| Completed | The decoded handler result, including `None` |
| Still running | `UpdateStillPending`; keep its `update_id` and read again |
| Handler failed or validator rejected | `UpdateFailed` or `UpdateRejected` |
| No retained update with this id | `UpdateNotFound`, including after pruning |
| Response does not identify the requested update or a recognized status | `UnrecognizedUpdateOutcome` |

Both read methods require Server discovery to advertise
`capabilities.update_outcome_reads`. An unsupported Server raises
`RuntimeCapabilityUnsupported`; it does not fall back to resubmitting the update.
`WorkflowHandle` and `durable_workflow.sync.Client` provide the same methods.
See the [complete update reference](../sdk-reference.md#workflow-signals-queries-and-updates)
for wait stages, validation and response checks.

## Trust a private certificate authority

The `tls_context` option requires Python SDK 2.10.0 or later.

Pass a verifying `ssl.SSLContext` to configure trust for this client without
changing trust for other clients in the process. Start with certifi's public
roots, then add your private CA:

```python
import ssl

import certifi

from durable_workflow import Client

context = ssl.create_default_context(cafile=certifi.where())
context.load_verify_locations(cafile="company-ca.pem")

client = Client("https://workflow.example.internal", tls_context=context)
```

The synchronous `durable_workflow.sync.Client` accepts the same option. Worker
query clients and Nexus namespace views reuse the context. Control and worker
requests, including runtime-owned external payload upload and fetch, use the
same configured transport. Separately configured storage drivers own their
storage-service transports.

An explicit context replaces httpx's default root selection. It does not
automatically add public roots: the example above keeps them by loading
certifi before adding the private CA. Contexts with disabled certificate or
hostname verification are rejected. Treat the supplied context as read-only
after client construction.

With no explicit context, the existing httpx behavior remains: trust comes
from the installed certifi bundle, or from `SSL_CERT_FILE` / `SSL_CERT_DIR`
when those environment variables are set. Installing a CA only into the OS
store does not update certifi. `SSL_CERT_FILE` replaces the root bundle for
httpx clients throughout the process; a private-CA-only file can therefore
remove public trust. Prefer a per-client context when applications need
different trust configurations. An explicit context takes precedence over
those environment root settings.

`Client.execute_nexus_operation(...)` calls a registered Nexus service
operation through the service-catalog API and returns `NexusOperationResult`.
Workflow authors should normally yield `ctx.call_nexus_service(...)`; the
worker submits a leased service command and resumes from durable service
history. The client method is available for calls outside workflow execution.

## Backfill windows

`Client.backfill_schedule()` includes the start time and excludes the end time:
`[start_time, end_time)`. An occurrence exactly at `end_time` is excluded.

Adjacent windows share a boundary without overlapping it:

```python
await client.backfill_schedule(
    "daily-orders",
    start_time="2026-03-08T07:00:00Z",
    end_time="2026-03-08T07:30:00Z",
)
await client.backfill_schedule(
    "daily-orders",
    start_time="2026-03-08T07:30:00Z",
    end_time="2026-03-08T08:00:00Z",
)
```

An occurrence at `07:30:00Z` belongs to the second window. Repeating a window
does not start another workflow for an occurrence already admitted by that
schedule.

::: durable_workflow.client
