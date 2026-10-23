# Client

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
