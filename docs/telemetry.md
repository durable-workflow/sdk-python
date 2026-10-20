# Optional execution telemetry

Install `durable-workflow[telemetry]` for the optional OpenTelemetry API. Configure
your application's tracer provider, sampler and exporter before constructing
the interceptor. The ordinary SDK install does not require OpenTelemetry, a
collector, a logging integration or an exporter.

```python
import logging
from durable_workflow import Worker
from durable_workflow.telemetry import ExecutionTelemetry, LogContextFilter

# Configure your application-owned OpenTelemetry provider/exporter first.
telemetry = ExecutionTelemetry(namespace=client.namespace)
worker = Worker(client, task_queue="orders", workflows=[OrderWorkflow],
                activities=[charge], interceptors=[telemetry])
handler = logging.StreamHandler()
handler.addFilter(LogContextFilter())
logging.getLogger().addHandler(handler)
```

The interceptor scopes workflow/query execution and actual activity attempts.
Existing task metadata supplies bounded workflow, task and attempt identifiers.
`current_log_context()` and the standard logging filter expose the current
application child span's trace/span IDs. Application request IDs and messages
are preserved. Arguments, results, headers, exception text and baggage are not
recorded by this integration. These identifiers are not metric labels.

Each task currently starts a fresh trace. Server durable admission, queue and
cross-task carriers are not adopted by this component. Retries have separate
attempt spans, and replay does not emit new attempt spans for recorded activity
results. Full service-mode propagation remains tracked in Workflow #773.

Async tasks and worker-managed callback threads retain isolated execution
contexts; completion, cancellation and failures restore the previous context.
Application-created background work follows Python's normal context-copying
rules and needs an explicit empty context when it should be detached.

Supervised callbacks run in spawned processes. Pass an importable
`provider_factory="my_application.telemetry:make_provider"` to construct a fresh
provider there. Providers, exporter sockets and thread pools are omitted from
serialized invocations. A factory can also configure the child's global
provider for application HTTP/database instrumentation. A supplied parent
provider is used only in that process. A forked process without a factory uses
a no-op tracer instead of reusing inherited exporter sockets.

Use a synchronous processor and bounded exporter I/O when child spans must be
exported before callback process exit. This integration never flushes or shuts
down a shared provider per task; the application owns shutdown and bounded
drain. Export failures preserve business results, while abrupt termination may
lose telemetry. Spans do not provide exactly-once delivery guarantees.
