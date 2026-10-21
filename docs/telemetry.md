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

Servers without the exact optional observability capability use independent
task traces. When available, a valid run origin becomes the task parent, while
task-bound dispatch and committed outcome contexts become links. Workers read
them after the actual poll identity and existing history/authority fences.
Insufficient claim identity omits propagation without changing business gates.
Retries have separate attempt spans, and replay emits no new attempt spans for
recorded activity results. Use the configured Server/SDK tuple qualified in
[Workflow #773](https://github.com/durable-workflow/workflow/issues/773).

For admission or commands outside worker execution, pass `telemetry=telemetry`
to `Client` and use its existing `await client.get_cluster_info()` discovery
during setup. Worker commands use the active interceptor. Only actual discovery
of the exact supported `worker_protocol.server_capabilities.observability_context`
object enables propagation. Unknown or malformed discovery disables it. No extra
business request, mandatory protocol floor or version-string inference is added.
Caller capture occurs before HTTP instrumentation starts; application arguments
and request keys retain their existing meaning.

Official W3C propagators validate headers. Baggage defaults empty; opt in with
`baggage_allowlist=("tenant",)`. Secondary links never carry baggage. Context and
link envelopes are limited to 4096 bytes, with at most 32 links. Invalid metadata
is ignored, with only fixed `discarded` or `filtered` span diagnostics.

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
