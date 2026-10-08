# Sticky execution

Use Python SDK 2.5.0 and Server 2.5.10 or newer for this feature.

Sticky execution keeps a bounded, process-local copy of durable workflow history.
Enable it for a worker with `sticky_cache_capacity`. The default of zero leaves
the cache disabled. Workflow code still replays from durable history.

```python
from durable_workflow import Worker

worker = Worker(
    client,
    task_queue="orders",
    workflows=[OrderWorkflow],
    sticky_cache_capacity=100,
    sticky_cache_max_bytes=16 * 1024 * 1024,
    sticky_cache_ttl_seconds=300,
)
```

The byte limit covers retained encoded history. It does not limit the memory
needed to decode or replay a workflow. Oversized histories run without being
retained. Entries expire and the least recently used entry is evicted when
either capacity limit is reached.

Cache eviction, expiry, another build and worker replacement use cold replay.
The worker validates the cached prefix before using it and fetches authoritative
history on a mismatch. Application correctness must never depend on the cache.

For histories spanning several pages, the worker can reuse a Server-issued page
cursor and download the tail after validating the inline prefix. Short histories
still arrive with the task. This cache reduces repeated history downloads. It
does not skip deterministic replay or promise a throughput increase.

`worker.sticky_cache_metrics()` reports hits, misses, evictions, forced cold
replays, retained entries and encoded history bytes.

## Run an example

With a local Server and the SDK installed, run the
[complete example](https://github.com/durable-workflow/sdk-python/blob/main/examples/sticky_execution.py):

```bash
SERVER_URL=http://localhost:8080 WORKFLOW_TOKEN=dev-token python examples/sticky_execution.py
```

It records a side effect, waits for two timers and prints the cache counters.
Shutting down clears retained history. The recorded result remains in Server.
