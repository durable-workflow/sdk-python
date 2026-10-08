# Sticky execution

Sticky execution keeps a bounded, process-local copy of durable workflow history.
Enable it for a worker with `sticky_cache_capacity`. The default of zero leaves
the cache disabled. Workflow code still replays from durable history.

```python
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

`worker.sticky_cache_metrics()` reports hits, misses, evictions, forced cold
replays, retained entries and encoded history bytes.
