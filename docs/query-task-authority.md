# Query loading and execution deadlines

Query loading and replay consume the original query budget. They do not give a
worker extra time to invoke a handler. When the query lease expires, the worker
stops loading and suppresses the handler. Server still decides whether a result
can be accepted.

Servers advertising `query_task_authority=true` expose a non-renewing query
status API for protocol 1.20. The SDK checks the original claim while loading
and again after replay, immediately before invoking the handler. Missing,
malformed, denied or unavailable observations suppress execution.

To require that capability before registering a worker:

```python
worker = Worker(
    client,
    task_queue="orders",
    workflows=[OrderWorkflow],
    require_query_task_authority=True,
)
```

The default retains compatibility with older Servers and explicit protocol
1.19. This path checks the supplied lease and any original caller deadline
against the local clock, then charges elapsed time monotonically. It cannot
observe an earlier caller timeout that Server did not expose. Use synchronized
clocks, or require the newer authority capability when that limitation is
unacceptable. A missing or malformed lease cannot authorize execution.

An already-running synchronous Python handler cannot be safely interrupted.
Keep query handlers short and free of side effects. Expiry can cancel async
execution and payload I/O, but an observation never reserves future commit
authority. A result refused by Server does not restart the query or its budget.

This change is undergoing source and published-runtime qualification under
[#143](https://github.com/durable-workflow/sdk-python/issues/143).
