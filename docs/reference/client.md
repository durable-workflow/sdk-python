# Client

`Client.execute_nexus_operation(...)` calls a registered Nexus service
operation through the service-catalog API and returns `NexusOperationResult`.
Workflow authors should normally yield `ctx.call_nexus_service(...)`; the
worker uses this client method to execute once and record the durable result
or typed service failure.

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
