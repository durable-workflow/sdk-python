# Cooperative cancellation source design

This describes the unfinished source candidate for shared cancellation issue 136.
The default worker protocol remains 1.19. Cooperation requires explicit protocol
1.20 opt-in and a compatible Server and Native backend. The shared specification
and published mixed-language qualification remain pending.

## Recorded context

After committed cancellation delivery, `WorkflowContext.cancellation_context`
provides the original immutable request object. The delivered `WorkflowCancelled`
exception carries the same object in `context`. Earlier workflow code sees
`None`. A poll or heartbeat observation does not expose future cancellation
metadata to earlier workflow execution.

```python
try:
    yield ctx.start_timer(60)
except WorkflowCancelled as cancelled:
    request = cancelled.context
    with ctx.cancellation_shield():
        yield ctx.schedule_activity("release-reservation", [
            request.root_request_id if request else None,
            request.reason if request else None,
        ])
    raise
```

`CancellationContext` includes local and root request IDs, root workflow instance
and run IDs, the immediate parent request ID, original reason, requester, source,
root request time, original cleanup deadline and ordered lineage. Requester
metadata is limited to caller type, ID and label. It is a read-only mapping and
lineage is a tuple of immutable `CancellationLineage` entries. Timestamp values
are immutable UTC dates. `to_dict()` returns a detached portable snapshot.

A child preserves the root request time and budget even when its local request
arrives later. Canonical request history supplies the context on cold replay,
while transport observation retains the opaque refresh route. The parser rejects
mismatched local request/run identities, cycles, invalid budgets and a delivery
that changes the accepted snapshot. Older cancellation histories without rich
context continue delivering cancellation with `context is None`.

## Remaining qualification

Rust context parity, portable operation policies, nested scopes and deterministic
remaining-time helpers still need completion. Remaining time must use the
replayed workflow clock. Do not subtract the host clock from the deadline in
workflow code. The runtime continues enforcing the original deadline and fencing
task and activity ownership.

Connected qualification must cover the PHP parent, Python child, Rust remote
activity and PHP local activity together. Callbacks must stop without application
heartbeats. A replacement after SIGKILL during cleanup must replay the same
boundary and finish before the original 30-second deadline. Record supported
workflow lease, heartbeat and repair settings with that scenario. Exact published
artifacts and one cascade inspection view remain required for release claims.
