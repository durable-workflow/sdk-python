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

## Child policies

`CancellationPolicy` and `ParentClosePolicy` are available from the package
root. Child commands accept these enums or their portable string values.

```python
yield ctx.start_child_workflow(
    "python.child",
    [],
    cancellation_policy=CancellationPolicy.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy=ParentClosePolicy.REQUEST_CANCELLATION,
)
```

`TRY_CANCEL` requests child cleanup and delivers parent cancellation without
waiting. `WAIT_CANCELLATION_COMPLETED` parks the parent until the child has a
recorded terminal outcome, releasing its task claim for other work. `ABANDON`
leaves the child independent and preserves the historical default. Parent
closure is a separate choice. `REQUEST_CANCELLATION` uses genuine cooperative
cleanup with the original lineage and budget. `REQUEST_CANCEL` retains legacy
terminal behavior. `TERMINATE` and `ABANDON` retain their existing meanings.

Both command encoders preserve the policies. Cold replay compares them for
ordinary calls, parallel groups, selections and cancellation delivery. Omitted
historical fields mean the original `ABANDON` defaults. Later events with
missing fields keep the scheduled snapshot. Changed options and invalid or
conflicting history fail replay. A worker without the negotiated cooperation
capability refuses these choices before completion, reporting its identity and
the required protocol. Server also checks the immutable task claim and backend.

## Remote callback-stop transport

`Client.acknowledge_activity_cancellation()` reports the original task, activity
attempt, lease owner and cancellation request. It requires explicit worker
protocol 1.20 and validates the Server's original receipt identity. Duplicate
retries retain those identities and share a five-second transport budget.
Refusals preserve the Server diagnostic. This receipt does not renew authority,
record an application heartbeat or extend the cleanup deadline.

The cooperative remote worker stops and joins its callback and supervisor
before sending this receipt. Each attempt gets an explicit
spawn context, an independent supervisor, a callback process and private payload
files. Single-byte control channels keep owner disconnect independent of a
payload transfer or callback progress. The supervisor joins the callback before
reporting stop. The owner then joins the supervisor. A failed supervisor alone
does not prove a live callback stopped.

The process tests cover a C call holding the callback interpreter's
GIL, ignored TERM followed by forced stop, actual owner SIGKILL, typed results,
application failure metadata, interceptors and authored heartbeats. The worker
observes ownership independently of callback progress, checks before result
encoding or failure reporting, and reports only the original canonical request
after confirmed stop. Unconfirmed stop retains activity capacity and refuses
successful worker shutdown. A receipt refusal cannot become result publication
or a new cleanup budget. Connected exact-source qualification remains required.

After application drain expires, cooperative remote shutdown permits up to
15 seconds for process reaping and bounded receipt transport. Application work
is stopped during this phase. The run's original cancellation deadline stays
unchanged. Failure to confirm stop leaves the worker registration active and
raises an explicit shutdown error.

Handlers, arguments and interceptors must be compatible
with Python's spawn serialization. Define importable handlers and protect the
application entry point with `if __name__ == "__main__"`. Captured memory changes
are local to the callback process. Open process-local connections in the
callback. Registration and remote polling refuse incompatible handler or
interceptor definitions before claiming work, naming the worker and activity.
Cooperative local callbacks still require their separate supervision and receipt
model. Legacy worker protocol 1.19 continues using its existing execution.
Cooperating downstream systems still need idempotency or reconciliation for
effects already performed.

## Remaining qualification

Rust policy parity, portable activity policies, nested scopes and deterministic
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
