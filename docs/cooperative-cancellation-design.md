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

Candidate context v2 exposes immutable `scope_origin`, a
`ScopedCancellationContext` with the original root context and every scope
address in order. Its `deadline` is the originating scope's budget, while
`root_deadline` retains the original global deadline. The child context's own
`deadline` may be earlier when parent authority is narrower. The immediate
parent request names the last scope hop, including multiple scopes in one run.
The parser verifies that run lineage derives from the complete tree and refuses
changed metadata, repeated addresses, reentry into an earlier run or a larger
budget. Cold replay and `remaining()` retain the original narrowed authority.
Reading this metadata does not enable scope execution. Both v1 and v2 contexts
remain readable.

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

## Remote Activity policies

`ctx.schedule_activity()` accepts `cancellation_policy` as a `CancellationPolicy`
enum or its portable string value. Both command encoders preserve it. Omission
keeps the historical `TRY_CANCEL` behavior and wire shape.

```python
yield ctx.schedule_activity(
    "rust.remote-work", [],
    cancellation_policy=CancellationPolicy.WAIT_CANCELLATION_COMPLETED,
)
```

`TRY_CANCEL` requests cancellation and continues without waiting for the stop
receipt. `WAIT_CANCELLATION_COMPLETED` delays workflow delivery until the
original remote attempt's physical stop acknowledgment is recorded. `ABANDON`
leaves the Activity independent after parent cancellation. It requires a finite
positive integer `schedule_to_close_timeout`, whose original deadline bounds
the independent work. It does not extend the parent's cleanup budget.

Every explicit remote policy requires negotiated cooperation, protocol 1.20
and a compatible installed backend. Missing worker capability is diagnosed
before submission. Server also checks the original immutable claim. Prepared
local Activity policies use their separate admission contract below.

Replay compares authored policy with original canonical history for ordinary,
parallel and selection calls and cancellation delivery. Later events that omit
the policy retain the original value. Unknown, conflicting and changed policies
fail replay explicitly. Historical histories without a policy retain Try.

The connected Source tests include explicit Try and Wait with both async and
sync callbacks and no application heartbeats. Wait checks physical stop receipt
ordering before workflow delivery. Bounded Abandon checks that a callback
survives parent closure, completes under its original total lifetime and cannot
publish a second outcome or reopen the cancelled parent. The exact published
mixed-language gate remains required.

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
Prepared local callbacks use the same physical process ownership with separate
durable admission and receipts. Legacy worker protocol 1.19 continues using its existing execution.
Cooperating downstream systems still need idempotency or reconciliation for
effects already performed.

## Prepared sequential local callbacks

`ctx.local_activity()` accepts `cancellation_policy=CancellationPolicy.TRY_CANCEL`
or `CancellationPolicy.WAIT_CANCELLATION_COMPLETED`. Omission, including Python's
optional `None`, preserves historical Try behavior and omits the wire field.
Explicit policies require prepared execution and Server discovery of
`prepared_local_activity_cancellation_policies`. The Worker advertises its policy
consumer only for the discovered installed policies. Local `ABANDON` is refused
because a callback owned by this workflow worker cannot outlive that ownership
under the prepared contract.

```python
yield ctx.local_activity(
    "release-reservation", [],
    cancellation_policy=CancellationPolicy.WAIT_CANCELLATION_COMPLETED,
)
```

Wait parks cancellation delivery until the original local attempt's stop receipt
is recorded. Both supported policies physically stop and join the owned callback
before acknowledgment, without requiring application heartbeats. Neither grants
a new cleanup budget. Replay compares the original policy for completed and
unresolved calls, groups and the committed delivery boundary. Unsupported
policies refuse the entire authored group before callbacks or checkpoint
submission. Queries, updates and validators use the same negotiated replay
consumer. Explicit policies are unavailable through the legacy inline path.

The source candidate can explicitly request both `cooperative_cancellation` and
`prepared_local_activities` in Worker capabilities. Registration requires source
protocol 1.20 and actual Server discovery of its installed admission bridge.
The manifest advertises `durable_sequential_admission`. Ordinary parallel groups
add explicit `prepared_local_activity_groups` capability and require the Server's
installed atomic admission bridge. Their manifest advertises
`durable_atomic_all_admission`. Selection and turn-closing waits remain refused.

Replay captures the authored local call and sequence before application code
runs. Earlier side effects, version markers and metadata commands obtain a
retained-claim checkpoint, followed by canonical history refresh. The Server
then creates the local execution and original attempt. The worker validates its
workflow claim, epoch, owner, backend IDs, nonce, fixed deadlines and cleanup
authority before spawning. Native owns retries, backoff and execution timeouts.

Independent control renews ownership without creating application heartbeat
history or advancing its timeout. Only a real callback heartbeat may do those
things. Requests, retries, payload transfer and result encoding share the
original conservative authority budget. A cancellation fence physically stops
and joins callback and supervisor before the stop receipt. An unconfirmed stop
retains workflow capacity and prevents successful worker deregistration.

Canonical outcome history supplies the next replay value. A lost or malformed
receipt abandons the claim. Cold replay skips completed callbacks and requests
Native recovery of unfinished Started attempts. Recovery records unknown stop
and may release the claim for a durable retry, without claiming that the
replacement observed the original callback stop.

Cleanup local calls require a shield after canonical delivery. Admission and
control preserve its original local request ID, root ID, delivery history event
ID and deadline. No replacement or duplicate request grants a fresh cleanup
budget. Connected exact-source qualification remains separate from publication.

## Prepared local parallel groups

An ordinary list may contain local activities, remote activities, children,
timers and nested lists, with at most 100 total leaves. The worker checkpoints
the complete authored batch atomically, including every nested position. It
validates all opening history and each canonical local execution identity before
preparing callbacks. Every local admission must validate before any callback
spawns. Earlier metadata and side effects use a separate retained checkpoint.

Callbacks execute concurrently with independent authority observation. Native
owns outcome history, deadlines, retries and unknown-stop recovery. A retry,
receipt loss or authority loss stops and joins siblings before returning the
claim. Cancellation joins the entire group before workflow delivery or cleanup
replay. A stop acknowledgment proves its own callback physically joined, or
that no callback spawned. An unconfirmed join retains workflow capacity and
worker registration.

Cold replay preserves completed siblings and recovers only unfinished Started
attempts. A durable retry must release the original claim before new admission.
Results preserve authored nested positions despite settlement order. A cleanup
group requires a shield after canonical delivery and every local member retains
the same original root, delivery event and immutable deadline.

## Candidate scope delivery

The explicit scope source opt-in supports scalar calls and fully admitted flat
or nested all-groups. One ancestor delivery restores each included descendant's
accepted request and context as workflow code unwinds. Shielded branches remain
unaffected. Cleanup timers in any included scope bind that scope's own request,
the original ancestor delivery and preparation, and its narrower authority
ceiling. Replacement replay retains the original clock and immutable metadata.
Invalid snapshots fail before application construction. Pending ancestor requests
retain their original authored boundary without entering cleanup.

Original-claim preparation and delivery use complete canonical history pages.
Run cancellation composes with scoped delivery. Scalar prepared local or timer
cleanup can replay before root delivery, keeping the original root identity,
deadline and narrower scope ceiling. Root cleanup begins after leaving the scope.
Physical workflow-worker SIGKILL during prepared scoped cleanup is qualified
with fresh replacement replay and callback loss before the original deadline.

Incomplete groups, local or selected scope groups, subtree local callbacks,
competing roots and overlapping deliveries remain gated.
This profile defaults off and does not advertise general scope execution.
Source qualification is separate from the published acceptance scenario.

## Remaining qualification

### Python deadline and remaining time

The Source `CancellationContext.deadline` is the original immutable cleanup
deadline. `remaining()` returns fractional seconds left at the replay boundary
consumed by workflow code, clamped to zero. Committed cancellation delivery sets
the initial clock. Blocking activity, prepared local activity, child, timer,
condition, selection and awaited-handle outcomes advance it. A selection uses
its committed winner marker. A group failure excludes later sibling outcomes.
Recorded clock skew cannot increase an already consumed budget.

Synchronous side effects, version markers, memo updates and inline local
callbacks preserve that clock because they return before their results are
persisted on first execution. Their later history timestamps cannot change
the same authored decision during cold replay. `WorkflowContext.now()` retains
its existing start-time contract.

Only the active replay that delivered the context can use its remaining-time
clock. Detached metadata and calls after replay ends fail explicitly. Missing
or invalid boundary timestamps also fail, without a host-time fallback.
The runtime supervisor independently enforces the actual deadline and task
ownership even when authoring code cannot run.

Connected process-loss scenarios record remaining time before SIGKILL and
require the same value and metadata in the replacement worker. Legacy inline
cleanup preserves that value through result persistence. Sequential and atomic
prepared cleanup, each under an original 30-second deadline, check the final
value against the committed completion timestamp and original deadline.

Rust helpers, explicit local operation policies, nested scopes and competitive
qualification still need completion.

Connected qualification must cover the PHP parent, Python child, Rust remote
activity and PHP local activity together. Callbacks must stop without application
heartbeats. A replacement after SIGKILL during cleanup must replay the same
boundary and finish before the original 30-second deadline. Record supported
workflow lease, heartbeat and repair settings with that scenario. Exact published
artifacts and one cascade inspection view remain required for release claims.
