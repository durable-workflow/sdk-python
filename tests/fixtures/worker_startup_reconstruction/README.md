# Historical worker startup reconstruction proposal

This is the unexecuted source/vector proposal for [issue
123](https://github.com/durable-workflow/sdk-python/issues/123), following
[General's historical reconstruction decision](https://github.com/durable-workflow/sdk-python/issues/123#issuecomment-6644072344).
It does not recover the missing original trace or provide an executable
experiment packet. The original failures remain failures. A new successful
reconstruction would establish behavior in its declared environment and leave
the original unexplained delay open.

`proposal.json` pins the available historical identities, existing observer
sources, six cases, budgets and remaining preparation gates. `vectors.json`
describes synthetic evidence and the limited conclusion each combination can
support. The vectors are review inputs, not captured observations or an
implemented classifier. Neither file is an execution manifest.

The first observer-source slice now labels metadata as complete, missing,
truncated or invalid, separates existing worker-history requests from
submissions, and reports HTTP-buffer and milestone-event loss. These are
availability/retention flags, not end-to-end phase verdicts. A complete JSON
metadata record does not prove task identity, hydration, handler admission or
publication. Additional clients remain grouped, and the before/after teardown
SQL/API comparator and full phase linkage are still pending. The pinned sources
in `proposal.json` remain the historical baseline; this revision is not a new
historical execution input or qualification receipt.

## Reuse the existing driver

Use `tests/integration/test_worker_task_families.py` and
`tests/worker_observation.py` from the exact source in `proposal.json`. Do not
copy the SDK source into the future test environment, install it editable,
subclass `Client`, or replace Worker registration, matching, retry or timeout
behavior. In the historical SDK, the query-poller thread requires
`type(client) is Client` to use its cloned client. HTTP observation patches
`httpx.AsyncClient.send` globally, so it also sees additional clients.

The current driver declares all six split/mixed/timer by run/run_until cases.
Keep the result and run_until budgets at 30 seconds, poll timeout at 0.5 seconds,
result polling at 0.1 seconds, run_until polling at 0.05 seconds, and both the
worker shutdown setting and runner drain wait at 10 seconds. The timer remains
two seconds. Run each case once, sequentially. Preserve an observed failure and
perform bounded cleanup without a retry or a replacement worker. Delayed replies
and recovery were separate historical experiments and are outside this proposal.

Static inspection of release source found the Client/Worker arguments,
interceptor types, task context fields and handle result API used by these
sources. This is source compatibility evidence only. Before any later SDK import,
the complete normally installed distribution must match the pinned wheel and
release source, with installed metadata and RECORD proof. A missing API or an
incompatible harness is a setup finding, not permission to patch the SDK.

## Preserve the actual observation boundaries

- The HTTP recorder counts discovery, registration, polls, submissions and
  deregistration. It records only allowlisted task identity and typed response
  metadata. It omits bodies over its size bound and does not consume a streamed
  response to inspect it. Missing metadata remains unknown.
- Its send duration includes response headers and ordinary body consumption.
  It cannot independently identify a header wait, body wait or backend delay.
  Any proposed extra boundary instrumentation needs its own reviewed source and
  controls before execution. Do not insert a proxy, reply delay or hidden retry.
- A present task in a poll response proves the observed response contained a
  task. It does not by itself prove handler admission or publication.
- `workflow_task_enter` is before `_load_workflow_claim_history` and input decode
  in the historical Worker. `workflow_handler_enter` is later. The intervening
  interval includes history loading, decode and replay. It is not a unique
  hydration-cause measurement.
- The activity interceptor runs after argument decode. An absent activity
  handler milestone does not by itself identify a decode failure.
- Submission metadata such as `recorded=false` must remain false. A returned
  handler, a sent request or a response without an authoritative recorded result
  does not prove durable publication. Match the original task/attempt/run against
  the Server comparator and terminal history.
- Existing primary HTTP clients have explicit labels; all other clients share
  the label `additional`. Capture their actual discovery/query-poll coverage,
  while retaining that limit on per-client attribution. A missing cloned-client
  trace cannot count as full coverage.

The driver now captures the same bounded observation before and after shutdown
on success and failure. Each capture has its own monotonic bracket; capture
failure is explicitly invalid. Reporting or shutdown failures cannot replace an
existing primary exception, and reporting failure still runs the original
stop/drain. Capture availability does not establish phase/SQL/API completeness.
No new probes, waits, timeout extensions or outer cleanup deadline are supplied
by this slice. Binding API/database snapshots to these boundaries and enforcing
the future ten-second overall cleanup bound remain pending. Return revised
source hashes for review rather than carrying the historical source pins.

## Server comparator review

Server's [canonical comparator response](https://github.com/durable-workflow/sdk-python/issues/123#issuecomment-6646942397)
establishes that claim commits before history fetch, and deregistration can
expire/repair the original lease and then delete its registration. Capture
read-only SQL for the exact original namespace/instance/run/task, including
task type/status, queue/compatibility, availability, lease times, attempt/repair
counters, dispatch times and error-present booleans; join run identity/status
and execution/run deadlines. Do not substitute the latest run or replacement
task. Compare private poll/claim/registration bindings internally and publish
only match/missing/invalid booleans. A registration row must also have current
incarnation, routing/capability compatibility and heartbeat freshness evidence.

Use only existing run-scoped GET describe/history for added API observation.
Do not add a history POST, heartbeat, poll, repair or other mutation as an
observer probe. Instrumentation can observe requests the driver already makes.
Require snapshots before teardown on success and failure, then a distinct
post-shutdown snapshot. Bracket reads with monotonic times and a DB/server time
sample; do not treat API and SQL views as globally atomic or clock-equivalent.
Snapshot reads have a proposed five-second bound inside the original case
window, never an extra five seconds of worker execution. If the read cannot
finish before the case/teardown boundary, mark it incomplete and clean up on
time. The future owner must enforce the ten-second overall cleanup boundary
around the existing stop/drain calls, not add their internal waits to the budget.

Report complete/missing/truncated/invalid for every required phase and snapshot.
An omitted task body, evicted observation, unmatched task/run/owner/attempt or
missing clone coverage cannot count as a completed phase. A cleanup repair
requires the original attempt binding, command/event identity and independently
bracketed cleanup path; nearby timestamps or RepairRequested alone do not prove
its cause. Attempt zero supports only no committed claim visible in that
snapshot; a committed claim without history delivery is a claim/delivery gap.

The original timer had attempt 1 and an observed `RepairRequested`. The source
shows that cleanup can create such a repair; the missing original
command/event/attempt/path proof does not establish that cleanup caused this
one. Post-shutdown ready state cannot prove it was never claimed. The original mixed
trace places its timeout in registration while waiting for response headers;
the missing wire trace does not establish why the reply was delayed. Do not
rewrite these facts as two unclaimed tasks or a single demonstrated cause.

API/history and database snapshots contain only this experiment's synthetic
identities and state. Exclude fixture authentication, request headers, payloads,
exception messages/stacks, arbitrary error text and SQL dumps. Review temporary
evidence before publishing bounded outcomes or hashes.

## Finish preparation before requesting execution

The original MySQL/Redis/runner image digests and host details were not retained.
Future compatible pins are explicitly reconstructed choices, never recovered
original identities. `proposal.json` leaves those pins and the dependency lock
unfilled. Do not resolve or download dependencies, retrieve images, import the
SDK or start a runtime from this source-authoring proposal.

The complete successor packet needs exact published image/platform digests,
an exact offline wheel closure, installed-package proof, revised observer and
snapshot source, a Server-reviewed comparator, and a reviewed bounded owner.
Proposed simultaneous caps are Server 1 CPU/768 MiB, MySQL 1 CPU/1 GiB, Redis
0.25 CPU/128 MiB and Python 1 CPU/512 MiB. These are planning ceilings, not a
capacity reservation. The owner must check fresh host/engine capacity and other
owners, use fixture-only credentials, isolate the database network, expose only
loopback if needed, bound PIDs/logs/state, and clean up all owned processes and
resources on exit, stop or deadline. Capture the proposed 900-second total
ceiling before setup and leave cleanup time inside it. No extension or automatic
retry is proposed.

Next owners: sdk-python prepares the revised sources and exact input packet;
Server reviews these vectors and the implementation of its comparator;
Security reviews the complete source, trust and
cleanup boundary; General decides any later exact invocation. The frozen 2.15
qualification, package/release decisions and all other stopped work remain
separate.
