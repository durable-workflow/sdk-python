# Cooperative cancellation source qualification

The published SDK defaults to worker protocol 1.19. These cases explicitly select
the candidate 1.20 protocol and require compatible Server capability discovery.

Run the `CI` workflow on the candidate branch with `cooperative_qualification=true`
and `server_commit` set to the exact 40-character public Server candidate SHA.
The workflow checks out that source, builds an isolated MySQL/Redis Server stack,
runs the integration suite, retains JUnit and removes its containers, images,
network and payload volume. Against an already isolated candidate stack:

```sh
export DURABLE_WORKFLOW_SERVER_URL=http://127.0.0.1:8080
export DURABLE_WORKFLOW_AUTH_TOKEN=test-token
export DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION=1.20
export DURABLE_WORKFLOW_COOPERATIVE_QUALIFICATION=1
pytest tests/integration/test_cooperative_cancellation.py -v --junitxml=cooperative-results.xml
```

The suite covers canonical delivery and cleanup, lost replies, waiting runs,
local callbacks, cleanup shutdown/replay and deadlines. Its remote cases use the
actual `Worker.run()` pollers and callback dispatcher, rather than only manually
claiming an attempt and sending a heartbeat. Async and synchronous callbacks are
tested with and without user heartbeats, through an actual accepted worker
registration heartbeat. Shutdown expiry fences callbacks before replacement
cleanup. Another case kills the real remote owner process, delivers the request
in a new process and rejects the dead owner's late completion/failure.
A separate case kills an active owner, waits for the real five-minute activity
lease and repair pass, and requires attempt 2 under a distinct owner before
requesting cancellation. It checks the killed attempt cannot publish a result,
failure or heartbeat, renew its lease, or change canonical history, then verifies
one cleanup with the original request identity.

Readonly status calls never record user progress or renew the activity lease.
They fail closed on refused/invalid ownership, elapsed execution/session bounds
or failed observation. A pending workflow request becomes an activity stop only
when the workflow worker durably delivers cancellation. Workflow capacity must
remain available while an activity is blocked. Here a Python worker provides it
through separate workflow execution and activity thread capacities.

Synchronous remote handlers use a lazy pool bounded by configured activity
concurrency. A running thread retains its execution slot until it actually
finishes, even after its durable attempt is abandoned. Worker availability and
poll admission reflect that occupied capacity. Its late heartbeats and result
publication are fenced. Python cannot forcibly stop a running thread or a
callable that suppresses cancellation. Such code may continue external effects
until it returns or its process supervisor stops the process. Activities still
need idempotency and reconciliation.

Only activity-authored heartbeats extend the existing five-minute activity
lease and any user heartbeat deadline. A positive readonly observation does not
reserve ownership for a later completion. The Server independently validates
completion and failure fences. Exact published Server/SDK qualification remains
a separate release gate from these source scenarios.
