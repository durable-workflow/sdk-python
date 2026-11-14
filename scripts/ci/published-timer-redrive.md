The timer-redrive cell uses the exact published tuple in
`tests/fixtures/published-timer-redrive.json` and the same closed install,
internal network, container authority, SDK inventory and independent cleanup
profile as [activity identity](published-activity-acceptance.md). The identity
tuple and its default command remain unchanged.

After the changed case, tuple and runner have source review, terminal normal
checks and normal integration, run the ordinary GitHub Actions cell:

```bash
gh workflow run ci.yml --ref main -f published_timer_redrive=true
```

One unchanged workflow definition first completes an activity, waits for a
timer, and fails its second activity. The source TimerFired must have a real
nonzero fractional event timestamp. The test never edits published history.
It obtains original start authority from the persisted run, then performs first
and repeated redrive through the public client API. Each of the three Workers
runs in a fresh Python process with the same definition and fingerprint.
The first successor fails the second activity again; the next succeeds.

The retained observations prove callback identity and clock, closed emitted
command/task inventories, new shared timer-pair identities, immediate-predecessor
lineage, original start anchor, completed-result reuse and unchanged source
history. Recorded nonempty Worker fingerprints must match every original and
successor `WorkflowStarted` fingerprint with its `worker` source. Successors must
already have the reused timer prefix before a Worker starts, issue no timer
command, expose no timer wait, and execute only the failed activity. Parsed UTC
instants retain all supported microseconds; unrepresentable precision refuses
rather than being silently rounded. The original definition does not read an
ordinary entry clock, so this does not advertise that separate SDK capability.
Unpruned public history exports bind the selected workflow/run and include every
task/wait status. Original stored timer tasks/waits must each count one;
successors must each count zero before and after their cold Worker. All emitted
completion task IDs and activity callback task IDs must match the stored export
inventory. Full timer payloads must match their predecessor modulo the explicitly
authorized new pair ID, immediate predecessor and preserved clock fields.
The paged public history supplies the original TimerFired `timestamp`.
First reuse compares the authoritative start and event clocks as full-precision
UTC instants. Native fallback serialization may change the public API's `Z` to
`+00:00` and omit insignificant fractional digits. Later reuse preserves the
immediate predecessor's inherited strings exactly as well as comparing their
parsed UTC instants.

An absent matching registration must refuse redrive without changing the chain
or source history. Admission then holds a matching source-cohort registration
that never polls or executes work; its metadata is copied from the actual source
Worker and its cleanup uses the returned registration generation token. Only the
fresh child may execute the accepted successor. Physical `workflow_run_timers`
counts remain a separate actual published-producer observation. Reconstructed
export timers and an immutable qualification link are not per-fixture physical
counts; this API case makes no such claim.

The trusted consumer captures the complete private installed-file inventory
before admitting SDK execution. Parent pytest and every cold child use the
read-only base interpreter with `-I -S -B`. Their literal entry verifies the
independently fixed case digest and installed-inventory commitment before adding
the exact private site directory, without processing `.pth` or sitecustomize.
Pytest uses captured source bytes and fixed fixtures instead of reopening the
mutable test or conftest as executable code. Each Worker has a 45-second SDK
budget and a 55-second child wall limit. The selected pytest case has a
180-second bound within the existing 240-second consumer exchange and
six-minute controller deadline; cleanup keeps its independent per-call bounds.
The older identity case retains its 90-second pytest bound. No local SDK or
native runtime run is part of source qualification.

The seven-day artifact retains the complete fixed wheel set, selected platform
and source, unchanged SDK inventory, before/after runtime authority, all three
cold-process fingerprints and command/callback observations, actual histories,
JUnit and full-ID/label absence. An installed cell is qualified only after its
retained evidence receives independent review.
