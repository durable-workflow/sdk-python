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

The retained observations prove callback identity and clock, emitted commands,
new shared timer-pair identities, immediate-predecessor lineage, original start
anchor, completed-result reuse and unchanged source history. Successors must
already have the reused timer prefix before a Worker starts, issue no timer
command, expose no timer wait, and execute only the failed activity. Parsed UTC
instants retain all supported microseconds; unrepresentable precision refuses
rather than being silently rounded. The original definition does not read an
ordinary entry clock, so this does not advertise that separate SDK capability.
Database timer/task invariants remain bound to the qualified native producer;
the SDK case observes public history, Worker commands and callbacks.

The trusted consumer binds case source to its Git-captured digest before each
cold child. A literal child entry verifies those captured bytes before compile
and execution; it opens no host dispatcher. Each Worker has a 45-second SDK
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
