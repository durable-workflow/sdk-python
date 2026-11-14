The retry-cap case qualifies the exact reviewed 2.18.0 wheel on the published
Server/Python tuple in `tests/fixtures/published-retry-cap.json`. It uses the
existing internal two-container profile, complete hash lock, captured no-site
entry, before/after installed inventory and independent ID/label cleanup.
It adds no host mount, port, capability or alternate local-activity carrier.

The release approval record supplies the SDK hash. Candidate mode reuses the
protected publication resolver and archive extractor to bind the successful
same-repository build, source/tree, handoff artifact and both distributions.
The controller captures that record from Git, reads the wheel through a held
descriptor and checks its approved hash again. Only the 14 already locked
dependencies are downloaded from PyPI. Registry mode downloads all 15 locked
wheels and requires the identical approved SDK hash. The final private installer
always receives the complete 15-package hash lock and has no network fallback.

After source, artifact, runner and accepted-main gates are complete, select one
bounded candidate invocation through the ordinary workflow:

```bash
gh workflow run ci.yml --ref main -f activity_retry_cap=true -f retry_cap_artifact_source=candidate
```

After protected publication, select `retry_cap_artifact_source=registry` for the
normal-registry repeat. Neither invocation runs in ordinary push/PR CI.

Three real scalar local executions exercise the factory's default cap, a short
explicit list and an empty list. The generated policy has two total attempts,
an initial interval of 61 seconds and the factory's omitted maximum, so its
single retry must wait at least 60 seconds. Five attempts with `[1, 2]` select
`1, 2, 2, 2`; the empty list selects zero waits. Monotonic callback observations
independently reject shortened nonzero waits, with a 10-millisecond clock
tolerance. No upper worker-start guarantee is inferred from scheduling time.

The case binds complete ordered attempt/callback counts, reported backoffs,
accepted command kinds and actual retained public activity snapshots/retry
events to the selected policies. Each committed history replays with a callback
that refuses execution. Public exports bind the requested workflow/run and
unredacted retained state. Every source history is re-read unchanged.

Each Worker has a 100-second or 30-second SDK budget. The complete selected
pytest case is bounded at 180 seconds within the existing 240-second consumer
exchange and six-minute controller workload. Independent cleanup retains its
per-call bounds. The seven-day artifact contains the wheel set, complete
effective lock, source/tuple hashes, command/history/timing observations,
installed inventory and full-ID/label absence. Failures remain NONPASS; source
or virtual-sleep controls do not qualify an installed tuple.
