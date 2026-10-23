These two `bundles/*.json` files are untouched output from the published
`workflow:v2:history-export --run --pretty` command in Server 2.9.7, using
Core 2.7.2. A real SQLite workflow scheduled one activity with argument `Ada`;
the first export was captured while waiting and the second after the actual
activity and workflow handlers completed. Both original execution and run
budgets were 30 seconds. No runtime source was replaced. All 615 loaded Core
source files independently matched the exact published source commit.

`provenance.json` records the immutable image digest, source commits, original
file hashes, canonical checksums and event counts. This October 23 confirming
cohort is separate from the earlier failed exports reported in SDK #173.
Never regenerate or normalize these files in place. Tests mutate only parsed
copies when constructing deliberately invalid controls.

`typed-memo-reference.json` is a separate synthetic typed-value control emitted
by the same published Core's `MemoPayload::mapEnvelope`, `decodeEntries` and
`AvroValueJsonProjection::project`. It covers bytes, nested empty/numeric-key
maps, integer boundaries, very long string keys and ordinary scalar kinds.
It is not another workflow history export. Tests place its authority and
projection into a freshly checksummed copy to isolate structural validation.
