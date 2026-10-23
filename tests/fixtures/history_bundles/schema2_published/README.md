The `bundles/*.json` files are untouched output from the published
`workflow:v2:history-export --run --pretty` command in Server 2.9.7, using
Core 2.7.2. A real SQLite workflow scheduled one activity with argument `Ada`;
the first export was captured while waiting and the second after the actual
activity and workflow handlers completed. Both original execution and run
budgets were 30 seconds. No runtime source was replaced. All 615 loaded Core
source files independently matched the exact published source commit.

The `unicode_*.json` pair comes from a second independent run with Unicode
memo values and a nested Unicode decimal-digit key. Published PHP accepts
those same raw files. Its first combined observer created the exports but
failed in its verification step due to a PHP namespace-alias typo; the later
independent published verifier accepted them without rewriting any bytes.

`provenance.json` records the immutable image digest, source commits, original
file hashes, canonical checksums and event counts. This October 23 confirming
cohort is separate from the earlier failed exports reported in SDK #173.
Never regenerate or normalize these files in place. Tests mutate only parsed
copies when constructing deliberately invalid controls.

Website #320's original raw exports were retired after their original
qualification, as linked in `provenance.json`. These files are fresh independent
outputs and do not recreate or replace those originals.

`typed-memo-reference.json` is a separate synthetic typed-value control emitted
by the same published Core's `MemoPayload::mapEnvelope`, `decodeEntries` and
`AvroValueJsonProjection::project`. It covers bytes, nested empty/numeric-key
maps, integer boundaries, very long string keys and ordinary scalar kinds.
It is not another workflow history export. Tests place its authority and
projection into a freshly checksummed copy to isolate structural validation.

`php-canonical-reference.json` contains 532 independent canonical-JSON byte
vectors from the published Core's `HistoryExport::canonicalJson`: Unicode,
ASCII controls, finite-float boundaries and 512 seeded finite float64 values.
It also contains a clearly marked synthetic HMAC control signed by the
published writer with its public test key. That control starts from a copy of
the Unicode export; the original unsigned exports remain unchanged.

`producer_invalid/*.json` is a separate negative cohort from an actual run
with memo `whole=1.0`. The published CLI writes the memo projection as JSON
integer `1` while its Avro authority remains a float. The published PHP
verifier refuses both these untouched files with projection/checksum failures.
Python must refuse them too; accepting supported schema 2 does not repair an
export that lost its original numeric kind. These files are deliberately
excluded from the valid `bundles/` simulation directory.
