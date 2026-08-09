# Avro Value benchmark

`python benchmarks/avro_value.py --enforce` loads the checked-in medium corpus
at `schema/avro-value-benchmark-v1.json`. It hard-checks the corpus identity,
typed wire identity, round-trip fidelity, and exact wire sizes. The bytes
sentinel is adapted only for the fixed typed path; compact JSON and the removed
wrapper use the corpus's documented JSON representation.

A release qualification run in the Linux Python 3 worker measured about 65 µs
to encode and 43 µs to decode through `fastavro`. Routine CI warms the codec,
takes seven paired samples in alternating order, and enforces the median ratio
between the production path and direct typed fastavro work from the same run.
The default encode/decode ratio ceilings are 1.75/2.00 and can be calibrated
with `AVRO_VALUE_ENCODE_RATIO_BUDGET` and
`AVRO_VALUE_DECODE_RATIO_BUDGET`.

The dedicated advisory benchmark runs with `--enforce-absolute`, publishes all
samples and median/p95 summaries, and retains the 125/100 µs absolute budgets.
Those findings remain visible without making shared-runner throughput a generic
lint failure. Set `AVRO_VALUE_ENCODE_BUDGET_US` and
`AVRO_VALUE_DECODE_BUDGET_US` only when using a benchmark runner with a known
performance envelope.
