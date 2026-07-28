# Avro Value benchmark

`python benchmarks/avro_value.py --enforce` loads the checked-in medium corpus
at `schema/avro-value-benchmark-v1.json` (SHA-256
`588771404977f2a95fe7d8969c24a15e1c7dd78fe498af9aa2406f82be54b666`).
The bytes sentinel is adapted only for the fixed typed path; compact JSON and
the removed wrapper use the corpus's documented JSON representation.

A release qualification run in the Linux Python 3 worker measured about 65 µs
to encode and 43 µs to decode through `fastavro`. The enforced 125/100 µs
defaults allow about 2x shared-runner variance while remaining far below the
slow recursive reference path. Set `AVRO_VALUE_ENCODE_BUDGET_US` and
`AVRO_VALUE_DECODE_BUDGET_US` to calibrate a different qualification runner.
