# Published entry-recovery HTTP fixture

This is the five-original installed-package acceptance fixture for
[shared issue 221](https://github.com/durable-workflow/.github/issues/221).
It uses a normal PyPI installation, not this checkout's runtime code.

The recorded cohort uses Python SDK 2.6.2 (`1b03ed2a`) and Server 2.9.9,
image index `sha256:d45d197e9556292ec096531030717e43d69ea4ea9ab197170971492f14cc0869`.
That Server image is historical: use synthetic data on an isolated internal
Docker network without published host ports when reproducing this exact tuple.
Production deployments should use a current patched Server release.

## Preparation

Copy these files into a disposable directory, mounted as `/phase` in the client,
and create its `data` and `consumer` subdirectories. The API resolves as
`http://api:8080` on their private network. Mount only `data` at `/data` in the
published API image, preserving its packaged bootstrap/seeder files.

Bootstrap a fresh SQLite database with `DB_CONNECTION=sqlite`,
`DB_DATABASE=/data/database.sqlite`, `CACHE_STORE=file`,
`QUEUE_CONNECTION=database`, `DW_POLLING_CACHE_PATH=/data/poll-cache`,
`DW_AUTH_TOKEN=general221-fixture`, `DW_ADMIN_TOKEN=general221-fixture`, and
`DW_WORKER_TOKEN=general221-worker`. These are synthetic fixture credentials.
Run the image's `server-bootstrap` and its normal Apache entry point.
Require `/api/ready` to return 200 before business admission.

Install `requirements.txt` into `/opt/venv` in the client. Save the PyPI wheel
as `/phase/sdk.whl`; its SHA-256 is
`28379c16bcd598c9aa545071d074bee297e449158a550e580d1df0ef9fc27ab8`.
Run `verify_install.py` before and after the cohort. It checks the installed
production files against both their RECORD hashes and the published wheel.
Do not put an SDK checkout on `PYTHONPATH`. An executable tmpfs is needed if
the venv, including extension modules, lives in tmpfs.

## Execute and inspect

For each of `required-before`, `required-after`, `optional`, `body-before`, and
`body-after`, use a fresh cohort identifier and run:

```sh
EXPECTED_BINDING_REFUSAL=1 /opt/venv/bin/python /phase/consumer.py CASE COHORT
```

Every original retains a 30-second run and 60-second execution deadline, and
each observer has its original 30-second total bound. The two changed required
signatures must refuse without entering the body. Supported run-scoped repair
and compatible code must finish the same original with unchanged history
prefix and deadlines. The optional argument completes normally; genuine body
TypeErrors remain terminal business failures. The fixture records every
business Worker exit and actual task submission, including failure reason.

For each completed original, start a separate process with
`/opt/venv/bin/python /phase/cold.py WORKFLOW_ID RUN_ID`. Its result must retain
the exact signed-int64 maximum, null and Unicode values.

Stop the API before SQL inspection. Run `audit.py` with the entire phase mounted
kernel read-only at `/phase`, a disposable writable `/audit`, no network and no
SDK dependency. It copies the stopped database/WAL, opens that copy read-only,
checks five originals and two deliberate repairs, and requires source and
copied database/WAL bytes to stay exact. Only a transient SQLite index may be
created in the disposable copy. Its report is written to stdout.

Retain concise results and source identities in the owning issue. Remove all
owned containers, network, dependencies, database, wheel and raw traces after
inspection; preserve borrowed images and other owners' resources.
