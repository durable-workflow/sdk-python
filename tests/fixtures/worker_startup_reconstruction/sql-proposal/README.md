# Original-row SQL source proposal

These two read-only SELECT texts are an unexecuted slice of the accepted #123
snapshot contract. They target the historical MySQL 8.0 schema, not current
Server schema. `sources.json` binds the exact historical source evidence and
local SQL bytes. No database, API, historical package or dependency was opened,
imported, installed or acquired to prepare this slice. It is not a driver,
execution manifest, complete comparator or new operating authority.

The task read requires the exact fixture namespace, original run and original
task. The separate run read also requires the original workflow instance.
Neither query selects a latest run, replacement task, status-filtered successor,
payload, raw error, lease owner, registration token or process metadata. Binary
comparisons prevent case-insensitive matching from substituting a different
identity. A null namespace does not become `default`: the historical Server
namespace scope uses the supplied namespace directly.

The row cap is two to expose an unexpected duplicate, not permission to use the
first result. Zero rows means missing; two rows or any inconsistent identity,
type or field means invalid. The pure `tests/worker_startup_snapshot.py` component
now validates supplied normalized row projections against the closed
column/identity/type contract. Driver-specific bound parameters, normalization
and actual collection remain unimplemented. Do not interpolate parameters or execute these texts directly
from this proposal. The field `lease_owner_present` says only that the column
is non-null. Error-present booleans also mean non-null, including an empty
string; they do not establish an error's contents, severity or cause.

Each returned row includes that query's database UTC sample. Zero rows also
means no clock sample from that query: mark it missing rather than borrowing the
other query's timestamp. The future collector must bracket each read with
observer monotonic times and keep before/after-teardown reads distinct. Returned
rows now also report `@@session.time_zone` and `@@system_time_zone`, without
changing either variable. These labels retain the timestamp-decoding context
described in the [MySQL timezone reference](https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html).
`UTC_TIMESTAMP(6)` does not make those views atomic or establish a shared clock.
The row cap does not bound database work or wall time, particularly with binary
casts: the future fixture-only owner must enforce the accepted five-second
snapshot budget inside the unchanged case window, cancel/close overdue reads,
and retain the ten-second overall cleanup boundary. No schema/index change,
timeout extension, lock, transaction or worker mutation is proposed here.

## Pure normalized-row validation

The decoder performs no SQL, driver/process/credential access, raw-output capture,
clock collection or timezone conversion. Its input is an explicitly supplied
source state plus a list containing zero or one exact row dictionary. Empty input
is missing, multiple rows are invalid, and missing/truncated/invalid upstream
states cannot be promoted by an otherwise valid-looking row. The future collector
must independently prove its source-state declaration and normalization; this
component cannot certify either. Capture size/time/cancellation bounds remain
pending with that collector.

The fixture contract requires printable ASCII labels and original IDs, exact
original namespace/run/task or instance matching, the historical status vocabulary
and workflow task family. Counters accept unsigned 32-bit integers or canonical
decimal strings, preserving zero. Presence fields accept only integer or textual
`0`/`1`; native bool, float, null and generic truthiness are refused. Nullable text
preserves null, empty and literal `NULL` distinctly. Timestamp normalization must
supply the declared six-fractional-digit string form or null; native datetime
objects and implicit string conversions are refused until the driver's
normalization is reviewed.

Every required column must exist, unknown columns are refused, and failed
projections expose only a fixed code and optional known field name. No raw row,
unknown column/value or exception text is echoed. Successful data is a fresh
primitive-valued projection. `projection_state=complete` means this supplied
projection has the expected fields/types/identity; `phase_verdict` stays
`unknown`, including for valid-looking but inconsistent lease/attempt states.
It proves no raw capture, shared clock, row consistency, claim, hydration,
publication or causal conclusion.

Private poll/claim/lease-owner binding, registration routing/current-incarnation
and freshness evidence, API/history linkage, the snapshot collector and terminal
classification are intentionally still missing. They must take the explicit
incomplete/unknown path; these task/run projections cannot establish a committed
claim, fresh eligible registration, hydration, publication or cleanup causality.
In particular, an incremented attempt is not proof that history was delivered,
and a ready row after teardown does not prove it was never claimed.

Next owners: Server reviews these exact source fields and MySQL texts;
sdk-python owns corrections and the later bounded collector/private comparator.
Any historical execution still requires its complete source/input/owner packet
and separate operating decision. The original untraced failures, frozen 2.15
evidence and all #184 policy/operation holds remain separate and unchanged.
