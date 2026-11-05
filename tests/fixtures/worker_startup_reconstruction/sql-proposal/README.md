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
type or field means invalid. Exact column/identity validation, driver-specific
bound parameters and explicit complete/missing/truncated/invalid records remain
unimplemented. Do not interpolate parameters or execute these texts directly
from this proposal. The field `lease_owner_present` says only that the column
is non-null. Error-present booleans also mean non-null, including an empty
string; they do not establish an error's contents, severity or cause.

Each query returns its own database UTC sample. The future collector must bracket
each read with observer monotonic times, retain the session timezone used to
decode timestamp columns, and keep before/after-teardown reads distinct.
`UTC_TIMESTAMP(6)` does not make those views atomic or establish a shared clock.
The row cap does not bound database work or wall time, particularly with binary
casts: the future fixture-only owner must enforce the accepted five-second
snapshot budget inside the unchanged case window, cancel/close overdue reads,
and retain the ten-second overall cleanup boundary. No schema/index change,
timeout extension, lock, transaction or worker mutation is proposed here.

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
