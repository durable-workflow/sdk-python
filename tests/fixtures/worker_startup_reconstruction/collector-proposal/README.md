# Bounded original-row collector source proposal

This is the next unexecuted #123 design after the supplied-row decoder in #283.
It specifies a fixture-only SQL collector, normalization and cancellation path;
it supplies no driver, launcher, installed dependency, database connection or
operating admission. `proposal.json` contains proposed transitions, bounds and
vectors, not observed results. `driver-sources.json` binds public source files
read as data, without importing them or retrieving a package archive.

The historical SDK, Server and Core identities, six cases and original failure
distinctions in `../proposal.json` remain authoritative. The updated observer and
decoder are source preparation, not an overlay on the historical installed SDK.
No #184 executor, acquisition source or blocked review is reused here.

## Proposed driver boundary

Use a dedicated observer process, with one connection and no retry, for each
before/after SQL pair. The proposed driver family is PyMySQL 1.2.3, source commit
`c990e86083c26493f3285b1e047902994c4b1811`. Its package archive, complete runtime
import closure, actual authentication/TLS inputs and installed input are **not
bound or acquired**. Public release/source identity is only an input to this
proposal; it does not establish a supported or qualified historical tuple.

The first pure source slice now lives in `tests/worker_startup_raw.py`.
`bind_original_query` verifies the two immutable SQL texts, translates their
closed placeholder inventory and returns values separately without invoking a
driver. `decode_raw_snapshot` validates supplied ordered aliases and positional
bytes/null values, enforces field-size limits, pads valid whole-second timestamp
text and calls the existing projection decoder. Its controls consume inert
tuples only. They do not prove that a driver produced those types or that a read
completed; the future collector must establish its bounded capture before using
this transform. A complete supplied projection still has an unknown phase.
`tests/worker_startup_read_budget.py` adds the next pure callback wrapper. It
reserves each requested count before the callback, enforces the proposed
32-KiB/64-KiB limits, never refunds a failed read, refuses further calls after
failure, and checks the same monotonic deadline before and after each callback.
Synthetic callbacks exercise these rules without a driver or socket. The
wrapper cannot interrupt a blocked callback. It is not wired to the pinned
driver's private method and proves no actual handshake or receive coverage.

`tests/worker_startup_buffered.py` places the same reservation beneath a stock
`io.BufferedReader`. Its raw input must be supplied before the first protocol
read. A logical request is bounded before buffered allocation; every raw
request, including read-ahead, is reserved before reading. Cached bytes are not
charged twice, and replacing a stream retains the same phase budget/deadline.
The inert predecessor control demonstrates that a one-byte logical callback
can otherwise fetch 8192 uncharged bytes through stock buffering. Successor
controls cover that reservation, cached reads, shared streams, oversize/expiry
refusals, short EOF and close without draining. They use fixture raw streams,
not a socket or the proposed driver.

The actual driver binding is still absent. The pinned connection source creates
its initial buffered reader and recreates it after TLS wrapping. Both sites
must install the budgeted raw boundary before any read; this primitive refuses
an already buffered input and does not establish either lifecycle hook. TLS
handshake deadlines, authentication, result terminators, process ownership,
framing and cleanup remain unimplemented. The collector vectors below remain
unexecuted, and all actual operating inputs remain unbound.

The process receives only the fixture identity and dedicated SELECT-only
fixture connection parameters through the future owner's private input channel.
Never obtain production endpoints, ambient credentials, config files, plugin
credentials or a user's home defaults. The [source/input decision](https://github.com/durable-workflow/sdk-python/issues/123#issuecomment-6673104109)
selects a dedicated SELECT-only fixture identity using `caching_sha2_password`
over required verified TLS, with peer hostname `mysql` and port 3306. This is
new reconstruction input, not recovered original authentication provenance.
Actual network/peer, CA, hostname-valid certificate, grants, backend/platform
and full installed closure remain unbound. No account/certificate/key creation
is selected by the source decision.
No opportunistic RSA package installation, plaintext fallback, server-key fetch,
reconnect, discovery or endpoint substitution is permitted.
For the proposed TCP path, supply an explicit SSLContext with the selected CA,
certificate verification and hostname verification. This pinned driver makes
explicit SSL configuration required, while its default preferred mode can fall
back to plaintext. Default TLS options are therefore inadmissible. The exact
context, peer and authentication closure still require concrete binding and
controls. Ambient CA defaults and authentication plugin switches are refused.

The driver source has consequential defaults:

- Connection setup sends `SET NAMES`; setting `autocommit=None`, `sql_mode=None`
  and `init_command=None` avoids its optional further setup commands. The
  collector must inventory the actual handshake and this observer-session
  character-set statement. Require the driver's latest authenticated OK status
  to show autocommit before either SELECT; refuse an inherited false state
  without changing it. No transaction or time-zone change is proposed.
- Socket read/write timeouts bound socket operations, not the complete pair.
  Stock buffered row reading and multipart packets have no such receive bound.
  `max_allowed_packet` is not a receive-size limit.
- Default converters produce Python values including datetimes. The proposed
  raw path disables Unicode and type-code decoders, retaining only the driver's
  parameter encoders. A tuple cursor preserves positional fields; dictionary
  mapping must follow exact, unique column-description validation.

These facts are bound to [connection source](https://github.com/PyMySQL/PyMySQL/blob/c990e86083c26493f3285b1e047902994c4b1811/pymysql/connections.py),
[cursor source](https://github.com/PyMySQL/PyMySQL/blob/c990e86083c26493f3285b1e047902994c4b1811/pymysql/cursors.py)
[converter source](https://github.com/PyMySQL/PyMySQL/blob/c990e86083c26493f3285b1e047902994c4b1811/pymysql/converters.py)
and [authentication source](https://github.com/PyMySQL/PyMySQL/blob/c990e86083c26493f3285b1e047902994c4b1811/pymysql/_auth.py).
The future adapter is still missing; stock driver use alone satisfies none of
the complete-read, byte-limit, absolute-deadline or cleanup obligations below.

## SQL and raw normalization

Keep the two accepted SELECT projections and original binary identity filters.
At adapter construction, translate only the closed named placeholder tokens
(`:namespace`, `:original_run_id`, `:original_task_id`,
`:workflow_instance_id`) to the driver's `%(name)s` syntax. Require the exact
expected token inventory in each immutable SQL text. Call `execute(query,
parameters)` with the validated identity dictionary; never format values into
SQL, quote them locally, log `mogrify`, or allow arbitrary SQL/token replacement.
The driver's parameter API is documented [here](https://pymysql.readthedocs.io/en/latest/modules/cursors.html#pymysql.cursors.Cursor.execute).
It performs driver escaping, not server-side prepared execution.

The proposed reader bounds the driver's logical requests and reserves raw
protocol reads, including buffered read-ahead, **before** their read/allocation
boundary. It refuses a request above 32 KiB or cumulative raw reservation above
64 KiB for the entire connection/pair, including greeting, auth/setup,
descriptions, rows and terminators. Multipart headers and every component body
consume the same protocol budget; cached bytes are not counted a second time.
These are MySQL protocol reservations, not a total TLS ciphertext, certificate
record or OpenSSL allocation cap. TLS negotiation, buffering and blocked I/O
still share the original absolute phase/case deadline and resource envelope.
They receive no separate allowance, and no custom TLS engine is proposed.
A failed read does not refund that reservation or permit a retry. The adapter
must bind both initial and post-TLS raw readers against the pinned connection
source before adoption. That lifecycle binding has not been implemented or
exercised here.
Construct with `defer_connect=True`, initialize the deadline/receive guard before
any handshake, and connect only after fixture identity/input validation. The
complete process lifetime and its output are also bounded by the owner. Send
driver stdout/stderr to no retained log; only the selected framed channel may
publish fixed codes and allowlisted projection data.

Require the expected 23 task / 9 run descriptions, exact column order and unique
ASCII aliases before constructing a dictionary. Require all row tuples to have
the same exact length, at most two rows, and the complete expected result
terminator with no extra result set. Protocol errors, premature EOF, unexpected
LOCAL INFILE/auth exchange, extra results or changed metadata are refusals.
No `fetchmany(2)` or SQL `LIMIT 2` is a substitute for complete bounded reading.

Each scalar must be exactly bytes or null. Decode bytes as strict ASCII; refuse
other Python types or malformed bytes without calling `str` or `repr`. Preserve
null, empty bytes and literal `NULL` distinctly. Counters and presence bits stay
canonical decimal text for the existing decoder. Retain timestamp text without
timezone arithmetic; accept the declared 26-character microsecond form, or
right-pad a valid 19-character whole-second form with `.000000`. Refuse partial
fractions, date-only, zero/invalid dates and extra suffixes through the decoder.
Right-padding records zero fractional digits, not a recovered clock sample.

The capped raw capture is the selected projection's scalar bytes plus ordered
column metadata and terminal/length flags, not a packet dump. Authentication,
SQL parameter expansion, private lease-owner values, payloads and exception
text must never enter it. Proposed framed output is at most 32 KiB, with one
terminal document, exact byte length and no trailing output. Invalid output is
not parsed partially. Resource termination is a refusal, never success.

`capture_state` and `projection_state` remain separate. A complete zero-row
result has complete capture but a missing projection and no database clock.
Never promote a partial capture because the first row looks valid. Never borrow
the task clock for a missing run, or overwrite a before result with an after
result. Store the raw length/hash, normalized projection and fixed refusal code
under the exact case/phase/read identity; expose no arbitrary driver errors.

## One budget, monotonic brackets and teardown

At case entry set its unchanged 30-second monotonic deadline. For each snapshot
phase set `snapshot_deadline = min(case_deadline, phase_started + 5s,
total_deadline)`. The same deadline covers connection/authentication/setup,
both sequential SELECTs, normalization and output. Future registration/API
reads must share this phase deadline, not receive another five seconds each.
Record monotonic before/after brackets for the pair and each actual read, plus
the returned database UTC/timezone context when present.

An exhausted case budget makes a new before/after read unavailable. A started
read that overruns or is interrupted is truncated. A known protocol/shape
violation is invalid. None is complete. In particular, a failure at the case
deadline cannot acquire a fresh diagnostic budget. Required evidence can remain
missing: preserve the original failure and report an incomplete comparison.

The original outer cleanup allowance remains ten seconds, ending no later than
`min(case_deadline + 10s, total_deadline)`. It is a cleanup ceiling, not extra
snapshot time. Retain before observation on success/failure, then bracket worker
shutdown/drain, then attempt a separate same-original-row after observation
only when the original case budget remains. Every action receives the existing
absolute ceiling, without resetting it per connection/process/container.

The future fixture owner must run blocking driver work in an owned process;
thread cancellation or a timed-out coroutine does not prove a driver stopped.
Before its deadline, ordinary close may send COM_QUIT within the remaining
socket budget. At the deadline stop reading/draining, stop the exact child and
close/reap it within the shared cleanup ceiling. Do not call cursor cleanup
that consumes unread results after expiry. Independent owner-exit/deadline
cleanup, confirmed process exit, and final fixture-resource absence are required
before reporting cleanup complete. Any survivor retains an explicit incomplete
cleanup result and owner escalation; it cannot be relabeled complete.
Client exit/socket closure alone does not prove a server-side SELECT stopped.
On an interrupted case, the fixture owner's existing cleanup must stop and
verify absence of its owned backend within that same ceiling. Until then,
server-side query termination remains unknown; add no KILL/session mutation or
new query solely to manufacture a cancellation receipt.

This describes an unimplemented #123 fixture owner. It selects no supervisor,
platform-specific kill mechanism or reusable #184 custody implementation. Those
exact sources and control results belong in the later operating packet.

## Limits and next work

Each SQL result is a separate view. A common connection, UTC sample, timezone
label or nondecreasing monotonic bracket establishes no shared database/worker
clock, atomic snapshot, DATETIME storage domain or lease/deadline comparison.
Those comparisons remain unavailable until actual driver/session/storage/DST
behavior is bound and qualified.

Even complete task/run projections establish no current registration/private
claim/owner/attempt binding, full cloned-client/body coverage, hydration,
publication, cleanup causality or original root cause. Those independent parts
of the accepted comparator still require implementation and explicit
complete/missing/truncated/invalid states.

sdk-python next implements the proposed raw reader, closed SQL-token adapter,
normalizer and process/deadline integration with inert source controls from the
vector list. Server reviews historical fields, views and timestamp limits;
General owns later exact historical input/execution selection. Package/TLS/auth
closure, byte-capped actual transport, owner cleanup, image/platform digests and
historical installed-SDK provenance remain genuinely pending. No SQL/driver
import, package solve/install, container, historical API or runtime invocation
is authorized or performed by this source proposal.
