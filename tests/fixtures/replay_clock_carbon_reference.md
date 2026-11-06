The adjacent JSON is byte-identical to SDK Rust's retained public reference
packet at commit `3bc747cd05cecbc47c3841193805f1a9490a4a61`,
`tests/fixtures/replay-clock-carbon-reference.json` (Git blob
`328f4d743cb74293ac6e5899fa94e820a317df9d`, 2,663 bytes, SHA-256
`09a9051249a6dc4c0e3c177d26f058b393d563cf9f42b120c48f5d86371ea38f`).
Its 17 answers were measured with public PHP 8.4.27 and Carbon 3.15.1.
Python independently checks its answers and source bindings; it does not claim
to have rerun that PHP cell or qualified an installed Core/native tuple.

Carbon tag `3.15.1` resolves to
`ce0e5636fae7d1e99856eb70cae4c719a284aa08`. The independently fetched
`src/Carbon/Traits/Timestamp.php` is Git blob
`43adc5b35a909be96c255b4bc0399ee75dba53e2`, SHA-256
`57b2a235656f8edad7c333087a64e45491adaaf1b01ac9c40e53185b73db356d`.
It forms a float microsecond value and calls `round(value / 1000)`.
PHP tag `php-8.4.27`, `ext/standard/math.c`, Git blob
`09d98298104333417c233f1f6ba6d78c249b03d7`, SHA-256
`a6454623cc0f758568724beb47096e8fe6c0d4510c87f1a2243cdcc2b65bd590`,
confirms the zero-place half-away-from-zero comparison. Reconstruction from an
integer millisecond uses the represented instant, including negative epochs.

The Python group clock qualifies only the exact float-integer microsecond
range `abs(epoch_microseconds) <= 2**53` and represented precision up to six
fractional digits. Other ranges/precision remain unavailable. Success folds
only validated consumed members in authored descriptor order, retains the
first member on equal Carbon milliseconds, and returns its full event time.
Caught failure preserves Python's legacy exception and reconstructs time only
when that member agrees with Core's earliest-millisecond/member-index choice.
The conflicting-winner case remains an OPEN versioned compatibility gap.

The finite cohort requires a unique schedule, at most one start and one terminal
per execution, complete matching authored Activity-all paths/counters/types and
root remote scope. It reads actual `recorded_at`, or actual `created_at` only
when the former is absent/null, as Core's grouped activity path does. It does
not substitute scalar redrive time, a start time, a sibling, or history maximum.
A conflicting `timestamp` alias or timestamp-only view is not qualified by this
model-field binding. Query body/callback group time remains unavailable because
Core's successful query group-send supplies no new time. Independent later
scalar boundaries can recover their own authority. Empty groups introduce no
new boundary. Mixed/local/select/scoped groups and unsupported lifecycles remain
unavailable; this source cohort does not complete original #215.
