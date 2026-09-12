# S0 ADR: bounded worker protocol replay

Status: accepted and implemented in S0, 2026-09-12.

## Problem and ownership

The normandy JSON replay file reached 212 MB with 4096 full signed responses.
Each exchange decoded and rewrote it, with reported peaks near 1.5 GB and
20–30% of a CPU core. The September 12 fleet feed confirms the reset was a stopgap.
The worker owns replay identity. Coordinator SQLite would put local recovery
behind a network boundary and is the wrong owner.

## Decision

Use worker-local SQLite with separate identity, sessions, requests and responses
tables. Request metadata contains no response bodies; each body is an individually
addressed BLOB row. Reuse the existing SQLite driver. Keep the interprocess lock
across the handler; commit pending identity before the effect and its exact signed
response afterwards. No SQL transaction spans the external handler.

An append-only metadata log with response files could remove amplification too,
but requires torn-tail recovery, compaction publication, orphan cleanup and another
metadata/filesystem commit protocol. SQLite already provides local atomic commit
and page reuse. Neither makes T3 effects atomic: the worker attempt journal still
owns idempotent effect recovery.

## Contract and limits

Completed responses have a 24-hour age limit, a 32 MiB aggregate body budget and
an 8 MiB individual ceiling. The caller's count cap is secondary. Metadata has
an 8 MiB admission budget and bounded identity lengths; session sequence fences
expire after 24 hours of inactivity. Pruning runs on store/exchange activity.
Idle disk pages need no background process and are reused on later writes.

FULL synchronization and DELETE journaling avoid an independently growing WAL.
A 2 MiB SQLite cache, disabled mmap and 32768-page cap bound working storage.
At the default 4096-byte page size the database ceiling is 128 MiB; its rollback
journal can temporarily approach that size. Logical body retention is not a
promise of a 32 MiB physical file.

Cached duplicates must match the digest, and the protocol server checks the
original response signature. Eviction retains the session's consumed sequence:
old requests are rejected rather than run again. Protocol timestamps/deadlines
and configured clock skew must stay below the retention horizon; session reuse
after that horizon is outside transport replay guarantees. Assignment and
command idempotency outlive transport sessions in the separate worker journal.

Identical pending requests resume through that journal. The existing two-minute
abandoned-pending policy remains: another request may retire an abandoned transport
record without reversing its consumed sequence. Oversized responses or storage
failure retain pending identity and return failure; no success is returned before
the response commit.

## Migration and recovery

The first open streams version-1 JSON one request body at a time into a single
SQL transaction. It validates identity, epochs, fields, request order and trailing
content. The input cap is 512 MiB; retention runs during import. An oversized old
body is omitted but session fences remain. One unusually large JSON value can
still dominate migration memory; this is a one-time compatibility limitation.

The original JSON is retained unchanged as evidence outside the new store budget,
and is never consulted again after the identity transaction commits. Malformed
migration rolls back; a corrected input can be retried. Corrupt SQLite or an
identity mismatch fails closed. Recovery preserves the database and any adjacent
rollback journal. A newer coordinator epoch advances the header without discarding
worker custody or session fences; stale store handles are rejected.

A JSON-only binary must not resume exchanges against its obsolete JSON history
after this migration. Rollback requires stopping exchanges, reconciling outstanding
execution and restoring a coherent pre-upgrade worker snapshot, or forward fixing.
S0 does not reset live state or exercise a downgrade.

## Evidence

Tests cover restart replay, signed-response tampering, changed request content,
sequence gaps, pending recovery, byte/count/age retention, oversized-response
failure, migration replay, corrupt migration rollback and stale epoch handles.
The benchmark preloads 256 responses of 128 KiB before measuring exchanges.
The Citadel S0 handoff records exact commits and gate results. This is local
disposable-state evidence, not a fleet soak.
