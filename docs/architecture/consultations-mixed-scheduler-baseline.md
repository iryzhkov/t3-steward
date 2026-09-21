# Mixed scheduler C0 comparison fixture

This fixture is a composition of two unchanged real-SQLite tests available at both
`f9f81b18195aa05c7cb186c754bf09af8252b76a` and `2316601`:

```sh
go test ./internal/store/sqlite \
  -run '^(TestTaskWaitParksAndResumesOneAttemptOnce|TestSupervisionBranchHoldRacesClaimOfAGateProtectedTask)$' \
  -count=20
```

The first test registers, parks, settles, resumes, delivers once, and checks restart replay
for an ordinary task wait. The second uses a supervised run and races a protected
ordinary assignment claim against a supervision hold. Together, one invocation performs
20 ordinary park/resume/restart paths and 20 supervised claim/hold races using migrated
SQLite stores. It creates no consultation records and invokes no model. The repository
does not expose model-session creation as a counter at this seam, so “zero extra sessions”
is inferred only from the absence of any model/runtime call in these tests, not measured.

For each revision I ran five isolated command samples on the same host. The first sample
included Go build/cache warmup and is retained separately rather than treated as scheduler
latency.

| revision | five wall samples (ms) | warm median | warm throughput |
| --- | --- | --- | --- |
| baseline `f9f81b1` | 1618.7, 602.5, 598.0, 598.8, 597.5 | 598.4 ms | 66.8 completed test paths/s |
| integrated `2316601` | 1630.5, 596.9, 598.3, 608.4, 597.4 | 597.8 ms | 66.9 completed test paths/s |

The integrated warm median is about 0.1% faster, within the frozen 10% throughput budget.
These are package-level end-to-end durable-path timings, useful beyond the earlier
no-work `WakeTaskWaits` benchmark, but process startup and fixture database creation are
included. They do not isolate coordinator scheduling latency. Five samples cannot
establish p95; the existing requirement for at least 30 latency observations remains
open and should be implemented as an in-process timed mixed harness before feature
admission.

The semantic accounting is fixed by the assertions: each ordinary path resumes exactly
once with stable delivery identity and restart replay; each supervised race permits only
the authoritative claim-or-hold outcome. Assignment/resource ownership is exercised but
not exported as a numeric time series. Consultation records and advisor sessions do not
exist on either revision. Capacity fencing added after the baseline is an intentional O0
semantic delta and must not be normalized as consultation overhead.

## Fair-service gap

Ordinary assignment offers and parked wait resumptions acquire capacity in separate
transactions and separate coordinator passes. `WakeTaskWaits` scans settled waits and
attempts reacquisition, while ordinary scheduling can commit a new offered owner before
that pass. The capacity fence prevents overcommit but supplies no ordering, age, or
reservation shared by the two contenders. A stream of ordinary offers can therefore
repeatedly take a released one-slot worker before an older parked wake; current tests
prove mutual exclusion, not bounded service.

Before consultation answering work competes for the same capacity, introduce one
deterministic admission arbiter seam for ordinary offers, activation offers, and parked
resumptions. Inputs should carry durable contender identity, ready time, class, worker
eligibility, and replay fence. A simple accepted policy is oldest-ready first with an
explicit bounded class rotation; the transaction must validate that the selected
contender is still live and atomically consume the slot. Tests need a fixed sequence that
continuously adds ordinary work and proves an older parked wake is selected within the
declared bound, plus replay/restart and shared/different quota-pool cases. This fixture
does not prove that property.
