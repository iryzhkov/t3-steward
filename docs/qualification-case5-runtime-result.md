# Case 5 runtime ownership qualification, 2026-09-15

**PASS within two disposable persistent-worker contexts using a synthetic provider.**
This replaces the earlier case 5 failure recorded at the old source in
qualification-fleet-limits-result.md; cases 6 and 17 retain their separate evidence.

## Exact source and execution

- Steward binary: `64de404a3eeaf8b4c8dc644948f8e18731f96d79`, clean,
  containing ownership integration `4856c52`.
- UpKeeper binary: `30e6b26db87f34cc7fb2ed4d705a9a08a8727f4a`, clean,
  containing ownership integration `4330291` and the enrollment-plan redaction fix.
- Command: `UPKEEPER_SOURCE=/home/igor/Work/wt-qual-plan-redaction bash scripts/qualification/case5_runtime.sh`.
- Exit: 0. All nine assertions pass.
- Evidence: `/tmp/t3qual.0883tv/evidence`.
- Full command log: `/tmp/case5-runtime-run2.log`.
- Binary identities: `case5-{steward,upkeeper}-build-metadata.out`.

The compiled UpKeeper target-side agent applies a baseline coordinator catalog
from a disposable committed release checkout. Its real CLI then compares new
intent against that actual baseline and reports exactly two proposed model
authorizations, with enrollment unapplied. Applying the new coordinator-only
host projection and restarting the actual coordinator changes its authored catalog.

Both actual persistent worker processes initially lack the illustrative
`claude-opus-4-5` model in their synthetic provider observations. Current-fenced
enrollment refuses both. Application, coordinator restart and subsequently adding
observations do not enroll either worker automatically. Explicit enrollment
advances both from revision 1 to 2.

For both workers, `case5-model-sets.json` records:

| State | Models |
| --- | --- |
| Desired | claude-opus-4-5, synthetic-model |
| Observed | synthetic-model, claude-opus-4-5, unapproved-observation |
| Effective after explicit enrollment | claude-opus-4-5, synthetic-model |

Each worker then executes a real disposable campaign whose only route uses
the illustrative Opus model. Both tasks succeed and produce a declared output;
see `case5-run-worker-a.json` and `case5-run-worker-b.json`.

## Discovered enrollment-plan defect and negative controls

The initial ownership run at UpKeeper `4330291` showed
`authorization_changes: "<redacted>"`: the global redactor treated the reviewable
authorization diff as a credential. A stronger harness assertion correctly
fails that preserved evidence at `/tmp/t3qual.7RkH3y/evidence`.
The negative-control command and failure are in
`/tmp/case5-plan-oracle-negative.log`.

The fix preserves only the exact `authorization_changes` key when it contains
a list of strings. Existing string redaction still removes secret-shaped contents.
Scalar, map and mixed-type values fail closed, and credential authorization
keys retain their existing redaction.

Before the production fix, the new redaction unit tests and CLI plan test
failed because the model diff was hidden:
`/tmp/plan-redaction-negative.log`.
Afterward the same focused command passed:
`go test ./internal/redact ./internal/cli -run 'TestAuthorizationChanges|TestFleetPlanReportsAndWritesNothing' -count=1`.
Evidence: `/tmp/plan-redaction-positive.log`.

UpKeeper `make go-gate` passed after the fix (format, vet, package tests,
race tests, linux amd64 and arm64 builds); see
`/tmp/plan-redaction-go-gate.log`. The gate ran before committing the same
source diff; the runtime harness subsequently built the clean exact commit.

## Boundaries

This proves the complete ownership and explicit enrollment path on two disposable
synthetic host contexts, not real homelab/omarchy installations or commercial Opus
provider calls. Local provider quota bindings remain present in the authored
allowed pool. No task was running during the ownership change. This case does
not claim to cover revocation of already stored routes or the third coordinator
document in release rollback.

All disposable processes were stopped. No live configuration, credentials,
worker enrollment, active wait or T3 thread changed. No push, publication or
deployment occurred.
