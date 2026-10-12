# Fix1 handoff

Original pinned base: 5ee6edb318608a51a43a48afe154c19d85ada681.
Consumed implementation: 9ee38ddf6074d84d9386b07792d5b3feec1043a3, confirmed from retained implementation provenance and checked out before source edits.
Resulting head: HEAD, declared commit fix. The commit includes the source repair, regression test and declared reports.

## Blocking finding repaired

The outer retry loop previously called a helper that could send twice during parent-field compatibility negotiation. A transient failure on the parentless send restarted negotiation, producing six sends under the documented three-attempt bound.

Each retry iteration now makes exactly one transport send. The existing unknown-parent-field fallback clears the parent on the captured request once, counts against the three-send limit, and emits the same retry diagnostic and backoff as every other resend. Subsequent transient retries retain that negotiated request, idempotency key and archive bytes. Other transport classes still stop immediately unless the existing parent-field compatibility fallback applies. The caller's request is unchanged.

Changed source paths:
- cmd/t3-steward/submission_retry.go
- cmd/t3-steward/submission_retry_compatibility_test.go

The regression failed before implementation with six sends and two retry lines. It now covers unavailable and timeout, both exhausted retries and successful replay returning the original run. It asserts the three-send limit, two diagnostics, identical parentless request and archive, and unchanged caller parent. Its final form injects the wait function instead of sleeping.

## Verification

Environment: GOMAXPROCS=3 GOFLAGS=-p=3 MAKEFLAGS=-j3 CARGO_BUILD_JOBS=3.
- Failing regression before implementation: exit 1, six sends and two retry lines.
- go test ./cmd/t3-steward -run '^TestSubmissionRetry' -count=1: exit 0, 2.028s after extending coverage.
- Huyang verify_run, tests, affected: partial/unavailable, exit -1, no stage ran. It supplies no passing test evidence. Used the required make target against the pinned original base.
- make test-affected BASE=5ee6edb318608a51a43a48afe154c19d85ada681: exit 0, cmd/t3-steward race tests with checkptr, 214.578s. Selected the changed package and its importers.
- git diff --check and git diff --cached --check: exit 0.
- Test integrity: inspected git diff 5ee6edb318608a51a43a48afe154c19d85ada681 -- '*_test.go'. Removed existing test lines: none. Both retry test files are additions relative to that base, and the original implementation's tests remain unchanged.
- Source/test diff against the original base: 359 changed lines, below the roughly 400-line limit.

Full make test and make lint are reserved for the declared worker gate. No live coordinator was exercised. Separate review owns acceptance.

## FOLLOW-UPS

The preceding review's note about real retry waits in the six existing lost-reply CLI fixtures remains unchanged. It is outside the blocking finding and this repair.
