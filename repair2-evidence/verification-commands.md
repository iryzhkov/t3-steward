# Foreground command ledger

All commands ran with GOMAXPROCS=2 GOFLAGS=-p=2. Complete output in declared verification.log, in this order:

1. BEFORE, four actual overlays installed with exact raw code-fence bytes (format=false):
`go test ./internal/backlog ./internal/store/sqlite -run 'TestIndependentRuntimeTerminalFences(ParkCleanupBoundary|MissingOldEvidenceMixed|TerminalOnlyCASReplay)$|TestIndependentRepair1' -count=1 -v`
Exit1 expected: Sol four release failures; Astra four release failures. Other leaves/SQLite pass. No overlay fixture/assertion changes. The two exact Sol raw overlays include an extra final blank line; default staged git diff --check reports those two blank-at-EOF findings. Bytes are intentionally preserved. git -c core.whitespace=-blank-at-eof diff --cached --check passes without changing Git configuration; all other whitespace checks remain enabled.

2. AFTER production CURRENT proof, same command/same assertions; permanent overlay gofmt only. Exit0, backlog0.691s/SQLite0.413s (see actual log for exact package line).

3. New tests initial:
`go test ./internal/store/sqlite ./internal/backlog -run 'TestRuntimeTerminalFences(CurrentParkReleaseProof|RegisteredParkReleaseAbandonment)' -count=1 -v`
Exit1: parent-shared reopened store was closed by child-subtest cleanup; snapshot Sequence increment needed later ObservedAt. Test fixture lifetime/timestamp corrected; production proof and assertions retained.

4. Same new-test command after fixture corrections: exit0 SQLite1.299s/backlog0.056s.

5. Exact requirements focused command:
`go test ./internal/store/sqlite ./internal/backlog -run 'TestRuntimeTerminalFences|TestReviewChildCancellationWorkerCustodyIntegration|TestIndependentRuntimeTerminalFences|TestIndependentRepair1|TestPlanWorkerStateTransitions|TestWorkerCommands|TestCommitWorkerStateTransition|TestReleasedWorkerState|TestFleetCoordinator|TestPlanWorkerCommands|Test.*Wait|Test.*Wake|Test.*Park' -count=1`
Exit0 SQLite10.278s/backlog2.782s. Includes final proof timestamp qualification and adversarial timestamp tests.

6. Added registered-test directory/repeated-state assertions; affected command only:
`go test ./internal/backlog -run '^TestRuntimeTerminalFencesRegisteredParkReleaseAbandonment$' -count=1 -v`
Exit0 backlog0.055s.

7. Final source audit (production predicate + tests, existing replay/epoch/finished/CAS/rollback ordering), raw-overlay exact and gofmt-only comparisons, git diff --check, seven Go SHA256 fingerprints. ONE:
`make check-review FAST_BASE=6ff61d9669a41c90d781ccc60b1cfa2faa442219`
Exit0: build/vet/full short/staticcheck/format and full affected races passed (backlog216.203s/SQLite886.382s). Full foreground log retained; no repeated gate or postgate Go edits. Export pins and final result are in handoff.md.

Initial and candidate bundle pins verified with git rev-parse/show, sha256sum, git bundle verify/list-heads before fetch/detach. Bundle export verification follows the gate and local commit in verification.log. Independent Sol/Astra reviews remain lead-owned, not performed here.
