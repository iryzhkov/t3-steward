# Test-time budget

The targets are **race CI under 12 minutes (720 seconds)** per race job and
**repair-loop tests under 5 minutes (300 seconds)** per affected-test invocation.
These are elapsed-time budgets, not permission to skip assertions, the race
detector, checkptr, or a declared gate. A breach needs a measured follow-up unit;
fixture changes are outside Unit 1.3a.

Implementers and fixers iterate with `make test-affected BASE=<pinned-original-base>`
where that target exists, or Huyang `verify_run` with `test_scope=affected`.
Reviewers reproduce suspected defects with a focused test or command first, then
run affected tests. The worker runs the full declared gate once on the final
candidate; reviewers do not repeat it. Record commands, exit codes, elapsed time,
source SHA, toolchain, OS, CPU limits, cache state and fixture conditions. For a
breach, identify the slow package/test with `go test -race -json -count=1` and
propose a scoped fixture unit that preserves the existing assertions.

## Measured baseline

Source: the bundled **Developer-speed audit, 2026-10-10 (annotated)**, section
**4. Test and CI cost**, supplied as `.t3/inputs/inputs/dev-speed-audit.md` in this
unit. This is a historical baseline, not a measurement of the current HEAD.
The audit is partial; its reviewer corrections take precedence. Raw timings
were retained in the audit's metrics.json (compressed recovery pointer in its
preamble).

Local Steward measurements pinned `c382eea6a91319ab72246066b5ed4bdfc75bec26`,
Go 1.25.0, Linux/amd64, fresh disposable clones with existing build/module caches,
normal fleet activity, `-count=1`, and `GOMAXPROCS=4`, `GOFLAGS=-p=4`,
`MAKEFLAGS=-j4`, `CARGO_BUILD_JOBS=4`. Commands ran sequentially.

| Command | Wall seconds | Exit |
| --- | ---: | ---: |
| Plain tests | 313.22 | 0 |
| Race tests with checkptr | 785.26 | 0 |
| Race tests with SQLite checkptr disabled (diagnostic only) | 401.02 | 0 |
| Build | 0.71 | 0 |
| Vet | 4.57 | 0 |
| Lint | 12.14 | 0 |

The slowest race packages were `internal/backlog` (258.250 s),
`internal/store/sqlite` (213.084 s), `cmd/t3-steward` (156.920 s),
`internal/workerruntime` (140.498 s), `internal/backlogadmin` (61.576 s), and
`internal/backupsnapshot` (38.443 s). None individually exceeds 300 s in this
sample. Their times cannot be summed to predict a concurrent affected-test run.
No direct affected-loop wall measurement was supplied, so compliance with the
five-minute invocation budget remains unproved.

The slowest backup fixture,
`internal/backupsnapshot/TestAManifestLargerThanAMegabyteStillVerifies`, creates
7000 real files and took 78.32 s plain / 35.38 s race. Other expensive race tests
included `cmd/t3-steward/TestRemoteTaskWaitRegistrationSettlementAndOwnerDelivery`
(15.81 s), `TestRemoteTaskWaitCancellationAndNoPollTimeout` (14.93 s), and
`internal/workerruntime/TestCollectionWithBundlesCollectsWheneverItCollectsWithoutAcrossLimits`
(12.74 s). These are fixture optimization candidates for Unit 1.3b and later,
not individual five-minute breaches. Keep real-size regression coverage in the
full gate and avoid selecting unrelated packages in repair loops.

For the audit's exact seven-day Actions window ending 2026-10-10 19:19:27Z,
Steward race jobs measured:

| Job | Samples | Median seconds | Worst seconds |
| --- | ---: | ---: | ---: |
| race | 71 | 706.0 | 2398.0 |
| race (macos-latest) | 7 | 830.0 | 1343.0 |
| race (ubuntu-26.04) | 4 | 531.5 | 1026.0 |
| race (ubuntu-latest) | 3 | 594.0 | 831.0 |

Job execution times differ from workflow wall time, which includes queueing and
updates. Samples include failures and skips; aggregate medians and maxima do
not attribute a CI overrun to a particular fixture. Local filesystem, compilation
and fixture preparation differ from CI. The diagnostic checkptr-off run never
replaces required gates.

## Current measured breaches and proposed follow-up units

These are proposals for later units, not submissions or fixes in this change.

| Test or package/job | Evidence against budget | Proposed follow-up unit |
| --- | --- | --- |
| Steward full race suite, led by `internal/backlog` and `internal/store/sqlite` | Local 785.26 s exceeds the 720 s race target; this local sample is a CI proxy, not a CI job | 1.3b: profile the slowest full-size fixtures and reduce setup cost without removing assertions |
| Steward `race` job | Worst 2398 s exceeds 720 s; median 706 s is within budget | 1.3c: reproduce the slow CI sample, attribute package/fixture cost, then fix measured setup bottlenecks |
| Steward `race (macos-latest)` job | Median 830 s and worst 1343 s exceed 720 s | 1.3d: measure macOS fixture/filesystem costs and optimize the proven slow fixtures |
| Steward `race (ubuntu-26.04)` job | Worst 1026 s exceeds 720 s; median 531.5 s is within budget | 1.3e: reproduce and profile the slow Ubuntu 26.04 race sample |
| Steward `race (ubuntu-latest)` job | Worst 831 s exceeds 720 s; median 594 s is within budget | 1.3f: reproduce and profile the slow Ubuntu latest race sample |
| `internal/backlog` race tests | Repeated local implementation/review sample at `3d55d6fffb82c7702b74121a46fe4fdbb2a73247`: 334.535 / 339.874 s exceeds 300 s for this package alone | 1.3g: profile backlog fixtures on an affected loop and preserve concurrency assertions while reducing setup |
| `internal/store/sqlite` race tests | Same repeated sample: 296.762 / 320.670 s; reviewer package time exceeds 300 s | 1.3h: profile SQLite fixture setup on an affected loop and preserve publication-fence assertions |
| This unit's `make test-affected BASE=d5615b134068816267f1750728cf11e9b790ca12`, selecting `internal/campaign` and `cmd/t3-steward` | Live process inspection on 2026-10-11 showed at least 355 s elapsed, exceeding 300 s; the final affected run passed (exit 0), with command-package race tests taking 507.860 s and campaign tests 16.334 s; Go 1.26.8, four-CPU limits, concurrent host CI, compilation included | 1.3i: separately measure compilation/cache and command-package fixture cost under the same affected-loop conditions |

The repeated sample ran full gates, not `test-affected`; it demonstrates package
cost capable of breaching a loop that selects that package, not a measured
five-minute affected invocation. Those full implementation/review gates took
449.730 / 471.399 s for the race command, and the reviewer repeated the entire
plain/race/vet gate in 736.50 s. Prompt adoption removes that duplicate reviewer
gate; it does not establish any timing improvement until measured.
