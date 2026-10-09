# rc120 integration handoff

Base: 9f653a84da040ff2d7140f09f9abfef752944bd5 (rc119).
Branch: release/rc120. No push, tag, version-file bump or deployment.

## Merge order

1. fix-timing-tests — 7b2151fb6fa458cd283d81242171b562a595f45f
2. fix-archive-decode — eaaa925861abe87961b72eb7d95e1e8c9c70bdc9
3. fix-tmpdir-symlink — be13f8f3e11b9ffae7f301b15a3bd74a06d6eabe
4. fix-review-provider-family — 5cb8febf48f05e79382595114e0e2fdc0c7d76e5
5. fix-provider-resume — 4761d401a39670d459c8983980978eb00982e774
6. fix-identifier-validation — 9bf0cbbf993f9da5c941abe8df38d983f3e22b18
7. fix-parked-capacity — c920e94e1340413e4ece3c31f7336138b30a3ede
8. fix-campaign-quota-view — 2587e2dc2a300c30f8a9650964a8a3e0d2b513e1

All eight bundles were fetched using their explicit branch refs (bundles have no HEAD ref). Every exact commit exists and descends from the base. Each merge used --no-ff --no-edit. The earlier 8d93ee7 timing ancestor remains in history; the accepted 7b2151f timing changes remain in the tree.

## Conflicts and resolutions

- tmpdir-symlink: internal/providercontainment/run_linux_test.go, internal/workerruntime/attention_stop_observation_test.go, internal/workerruntime/live_commands_linux_test.go: import conflicts; retained both testtiming and testutil, preserving scaled timing and real temporary directories.
- review-provider-family: CHANGELOG.md: retained archive-decode and provider-family entries.
- provider-resume: CHANGELOG.md retained tmpdir and resume entries; cmd/t3-steward/triage.go retained workerList for deferred collection, triageProviderErrors, and both ordering entries; internal/workerruntime/journal.go retained CollectionDeferred and ProviderResume plus one copy of shared fields; internal/workerruntime/runtime.go reports the deferred-collection note and provider-error independently with their original capability gates.
- identifier-validation: CHANGELOG.md retained existing entries plus all identifier-validation entries.
- parked-capacity: CHANGELOG.md had two conflicts, retained both sides; cmd/t3-steward/triage.go retained capacity-deadlock, collection-deferred and provider-error ordering.
- campaign-quota-view: CHANGELOG.md retained both sides; cmd/t3-steward/route_ranking.go uses the new shared RoleQuotaSnapshot builder; internal/backlogadmin/role_quota_snapshot.go retains new freshness/admission/disabled-check handling and carries Active/MaxConcurrent into RouteRankPool so parked-capacity saturation is preserved; internal/campaign/help.go (two occurrences) retains candidate effort and the new freshness/disabled-check explanation.

No behavioural conflict required guessing. Read the actual unit diffs and commit histories. Unit-root handoff.md/continuation.md were not present in the bundle commits; only unrelated historic handoffs exist in those trees, so there was no unit handoff to consult.

## Migration compatibility

PASS against release-checklist.md decision 95 and migration-findings.md findings 1–2. Integrated base-to-head diff has no schema migration. The schema registry, store.go, migration*.go, lease.go, registered DDL and every other production SQL definition site are unchanged. The only changed DDL-bearing files, submission.go and task_wait.go, retain their exact existing DDL. Reviewed executable SQL changes and their added callers/helpers rather than relying only on literal matching: graph clone/rerun add ID validation before existing writes; submission adds key validation while legacy records remain readable; task_wait records optional JSON wakeDeferral through unchanged saveTaskWaitTx DML. No new SQL execution sink, runtime-assembled DDL or alternate store opener enters the train.

Side-store files are byte-identical to rc119: internal/quotatelemetry/store.go (meta schemaVersion), internal/backlogadmin/remote_replay.go (identity version 1), internal/workerruntime/protocol_replay.go (identity version 2). No side-store compatibility version, reader or writer changes. Worker journalVersion remains 1; rc119's existing non-strict JSON decoder accepts the added optional CollectionDeferred, ProviderResume and ProviderResumePolicy fields. Run lineage, placement receipt additions and wakeDeferral are optional JSON metadata within existing records; rc119 uses json.Unmarshal to read those records and ignores unknown fields. Artifact/verification/gate reports gain optional metadata without changing containers or version headers. Thread archives retain raw source JSON. No new on-disk format unreadable by rc119 was identified. This is source compatibility evidence, not a rollout/restore drill or deployed-binary claim.

## Verification

Four-CPU limits: GOMAXPROCS=4 GOFLAGS=-p=4 MAKEFLAGS=-j4 CARGO_BUILD_JOBS=4.
- gofmt gate: exit 0.
- go build ./... (30m timeout): exit 0.
- go vet ./... (30m timeout): exit 0.
- make lint (30m timeout): exit 0, including pinned Go 1.25 gofmt gate.
- First make test (60m outer timeout, repository 10m plain/25m race limits): plain pass found TestRoleHelpStatesRankingAndAdmission failure in internal/campaign; command finished with exit 2 before the race pass. The conflict resolution had changed the asserted phrase "candidate bands, pools and reasons". Restored that phrase in both help surfaces while retaining candidate effort and freshness. Rerun on the corrected tree: gofmt, go build ./..., go vet ./... and make lint each exited 0. make test exited 0: complete plain suite, complete race suite with checkptr kept, and final go vet ./... all passed. Race-package timings include cmd/t3-steward 152.905s, internal/backlog 294.005s, internal/store/sqlite 244.330s and internal/workerruntime 174.311s. No checks were weakened. The final help correction changes only prose, and the final integration diff passes git diff --check.

The final implementation commit contains the rc120 release changelog, the verified help-text correction and these declared outputs, on top of the eight ordered merge commits. HEAD is the final integration commit. The only allowed untracked paths are Steward's .t3 directory. Nothing was pushed; no PR, tag, version-file change, CI claim or fleet operation was made. Independent integration review and exact-head CI (including macOS) remain the coordinator's next release gates.
