# M16 initial-session-title independent review

Verdict: **changes-requested**. One blocking reliability finding; no additional nonblocking findings.

## Identity and scope

Reviewer: Codex harness, GPT-6-Astra (`gpt-6-astra`), medium effort. Run `run-467e81be1d4c95dae79d8a3d19343139`; task `task-7d36eb7438960153aa966513671abc7a`; attempt `attempt-35294be8bf7d1023c0994ce0db90b694`. One unattended session; no provider/model switch, escalation, subagent, nested review or task submission. Token/cost totals are unavailable.

Initial workspace HEAD asserted before import: `6b0a6798736d3c9277e7688ef3016c690bf7a9ee`.
Reviewed commit: `6f37c01c6bf1525aebd91ab7a455d33761e2a165`.
Review base / sole parent: `0ef5fb3b0406fb7ff9d062fc681bb0b8df9b06f8`.
Reviewed tree: `cfa67ae2c87ddc9d38686d7a78bf4f92c8727c14`.
Accepted foundation tree: `c5906ff55b0215be715d3d60ef10e3228382e75f`, with the baseline as its sole parent.
Bundle SHA256: `363bd61b73fc47693bb8a5b6580f3ee94339f3d8fc2a17ffa43436cbbdc34aa6`.

All identities match. The title delta is exactly 12 paths, 566 insertions and 11 deletions, matching the supplied map. Only that delta was reviewed; the accepted inert foundation was neither changed nor reopened for acceptance.

Read requirements.md (all 15 lines), handoff.md, title-contract.md and verification.log in full. These are retained producer `run-a7cf03b242c5c5ade6486444b1d84a7d` evidence, not review authority. Source was read/searched through Huyang. The input symlink leaves the source workspace; resolving it and opening a separate documents workspace allowed the required reads without bypassing Huyang guards.

## Blocking finding B1 — optional inventory recovery changes an already offered package

**P2, acceptance blocker.** Location: `internal/backlog/session_display.go:13-18`, invoked on every build at `internal/backlog/execution_package.go:197`.

The optional display decision is recomputed from current inventory each time an assignment offer is built. An ordinary package first built during an inventory read failure omits display and its capability. When inventory recovers, the same assignment gains both, changing its content address. There is no retained per-assignment display/omission decision.

This conflicts with the existing worker replay contract: `internal/workerruntime/runtime.go:250-252` withholds a same-epoch offer whose package differs, and `samePackageIgnoringCoordinatorEpoch` at lines 308-311 compares every other field. Consequently, after a claim response is lost or cannot be persisted, the recovered inventory prevents the worker from returning that claim again. This is an ordinary-work denial introduced by optional UI support, not an existing mandatory-capability refusal.

The coordinator actually rebuilds offers at `internal/backlog/worker_exchange.go:368`. Claim persistence failure explicitly leaves the assignment offered at lines 417-420. `internal/store/sqlite/fleet.go:119-177` saves the worker snapshot but does not convert the assignment into a persisted claim. The original worker journal package therefore remains relevant to the next delivery. This finding does not depend on a worker upgrade, malicious inventory, provider activity or a changed task identity.

Independent isolated repro, with Go overlay copies of two existing test files and no source edits:
- Producer fixture: unavailable inventory -> inventory advertising session-display-v1, with identical assignment and identity. Hash changes from `69e42ec068b29235afc223f7653d3587d4fb01bf65f434d8cc9b07727e588977` to `27a5e75e9204178163d9fb14db5f0b85c92ccda8371c8ffdcfde63338539151d`.
- Runtime fixture: first valid ordinary offer returns one claim; replay with only display/capability added and a valid recomputed manifest returns **zero claims**, with warning `offer replay changed the execution package; offer withheld`.

Required repair: make the negotiated display/omission decision stable for an assignment's replay lifetime, including transient inventory failure and recovery. Preserve strict package/content-address and mandatory capability fences; do not simply ignore arbitrary package changes. Add a regression covering lost/unpersisted claim plus inventory recovery (and the reverse display-availability transition). Existing replay tests at `internal/backlog/session_display_test.go:47-56` repeat with unchanged capability inputs, so they do not exercise this case.

## Other acceptance evidence

- `internal/workerproto/session_display.go:31-61,64-77` bounds names at UTF-8 rune boundaries, collapses controls/whitespace/Unicode Cf, removes replacement/invalid runes, and validates canonical names at 64 runes/256 bytes. Title components are separately bounded to 32 runes/128 bytes; the fixed format remains below 512 bytes, with the digest and startup suffix outside truncation. No additional Unicode/bounds blocker found.
- Title role uses the recorded ReviewJudge flag from the resolved task, never name inference (`execution_package.go:100,197,405-441`). Supervision comes from the activation object. Names fall back to identities; the full run ID feeds a six-byte SHA256 display digest (`session_display.go:82-106`). Retry attempt/thread/assignment changes do not affect a fixed package's title. Short digest collision resistance is a display aid, not uniqueness authority.
- Required display capability and display presence are enforced in both directions (`workerproto/package.go:410-415`). Existing strict nested DecodePayload remains at `protocol.go:361-374`. Manifest marshal/hash validation at `package.go:176-212` includes display; omitted nil display preserves old golden bytes. Independent workerproto tests passed, including tampering, nested unknown field rejection, malformed UTF-8, and legacy golden.
- Ordinary mandatory capability validation still precedes optional negotiation (`execution_package.go:194-198,257-311`). Inventory failure itself is swallowed only for optional display. This fixes the immediate second-lookup rejection but does not fix B1's later replay. Activation negotiation still refuses unknown/missing mandatory supervision inventory at `supervision_package.go:74-84`; optional display uses that advertised inventory at lines 162 and 514-517. The exchange's existing activation-only mandatory gate remains at `worker_exchange.go:635-646`.
- Standard and activation paths change only Title in their existing CreateAndStartThread input (`local_driver.go:815-821`, `supervision_activation.go:241-247`). Unchanged control code sets title on thread.create, then starts the first turn, retaining deterministic dispatch IDs (`internal/control/t3/control.go:551-606`). Focused tests passed for unchanged provider input/identity, preflight failure without provider effects, activation fallback/naming and deterministic control dispatch.
- No public manifest/catalog/Task/Workflow/Attempt schema changes, authority activation, dynamic progress, metadata updates or extra T3/provider actions appear in this delta. The startup label makes no completion-fraction claim. Dynamic progress, rename ownership and live field proof are later-unit work and are not blockers here.

## Commands and results

All commands were local; Go checks used `GOMAXPROCS=2 GOFLAGS=-p=2`.

- `git rev-parse HEAD`: exit 0, exact baseline above.
- `sha256sum .t3/inputs/inputs/session-titles.bundle`: exit 0, exact SHA256 above.
- `git bundle verify .t3/inputs/inputs/session-titles.bundle`: exit 0; advertised reviewed HEAD, exact baseline prerequisite.
- `git fetch .t3/inputs/inputs/session-titles.bundle HEAD:refs/review/m16-titles` and `git checkout --detach 6f37c01c6bf1525aebd91ab7a455d33761e2a165`: exit 0 each.
- `git show -s --format='%H%n%T%n%P' HEAD`, `git show -s --format='%H %T %P' HEAD^`, `git diff --stat HEAD^ HEAD`: exit 0, exact identities and change map above.
- `GOMAXPROCS=2 GOFLAGS=-p=2 go test -short -count=1 ./internal/workerproto`: exit 0, 0.088s.
- `GOMAXPROCS=2 GOFLAGS=-p=2 go test -short -count=1 ./internal/backlog ./internal/workerruntime ./internal/control/t3 -run 'Test(OfferSessionDisplayNegotiation|DisplaySupportDoesNotBypassMandatoryPreflight|ActivationPackageCarriesRetrievableFrozenEvidence|InitialTitleDispatchPreservesProviderInput|DisplayTitlePreflightFailureHasNoProviderEffect|InitialSupervisionTitleDispatch|CreateAndStartThread)'`: exit 0; packages 0.008s / 0.005s / 0.003s.
- `GOMAXPROCS=2 GOFLAGS=-p=2 go test -overlay=/tmp/m16-title-review-iXhSmE/overlay.json -short -count=1 ./internal/backlog ./internal/workerruntime -run '^TestReview(InventoryRecoveryChangesOrdinaryReplay|DisplayOnlyReplayMustReclaim)$'`: exit **1**, both assertions reproduce B1 (0.004s / 0.005s). This is an intentional isolated regression check, not a reviewed-source edit.
- `git diff --check`, `git diff --check 0ef5fb3b0406fb7ff9d062fc681bb0b8df9b06f8..HEAD`, `git diff HEAD --exit-code`: exit 0 each. Reviewed source remains unchanged.

No full review/normal/race/build/lint gates were repeated. Huyang reports the source root untrusted for configured verification commands; the explicitly requested bounded Go checks ran directly through the shell, without trust/config mutation.

## Producer verification assessment and limitations

The retained log honestly records an initial affected-suite exit 1: the ordinary-worker fixture accidentally set ReviewJudge and encountered the existing project-context mandatory gate (whose diagnostic says preflight). The final test splits ordinary and judge cases and advertises project-context for the latter (`backlog/session_display_test.go:23-28,83-103`); source gates were not weakened. The log records successful backlog repair and final ordering checks, then one final check-review against the accepted parent.

That supplied gate reports build, vet, full **short** suite, staticcheck/gofmt, and non-short race tests for exactly backlog/workerproto/workerruntime all passing. It is not evidence of a separate full non-short non-race suite, all-package race coverage, source CI, live T3 qualification or deployment. These are producer receipts, not independently repeated gates; they do not cover the newly reproduced inventory-transition replay failure.

No remote pushes, PRs/tags, UpKeeper operations, host/config/service/timer/schedule/enrollment changes, or live T3 writes occurred. No production diversity waiver follows from this same-provider review. Full M16 acceptance remains pending.

## Reproduction source

Append these functions only to isolated copies of the named existing test files, retaining their original imports/content, and use a Go overlay mapping the original absolute file paths to those copies. The exact overlay used above is under `/tmp/m16-title-review-iXhSmE`; the complete added functions are retained here so the finding does not depend on temporary-file retention.

### internal/backlog/session_display_test.go

```go
func TestReviewInventoryRecoveryChangesOrdinaryReplay(t *testing.T) {
 now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
 records, assignment := packageBuilderFixture(now)
 builder := packageBuilder(t, records)
 builder.Store = unavailableDisplayInventory{packageRecordStore{records: records}}
 first, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
 if err != nil { t.Fatal(err) }
 builder.Store = packageRecordStore{records: records, snapshots: []domain.WorkerSnapshot{
  {WorkerID: assignment.WorkerID, Inventory: domain.WorkerInventory{Capabilities: []string{workerproto.PackageCapabilitySessionDisplay}}},
 }}
 second, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
 if err != nil { t.Fatal(err) }
 if first.Package.Package.Display != nil || second.Package.Package.Display == nil { t.Fatal("invalid repro setup") }
 if !reflect.DeepEqual(first.Package.Package.Identity, second.Package.Package.Identity) { t.Fatal("identity changed") }
 if first.Package.SHA256 != second.Package.SHA256 { t.Fatalf("optional inventory recovery changed same-assignment manifest: %s -> %s", first.Package.SHA256, second.Package.SHA256) }
}
```

### internal/workerruntime/session_display_test.go

```go
func TestReviewDisplayOnlyReplayMustReclaim(t *testing.T) {
 runtime := newTestRuntime(t, t.TempDir(), &fakeDriver{})
 offer := testOffer(t)
 claims, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}})
 if err != nil || len(claims.Claims) != 1 { t.Fatalf("first claim: %+v %v", claims, err) }
 // Inventory recovers after this claim is lost before coordinator persistence.
 offer.Package.Package.Display = &workerproto.SessionDisplay{WorkflowName: "Campaign", TaskName: "Build"}
 offer.Package.Package.RequiredCapabilities = append(offer.Package.Package.RequiredCapabilities, workerproto.PackageCapabilitySessionDisplay)
 offer.Package, err = workerproto.BuildExecutionPackageManifest(offer.Package.Package)
 if err != nil { t.Fatal(err) }
 claims, err = runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}})
 if err != nil { t.Fatal(err) }
 if len(claims.Claims) != 1 { t.Fatalf("same assignment replay with display-only change returned %d claims; want 1", len(claims.Claims)) }
}
```
