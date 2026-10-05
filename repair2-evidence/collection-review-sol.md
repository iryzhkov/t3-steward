# CHANGES REQUESTED — independent collection repair1 review

Verdict: **CHANGES REQUESTED** for exact **fddd608a987b213fa3009f151bdb0c028fa290f7**, tree **04f795563d8c805ff9a70464c82530eead074117**, sole parent **47e53bed8bd1b0a3597519a789e0f0f907489a7c**. One deduplicated P1 retention finding below. No production fix was made.

Controlling review plan jocasta:6dd262c9c96737cbc83583314c16d977@1, supplied collection-review-plan.md; repair plan jocasta:b1eab7893b9cca0eb143383d0dcfc738@1; original plan jocasta:5c0e85983abb2da58beb5a306830188d@1. This is the independent Codex medium custody/import/failed-evidence lens. Both lead-controlled fresh independent reviews are required; this report is not acceptance of the candidate, sibling profile or whole M16.

## R1 — P1: preserve recovery-only evidence when a different turn replaces it

Primary path: internal/workerruntime/local_driver.go:1085, :1089, :1106–1112. The preservation branch requires found, which requires the old TurnID to equal the current TurnID. A different turn bypasses preservation and removes collected-turn.json, even when the old snapshot is RecoveryOnly. The subsequent privateJSON stores only the new observation.

The real bounded probe below first observes a failed turn and encounters ordinary untyped transient publication failure. Its full 2254-byte original archive/message and execution binding are durably recorded, and the journal correctly remains collecting. The next production collection observes a genuinely successful turn-2, verifies once, and encounters the actual archive size boundary. Reopen and bounded fallback then reach completed. Only the turn-2 snapshot remains: snapshotFiles=1, originalExactJSONRetained=false. The old failed archive was never stored in custody because the first publisher returned a transient error before publication.

This violates the repair/review requirement that genuine later success preserve earlier failed recovery bytes separately. It is not a request to reuse a different turn as successful authority. The old successful repeated-turn rule can remain intact while prior recovery-only bytes are preserved independently of the new turn identity. The supplied same-turn preservation control does not exercise this branch.

Required repair: before replacing any existing recovery-only snapshot, durably preserve its exact original bytes and identity in a separate immutable recovery file, including turn changes; refuse replacement/publication on retention failure. Retain the original repeated-turn success behavior. Add this real transient-first-publication → changed-turn successful observation → size fallback/reopen regression. No limit/schema/trust relaxation is needed.

The probe changes only disposable fixture provider state and uses actual LocalDriver/finalizer/CustodyStore/journal code. A later terminal turn is already a supported source branch, as TestRepeatedCollectionJudgesTheTurnItRecorded demonstrates. This proves local evidence loss at replacement, not live provider deletion, OS restart, or fleet loss. It is one retention finding, not a duplicate settlement/custody finding.

## Prior blockers and inspected behavior

The exact prior settlement witness is repaired: failed publication precedes observation uncertainty, GetThread errors return ErrSettleUnproven, runtime persists SettlePending, and reopen retries only settlement. Missing/already-settled observations do not create settlement effects. Both original portable tests and the expanded observation replay controls passed in the required current selection; receipt bytes, exports and verification counts remain stable.

ResultDurable now requires the exact result receipt ID and assignment epoch; package/store worker/coordinator authority remains checked by ResultDurable/loadPending/validateManifest. Upload direction, minimum package coordinator epoch and exact worker→outbox coordinator endpoints are required for pending and acknowledged paths. The existing complete chain validation retains length, object ID/size/checksum, sequence, previous digest and record digest checks. Actual CoordinatorResultImporter validates the same endpoint/order bindings. Both rehashed endpoint substitutions are refused without recapture, reclassification or receipt replacement. Valid first receipts and acknowledged success remain immutable. The retained/new invalid-receipt tests use real migrated SQLite importer and require zero transitions/artifacts; deliberate rejection is not claimed as coordinator success.

The normal failed-envelope test uses actual finalizer/custody/journal/SQLite importer and ProjectWorkflowRuns, verifies failed sink/skipped dependent and idempotent import. It passed in the current selection. Original artifact-size classification remains narrow and typed; unknown I/O and forged prefix text remain transient. No change was made to same-current assignment/epoch/attempt, pause/accepted-stop, containment quiescence, stale async result, task wait/park, missing workspace, healthy long finalization, or separate settlement retry guards.

Failed terminal snapshots now contain exact message/archive, full ExecutionIdentity and RecoveryOnly before finalization/publication. Same-turn recovery-only bytes cannot witness success; genuine same-turn success first retains a separate immutable recovery copy. Empty turn ID is honestly retained as empty recovery-only evidence, without manufacturing a successful identity. Legacy successful snapshot compatibility and original repeated-turn success controls remain. Direct/scoped failed archive controls prove zero successful verification/capture, unchanged workspace/archive and preservation beyond retention. Tiny budgets remain failed/claimed with durable attention and no false coordinator completion.

Read/decode/binding/regular-file errors are refused. Inspected privateJSON writes a private temporary file, syncs/closes it, links without replacement, and syncs its directory; its errors propagate before finalization/publication. The independent conflict control confirms an existing conflicting recovery file refuses same-turn success replacement and preserves both source and conflicting bytes. No injected fsync failure, hostile concurrent filesystem race, or production scoped attachment is claimed. R1 concerns the separate different-turn branch.

All eight changed files were reviewed: custody.go, local_driver.go, collection_fix1_test.go, collection_fix1_snapshot_test.go, BOTH full portable test files, and BOTH complete retained prior review documents. Surrounding original protocol, publication/acknowledgement, importer, collection flight, lifecycle/reopen/retention and repeated-turn guards were inspected through Huyang. The full original permanent-failure tests, including actual importer/sink paths, were read. No sibling profile input was read/imported.

## Integrity, complete evidence and historical audit

Initial HEAD was 6b0a6798736d3c9277e7688ef3016c690bf7a9ee with only .t3 untracked. Before import, bundle size/hash/header/list-heads/verify established 441843 bytes / SHA256 0c62b716778b27a3c7e2ed5f2cf3e20058e38f8090903c2a39b4616222804944, sole prerequisite baseline6b0a and sole export exactfddd608a HEAD. Fetch inspection matched tree/sole parent before detach.

Read complete review/repair/original plans, current handoff/contract, BOTH full actual prior review docs and BOTH complete portable fences (the final second Sol fence includes the actual importer). Permanent copies match supplied docs byte-for-byte: Sol35915bytes/fc8059287b87bebdb85e11fe30843b120e48d46dba94cae7a5e28195a9c270c0 and Astra19831bytes/cea451f07e1660d670f71fe57cc0d3609d4186a3ddca6b86d82524585f8b0530. Independently extracted full Go fences and passed each through gofmt; exact complete byte equality with Git blobs passed, not just function-name/assertion counts. Sol5984bytes/e872b193d15e141aee26f73f13b3bf5ac4f0a200a2629fa6267f2506226065fd; Astra3563bytes/9db84fe4065eccd726ff18f584932b437330eb711638d1c1109651a573fc7d34.

Verified XZ before decompression: 297548bytes / 3d514321a34fe6f698afa96095526df4541f1e74546919fd67ce7d3c4b248a8a. Lossless decompression is 2330917bytes,33127lines / 048c2e7926c35aa3b965e3216f523b84f741832da79a06cc910b80abadc6b161, retained externally at /var/tmp/r.kBGOTl/historical.log. Read full meaningful command/failure/gate chronology with line references; repetitive successful test enumeration/runtime chatter and inventory rows were summarized/compared programmatically, not repeatedly dumped into context. Historical records are not execution in this review.

Historical repair chronology:
- Lines33–80: actual prior portable witnesses fail1 (settlement observation, wrong direction/endpoints, failed archive absent); positive epoch/digest/untyped controls remain.
- Lines86–125: initial repairs pass0 portable probes.
- Lines131–244 and250–361: added fixture checks fail1, including absent success capture for failed finalization, changing AttemptID selecting a different snapshot location, and no-root runtime hitting missing-workspace before the intended driver seam. All failures retained.
- Lines363–458: relevant checks fail1, including those fixture issues and real unchanged repeated-turn regression.
- Lines464–552: fixture corrections leave real repeated-turn regression fail1; lines559–568 source correction/relevant checks pass0.
- Lines580–619: complete portable probes pass0.
- **First make invocation lines621–930 exits2**. Build/vet ran, then short suite failed TestLocalDriverBindsCatalogArtifactsWorkspaceAndT3 at790: missing turn ID was wrongly refused. Cleanup permission failure is also retained at791. This invocation did not reach lint/races.
- Lines932–1053: source correction plus added empty-turn recovery control and affected/original repeated-turn checks pass0.
- **Second justified make invocation lines1065–1107 exits0**: full build/vet/short suite, pinned Go1.25.0/staticcheckv0.7.0 and whole format gate, then workerruntime race53.474s. No source edit follows this successful gate.
- Lines1109–1213: post fingerprint, commit/export, fresh baseline absence/import/fsck/closure, process and index receipts pass0. Earlier original-producer audit embedded at624–712 is historical-within-historical, not another repair gate.

Every embedded structured block was extracted in full and its own byte count/SHA256 checked: tool-events, retention-audit, pre-first-failed-gate, pre, post, index-current, package-receipt, source-closure and BOTH all-object inventories. Full pre/post JSON equality passed. All1428 source path/blob rows equal current index rows; all1428 complete mode/OID/stage/path index entries equal index-current and candidate tree. Independently reproduced logical index digest 0b5e8cb983045ec2d5b7d186745588f9b569d8323f0cb6f5e966b92767a35b40 using git ls-files -s -z. Historical diff digest3031d97f9171feb94eb267c6d7428185b235807d1fb6f820408c527f8c074ae9 is a matching pre/post receipt; direct commit-diff serialization here has a different digest and is not represented as the same command. Source/index/tree row equality provides the complete final-state comparison.

Historical source-all-before/after inventories are exact byte equal,10708 objects, including369 outside candidate closure. They were actually supplied/extracted/compared; no digest-only historical witness is claimed.

Current independent closure used /var/tmp/r.kBGOTl/fresh.git. A temporary named source baseline ref exported a standalone baseline bundle, then was removed. Fresh bare repository had only the baseline ref/closure; accepted49b99fd, parent47e53bed and candidatefddd608a were each absent with actual128 before candidate import. Sole collection bundle verify/import and source/fresh strict full fsck exited0. Complete sorted typed reachable closure equality source==fresh==supplied historical rows: **10339objects,527798bytes,SHA25620ad45641441aa188ef8b7744bb08beada0d0b7a6401ec9e8a5e7b456bf82819**, zero missing objects. Full rows remain externally retained; no HEAD-only/sample substitute. Current all-object inventory had10702 objects and was unchanged through closure verification, preserving unrelated source objects. This review checkout's all-object count need not equal the producer's10708; neither ALL inventory is equated to reachable closure.

## Required current focused command — ONCE

Private GOTMPDIR /var/tmp/r.kBGOTl mode0700. Huyang remains untrusted; no trust/config change and no isolated Huyang verification claim. Authorized foreground Git/Go/gofmt used.

```sh
GOTMPDIR=/var/tmp/r.kBGOTl GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerproto ./internal/workerruntime ./internal/backlog -run 'TestPermanentCollectionFailure|TestArtifactSizeError|TestCollectionFix1|TestReviewCollection|TestIndependentCollectionReceiptAndRetention|Test.*(Collection|Custody|Importer|StopYields|DeferredStop|TaskWait|Parks|Parked|Pause|MissingWorkspace|RepeatedCollect|Unproven|Settle)' -count=1
```

Actual exit **0**, completed foreground session69026. Full stdout:
```text
ok  	github.com/iryzhkov/t3-steward/internal/workerproto	0.005s
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	16.307s
ok  	github.com/iryzhkov/t3-steward/internal/backlog	12.098s
```
Full stderr: empty. Receipt gate.stdout192bytes/SHA2569ce1e2d86c3a0bc4c9647794e5bcf0ec491265ee2a728957a46da84d54ce98e5; separate gate.stderr/gate.exit retained externally. No current whole gate or race rerun.

## Complete independent portable probe and actual execution

Huyang guarded create of internal/workerruntime/repair1_independent_overlay_test.go, authorized foreground gofmt, then:
```sh
GOTMPDIR=/var/tmp/r.kBGOTl GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerruntime -run '^TestRepair1Independent' -count=1 -v
```
Actual exit **1**, completed session20047; one real assertion failure, positive conflict control passes. No compiler/fixture correction or repeated probe run. Full stdout:
```text
=== RUN   TestRepair1IndependentLaterTurnRetention
    repair1_independent_overlay_test.go:60: first=collection deferred: publish result custody: permanent collection size failure: I/O unknown (untyped) second=collection failed permanently; bounded failure custody pending: artifact object size exceeds limit: limit=1024 rejected=2254 identity=95bc57852914c1fc phase=completed originalTurn=turn-1 laterTurn=turn-2 originalArchiveBytes=2254 snapshotFiles=1 originalExactJSONRetained=false verification=1
    repair1_independent_overlay_test.go:62: original failed recovery evidence discarded when a different turn became successful
--- FAIL: TestRepair1IndependentLaterTurnRetention (0.08s)
=== RUN   TestRepair1IndependentRecoveryConflictRefuses
    repair1_independent_overlay_test.go:91: replacement=retain failed collected turn: contained receipt identity changed originalUnchanged=true conflictingRecoveryUnchanged=true
--- PASS: TestRepair1IndependentRecoveryConflictRefuses (0.02s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.107s
FAIL
```
Full stderr: empty. probe.stdout1064bytes/SHA256de2ec6df91f5002b7c08a5f59fda47f15047aa6cca47cdac8edff7693c239b3c; separate probe.stderr/probe.exit retained externally.

Full formatted portable code (all mutations confined to t.TempDir fixture state):
```go
package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepair1IndependentLaterTurnRetention(t *testing.T) {
	f := newCollectionFixture(t, 1024, 4096, 100, 2000)
	c := &fix1CountingControl{recordingT3: f.control}
	f.driver.T3 = c
	c.message = FailedMarker + "\noriginal failed turn\n"
	originalArchive := bytes.Clone(c.archive)
	f.driver.Publisher = independentPrefixPublisher{f.custody}
	firstErr := f.runtime.collect(context.Background(), "assignment-1")
	if firstErr == nil || f.record(t).Phase != PhaseCollecting {
		t.Fatalf("transient publication fixture: %v", firstErr)
	}
	path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snap collectedTurn
	if err = json.Unmarshal(original, &snap); err != nil || !snap.RecoveryOnly || !bytes.Equal(snap.Archive, originalArchive) {
		t.Fatal("failed observation not retained")
	}
	c.thread.TurnID = "turn-2"
	c.message = "genuine later success"
	c.archive = []byte(strings.ReplaceAll(string(c.archive), "turn-1", "turn-2"))
	f.driver.Publisher = f.custody
	secondErr := f.runtime.collect(context.Background(), "assignment-1")
	if f.record(t).Phase != PhaseFailed {
		t.Fatalf("real later archive boundary not hit: %v", secondErr)
	}
	f.reopen(t)
	if err = f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(path), "collected-turn*.json"))
	if err != nil {
		t.Fatal(err)
	}
	retained := false
	for _, p := range paths {
		raw, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		if bytes.Equal(raw, original) {
			retained = true
		}
	}
	t.Logf("first=%v second=%v phase=%s originalTurn=turn-1 laterTurn=turn-2 originalArchiveBytes=%d snapshotFiles=%d originalExactJSONRetained=%v verification=%d", firstErr, secondErr, f.record(t).Phase, len(originalArchive), len(paths), retained, f.process.calls)
	if !retained {
		t.Error("original failed recovery evidence discarded when a different turn became successful")
	}
}

func TestRepair1IndependentRecoveryConflictRefuses(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	thread := *f.control.thread
	if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "failed", []byte("{}"), "original failure", ""); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	recovery := filepath.Join(filepath.Dir(path), "collected-turn-recovery-"+shortDigest(before)+".json")
	foreign := []byte("{}")
	if err = os.WriteFile(recovery, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = f.driver.settleCollectedTurn(f.pkg, thread, "genuine success", f.control.archive, "", "")
	after, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	old, e := os.ReadFile(recovery)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("replacement=%v originalUnchanged=%v conflictingRecoveryUnchanged=%v", err, bytes.Equal(before, after), bytes.Equal(foreign, old))
	if err == nil || !bytes.Equal(before, after) || !bytes.Equal(foreign, old) {
		t.Fatal("conflicting recovery replaced or source lost")
	}
	var permanent *permanentCollectionFailure
	if errors.As(err, &permanent) {
		t.Fatal("retention refusal became permanent size failure")
	}
}
```

Guarded Huyang deletion used the formatted file's exact document revision; removal req_11726/wsrev_44. No production source/index mutation.

## Final scope and limits

Tracked source/index clean, exact HEAD/tree/sole parent unchanged; disposable overlay removed. All foreground processes started here completed; no task-started background job remains. Declared review.md and continuation.md are each below256KiB.

Local review only: publishing deferred and schedules disabled. No push/PR/tag/release/UpKeeper/CI, fleet/service/provider/admin/live config/trust mutation, Claude/delegation/nested review/effort escalation. No whole M16/public activation/final physical HEAD/compiler/M17/restart/scalability/diversity acceptance. Production two actual provider-family gate remains. Both independent reviews and a validated replacement are still needed before lead acceptance.
