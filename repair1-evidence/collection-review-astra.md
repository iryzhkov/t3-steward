# CHANGES REQUESTED — independent collection failure review

Exact reviewed commit: **47e53bed8bd1b0a3597519a789e0f0f907489a7c**.
Tree: **94b61978230c3af2b20e11415900f70148443689**.
Sole parent: **49b99fd2e2763ead3f68babd37c41af2783bf9c0**.

This is the independent Astra medium review, focused on async completion, durable intent, reopen, settlement uncertainty and cancellation/supersession. Three distinct blockers below prevent acceptance. No production changes were made. Both independent lead reviews are required; this report grants no acceptance to the sibling profile candidate or whole M16.

## Blockers

### R1 — P1: thread observation failure silently loses settlement retry

Paths: internal/workerruntime/local_driver.go:1147 and :1159; internal/workerruntime/runtime.go:1182.

After a real object overflow has persisted PhaseFailed, make GetThread unavailable only during bounded fallback. CollectFailure successfully publishes failed custody, skips SettleThread because the lookup returned an error, then returns nil. Runtime records PhaseCompleted with SettlePending=false. Reopen and three reconciles after observation recovers never call settlement.

Actual probe: collectErr=<nil>, firstPhase=completed, firstSettlePending=false, recoveredSettleCalls=0, finalPending=false, verification=1. This violates the separate retriable settlement contract. The producer's existing test covers a SettleThread error, but not a GetThread error. The nil-thread successful observation case is different from an unavailable observation.

Required repair: after durable failure custody, propagate observation uncertainty as ErrSettleUnproven (or equivalently preserve explicit settlement pending), and prove recovery across reopen without republishing/reverifying original outputs. Preserve actual missing/already-settled semantics. This preexisting CollectFailure behavior becomes part of the new permanent-failure route and is not covered by the changed SettleThread error wrapper.

### R2 — P2: ResultDurable accepts a non-upload receipt

Paths: internal/workerruntime/custody.go:177–190 and :631; internal/workerruntime/local_driver.go:906–918.

ResultDurable calls loadPending, which validates the manifest against its own Direction, and checks ID/assignment/epoch without requiring Direction=="upload". A structurally valid receipt at the exact result filename whose Direction is "download" returns durable=true. LocalDriver returns successful replay without finalization. PendingUploads explicitly rejects the same receipt as a non-upload, leaving zero discoverable uploads.

The probe first creates genuine successful custody, changes only the direction in the disposable fixture receipt, and exercises the real lookup, collection and discovery paths. Actual: durable=true, error=<nil>, replay=<nil>, discoverableUploads=0, verification=1. It models corrupt/substituted custody, not an assertion that an untrusted provider can edit the private worker store.

Required repair: validate fixed result upload purpose/direction and all relevant binding before treating a receipt as terminal custody; refuse ambiguous state without reclassification or overwrite. Preserve the first valid pending/acknowledged result. Existing epoch and checksum checks remain necessary but are not enough to reject this substitution.

### R3 — P1: an already-failed oversized turn has no retained original archive

Paths: internal/workerruntime/local_driver.go:987, :1070–1087 and :1146–1151; internal/workerruntime/permanent_collection_failure.go:50–51.

The repair promises that original thread archive bytes remain in collected-turn.json before bounded fallback substitutes {} and settles the thread. But settleCollectedTurn only saves a first snapshot when completion failure is empty. A terminal turn whose assistant message is FailedMarker already has a failure, so no snapshot is written. If its output publication then exceeds a real size boundary, the new permanent route advertises archive retention, emits {}, and completes/settles with no original worker archive at the promised location.

The probe uses a real finalizer/custody fixture with 1400-byte output against a 1024-byte object limit and a 2254-byte original archive, changes the original message to an explicit failed result, then completes bounded fallback. Actual PhaseCompleted and collected-turn.json ENOENT. Verification count is **0**, honestly: this failed-outcome finalization does not execute the success verification; it is not evidence of once-only successful verification.

Required repair: retain the exact original failed-turn archive before abandoning original publication, including stopped failed outcomes and direct/scoped paths, without making it a reusable successful-turn witness. Bounded publication must not settle on an unsupported retention claim. This probe does not claim that every external T3 copy was deleted; it proves the promised worker-retained recovery copy is absent.

## Source review and fences

All nine changed Go files were reviewed: workerproto/artifact.go, artifact_size.go, artifact_size_test.go; workerruntime/collection_flight.go, custody.go, local_driver.go, permanent_collection_failure.go, permanent_collection_failure_test.go, runtime.go. Huyang source reads included complete new helper/tests and changed production seams, surrounding original collection/stop/supersession/settlement/retention guards, custody parsing/publication/acknowledgement, and scoped attachment.

The new narrow object error is generated only after ID/path/kind/media/size/checksum/archive-format validation. Aggregate arithmetic uses the existing valid custody budget and unsigned observed sum. Diagnostic identity is hashed. LocalDriver classifies typed publishing rejection; untyped I/O text stays transient. The prefix is consulted for journal replay/retention, not in finishCollection's error classification. Caller-forged journal contents are not a supported external authority, and no caller policy or trust bypass is authorized by this review.

failCollection rechecks sameCollection after quiescence inside journal update. The check retains assignment ID, epoch, attempt, PhaseCollecting and local-pause fencing, plus the accepted stop request guard. A failed journal write/quiescence leaves the completed flight registered. Stale records release only the matching flight. The original successful completion and ErrSettleUnproven handling remain separate. Lease lapse intentionally is not part of sameCollection; coordinator assignment/lease fencing owns that decision.

The producer tests exercise both constructed and genuinely asynchronous stale/cancel/supersede outcomes, quiescence refusal, tiny budgets, interrupted fallback before/after custody write, ordinary transient publication, successful pending/acknowledged replay, and explicit failed-custody settlement error. The original long-finalization, lifetime, deferred stop, task-wait/park/pause, missing-workspace and importer controls are included in the required current gate. No new race-suite run was needed.

The PhaseFailed fallback completion update checks phase, assignment epoch, attempt and unchanged failure. The producer's real import test crosses LocalDriver/finalizer/custody/journal/SQLite importer and workflow projection, verifying failed sink/skipped dependent and successful-turn raw evidence preservation beyond retention. Rejected outputs intentionally are absent from the small manifest; the importer appends missing-evidence reasons. That positive test does not cover R3's originally failed turn.

Tiny failure budgets deliberately remain claimed with persistent failure intent and retained bytes, and retry the small path. They do not prove coordinator completion. First durable receipts are preserved without recapture, including acknowledged receipts, but R2 shows that receipt validation is incomplete. Readable manifest/custody digests are not independent proof of all object bytes; no stronger claim is made.

Scoped execution still attaches through scopedDriver and refuses attachment failure; direct/scoped copies retain the same Publisher. The supplied preservation fixture marks the driver scoped; it does not prove production containment, systemd, OS restart or provider diversity. No profile changes were imported or reviewed.

## Input integrity and historical evidence audit

Read **both complete** supplied plans, collection-handoff.md and collection-contract.md. Controlling review plan: jocasta:8f700f70601f44c4918d8149c504712d@1; original: jocasta:5c0e85983abb2da58beb5a306830188d@1. Lead receipt is full handoff/contract plus bounded gate/proof windows, not an independent historical execution.

Initial local HEAD was 6b0a6798736d3c9277e7688ef3016c690bf7a9ee, with only .t3 untracked. Before import, Python byte/SHA256 assertions and git bundle verify/list-heads confirmed:
- collection-candidate.bundle: 411592 bytes, SHA256 82de180803b8a22b9ce2f5dbd89092a20cc90b8c180eadd98783a2b9cf279817.
- Sole prerequisite: 6b0a6798736d3c9277e7688ef3016c690bf7a9ee; sole export: 47e53bed8bd1b0a3597519a789e0f0f907489a7c HEAD.
- XZ: 10444 bytes, SHA256 5276dd8fe8d84038649e3c8306326eb212f179ed01d6e387a234e3ccbacb4d5a.
- Losslessly decompressed externally: **92865 bytes, 995 lines**, SHA256 988cb60d628f63420520fbfe0642b7c466150512132d358a879aba822e0a2bd7.

All 995 meaningful raw log lines were read in complete windows, with overlapping reads for tool-output truncation. Raw retained at /var/tmp/r.3rg6hvhj/historical.log. The log is historical producer evidence, not this review's execution.

Historical audit:
- BEFORE real production regression actual exit 1, including sealed-fixture cleanup diagnostics.
- First AFTER actual exit 1: raw-text search of base64 JSON []byte was wrong.
- Expanded AFTER actual exit 1: fixture coordinator epoch 1 vs worker epoch 9.
- Corrected focused, relevant collection/custody/import, and later real lifecycle/retention checks actual exit 0.
- Exactly one final make check-review FAST_BASE=49b99fd2e2763ead3f68babd37c41af2783bf9c0 recorded exit 0: build, vet, full short suite, pinned Go 1.25.0/staticcheck v0.7.0, formatting and full changed-package race tests. Race receipts workerproto 7.152s and workerruntime 32.588s.
- Pre/post working source digest a4f61fb0aeff92539c9a97bfaf4598dbbe07f3dce48f92a79b08266b019fbcb6, logical index digest 9e66fa0b209d53f6d31e92423601d185f95b60593d88aa8f3f39ad81893829f9 and tree 94b61978230c3af2b20e11415900f70148443689 match. Historical ledger records no source change after gate.
- Raw-SHA baseline bundle creation failed 128, then named private-ref export corrected it. Missing-object probe initially expected 1 but returned 128; corrected absence proof preceded import.
- Producer full typed inventory was **not supplied and not read**. Its historical digest does not substitute for reading its rows. Producer's source 10689 / outside-closure 363 preservation receipt is historical only.

Current independent closure verification created /var/tmp/r.3rg6hvhj/fresh.git, fetched only baseline into its sole baseline ref, proved accepted parent and candidate absent (cat-file exit 128 each), verified/imported the sole candidate bundle, and ran strict full fsck in source and fresh import (both exit 0). Full reachable object IDs were batch-typed in each; no missing objects. Sorted full rows compare equal: **10326 objects, 527130 bytes, SHA256 68a22b34362af5e9c7a22b9863ad80c72bd6669ff8830f5554bc4a7751072374**. Current complete inventories retained externally as source.types and fresh.types; no repetitive object dump is embedded here. This is current independent verification, not access to the absent producer file or a claim SOURCE_ALL equals candidate closure.

## Current execution evidence

Huyang workspace ws_8e63cea530b3b742ea54cbf4cbc455d6; initial wsrev_1, imported candidate wsrev_37. Root remained untrusted. No trust, LSP installation or isolated Huyang verification success is claimed. Foreground shell Git/build/test operations were explicitly authorized.

Required gate ran **once**, before the disposable probe, with private 0700 GOTMPDIR created by mktemp -d /var/tmp/r.XXXXXX. Full stdout/stderr/command/exit are separately retained in /var/tmp/r.3rg6hvhj/gate.*; session **22634**, completed actual exit **0**.

```sh
GOTMPDIR=/var/tmp/r.KOnrAA GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerproto ./internal/workerruntime ./internal/backlog -run 'TestPermanentCollectionFailure|TestArtifactSizeError|Test.*(Collection|Custody|Importer|StopYields|DeferredStop|TaskWait|Parks|Parked|Pause|MissingWorkspace|RepeatedCollect|Unproven|Settle)' -count=1
```

Full stdout:
```text
ok  	github.com/iryzhkov/t3-steward/internal/workerproto	0.004s
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	7.158s
ok  	github.com/iryzhkov/t3-steward/internal/backlog	8.142s
```
Full stderr: empty.

Independent probe session **37590**, completed actual exit **1** (three assertions failed, no build failure). Files probe.command, probe.stdout, probe.stderr, probe.exit and probe.session separately retain the complete receipt in the same external directory.

```sh
GOTMPDIR=/var/tmp/r.KOnrAA GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerruntime -run '^TestReviewCollection' -count=1 -v
```

Full stdout:
```text
=== RUN   TestReviewCollectionSettlementObservationUnknown
    review_collection_overlay_test.go:37: collectErr=<nil> firstPhase=completed firstSettlePending=false recoveredSettleCalls=0 finalPending=false verification=1
    review_collection_overlay_test.go:38: unknown observation was treated as proven settlement; reopen never retries
--- FAIL: TestReviewCollectionSettlementObservationUnknown (0.07s)
=== RUN   TestReviewCollectionReceiptDirection
2026/10/05 07:51:22 WARN custody outbox entry is not an upload; skipped entry=upload-assignment-1-result.json
    review_collection_overlay_test.go:53: wrong-direction durable=true error=<nil> replay=<nil> discoverableUploads=0 discoveryError=<nil> verification=1
    review_collection_overlay_test.go:54: download receipt accepted as durable result; normal upload discovery rejects it
--- FAIL: TestReviewCollectionReceiptDirection (0.08s)
=== RUN   TestReviewCollectionFailedTurnArchiveRetention
    review_collection_overlay_test.go:64: phase=completed originalArchiveBytes=2254 retainedArchiveError=stat /var/tmp/r.KOnrAA/TestReviewCollectionFailedTurnArchiveRetention2081842004/001/runs/run-1/task-1/attempt-1-e2/collected-turn.json: no such file or directory verification=0
    review_collection_overlay_test.go:65: failed original turn archive was never retained before bounded replacement and settlement
--- FAIL: TestReviewCollectionFailedTurnArchiveRetention (0.07s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.230s
FAIL
```
Full stderr: empty.

## Full portable adversarial probe

At the exact candidate, create internal/workerruntime/review_collection_overlay_test.go with these bytes through Huyang and run the probe command above. It uses the candidate's real newCollectionFixture, not copied production logic. All mutations are confined to temporary fixture state. No production files change.

```go
package workerruntime

import (
 "context"
 "encoding/json"
 "errors"
 "os"
 "path/filepath"
 "testing"

 "github.com/iryzhkov/t3-steward/internal/domain"
)

type reviewUnavailableThread struct { *recordingT3; unavailable bool; settles int }
func (c *reviewUnavailableThread) GetThread(ctx context.Context, id string) (*domain.Thread, error) {
 if c.unavailable { return nil, errors.New("review: observation unavailable") }
 return c.recordingT3.GetThread(ctx,id)
}
func (c *reviewUnavailableThread) SettleThread(ctx context.Context,id,token string) error {
 c.settles++
 return c.recordingT3.SettleThread(ctx,id,token)
}

func TestReviewCollectionSettlementObservationUnknown(t *testing.T) {
 f:=newCollectionFixture(t,1024,4096,1400,100)
 _=f.runtime.collect(context.Background(),"assignment-1")
 if f.record(t).Phase!=PhaseFailed { t.Fatal("real size boundary not reached") }
 c:=&reviewUnavailableThread{recordingT3:f.control,unavailable:true}
 f.driver.T3=c
 err:=f.runtime.collect(context.Background(),"assignment-1")
 pending,pe:=f.custody.PendingUploadByPurpose("result")
 if pe!=nil || pending==nil { t.Fatalf("custody missing: %v",pe) }
 first:=f.record(t)
 f.reopen(t)
 c.unavailable=false
 for i:=0;i<3;i++ { if e:=f.runtime.Reconcile(context.Background());e!=nil { t.Fatal(e) } }
 t.Logf("collectErr=%v firstPhase=%s firstSettlePending=%v recoveredSettleCalls=%d finalPending=%v verification=%d",err,first.Phase,first.SettlePending,c.settles,f.record(t).SettlePending,f.process.calls)
 if !first.SettlePending || c.settles==0 { t.Fatal("unknown observation was treated as proven settlement; reopen never retries") }
}

func TestReviewCollectionReceiptDirection(t *testing.T) {
 f:=newCollectionFixture(t,8192,16384,100,100)
 if e:=f.runtime.collect(context.Background(),"assignment-1");e!=nil { t.Fatal(e) }
 p,e:=f.custody.PendingUploadByPurpose("result"); if e!=nil || p==nil { t.Fatal(e) }
 p.Manifest.Direction="download"
 raw,e:=json.Marshal(p);if e!=nil { t.Fatal(e) }
 path:=filepath.Join(f.custody.config.Root,"outbox",p.Manifest.ID+".json")
 if e=os.Chmod(path,0600);e!=nil { t.Fatal(e) }
 if e=os.WriteFile(path,raw,0600);e!=nil { t.Fatal(e) }
 durable,e:=f.custody.ResultDurable(f.pkg)
 replay:=f.driver.Collect(context.Background(),f.pkg,f.workspace)
 uploads,ue:=f.custody.PendingUploads()
 t.Logf("wrong-direction durable=%v error=%v replay=%v discoverableUploads=%d discoveryError=%v verification=%d",durable,e,replay,len(uploads),ue,f.process.calls)
 if durable || e==nil || replay==nil { t.Fatal("download receipt accepted as durable result; normal upload discovery rejects it") }
}

func TestReviewCollectionFailedTurnArchiveRetention(t *testing.T) {
 f:=newCollectionFixture(t,1024,4096,1400,2000)
 f.control.message=FailedMarker+"\noriginal provider failure"
 _=f.runtime.collect(context.Background(),"assignment-1")
 if f.record(t).Phase!=PhaseFailed { t.Fatal("real size boundary not reached") }
 if e:=f.runtime.collect(context.Background(),"assignment-1");e!=nil { t.Fatal(e) }
 _,e:=os.Stat(filepath.Join(f.driver.workspacePath(f.pkg),"collected-turn.json"))
 t.Logf("phase=%s originalArchiveBytes=%d retainedArchiveError=%v verification=%d",f.record(t).Phase,len(f.control.archive),e,f.process.calls)
 if e!=nil { t.Fatal("failed original turn archive was never retained before bounded replacement and settlement") }
}
```

The overlay was created with Huyang at document revision docrev_56f1fefcaf20b5cdc91d04e63ab47f0b9784474af93bf87cd7294d895a3ed7d3, then deleted through Huyang using that exact revision (receipt req_11410, wsrev_42). Its full code remains above. No source fixes were attempted.

## Final state and limits

After overlay removal, git diff --check, git diff --cached --check, git diff --exit-code and git diff --cached --exit-code passed; tracked/index source is unchanged. Exact candidate HEAD/tree/sole parent remain as stated. Only .t3 and declared review documents are untracked. Both foreground test sessions completed; no task-started background process remains. Documents are each below 256 KiB.

Current gate success does not supersede the three negative probe witnesses or historical fixture failures. No full M16/public activation/final physical HEAD/compiler/M17/CI/deployment/OS-restart/scalability/real-provider-diversity acceptance is claimed. Existing two-family production policy is not waived. No live config/admin/trust/provider/worker/service/schedule/fleet/push/PR/tag/release/UpKeeper effects occurred. Publishing remains deferred. Lead must arrange bounded repair and both exact-candidate independent reviews before accepting a replacement.
