# CHANGES REQUESTED — independent repair1 collection review

Exact reviewed commit: **fddd608a987b213fa3009f151bdb0c028fa290f7**.
Tree: **04f795563d8c805ff9a70464c82530eead074117**.
Sole parent: **47e53bed8bd1b0a3597519a789e0f0f907489a7c**.

Independent Astra medium review, settlement/reopen and recovery-only durability lens. Controlling review plan jocasta:6dd262c9c96737cbc83583314c16d977@1; repair plan b1eab7893b9cca0eb143383d0dcfc738@1; original plan 5c0e85983abb2da58beb5a306830188d@1. Two deduplicated P2 blockers remain. No production fixes. BOTH fresh independent reviews are required by the lead; this is only this review's verdict, not sibling/profile or whole-stack acceptance.

## R1 — P2: preserve recovery-only evidence when the turn identity changes

**internal/workerruntime/local_driver.go:1087–1107**, specifically the preservation condition at 1091 and unconditional old-path removal at 1107.

The separate recovery copy is created only when `found && recorded.RecoveryOnly && failure == ""`. Here `found` requires the old and current turn IDs to match. A genuinely successful later observation with a newly available turn ID, or a different turn ID within the same execution identity, takes the !found branch and deletes the old recovery-only snapshot without preserving it.

The exact portable probe below records failed evidence through the actual settleCollectedTurn helper, then records genuine success. Same-turn control retains two files and exact original bytes. Empty-to-known and old-to-new turn cases each return success with only the new file and no exact original retained. No assertion was softened. This is the new failed-evidence preservation obligation, separate from the established rule that an old **successful witness** must not authorize a different turn. The existing successful different-turn semantics can remain while archiving recovery-only bytes before replacement.

Requested repair: preserve an existing valid recovery-only record before any replacement that would remove it, including absent/different turn identity; keep its original execution/thread/turn identity and exact message/archive. Do not make the old failed observation a successful witness. Cover both identity transitions and retention-write refusal.

Limit: this witness exercises the production snapshot helper and real filesystem, as the producer's same-turn success test does. It does not claim a live provider changed turn IDs or that every external copy was deleted. It proves deletion of the promised worker recovery copy under supported helper inputs.

## R2 — P2: retry must finish snapshot durability before publication

**internal/workerruntime/local_driver.go:1074–1087, 1113–1119**, using **internal/workerruntime/contained_lifecycle.go:65–82** (privateJSON).

privateJSON writes and fsyncs the temporary file, links it at the final path, then opens/fsyncs the parent directory. An error after the link leaves a visible snapshot and correctly fails the first collection. On retry, settleCollectedTurn merely reads that snapshot, finds the same turn, and returns without completing/retrying directory durability. Visibility is treated as proof that the failed durable write completed.

The independent real-filesystem probe gives the attempt directory mode 0300: creation/traversal succeed but opening it for directory fsync fails. The first actual LocalDriver.Collect refuses after the snapshot becomes visible, with no result receipt. The directory remains unopenable. The second actual Collect returns nil, creates custody, and calls settlement once. Exact output: firstRefused=true, snapshotVisible=true, directoryStillUnopenable=true, retryErr=<nil>, published=true, settles=1, unchanged=true.

Requested repair: ensure a previously visible snapshot is durably established before permitting finalization/publication/settlement after a failed write, including the post-link directory-open/sync boundary. Apply the same discipline to recovery-copy replay, whose equal-bytes fast path also does not retry directory sync. Do not delete evidence merely to retry it.

Limit: the test injects a real permission failure before directory fsync, not an EIO from fsync itself and not a power-loss experiment. The source shows both post-link errors leave the same visible-path state. The failure is unsupported durability authorization, not observed physical loss after a machine crash. No live permissions/configuration changed; chmod is confined to t.TempDir.

## Status of the original three blockers and surrounding contracts

The original observation-uncertainty blocker is repaired on the tested paths: CollectFailure publishes first, then wraps GetThread errors with ErrSettleUnproven; runtime persists SettlePending. Reopen/reconcile retries only settlement, preserving receipt bytes and original export/verification counts. Actual nil-thread and already-settled observations cause no new settle effect. Publication failure still wins over settlement uncertainty.

The original receipt-authority blocker is repaired in the inspected seams. ResultDurable fixes the exact upload-assignment-result ID, package/store worker and coordinator authority, assignment epoch, upload direction, minimum package coordinator epoch and worker/outbox endpoints. loadPending validates version, no trailing content, protocol limits, worker/epoch, complete chain length/order/object/size/checksum/previous digest and record digests. Current authority bounds allow legitimate coordinator failover. Pending and acknowledged controls preserve valid first receipts; rehashed endpoint substitutions and direction/epoch/digest/order/object corruption refuse without recapture or typed reclassification. The real SQLite-backed importer refuses invalid receipts with zero transitions/artifacts. Exact result filename fixes purpose; no public schema change.

The original failed-archive absence blocker is repaired for the ordinary same-turn path: full message/archive plus ExecutionIdentity and thread/turn are recorded before finalizer/publication, including already-failed outcomes. RecoveryOnly prevents failed evidence from authorizing successful repeated-turn recovery. Missing turn ID is retained as empty recovery-only identity. Newly written identity mismatches refuse; legacy successful snapshots without Identity retain compatibility. Genuine same-turn success first preserves old bytes separately. R1 and R2 describe remaining preservation/durability boundaries, not repeats of the original no-snapshot finding.

Reviewed all eight changed files, including both complete new test files, both retained portable files, both complete retained review documents, full production diff and surrounding collection/snapshot/custody/privateJSON helpers. Also read original permanent-failure helper and full 629-line tests, relevant protocol validation, actual importer authority, original repeated-turn control, runtime PhaseFailed/Completed/reconcile/retention and async finish/sameCollection guards.

Typed object errors follow metadata validation; aggregate size remains bounded with unsigned observed sum. Unrecognized I/O and forged prefix text remain transient. Quiescence, same assignment/epoch/attempt, collecting phase, local pause and accepted stop fences remain in place; stale/cancelled/superseded flights cannot write a new failure intent. Journal/quiescence failures retain the finished flight; healthy long finalization has no invented retry deadline. Permanent failed intent stays claimed until small failed custody is durable. Tiny budgets persist actionable failure without re-verification or false completion. Successful and failed uncertain settlement remain distinct.

The existing actual importer test crosses finalizer, custody, journal, SQLite and workflow projection, proving failed sink/skipped dependent and replay idempotency. Successful original output/capture/archive survive retention; already-failed finalization honestly performs zero success verifications and no success capture, while workspace/archive remain recoverable. Direct/scoped-flag fixtures exercise local branches; they are not production containment or provider attachment evidence.

Snapshot reads reject nonregular files/symlinks and decode/identity errors. Existing no-root/directory/symlink controls pass. New conflict/symlink/unwritable-directory recovery-copy controls also refuse with unchanged active bytes, zero verification, zero custody and zero settlement. R2 is specifically the post-link retry gap, not a claim that the first error is ignored. No race rerun or whole gate was necessary for these deterministic probes.

## Input, chronology and complete structured evidence audit

Initial HEAD was 6b0a6798736d3c9277e7688ef3016c690bf7a9ee, tracked/index clean. Before import, bundle verify/list-heads/hash and size matched sole prerequisite baseline and sole export fddd608a. Imported only the collection bundle; inspected target tree and sole parent before detaching. No profile sibling content was read or imported.

Read the full review/repair/original plans, current handoff/contract and BOTH full actual prior reviews, including both Sol portable versions and the final second version with the importer. The retained documents read through Huyang compare byte-for-byte to the supplied inputs. Complete portable fences compare exactly after gofmt:
- Astra 3563 bytes / 9db84fe4065eccd726ff18f584932b437330eb711638d1c1109651a573fc7d34.
- Sol final second fence 5984 bytes / e872b193d15e141aee26f73f13b3bf5ac4f0a200a2629fa6267f2506226065fd.
No functions/assertions removed or softened.

Bundle: 441843 bytes / SHA256 0c62b716778b27a3c7e2ed5f2cf3e20058e38f8090903c2a39b4616222804944.
XZ verified before decompression: 297548 bytes / 3d514321a34fe6f698afa96095526df4541f1e74546919fd67ce7d3c4b248a8a.
Complete decompressed raw: 2330917 bytes, 33127 lines / 048c2e7926c35aa3b965e3216f523b84f741832da79a06cc910b80abadc6b161, retained at /var/tmp/r.e71ylt13/historical.log.

Read the full meaningful chronological command/failure/gate/packaging portion (lines 1–1215, with overlap to recover transport truncation). Parsed every complete STRUCTURED_EVIDENCE block by advertised byte length and independently checked its SHA256, then compared full source/index/inventory rows programmatically. Read the complete tool-events correction ledger. Repetitive 10k-object rows were not dumped into context or represented as manual reading from a hash.

Historical chronology is producer evidence, not execution in this review:
- BEFORE portable tests exit1 with all three original blockers; after initial repair portable tests exit0.
- Expanded controls exit1 for incorrect successful-capture assumption, foreign AttemptID selecting another directory, and no-root runtime fixture reaching missing-workspace logic; subsequent corrections and their still-failing runs remain.
- Relevant runs exposed the real unchanged repeated-turn regression; repaired with recovery-copy-before-genuine-success logic, then focused relevant/portable runs exit0.
- FIRST repair make check-review FAST_BASE=47e53bed... starts at line621; its output interleaves an explicitly historical older gate audit and ps receipt. Repair gate output starts line723. It fails the unchanged deterministic-failure/missing-turn-ID test at 789–791, short runtime suite31.130s, make exit2 at930, before lint/race.
- Missing-turn-ID source correction, new test and final affected controls pass0 at936–1053.
- SECOND justified repair make at1065–1107 passes build, vet, full short suite, pinned Go1.25.0/staticcheckv0.7.0, full formatting and changed-package workerruntime race53.474s; exit0. Some unchanged short-suite packages are cached. This is not a single clean historical invocation.
- Final source/index/tree post comparison, commit, bundle/fresh closure and process receipts pass. All failed fixture/source corrections remain in the lossless raw log.

Complete final pre.json == post.json, all1428 working (path, blob) rows equal the candidate tree; all1428 logical-index rows equal current Git index and candidate tree, including modes/stage0. Index SHA256 0b5e8cb983045ec2d5b7d186745588f9b569d8323f0cb6f5e966b92767a35b40; recorded final diff SHA2563031d97f9171feb94eb267c6d7428185b235807d1fb6f820408c527f8c074ae9. The earlier failed-gate fingerprint is different and was preserved, not substituted for the final one.

Full historical SOURCE_ALL before/after rows are equal:10708 objects, with369 outside the10339 candidate closure. Full historical closure rows were supplied and compared, not inferred from a digest.

## Current independent complete closure proof

Created an external baseline bundle using a temporary local ref, removed that ref, then populated a new bare repository with only refs/heads/baseline. Before candidate import, accepted parent49b99fd, prior candidate47e53bed and targetfddd608a were all absent. A first assertion incorrectly expected cat-file -e to return128; this environment returns1 for absence. It stopped before import. Corrected to nonzero absence, repeated all three checks (actual1 each), then verified/imported only the supplied collection bundle.

Strict full fsck source and fresh repository both exit0. Enumerated every reachable object using rev-list, batch-typed every ID, rejected missing rows, sorted and compared complete bytes in source, fresh import and historical closure. Exact equality: **10339 objects,527798 bytes,SHA25620ad45641441aa188ef8b7744bb08beada0d0b7a6401ec9e8a5e7b456bf82819**. Current source ALL10702 before/after identical, including363 outside closure. Different historical/current unrelated object totals are expected; no SOURCE_ALL==closure assertion.

External receipts: /var/tmp/r.e71ylt13/closure.commands.json, current-source.types, current-fresh.types, current-all-before.types, current-all-after.types, plus all extracted historical structures. These are independent current comparisons; historical gate timing is not a current witness.

## Current execution — exact required command once

Private short GOTMPDIR /var/tmp/r.Of0tcT (0700), GOMAXPROCS=2, GOFLAGS=-p=2. Root remains Huyang-untrusted; no trust change. Source reads and overlay creation/deletion used Huyang. Initial relative-root request was refused and corrected to absolute root. Mounted .t3/inputs is a symlink outside the repository; source-workspace reads were refused, then supplied external artifacts were read via their external mount without mutation.

```sh
GOTMPDIR=/var/tmp/r.Of0tcT GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerproto ./internal/workerruntime ./internal/backlog -run 'TestPermanentCollectionFailure|TestArtifactSizeError|TestCollectionFix1|TestReviewCollection|TestIndependentCollectionReceiptAndRetention|Test.*(Collection|Custody|Importer|StopYields|DeferredStop|TaskWait|Parks|Parked|Pause|MissingWorkspace|RepeatedCollect|Unproven|Settle)' -count=1
```

Actual exit **0**. Full stdout:
```text
ok  	github.com/iryzhkov/t3-steward/internal/workerproto	0.005s
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	18.656s
ok  	github.com/iryzhkov/t3-steward/internal/backlog	14.115s
```
Full stderr: empty. Foreground session73824 completed. Full argv/env/stdout/stderr/exit retained in gate.* under the external evidence directory. No current whole-gate or race rerun.

## Complete portable independent probes

Create internal/workerruntime/repair1_independent_astra_test.go through Huyang with the complete code below. It calls actual candidate helpers and LocalDriver. All filesystem mutations are disposable fixture data. First execution used the file through the end of TestRepair1ReviewRecoveryWriteRefusal. The later durability function was appended to address the concrete remaining post-link concern; only that new function was then executed. Earlier functions and assertions were unchanged.

```go
package workerruntime

import (
 "bytes"
 "context"
 "encoding/json"
 "errors"
 "os"
 "path/filepath"
 "testing"
)

func TestRepair1ReviewRecoveryReplacement(t *testing.T) {
 for _, firstTurn := range []string{"turn-1", "", "turn-old"} {
  t.Run("first="+firstTurn,func(t *testing.T) {
   f:=newCollectionFixture(t,8192,16384,100,100)
   thread:=*f.control.thread
   thread.TurnID=firstTurn
   message:="original failed observation"
   archive:=[]byte("original failed archive bytes")
   if _,_,_,err:=f.driver.settleCollectedTurn(f.pkg,thread,message,archive,"original failure","");err!=nil {t.Fatal(err)}
   path:=filepath.Join(f.driver.workspacePath(f.pkg),"collected-turn.json")
   before,err:=os.ReadFile(path);if err!=nil {t.Fatal(err)}
   thread.TurnID="turn-1"
   if _,_,_,err=f.driver.settleCollectedTurn(f.pkg,thread,"genuine finished",f.control.archive,"","");err!=nil {t.Fatal(err)}
   files,err:=filepath.Glob(filepath.Join(filepath.Dir(path),"collected-turn*.json"));if err!=nil {t.Fatal(err)}
   retained:=false
   for _,file:=range files {raw,e:=os.ReadFile(file);if e!=nil {t.Fatal(e)};if bytes.Equal(raw,before) {retained=true}}
   t.Logf("firstTurn=%q laterTurn=%q snapshots=%d exactOriginalRetained=%v",firstTurn,thread.TurnID,len(files),retained)
   if !retained {t.Error("genuine later success deleted original recovery-only evidence")}
  })
 }
}

func TestRepair1ReviewRecoveryWriteRefusal(t *testing.T) {
 for _,obstacle:=range []string{"conflict","symlink","mode"} {
  t.Run(obstacle,func(t *testing.T) {
   f:=newCollectionFixture(t,8192,16384,100,100)
   c:=&fix1CountingControl{recordingT3:f.control};f.driver.T3=c
   thread:=*c.thread
   if _,_,_,err:=f.driver.settleCollectedTurn(f.pkg,thread,"failed",[]byte("{}"),"original failure","");err!=nil {t.Fatal(err)}
   path:=filepath.Join(f.driver.workspacePath(f.pkg),"collected-turn.json")
   before,err:=os.ReadFile(path);if err!=nil {t.Fatal(err)}
   var recorded collectedTurn
   if err=json.Unmarshal(before,&recorded);err!=nil {t.Fatal(err)}
   raw,err:=json.Marshal(recorded);if err!=nil {t.Fatal(err)}
   recovery:=filepath.Join(filepath.Dir(path),"collected-turn-recovery-"+shortDigest(raw)+".json")
   switch obstacle {
   case "conflict":
    if err=os.WriteFile(recovery,[]byte("conflicting evidence"),0600);err!=nil {t.Fatal(err)}
   case "symlink":
    if err=os.Symlink(filepath.Join(t.TempDir(),"absent"),recovery);err!=nil {t.Fatal(err)}
   case "mode":
    if os.Geteuid()==0 {t.Fatal("mode refusal requires ordinary user")}
    if err=os.Chmod(filepath.Dir(path),0500);err!=nil {t.Fatal(err)}
    defer os.Chmod(filepath.Dir(path),0700)
   }
   err=f.driver.Collect(context.Background(),f.pkg,f.workspace)
   var permanent *permanentCollectionFailure
   after,readErr:=os.ReadFile(path)
   pending,pendingErr:=f.custody.PendingUploadByPurpose("result")
   t.Logf("obstacle=%s refused=%v unchanged=%v verifies=%d settles=%d pending=%v",obstacle,err!=nil,bytes.Equal(before,after),f.process.calls,c.settles,pending!=nil)
   if err==nil||errors.As(err,&permanent)||readErr!=nil||!bytes.Equal(before,after)||f.process.calls!=0||c.settles!=0||pendingErr!=nil||pending!=nil {t.Fatalf("unsupported recovery publication: %v",err)}
  })
 }
}

func TestRepair1ReviewUnprovenDirectoryDurability(t *testing.T) {
 if os.Geteuid()==0 {t.Fatal("permission boundary requires ordinary user")}
 f:=newCollectionFixture(t,8192,16384,100,100)
 c:=&fix1CountingControl{recordingT3:f.control};f.driver.T3=c
 c.message=FailedMarker+"\noriginal failed turn\n"
 dir:=f.driver.workspacePath(f.pkg)
 if err:=os.Chmod(dir,0300);err!=nil {t.Fatal(err)}
 defer os.Chmod(dir,0700)
 first:=f.driver.Collect(context.Background(),f.pkg,f.workspace)
 path:=filepath.Join(dir,"collected-turn.json")
 raw,readErr:=os.ReadFile(path)
 if first==nil||readErr!=nil {t.Fatalf("did not reach post-link directory-open failure: first=%v read=%v",first,readErr)}
 if p,e:=f.custody.PendingUploadByPurpose("result");e!=nil||p!=nil {t.Fatalf("first refusal published: %v",e)}
 reopened,e:=os.Open(dir)
 if e==nil {reopened.Close();t.Fatal("directory unexpectedly readable")}
 second:=f.driver.Collect(context.Background(),f.pkg,f.workspace)
 p,pendingErr:=f.custody.PendingUploadByPurpose("result")
 after,afterErr:=os.ReadFile(path)
 t.Logf("firstRefused=%v snapshotVisible=%v directoryStillUnopenable=%v retryErr=%v published=%v settles=%d unchanged=%v",first!=nil,readErr==nil,e!=nil,second,p!=nil,c.settles,bytes.Equal(raw,after))
 if pendingErr!=nil||afterErr!=nil {t.Fatal(pendingErr,afterErr)}
 if second==nil||p!=nil||c.settles!=0 {t.Error("retry authorized custody/settlement from snapshot whose directory durability remains unproven")}
}
```

First command:
```sh
GOTMPDIR=/var/tmp/r.Of0tcT GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerruntime -run '^TestRepair1Review' -count=1 -v
```
Actual exit **1**; full stdout:
```text
=== RUN   TestRepair1ReviewRecoveryReplacement
=== RUN   TestRepair1ReviewRecoveryReplacement/first=turn-1
    repair1_independent_astra_test.go:29: firstTurn="turn-1" laterTurn="turn-1" snapshots=2 exactOriginalRetained=true
=== RUN   TestRepair1ReviewRecoveryReplacement/first=
    repair1_independent_astra_test.go:29: firstTurn="" laterTurn="turn-1" snapshots=1 exactOriginalRetained=false
    repair1_independent_astra_test.go:30: genuine later success deleted original recovery-only evidence
=== RUN   TestRepair1ReviewRecoveryReplacement/first=turn-old
    repair1_independent_astra_test.go:29: firstTurn="turn-old" laterTurn="turn-1" snapshots=1 exactOriginalRetained=false
    repair1_independent_astra_test.go:30: genuine later success deleted original recovery-only evidence
--- FAIL: TestRepair1ReviewRecoveryReplacement (0.12s)
    --- PASS: TestRepair1ReviewRecoveryReplacement/first=turn-1 (0.04s)
    --- FAIL: TestRepair1ReviewRecoveryReplacement/first= (0.04s)
    --- FAIL: TestRepair1ReviewRecoveryReplacement/first=turn-old (0.03s)
=== RUN   TestRepair1ReviewRecoveryWriteRefusal
=== RUN   TestRepair1ReviewRecoveryWriteRefusal/conflict
    repair1_independent_astra_test.go:62: obstacle=conflict refused=true unchanged=true verifies=0 settles=0 pending=false
=== RUN   TestRepair1ReviewRecoveryWriteRefusal/symlink
    repair1_independent_astra_test.go:62: obstacle=symlink refused=true unchanged=true verifies=0 settles=0 pending=false
=== RUN   TestRepair1ReviewRecoveryWriteRefusal/mode
    repair1_independent_astra_test.go:62: obstacle=mode refused=true unchanged=true verifies=0 settles=0 pending=false
--- PASS: TestRepair1ReviewRecoveryWriteRefusal (0.09s)
    --- PASS: TestRepair1ReviewRecoveryWriteRefusal/conflict (0.03s)
    --- PASS: TestRepair1ReviewRecoveryWriteRefusal/symlink (0.04s)
    --- PASS: TestRepair1ReviewRecoveryWriteRefusal/mode (0.03s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.216s
FAIL
```
Full stderr: empty. Foreground session13189 completed.

Second, new durability concern only:
```sh
GOTMPDIR=/var/tmp/r.Of0tcT GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerruntime -run '^TestRepair1ReviewUnprovenDirectoryDurability$' -count=1 -v
```
Actual exit **1**; full stdout:
```text
=== RUN   TestRepair1ReviewUnprovenDirectoryDurability
    repair1_independent_astra_test.go:86: firstRefused=true snapshotVisible=true directoryStillUnopenable=true retryErr=<nil> published=true settles=1 unchanged=true
    repair1_independent_astra_test.go:88: retry authorized custody/settlement from snapshot whose directory durability remains unproven
--- FAIL: TestRepair1ReviewUnprovenDirectoryDurability (0.06s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.066s
FAIL
```
Full stderr: empty. Foreground session62343 completed. Both failures are meaningful assertions, not build/fixture failures. No strace was available; no tool was installed. The durability probe uses ordinary filesystem permissions instead.

## Cleanup and limits

Both temporary overlay revisions were guardedly removed; final deletion Huyang req_11727/wsrev_42 using docrev_96142df986cc0546f59edfc69ceaae9cdc09aeb1eff7de5416156dd2677b4279. Tracked worktree/index unchanged, diff and cached diff checks clean, exact HEAD/tree/sole parent unchanged. Only .t3 and declared documents remain untracked. All task-started foreground test/Git processes completed; no background jobs launched. Both outputs are below256KiB.

Local review only. Source publication:none; release publication:none; deployment:none. Publishing remains deferred and schedules remain disabled; no live schedule/config/admin/trust/service/fleet/provider/CI changes, push, PR, tag, release or UpKeeper operations. No Claude, delegation, nested review or escalation. No whole M16, public activation, final physical HEAD, compiler, M17, OS restart, scalability or actual provider-diversity acceptance. The production two-actual-provider-family gate remains mandatory. Lead must resolve these bounded findings and obtain BOTH exact-candidate independent reviews.
