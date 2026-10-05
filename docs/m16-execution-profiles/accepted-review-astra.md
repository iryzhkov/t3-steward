Independent bounded M16 first-lock-publication repair3 review

Verdict: **ACCEPT**. No blocking findings in this bounded review. The prior two reviews' deduplicated P2 first-publication race is resolved. This is one independent review, not a waiver of the required other review or the original production two-family policy.

Reviewer: GPT-6-Astra (gpt-6-astra), Codex harness, medium effort. Review run run-9b5dd784dfa42cca65920479b832b31c / task task-43c59a8b5d21af45f203651ebb53bc2f. Producer run-9dc231473de1fece80ebd34d87f8c839. Lens: publication failure/cancellation, concurrent loser cleanup/descriptor identity, deterministic before-owner barriers and late custody preservation. Controlling plan jocasta:50688c8fe129864d5ba9044048f9204e@1, supplied plan.md; the shortened document ID and requirements.md wording in producer handoff/contract are transcription errors, not alternate authority.

**Exact source and input custody.** Read the complete supplied plan, prior-plan, original-plan, handoff, child-staging-fix3-contract and BOTH full actual prior reviews, including their complete Go fences. The initial combined delivery truncated part of original-plan; I reread that entire file before proceeding. Mounted inputs resolve outside the workspace; Huyang's symlink refusal was handled by authorized external-artifact reads, not by bypassing source guards.

Initial HEAD/sole prerequisite was 6b0a6798736d3c9277e7688ef3016c690bf7a9ee. Verified bundle SHA256 d1257c33fd06ee5cbf256af39c4c78446f3e43f26d51fa1b3326c3be52941728 /400401 bytes, sole HEAD export and sole prerequisite, then git bundle verify. Imported/detached exact commit **49b99fd2e2763ead3f68babd37c41af2783bf9c0**, tree **a10fed8f34d6c9ca77ee0965f6868fd09f37e18c**, sole parent **94125f42a9dad2ea1967bad1b4e826817c2db2ce**. Full strict Git fsck passed. No material mismatch.

Huyang workspace ws_8ff717636dc5b6966d22319e2364c3e6 opened before repository file operations. Initial relative-root refusal corrected to the absolute root. Source read in semantic outline/complete bounded windows; overlay creation/removal guarded. Trust remained untrusted and unchanged; no Huyang isolated-check, LSP or project-verification success claimed. Foreground Git/Go and external artifact processing used the explicit authorization.

**Resolved P2 and audited implementation.** Read all 790 lines of internal/backlog/review_child_staging.go and all 316 lines of repair3 tests. At :550 existing canonical custody is opened noncreating, nonblocking and nofollow. Only ENOENT with initially absent owner reaches private creation. Existing-canonical callers allocate no candidate. Key validation at :520 bounds a single component to200 bytes.

At :648–675 a fresh O_EXCL0600 zero-size private candidate is verified and its actual descriptor exclusively flocked and synced. Identity/full-mode/size and private-directory checks precede atomic no-replace hardlink at :697. Thus a cooperating caller cannot observe this newly published canonical inode before its publisher owns flock. The descriptor remains held across canonical sync and private cleanup, the outer flock loop, postwait checks and owner creation. This corrects the old O_EXCL-before-flock window, rather than masking it with retries or adopting released ownerless custody.

The :656–668 deferred cleanup checks original/path/opened inode identity. Ordinary failure removes only the owned private path and syncs the directory; a publication loser then closes its own descriptor and opens canonical noncreating at :555–556. Identity refusal preserves replacement evidence and closes the private descriptor. Error after canonical link never unlinks canonical custody. Sync errors remain errors. Context is checked before work, before publication, during flock waiting and after the published-held hook. Cancellation cannot unlock an independently opened winner descriptor. Actual error/cancellation and descriptor-close behavior were exercised by the portable probes below.

Directory checks reject symlinks and full-mode mismatches; candidate path/descriptor checks detect same-mode substitution before publication. These are explicit boundary checks, not immunity to arbitrary hostile path rewriting between every syscall. Healthy calls remove their private candidates. Unrelated crash leftovers are never scanned for adoption or GC; no global growth bound over repeated process crashes is claimed. Published, released ownerless canonical custody still refuses at :626–630 without owner creation or evidence deletion.

Committed TestReviewChildStagingFix3PublishedBeforeOwner reaches actual canonical visibility with owner absent and independently probes EWOULDBLOCK on that inode. An identical actual StageDeclared timeout changes neither full private FS nor every logical SQL/native audit record. Identical and conflicting callers reach kernel waiting before release, then respectively reuse exact preparation/deadline or refuse conflicting intent. Complete winner FS and logical SQL/native audit snapshots remain equal, and the canonical inode stays identical. Private contenders, fault/cancellation, real hardlink failure, unrelated leftover, publication-before-owner interruption and postwait inode replacement controls are included. The independent required selection passed these committed controls.

**Preserved late custody and recovery.** Completed-state classification still precedes retention. Exact request/receipt/blob/pending inspection is bounded by 2*expected+1 entries and checks bytes, full modes, nofollow descriptors and identity. Final read-only inspection/custody at :252–256 remains AFTER PrepareReviewChild, pure Build, current owning writer validation and final deadline callback, with no subsequent retention or mutable callback before return. The permanent eight final-clock negative controls remain unchanged and passed the required selection.

Allocation-only without a lock, intact empty owner, exact partial intent/file/receipt/pending, completed replay and reopened attachments remain supported. Released ownerless custody is explicitly unsupported/fail-closed. Spot reads confirm declared validation remains inside AllocateReviewCheckpoint's owning transaction before initial insertion AND replay; configured factory is still internal in cmd/t3-steward/review_declared_admission.go. The five-file repair3 delta adds no activation/public CLI/transport/profile/materialization/parking boundary. Existing configured clone/rerun, current authority, prune, retained input/criteria/prompt, deadline, legacy and source-protection controls are preserved by unchanged source plus the required selection; this is not a fresh audit of every historical prerequisite.

**Permanent portable regressions.** Extracted the COMPLETE single Go fence from each actual prior review externally, gofmt'd it externally, and compared exact committed bytes, including original symbols/assertions and original2000-iteration stress bounds:

- independent_fix2_first_lock_test.go: SHA256072f70cffda0e256721dcefaaefa8663c7aeb841c20d115ad0b6871b5556a539 /1664 bytes.
- independent_fix2_race_overlay_test.go: SHA2564ce1adc5d84bda94cb0ffa6771ae2a2cc658becc10ee9c7a818afaaf12f694e9 /1016 bytes.
- Prior independent_fix1_entry_overlay_test.go: beb6670ac17a841428a03156fd2abf2afe397397103b41c859ed9088bef1def9 /4027 bytes.
- Prior independent_fix1_late_identity_test.go: 2efff55ef613b24b66125e4c970b6da9e1c6e0b99d63a6df71b240971d91332d /3148 bytes.
- Earlier independent_stage_overlay_test.go: bab8a7b156968ed26f9d74f6aa30de700110995108304a5acb58370e4dc06201 /3177 bytes.
- Earlier independent_staging_astra_overlay_test.go: 364395ed67eef65bdedbe74ed40055e6e82389bee71509e4365216f311c43cbe /5408 bytes.

All four earlier files equal their parent blobs exactly. The original full stress/API tests ran in the required selection below; their bounds were not reduced.

**Complete historical evidence audit.** Losslessly decompressed verification.log.xz externally to /var/tmp/m16-astra-fix3-d_mupger/producer.log. Asserted raw SHA256f05198e7f5a6a762e8e4239a93082cc927a0da742a0e4ba3dbe8a1bfd166d093,12193470 bytes,232159 lines. Programmatically classified the complete chronology before fingerprints, separated the complete source-diff region, retained every failure banner and inspected all88 unique non-banner diagnostics/receipts. Classification outside the diff:226 RUN,221 PASS,561 FAIL banners,579 other lines; repeated and nested banners are not independent test counts.

BEFORE real helper failures are Sol iteration23 and Astra282, actual Go exit1; the100 API pairs passed. No API failure claim. AFTER both original2000-iteration tests and100 API pairs passed; the two initial new private fixtures were refused because default TempDir permissions did not satisfy0700. Producer records an explicit0700 fixture-only correction with unchanged assertions, then all affected repair3 controls pass actual0. Final source uses those explicit private directories. The supplied log cannot independently reconstruct every intermediate source byte; its correction account is historical evidence, not an independently witnessed edit.

Required first producer selection retained cmd compile disk-quota failure plus backlog/SQLite migration I/O778, actual1. Same unchanged-source selection with private GOTMPDIR passed actual0:210.366s/47.295s/2.251s. These observations do not establish a root cause for I/O778 and do not waive verification. Prior review honesty also remains: original Sol helper race and corrected100 API pairs, unavailable/truncated original Sol selection exit, and original Astra quota failure followed by successful scoped recovery are historical evidence.

The full27552-byte source audit at1578–2247 exactly matches actual git diff HEAD^ HEAD with c/i prefixes. All1418 pre2258–5097 and post5140–7989 tracked fingerprints, including1201 Go files, match actual HEAD blobs. Every logical index entry matches current index and tree a10fed8f34d6c9ca77ee0965f6868fd09f37e18c. Raw producer index hash is separately recorded byte-equal a113667351ec4c3d743d82917c3c390ef8ea4d4d3ad8b8d15ebb49b8a28378c3; no claim my index metadata bytes equal the producer's.

Read FULL actual gate5098–5139: ONE GOTMPDIR=/var/tmp/m16-fix3-go GOMAXPROCS=2 GOFLAGS=-p=2 make check-review FAST_BASE=94125f42a9dad2ea1967bad1b4e826817c2db2ce. Actual make0 includes build, vet, full short suite, staticcheck, format and full backlog race714.114s. I did not repeat the whole gate or race suite.

Read complete7978–7989 snapshot failure/recovery: actual1 caused by in-memory tuples versus decoded JSON lists; normalized SAVED pre/post comparison then actual0. Full records independently match here; no recapture/source/gate/race repetition is needed. Read complete commit/export7990–8015; raw-SHA baseline bundle refusal8016–8029 actual128/wrapper1; artifact-only recovery8030–8055 using temporary refs/fix3-proof/baseline, removed immediately. Read all proof/import non-object headers, actual candidate absence cat-file exit1 plus batch missing107330–107333, strict fsck/import/exact identities107334–107358, and FULL final232135–232159 including proof-ref absence.

Parsed ALL23 complete object-list blocks, including repeated command outputs and labeled lists:
- Baseline8056–107329:9926 complete SOURCE/FRESH closure and FRESH ALL IDs/type-size sets.
- Candidate107359–210488:10312 complete SOURCE/FRESH closure and FRESH ALL sets.
- SOURCE ALL210489–232134:10651 objects, candidate closure plus339 preserved unrelated objects, including the complete outside-closure list.

All reachable baseline/candidate IDs and every type-size record match actual Git, zero missing in either closure. Every repeated inventory agrees. SOURCE ALL is not claimed equal to closure. Of339 unrelated source-only objects,338 are independently available and type-size checked here; commit6cb9d5503cf91a0058c34b5f5b7e6e1fac3b32c3 (recorded commit280 bytes) is unavailable locally, so that one type-size record is only cross-checked across full supplied inventories. It is outside candidate closure and not required by this bundle. No deletion or additional import was used to force source ALL equality.

My first parser assumed every source-only object would be available; it correctly stopped at that one missing unrelated commit. The next pass exposed that raw rev-list output includes paths, unlike labeled ID-only lists. I corrected record parsing, processed all complete lists, and distinguished reachable verification from unrelated historical evidence. No partial parser output is promoted to full verification. Repetitive records were programmatically compared, not claimed individually human-read. These are local historical receipts with independent content comparison, not CI or an independent witness of historical execution/fresh-bare isolation. Fresh-bare proof was not rerun.

**Independent execution.** Required command invoked ONCE, with a newly created private Go temporary directory, and no recovery needed:
```sh
GOTMPDIR=/var/tmp/m16-astra-fix3-d_mupger/go GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/backlog ./internal/store/sqlite ./cmd/t3-steward -run 'TestIndependentFix2|TestIndependentFix1|TestIndependentStage|TestReviewChildStaging|TestIndependentDeclared|TestReviewDeclar|TestReviewConfigured|TestReviewAdmission|TestReviewAuthority|TestReviewChild|Test.*Clone|Test.*Rerun' -count=1
```

Foreground session53997 completed actual Go exit0; full output:
```text
ok  github.com/iryzhkov/t3-steward/internal/backlog       248.418s
ok  github.com/iryzhkov/t3-steward/internal/store/sqlite  52.720s
ok  github.com/iryzhkov/t3-steward/cmd/t3-steward         2.534s
ACTUAL_GO_EXIT 0
```

Exact argv/environment, untruncated output and actual subprocess exit are separately saved as selection-command.json, selection-output.log and selection-exit under /var/tmp/m16-astra-fix3-d_mupger. Foreground tool session/exit are also retained in this review's tool receipts. The disposable probe was added while the selection was outstanding; its distinct TestIndependentFix3 prefix is excluded from that required selection.

Bounded additional probes used actual helper/flock/link operations, no production edits or mocked helper returns:
```sh
GOTMPDIR=/var/tmp/m16-astra-fix3-d_mupger/go GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/backlog -run '^TestIndependentFix3' -count=1 -v
```

Foreground session60353 completed actual Go exit0; full output:
```text
=== RUN   TestIndependentFix3LoserCancelPreservesHeldWinner
--- PASS: TestIndependentFix3LoserCancelPreservesHeldWinner (0.06s)
=== RUN   TestIndependentFix3PrivateReplacementPreserved
=== RUN   TestIndependentFix3PrivateReplacementPreserved/candidate
=== RUN   TestIndependentFix3PrivateReplacementPreserved/directory
--- PASS: TestIndependentFix3PrivateReplacementPreserved (0.03s)
    --- PASS: TestIndependentFix3PrivateReplacementPreserved/candidate (0.02s)
    --- PASS: TestIndependentFix3PrivateReplacementPreserved/directory (0.02s)
=== RUN   TestIndependentFix3PublishedCancellationRemainsOwnerless
--- PASS: TestIndependentFix3PublishedCancellationRemainsOwnerless (0.03s)
PASS
ok  github.com/iryzhkov/t3-steward/internal/backlog 0.126s
ACTUAL_GO_EXIT 0
```

These four leaf cases cover loser cleanup before canceled waiting while the winner descriptor stays locked, candidate and directory substitution preservation/descriptor closure, and canonical-published cancellation followed by fail-closed ownerless replay. No assertions or fixtures needed correction. Complete portable executed bytes follow; place at internal/backlog/independent_fix3_astra_overlay_test.go through guarded Huyang and remove guarded afterward:
```go
package backlog

import (
 "context"
 "errors"
 "os"
 "path/filepath"
 "strings"
 "syscall"
 "testing"
 "time"
)

func TestIndependentFix3LoserCancelPreservesHeldWinner(t *testing.T) {
 ns := filepath.Join(t.TempDir(), "stages")
 if e := os.Mkdir(ns, 0700); e != nil { t.Fatal(e) }
 ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
 defer cancel()
 var winner *fileLock
 var winning os.FileInfo
 var candidate string
 waited := false
 loser, err := childStageLockWithBoundary(ctx, ns, "same", func(phase string) error {
  switch phase {
  case "lock-private-held":
   entries, e := os.ReadDir(filepath.Join(ns, ".locks")); if e != nil { return e }
   if len(entries) != 1 { t.Fatalf("private entries: %v", entries) }
   candidate = filepath.Join(ns, ".locks", entries[0].Name())
   winner, e = childStageLock(ctx, ns, "same"); if e != nil { return e }
   winning, e = winner.file.Stat(); return e
  case "lock-waiting":
   waited = true
   if _, e := os.Lstat(candidate); !errors.Is(e, os.ErrNotExist) { t.Fatalf("loser candidate not cleaned before wait: %v", e) }
   cancel()
  }
  return nil
 })
 if winner == nil { t.Fatal("winner never acquired", err) }
 defer winner.Close()
 if loser != nil { loser.Close(); t.Fatal("canceled loser acquired") }
 if !waited || !errors.Is(err, context.Canceled) { t.Fatal("wrong cancellation", waited, err) }
 current, e := os.Lstat(filepath.Join(ns, ".locks", "same.lock"))
 if e != nil || !os.SameFile(winning, current) { t.Fatal("winner replaced", e) }
 probe, e := os.OpenFile(filepath.Join(ns, ".locks", "same.lock"), os.O_RDWR, 0)
 if e != nil { t.Fatal(e) }
 e = syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
 probe.Close()
 if !errors.Is(e, syscall.EWOULDBLOCK) { t.Fatal("loser unlocked winner", e) }
 evidence := stageFix1Evidence(t, ns)
 if e = winner.Close(); e != nil { t.Fatal(e) }
 next, e := childStageLock(context.Background(), ns, "same")
 if e != nil { t.Fatal(e) }
 defer next.Close()
 info, e := next.file.Stat()
 if e != nil || !os.SameFile(winning, info) || evidence != stageFix1Evidence(t, ns) { t.Fatal("reopen changed custody", e) }
}

func TestIndependentFix3PrivateReplacementPreserved(t *testing.T) {
 for _, kind := range []string{"candidate", "directory"} {
  t.Run(kind, func(t *testing.T) {
   ns := filepath.Join(t.TempDir(), "stages")
   if e := os.Mkdir(ns, 0700); e != nil { t.Fatal(e) }
   var evidence string
   var moved string
   lock, err := childStageLockWithBoundary(context.Background(), ns, "same", func(phase string) error {
    if phase != "lock-private-held" { return nil }
    locks := filepath.Join(ns, ".locks")
    entries, e := os.ReadDir(locks); if e != nil { return e }
    if len(entries) != 1 { t.Fatalf("entries: %v", entries) }
    candidate := filepath.Join(locks, entries[0].Name())
    if kind == "candidate" {
     moved = candidate + ".saved"
     if e = os.Rename(candidate, moved); e != nil { return e }
     if e = os.WriteFile(candidate, []byte("replacement evidence"), 0600); e != nil { return e }
    } else {
     moved = filepath.Join(ns, "saved-locks", entries[0].Name())
     if e = os.Rename(locks, filepath.Join(ns, "saved-locks")); e != nil { return e }
     if e = os.Mkdir(locks, 0700); e != nil { return e }
     if e = os.WriteFile(candidate, nil, 0600); e != nil { return e }
    }
    evidence = stageFix1Evidence(t, ns)
    return nil
   })
   if lock != nil { lock.Close(); t.Fatal("replacement accepted") }
   if err == nil || !strings.Contains(err.Error(), "identity refused") { t.Fatal("wrong refusal", err) }
   if evidence == "" || evidence != stageFix1Evidence(t, ns) { t.Fatal("replacement or moved evidence deleted") }
   // The abandoned descriptor must be closed even on cleanup refusal.
   f, e := os.OpenFile(moved, os.O_RDWR, 0); if e != nil { t.Fatal(e) }
   defer f.Close()
   if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil { t.Fatal("private descriptor leaked locked", e) }
   if _, e = os.Lstat(filepath.Join(ns, ".locks", "same.lock")); !errors.Is(e, os.ErrNotExist) { t.Fatal("bad publication", e) }
  })
 }
}

func TestIndependentFix3PublishedCancellationRemainsOwnerless(t *testing.T) {
 ns := filepath.Join(t.TempDir(), "stages")
 if e := os.Mkdir(ns, 0700); e != nil { t.Fatal(e) }
 ctx, cancel := context.WithCancel(context.Background())
 defer cancel()
 var original os.FileInfo
 lock, err := childStageLockWithBoundary(ctx, ns, "same", func(phase string) error {
  if phase == "lock-published-held" {
   var e error
   original, e = os.Lstat(filepath.Join(ns, ".locks", "same.lock"))
   if e != nil { return e }
   cancel()
  }
  return nil
 })
 if lock != nil { lock.Close(); t.Fatal("cancel accepted") }
 if !errors.Is(err, context.Canceled) || original == nil { t.Fatal("wrong cancellation", err) }
 before := stageFix1Evidence(t, ns)
 lock, err = childStageLock(context.Background(), ns, "same")
 if lock != nil { lock.Close(); t.Fatal("ownerless adopted") }
 if err == nil || !strings.Contains(err.Error(), "existing owner missing") { t.Fatal("wrong replay refusal", err) }
 current, e := os.Lstat(filepath.Join(ns, ".locks", "same.lock"))
 if e != nil || !os.SameFile(original, current) || before != stageFix1Evidence(t, ns) { t.Fatal("released custody changed", e) }
}
```

**Final hygiene and limits.** Disposable overlay removed via exact document revision docrev_72c04c82d11631aa05ecdbc04d4fed59737715adca56ca2bc89addd9c4e5f25a, Huyang wsrev38. git diff --exit-code, cached diff, diff --check and parent..HEAD diff --check passed after removal. Exact HEAD/tree/sole parent preserved, tracked worktree/index clean, no temporary proof refs. All foreground tests completed. Only mounted inputs and declared review/continuation outputs remain untracked.

Local only, publishing deferred, schedules untouched/disabled. No production fixes, delegation, nested review, Claude, escalation, live config/trust/provider/workers/admin/enrollment/services/fleet/push/PR/tags/release/UpKeeper effects. No fullM16/M17/CI/deployment/scalability/real-diversity/OS-restart/publicactivation/CLI/authtransport/profile/materialization/parking/finalphysicalHEAD/compiler/quota claims. Reopened attachment is not OS restart, logical SQL/native audit is not page equality, and filesystem plus SQL is not distributed atomicity. No arbitrary later hostile-write immunity. Cost totals unavailable. Acceptance applies only to this pinned bounded repair; lead must collect both independent reviews before advancing.
