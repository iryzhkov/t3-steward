# Independent bounded M16 first-lock-publication repair3 review

Verdict: **ACCEPT** for exact commit **49b99fd2e2763ead3f68babd37c41af2783bf9c0**. No blocking findings. The two prior reviews' deduplicated P2 first-publication race is resolved within the specified cooperating-caller/private-custody boundary. Completed-custody preservation and original R1/R2 remain intact.

Reviewer: GPT-6.1-Sol (gpt-6.1-sol), Codex harness, medium effort; run run-9b5dd784dfa42cca65920479b832b31c, task task-a373da4c7a41934cb21712ae3afc9aeb. Producer run-9dc231473de1fece80ebd34d87f8c839. Controlling supplied plan: jocasta:50688c8fe129864d5ba9044048f9204e@1 / plan.md. The producer handoff/contract shorten the document ID and call the supplied plan requirements.md; those documentary typos do not replace the supplied controlling plan.

## Exact custody and inputs

Complete plan.md, prior-plan.md, original-plan.md, handoff.md, child-staging-fix3-contract.md and BOTH actual prior Sol/Astra reviews, including their complete Go fences, read. Mounted inputs leave the repository through a symlink; Huyang refused those relative paths, so the external artifacts were read at their resolved external location. Repository source was read through Huyang workspace ws_8eb82f092f97823cb225c5640082a20e, initially wsrev_1. Trust remained untrusted/unchanged; no isolated Huyang check, LSP or diagnostic success claimed.

Initial HEAD and sole bundle prerequisite: 6b0a6798736d3c9277e7688ef3016c690bf7a9ee. Independently verified bundle SHA256 d1257c33fd06ee5cbf256af39c4c78446f3e43f26d51fa1b3326c3be52941728 /400401 bytes, sole HEAD export49b99fd2e2763ead3f68babd37c41af2783bf9c0, sole prerequisite, and git bundle verify before import. Fetch/detach yielded tree a10fed8f34d6c9ca77ee0965f6868fd09f37e18c / sole parent94125f42a9dad2ea1967bad1b4e826817c2db2ce. Full strict local fsck passed. No material mismatch.

Both complete actual original Go fences were extracted externally, externally gofmt-formatted, and compared byte-for-byte to committed blobs:

- independent_fix2_first_lock_test.go: 1664 bytes, SHA256072f70cffda0e256721dcefaaefa8663c7aeb841c20d115ad0b6871b5556a539.
- independent_fix2_race_overlay_test.go: 1016 bytes, SHA2564ce1adc5d84bda94cb0ffa6771ae2a2cc658becc10ee9c7a818afaaf12f694e9.

All four earlier portable files equal the parent blobs exactly: independent_fix1_entry_overlay_test.go (beb6670ac17a841428a03156fd2abf2afe397397103b41c859ed9088bef1def9), independent_fix1_late_identity_test.go (2efff55ef613b24b66125e4c970b6da9e1c6e0b99d63a6df71b240971d91332d), independent_stage_overlay_test.go (bab8a7b156968ed26f9d74f6aa30de700110995108304a5acb58370e4dc06201), independent_staging_astra_overlay_test.go (364395ed67eef65bdedbe74ed40055e6e82389bee71509e4365216f311c43cbe). Original symbols/assertions and full 2000-iteration bounds remain permanent.

## Publication and recovery findings

Read the complete 790-line staging implementation in Huyang windows, full316-line fix3 tests, and semantic/source owning-writer/configured-factory evidence. The five-file delta has one production file.

At internal/backlog/review_child_staging.go:548 existing canonical custody opens without creation; only missing canonical custody with absent owner enters private publication. At :649 the unique O_EXCL0600zero-size candidate is validated; actual descriptor flock at :675 precedes canonical hardlink at :697. The same open descriptor remains held through sync, private-name cleanup (:657–671), canonical postwait identity checks (:608–626), and owner creation (:631). Reflocking that descriptor does not release its existing exclusive flock. An EEXIST publication loser cleans/closes only its own candidate and opens/waits on the canonical winner. Existing canonical callers create no candidate. This removes the prior canonical-visible/before-flock interleaving.

Actual deterministic StageDeclared tests independently rerun in the required selection demonstrate canonical visible/owner absent/creator-held kernel flock, identical timeout without full private-FS/every-logical-SQL/native-audit mutation, subsequent exact winner/same inode, and conflicting wait/refusal preserving the completed winner/deadline. Both unchanged original2000 stress families and100 API pairs pass independently.

Keys are bounded to200 bytes/single component; CreateTemp names are bounded, exclusive and not adopted from existing entries. Descriptor/path/full-mode/size checks, private0700 real-directory guards, nofollow/nonblocking canonical opens and postwait identity checks remain. Candidate contents and publication/cleanup directory syncs precede success. Failure/cancellation closes the caller's descriptor; published canonical custody is retained on failure. Identity refusal preserves potentially substituted evidence. Healthy candidates are removed; unrelated process-crash leftovers are neither enumerated for adoption nor GC'd. No global bound on repeated crash leftovers follows.

Released canonical custody without an owner remains fail-closed (:627–630); the implementation does not distinguish a pre-owner interruption from removed completed custody. Allocation-only without a lock, intact empty-owner, exact partial intent/file/pending/receipt, completed/reopened replay and configured clone/rerun controls pass in the independent selection. Complete receipt evidence still selects inspection before retention. Final read-only childStageInspect/childStageCustody remains after Prepare, Build, current owning allocation writer and final deadline callback (:237–256). No completed-custody repair or deadline extension is introduced. Current declared validation remains inside the checkpoint owning writer before replay/insertion. Configured internal factory, source selection/whole manifest/criteria/prompts, authority/admission/prune/legacy fences are preserved by the scoped diff and selected tests; this is not a new full historical audit of every prerequisite.

## Complete producer artifact audit

Lossless external decompression to /tmp/fix3-review-ib8go4id/verification.log asserted raw SHA256f05198e7f5a6a762e8e4239a93082cc927a0da742a0e4ba3dbe8a1bfd166d093,12193470 bytes,232159 lines. Complete meaningful chronology before2258 was classified programmatically, excluding the separately compared full source diff1578–2247:1008 test banners,532 diagnostics,47 receipts. All seven distinct normalized diagnostics and all execution/recovery receipts inspected. Repetitive inventories were parsed, not represented as a human full-read.

BEFORE helper actual1: Sol iteration23/Astra282 first-existing-owner refusal;100 API pairs PASS. AFTER both original2000 stress families/API100 and deterministic publication controls PASS; initial private-contender/postwait fixtures refuse default TempDir mode. Only explicit0700 fixture setup was corrected; affected controls actual0. First required selection actual1 retains cmd compile disk quota and backlog/SQLite migration I/O778 (including migration14 commit), without a root-cause claim. Same selection/private GOTMPDIR recovery actual0:210.366/47.295/2.251s. No verification waiver.

Exact27552-byte parent..HEAD c/i-prefix diff equals the logged source audit. All1418 tracked fingerprints/1201Go, full pre2258–5097 and post5140–7989 logical index/tree match each other and this HEAD/index. Logged raw-index observed equality a113667351ec4c3d743d82917c3c390ef8ea4d4d3ad8b8d15ebb49b8a28378c3 is separate from logical equality.

FULL gate5098–5139 inspected: ONE make check-review with GOTMPDIR=/var/tmp/m16-fix3-go and FAST_BASE94125f42a9dad2ea1967bad1b4e826817c2db2ce, actual make0; build/vet/fullshort/staticcheck/format/full backlog race714.114s pass. Whole gate/race not repeated here.

Snapshot assertion7978–7989 failed actual1 because Python tuples were compared with decoded JSON lists. Normalized SAVED pre/post JSON independently compares equal now; no recapture or source/gate/race repetition. Commit/export7990–8015 inspected. Raw-SHA baseline export8016–8029 failed actual128/wrapper1; artifact-only8030–8055 recovery created a temporary local proof ref, exported baseline and removed that ref. These failures are retained.

Parsed ALL22 inventory blocks, including raw command outputs and repeated named lists: baseline9926 source/fresh reachable and type-size/freshALL8056–107329; candidate10312 source/fresh reachable/type-size/freshALL107359–210488; historical sourceALL10651 lists210489–231792 and every339 outside-closure type-size record through232134. All closure sets match actual Git; every historical type-size record matches current producer Git, zero missing. Existing freshALL10312 matches exactly. Current producer object database has six additional objects beyond the historical10651; no current-count equality to that historical snapshot is claimed. An initial local-export-only classifier stopped on one unavailable outside-closure object, and a subsequent overstrict current SOURCEALL equality stopped on those six additions. Corrected checks compare all historical records, preserve unrelated objects, and verify exact freshALL. No sourceALL==closure claim or deletion.

FULL import/absence107330–107358 and final232135–232159 inspected: actual pre-import cat-file exit1 plus batch missing, exact imported commit/tree/sole parent, strictfsck and no-alternates receipts. Temporary proof ref is currently absent. Producer source/fresh locations were queried read-only through Git; no fresh-bare reconstruction was rerun. These are local historical receipts with independent content comparisons, not CI or an independent witness of historical isolation/execution.

## Independent execution and portable adversarial evidence

Required command invoked ONCE, captured separately as exec session50745 and completed actual exit0:

```sh
review_tmp=$(mktemp -d /var/tmp/m16-sol-fix3-review-XXXXXX)
chmod 700 "$review_tmp"
GOTMPDIR="$review_tmp" GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/backlog ./internal/store/sqlite ./cmd/t3-steward -run 'TestIndependentFix2|TestIndependentFix1|TestIndependentStage|TestReviewChildStaging|TestIndependentDeclared|TestReviewDeclar|TestReviewConfigured|TestReviewAdmission|TestReviewAuthority|TestReviewChild|Test.*Clone|Test.*Rerun' -count=1
```

Actual GOTMPDIR=/var/tmp/m16-sol-fix3-review-VxdcnT. Complete output:

```text
ok github.com/iryzhkov/t3-steward/internal/backlog 252.716s
ok github.com/iryzhkov/t3-steward/internal/store/sqlite 48.756s
ok github.com/iryzhkov/t3-steward/cmd/t3-steward 2.417s
```

No independent failure/recovery or selection retry. Two bounded additional probes cover replacing an owned private candidate and substituting .locks while the real private descriptor is held. They verify refusal preserves original/replacement evidence and creates neither canonical nor owner. Complete portable code follows; created via Huyang format=false, then removed with exact docrev_9b2c9481b01e2db5e5a38d68e3afde98b17994142b1112bced5f47953880524b, resulting wsrev_38.

```go
package backlog

import (
 "context"
 "errors"
 "os"
 "path/filepath"
 "strings"
 "testing"
)

func TestIndependentFix3PrivateIdentity(t *testing.T) {
 for _, kind := range []string{"private-replaced", "directory-substituted"} {
  t.Run(kind, func(t *testing.T) {
   ns := filepath.Join(t.TempDir(), "stages")
   if err := os.Mkdir(ns, 0700); err != nil { t.Fatal(err) }
   locks := filepath.Join(ns, ".locks")
   var evidence string
   var saved string
   lock, err := childStageLockWithBoundary(context.Background(), ns, "same", func(phase string) error {
    if phase != "lock-private-held" { return nil }
    entries, e := os.ReadDir(locks); if e != nil || len(entries) != 1 { t.Fatal(e, entries) }
    private := filepath.Join(locks, entries[0].Name())
    if kind == "private-replaced" {
     saved = private + ".saved"
     if e = os.Rename(private, saved); e != nil { t.Fatal(e) }
     if e = os.WriteFile(private, []byte("replacement-evidence"), 0600); e != nil { t.Fatal(e) }
    } else {
     saved = locks + ".saved"
     if e = os.Rename(locks, saved); e != nil { t.Fatal(e) }
     if e = os.Mkdir(locks, 0700); e != nil { t.Fatal(e) }
     if e = os.WriteFile(private, []byte("replacement-evidence"), 0600); e != nil { t.Fatal(e) }
    }
    evidence = stageFix1Evidence(t, ns)
    return nil
   })
   if lock != nil { lock.Close(); t.Fatal("substituted private custody accepted") }
   if err == nil || !strings.Contains(err.Error(), "identity refused") { t.Fatal("wrong refusal", err) }
   if evidence != stageFix1Evidence(t, ns) { t.Fatal("refusal deleted or mutated replaced evidence") }
   if _, e := os.Lstat(saved); e != nil { t.Fatal("original evidence lost", e) }
   if _, e := os.Lstat(filepath.Join(locks, "same.lock")); !errors.Is(e, os.ErrNotExist) { t.Fatal("canonical unexpectedly published", e) }
   if _, e := os.Lstat(filepath.Join(ns, "same")); !errors.Is(e, os.ErrNotExist) { t.Fatal("owner unexpectedly created", e) }
  })
 }
}

```

Exact foreground command:

```sh
probe_tmp=$(mktemp -d /var/tmp/m16-sol-fix3-probe-XXXXXX)
GOTMPDIR="$probe_tmp" GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/backlog -run '^TestIndependentFix3PrivateIdentity$' -count=1 -v
```

Actual GOTMPDIR=/var/tmp/m16-sol-fix3-probe-7Yuoba; session5211, actual exit0, complete output:

```text
=== RUN   TestIndependentFix3PrivateIdentity
=== RUN   TestIndependentFix3PrivateIdentity/private-replaced
=== RUN   TestIndependentFix3PrivateIdentity/directory-substituted
--- PASS: TestIndependentFix3PrivateIdentity (0.03s)
    --- PASS: TestIndependentFix3PrivateIdentity/private-replaced (0.01s)
    --- PASS: TestIndependentFix3PrivateIdentity/directory-substituted (0.02s)
PASS
ok github.com/iryzhkov/t3-steward/internal/backlog 0.032s
```

Overlay name was excluded from the outstanding required selection; its execution was separate. No production fixes or assertion corrections. git diff --exit-code, cached diff, diff --check and HEAD^..HEAD diff --check all pass after removal. Exact HEAD/tree/sole parent retained, tracked worktree/index clean, all foreground processes completed.

This is one independent bounded ACCEPT; producer remains provisional until BOTH fresh reviews accept. Local only, publishing deferred, schedules untouched/disabled. No delegation/nested review/Claude/escalation/config-trust/live-provider/worker/admin/enrollment/service/fleet/push/PR/tag/release/UpKeeper effects. No fullM16/M17/CI/deployment/scalability/OSrestart/real-diversity/public CLI/auth transport/activation/profile/materialization/parking/final physical HEAD/compiler/quota claim. Production two-family policy unwaived. Logical SQL/native audit equality is not physical SQLite-page equality; reopened attachments are not OS restart. No arbitrary later hostile-write immunity or FS-SQL atomicity claim. Cost totals unavailable.
