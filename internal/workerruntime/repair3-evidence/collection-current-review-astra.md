# CHANGES REQUESTED — independent collection repair2 review

Verdict: **CHANGES REQUESTED**. Exact commit **742639f540f3ccfd3ea38b3a4117f64e416d11ab**, tree **736e6e5c89e7532e0f14bd5eb894c610f4105eb2**, sole parent **fddd608a987b213fa3009f151bdb0c028fa290f7**.

Controlling review jocasta:2ccbdf98670fa90c467ad891a34af313@1 and supplied collection-fix2-review-plan.md; repair2 jocasta:b5295287d9965c68a58e2606027e2430@1. Independent Codex Astra medium durability/identity/settlement lens. One deduplicated P2 finding. No production fixes, sibling reviewer consultation, delegation, nested review or escalation. This is only this review's verdict.

## R1 — P2: identical large recovery copies cannot recover after permissions are restored

Paths: **internal/workerruntime/contained_lifecycle.go:63–70**, called by **internal/workerruntime/local_driver.go:1101**; rejection originates at **internal/workerruntime/local_driver.go:1596**.

privateBytes can create and sync a recovery file of any supplied size, but its existing-file branch calls readBoundedRegularFile with a fixed16MiB limit. The active collected-turn reader and syncPrivateFile accept the larger record. Thus a valid original recovery snapshot can be written, copied by the production helper and left visible after the required post-link directory-open failure, but the equal-existing retry never reaches the new durability proof when its JSON exceeds16MiB.

The complete probe below seeds a failed snapshot through actual settleCollectedTurn, using a valid JSON archive containing13MiB of padding. Base64 encoding makes the resulting valid collected-turn JSON **18,175,644bytes**. Under ordinary-user directory mode0300, actual privateBytes links the exact recovery copy and refuses directory durability. After restoring0700 and reopening the journal, actual Runtime.collect calls LocalDriver, which refuses with:
`collection deferred: retain failed collected turn: file is not a bounded regular file`.
The attempt remains collecting, with **zero verification, zero publication, zero settlement**, and exact original recovery bytes intact. The **312-byte positive control** completes with one verification, one settlement and a result receipt. Actual probe exit1, a meaningful assertion failure, no compiler/fixture correction.

This is a remaining recovery/liveness failure, not lost bytes or false successful authority. The task expressly repairs oversized archive handling and requires identical recovery-copy retries to succeed once permissions are restored. No collection-snapshot16MiB restriction is enforced at creation or stated as a supported-input constraint; publication budgets cannot be applied to raw evidence that must survive their rejection. The fixed bound originated in the older generic helper, but the repair2 contract now relies on this extracted helper to recover *every valid existing recovery record*. Merely returning the bounded-file error indefinitely does not complete that repair.

Requested change: permit exact, regular-file, identity-bound durability replay for every recovery snapshot the writer accepts, without deleting/truncating evidence or relaxing publication limits. A length-aware bounded/streamed equality check can preserve resource bounds without inheriting an unrelated fixed replay ceiling. Keep conflicting/symlink/binding/durability refusal and both full portable files. Retain this large positive-recovery expectation alongside the small control.

Probe limits: the failed snapshot is seeded through the real helper and the post-link fault through the real privateBytes helper, as in the supplied recovery-copy test; it does not simulate the helper implementation. Subsequent collection/reopen/finalization/custody/journal behavior is real fixture behavior. The padded archive is JSON accepted for recovery storage, not a claim that the fixture contacted a provider or validated a complete live T3 export. This demonstrates the supported snapshot/recovery-copy seam. No EIO, power loss or OS restart was injected.

## Repair and safety assessment

All seven changed paths were inspected: contained_lifecycle.go, local_driver.go, collection_fix2_test.go, both repair1_independent portable files, and both repair2-evidence reports. Full controlling/original/repair1/review plans, current handoff/contract and BOTH actual preceding reports were read. Current reports are the required preceding evidence, not consultation with this campaign's other reviewer. No profile sibling content was read/imported.

P1 retention is repaired for the inspected same/different/empty-to-known turn paths: any existing RecoveryOnly snapshot selected for replacement is copied before removal; privateBytes preserves the exact original JSON, including noncanonical formatting and unknown fields rather than re-marshaling. Thread/full execution binding is checked. Failed evidence is never selected as a successful witness. Same-turn failed observations retain their first failed record; genuine same-turn success and changed-turn success/failure first preserve it separately. Tests cover original bytes, both later outcomes, different-turn conflict/symlink/wrong binding/permission refusal, zero finalizer/custody/settlement effects and real later-turn overflow/reopen. Legacy successful snapshots without Identity still use the ordinary same-thread/same-turn successful witness branch; existing repeated-turn controls pass.

P2 visible active-snapshot retries now call syncPrivateFile before any use. Equal-existing recovery/privateJSON replay also calls it, subject to R1's size restriction. New active failed/successful and recovery-copy controls pass repeated0300 refusal, restoration, journal reopen, exact-byte retention and stable effect counts. An operational read/sync failure propagates before finalization/publication/settlement and is not converted to a typed size failure.

Identity/symlink boundary: syncPrivateFile opens a regular file, compares descriptor Stat with path Lstat using os.SameFile, reads and compares expected exact bytes, fsyncs that descriptor and parent directory, then rechecks regular path identity. Stable final-component symlinks, conflicting bytes and path replacement observed at these checks refuse. It preserves exclusive-Link/no-overwrite semantics; a failure leaves visible evidence for retry. File permissions remain those of private creation, or the pre-existing readable file; no new chmod/rewrite is performed. Readable legacy0400 files are not rejected by a new mode-equality policy.

These are point-in-time checks, not an atomic hostile-filesystem protocol. openRegular uses Lstat followed by Open, parent directory sync is path-based, and there is no locked descriptor-relative parent chain or content re-read after Sync. A concurrent same-inode writer, ABA namespace change or replacement after the last check is outside what this evidence establishes. No adversarial namespace race or global race suite was run. I found no demonstrated additional repair2 regression requiring a second finding; this report does not elevate those checks into a hostile concurrent-writer/power-loss guarantee.

Shared helper callers were inspected: contained preparation/capture (contained_lifecycle.go226/351), preflight receipt (contained_preflight.go49), verification receipt (contained_verification.go111), and local collection. Request/worker/execution/directory binding, supervisor stopped-state and launch guards remain before their external effects. New/equal writer refusal propagates; no guard bypass or new launch authority is introduced. Existing preparation/capture *read* shortcuts do not independently re-run this writer; this patch does not qualify all historic contained read-path durability. Relevant contained/preflight/verification controls were included in the required current command. The general fixed replay-size asymmetry also applies to large privateJSON values; R1 is one finding rather than duplicates per caller.

The original settlement repair remains: CollectFailure publication precedes GetThread observation-error handling, which wraps ErrSettleUnproven; runtime persists SettlePending and reopens/reconciles via Settle alone. TestCollectionFix1ObservationReplay independently passed unknown/missing/already-settled branches, exact receipt-byte equality, unchanged export/verification counts, and exactly one recovered settlement for the unknown case.

ResultDurable retains exact upload-assignment-result purpose/name, package/store worker/coordinator authority and epochs for pending plus acknowledged receipts. loadPending validates version/trailing content, manifest limits, full custody length, ordered object ID/size/checksum/sequence/previous digest and record digest. ResultDurable adds upload direction and exact worker→outbox coordinator endpoints; actual importer enforces the same endpoint/order bindings. Existing invalid-receipt tests and actual importer controls passed. Ambiguous receipts are not recaptured, replaced or reclassified as fresh typed overflow.

Unchanged typed ArtifactSizeError classification remains at publication; unknown I/O and forged prefix text remain transient. failCollection keeps the finished flight until quiescence and same-current failed intent are durable, preserving assignment/epoch/attempt, accepted-stop, pause and supersession fences. Failed intent retries only the small failure path; tiny budgets remain failed/claimed with attention rather than pretending completion. Required collection/custody/importer/stop/wait/park/pause/missing-workspace/settlement controls passed. No compiler/M17 or whole-M16 qualification is inferred.

## Current execution, once

Private0700 GOTMPDIR **/var/tmp/r.sR907O**, ordinary user. Huyang root remained untrusted; no trust/config change. Authorized foreground Git/Go/gofmt and external-artifact programs used.

```sh
GOTMPDIR=/var/tmp/r.sR907O GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerproto ./internal/workerruntime ./internal/backlog -run 'TestPermanentCollectionFailure|TestArtifactSizeError|TestCollectionFix1|TestCollectionFix2|TestRepair1Independent|TestRepair1Review|TestReviewCollection|TestIndependentCollectionReceiptAndRetention|Test.*(Collection|Custody|Importer|StopYields|DeferredStop|TaskWait|Parks|Parked|Pause|MissingWorkspace|RepeatedCollect|Unproven|Settle|Contained|Preflight|Verification)' -count=1
```

Actual exit **0**, foreground session14915 completed. Full stdout:
```text
ok  	github.com/iryzhkov/t3-steward/internal/workerproto	0.006s
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	16.693s
ok  	github.com/iryzhkov/t3-steward/internal/backlog	11.286s
```
Full stderr: empty. focused.stdout192bytes/SHA2569d824753382091ca3d3dc1355e08ac73b4cd82c3818c93b69d9a5aedacef3b5b; argv/stdout/stderr/exit stored separately under the evidence root. No current full gate or race rerun.

## Complete independent portable probe

Created with Huyang, formatted with foreground `gofmt -w internal/workerruntime/repair2_review_probe_test.go` (exit0). Only disposable fixture state is mutated.

```go
package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRepair2ReviewLargeRecoveryReplay(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("ordinary-user permissions required")
	}
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "small-control", true: "large"}[large], func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			thread := *c.thread
			archive := []byte("{}")
			if large {
				archive = append([]byte("{\"padding\":\""), bytes.Repeat([]byte("x"), 13<<20)...)
				archive = append(archive, []byte("\"}")...)
			}
			if !json.Valid(archive) {
				t.Fatal("archive fixture")
			}
			if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "original failed", archive, "original failure", ""); err != nil {
				t.Fatal(err)
			}
			dir := f.driver.workspacePath(f.pkg)
			active := filepath.Join(dir, "collected-turn.json")
			raw, err := os.ReadFile(active)
			if err != nil {
				t.Fatal(err)
			}
			recovery := filepath.Join(dir, "collected-turn-recovery-"+shortDigest(raw)+".json")
			if err = os.Chmod(dir, 0300); err != nil {
				t.Fatal(err)
			}
			defer os.Chmod(dir, 0700)
			first := privateBytes(recovery, raw)
			copyRaw, e := os.ReadFile(recovery)
			if first == nil || e != nil || !bytes.Equal(copyRaw, raw) {
				t.Fatalf("post-link fixture: %v %v", first, e)
			}
			if err = os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			// Real collection reopens the journal, sees the earlier failed snapshot,
			// and must adopt the identical recovery copy before genuine success.
			f.reopen(t)
			retry := f.runtime.collect(context.Background(), "assignment-1")
			copyRaw, e = os.ReadFile(recovery)
			if e != nil || !bytes.Equal(copyRaw, raw) {
				t.Fatal("original recovery changed", e)
			}
			pending, pe := f.custody.PendingUploadByPurpose("result")
			t.Logf("large=%v jsonBytes=%d firstRefused=%v retry=%v phase=%s verifies=%d settles=%d published=%v exactRecovery=true", large, len(raw), first != nil, retry, f.record(t).Phase, f.process.calls, c.settles, pending != nil)
			if pe != nil {
				t.Fatal(pe)
			}
			if retry != nil || f.record(t).Phase != PhaseCompleted || f.process.calls != 1 || c.settles != 1 || pending == nil {
				t.Error("restored permissions did not recover identical valid snapshot")
			}
		})
	}
}
```

Exact execution, once:
```sh
GOTMPDIR=/var/tmp/r.sR907O GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerruntime -run '^TestRepair2ReviewLargeRecoveryReplay$' -count=1 -v
```
Actual Go exit **1**, foreground session29834 completed. The capture wrapper itself exits0 after recording the actual child exit in probe.exit. Full stdout:
```text
=== RUN   TestRepair2ReviewLargeRecoveryReplay
=== RUN   TestRepair2ReviewLargeRecoveryReplay/small-control
    repair2_review_probe_test.go:61: large=false jsonBytes=312 firstRefused=true retry=<nil> phase=completed verifies=1 settles=1 published=true exactRecovery=true
=== RUN   TestRepair2ReviewLargeRecoveryReplay/large
    repair2_review_probe_test.go:61: large=true jsonBytes=18175644 firstRefused=true retry=collection deferred: retain failed collected turn: file is not a bounded regular file phase=collecting verifies=0 settles=0 published=false exactRecovery=true
    repair2_review_probe_test.go:66: restored permissions did not recover identical valid snapshot
--- FAIL: TestRepair2ReviewLargeRecoveryReplay (0.36s)
    --- PASS: TestRepair2ReviewLargeRecoveryReplay/small-control (0.15s)
    --- FAIL: TestRepair2ReviewLargeRecoveryReplay/large (0.21s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.369s
FAIL
```
Full stderr: empty. probe.stdout943bytes/SHA256b70dd73e768edb0dbeabb6ab80c5400b2de9a40de78abde251f72877f791aa6c. No probe source/assertion corrections or repeated execution. Guarded removal req_12005 used exact formatted docrev_976f18700c7b004db65f962746b6b66687194b7ec15696e8d0342ace5622c0fd. No production file/index edits.

## Independent custody and complete retention audit

Initial HEAD6b0a6798736d3c9277e7688ef3016c690bf7a9ee, only mounted .t3 untracked. Verified ONLY collection bundle **467677bytes/SHA256c6993e7f0c0cbef71a263be0447958f98684b8f6c33a4767d80a955343f46824**, sole baseline prerequisite/export742639f; bundle verify/list-heads exit0 before import. Fetched metadata matched exact tree/sole parent before detach. No sibling bundle imported.

Both preceding reports compare exact committed bytes against mounted inputs:
- Sol19626bytes/094aa96f6df60be5a92c840d4672e6fb84f57f528f62c53d2ea5954a3a384fac.
- Astra24863bytes/8ec861f6214a39e4bd213b6f7676bb97611578b4e122b7cf544daeff4f68d1ea.

All complete Go fences were independently extracted and gofmt compared against Git blobs:
- repair1_independent_overlay_test.go:3440bytes/fc26d63a8c2e120acd6b6e3b356ad9796dce1eb6c3a51a05568c3000a6d6f868, BOTH Sol functions.
- repair1_independent_astra_test.go:5226bytes/45317b19d774fe3ffc0b90743b8055ec7d8030bed41c0d20c20604152103e5d3, all THREE final Astra functions including durability.
- Older final Sol fence including actual importer:5984bytes/e872b193d15e141aee26f73f13b3bf5ac4f0a200a2629fa6267f2506226065fd.
- Older Astra fence:3563bytes/9db84fe4065eccd726ff18f584932b437330eb711638d1c1109651a573fc7d34.

Older reports/fences also equal parent bytes and every recorded length/hash in the retention inventory. No assertion weakening. Only seven repair2 paths differ from parent, so prior custody/lifecycle tests and guards are unchanged.

## Lossless log and complete structured evidence

XZ independently checked before decompression:535672bytes/90d7aeb2cd04cd6defe4fb34bf9fe02737f3020bd6d9df76bb25c9b8151770c8. Full raw **15050100bytes/29b4cc60ee874c99643928fe0a9b1266c0bf42ea657a916a4e3f5bbb994fbe6f**, retained as /var/tmp/r.sR907O/producer.log. All **42 outer structured blocks** and **10 nested historical blocks** extracted by declared byte length, SHA256 independently verified and terminator checked. No inventory sampling or acceptance based only on lead hashes.

Complete meaningful current command chronology and historical command/failure/correction/gate chronology were audited; repetitive successful-test chatter and object/source rows were handled programmatically. Current producer: BEFORE portable1 reproduces changed/empty-turn loss and post-link retry authorization; repair/gofmt0; augmented AFTER0; exact portable AFTER0; relevant0; audit/stage/fingerprint0; **one** make check-review FAST_BASE=fddd... exit0, build/vet/short suite/pinned Go1.25.0 staticcheckv0.7.0/full format/full workerruntime race**55.004s**; post fingerprint/commit/package/process0. The rejected ps --ww command and corrected status command are recorded honestly. No producer source edits after final gate.

Historical repair1 raw2330917bytes/048c2e7926c35aa3b965e3216f523b84f741832da79a06cc910b80abadc6b161 remains embedded/extracted. Chronology:
- Lines33–80: original three portable blockers fail1; initial repair86–125 passes0.
- Lines131–361: expanded controls fail for mistaken successful-capture assumption, wrong AttemptID fixture selecting another directory and no-root runtime hitting missing-workspace before the intended seam.
- Relevant363–458 retains those issues and real repeated-turn regression. Fixture correction464–552 leaves repeated-turn failure. Source correction559–568 and exact portable580–619 pass0.
- First repair1 make621–930 exits2 on missing-turn-ID regression (789–791), before lint/races. Historical older-producer audit interleaved624–712 is not another repair1 gate.
- Missing-turn correction932–1053 passes affected controls.
- Second justified make1065–1107 exits0: build/vet/short/pinned lint/format/workerruntime race**53.474s**.
- Post fingerprints/commit/fresh package/process/index receipts1109–1213 pass0.
These are TWO historical repair1 gates, separate from ONE producer repair2 gate and this review's focused command. Historical results are not claimed as current re-execution.

Full **1433** current working path/blob rows and logical index mode/OID/stage/path entries compare pre==post==exact candidate tree; current logical index digest independently matches c1dd89dad1e2120dd2d1ec7ec8bfbf5d9c91e204184add6dbed8a9868019afda. Full **1428** historical working/index rows compare pre==post==parent tree. Historical first failed-gate fingerprint remains separate. Complete source ALL10714 before/after equal with363 extras; historical ALL10708 before/after equal with369 extras. Historical full typed parent closure independently reproduced10339objects/527798bytes/20ad45641441aa188ef8b7744bb08beada0d0b7a6401ec9e8a5e7b456bf82819. No claim that ALL equals candidate closure or that unavailable historical extras were reconstructed.

Correction in this review: the first comparison script treated historical-index-current.json as an array, exited1 after current comparisons. Inspection showed an object containing entries; corrected script compares all entries and exits0. No source changes or evidence substitutions. Mounted-input Huyang symlink refusal was resolved by external artifact access. A few transport-truncated reads were recovered through bounded reads/structured comparisons, not counted as complete from truncated text.

## Fresh independent import and typed closure

External fresh bare repository /var/tmp/r.sR907O/fresh.git was populated from a standalone baseline bundle using a temporary local baseline ref, removed in finally. It had only baseline ref/closure and no alternates. Before collection import, accepted49b99fd, original47e53bed, parentfddd and candidate742639f were each absent: actual cat-file -e exit**1** for all four. Producer's historical absence exit128 is not substituted for this environment's actual1.

Only supplied collection bundle verified/imported. Source/fresh strict full fsck exit0. Full rev-list reachable IDs batch-typed with missing-row rejection, sorted and compared source==fresh==complete supplied closure:
**10351objects /528414bytes /SHA25685007247b57545ab4c37eb0c114c452c38b77a64fb4c5b674acd0aa78a42a45e**.
Current source ALL10714 before/after exact equality preserves363 unrelated objects. Full scripts, argv/stdout/stderr/actual exits and inventories retained under /var/tmp/r.sR907O (closure.py, closure.commands.json, current-*.types, compare.py, blocks/, block-audit.json). No HEAD-only substitute.

## Terminal state and limits

Exact candidate unchanged. Tracked worktree/index clean; disposable probe removed with guarded Huyang deletion. Only mounted inputs and declared continuation.md/review.md remain untracked. Both outputs below256KiB. All task-started foreground executions finished; no background process was launched or remains pending.

Local independent review only. Source publication:none; release publication:none; verified deployment:none. Publishing deferred, schedules untouched/disabled. No push/PR/tag/release/UpKeeper/CI/fleet/live config/admin/trust/service/provider effects. No full M16, public activation, final physical HEAD, compiler/M17, OS restart or provider-diversity completion claim. Required production two-provider-family gate remains. Repair and fresh exact-candidate reviews are required before acceptance.
