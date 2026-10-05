# ACCEPT — independent collection repair2 review

Verdict: **ACCEPT** for the bounded local collection repair at **742639f540f3ccfd3ea38b3a4117f64e416d11ab**, tree **736e6e5c89e7532e0f14bd5eb894c610f4105eb2**, sole parent **fddd608a987b213fa3009f151bdb0c028fa290f7**. Deduplicated actionable findings: **none** (no P0/P1/P2/P3 finding). Both preceding retention/durability blockers are resolved in the reviewed paths. This is one independent Codex medium runtime/retention/publication/importer-authority review; lead acceptance still requires BOTH independent exact-candidate reviews.

Controlling review jocasta:2ccbdf98670fa90c467ad891a34af313@1; full mounted collection-fix2-review-plan.md, collection-fix2-plan.md (repair jocasta:b5295287d9965c68a58e2606027e2430@1), collection-original-plan.md, collection-fix1-plan.md, collection-review-plan.md, current collection-handoff.md/collection-contract.md and BOTH actual preceding collection-review-{sol,astra}.md were read. No profile sibling content was read/imported and no sibling reviewer consulted. No delegation, nested review, provider or effort escalation.

## Runtime and shared helper assessment

**local_driver.go:1075–1130:** valid existing records are decoded and bound to the complete execution identity and thread before use; recovery-only records require Identity. Active bytes must pass syncPrivateFile before reusable witness/retention/replacement decisions. Every recovery-only record that would be replaced is copied using its exact original JSON and digest, regardless of different or empty-to-known TurnID and later success/failure. Copy precedes removal. Conflict/symlink/binding/permission/durability refusals precede finalization, new result custody and settlement, leaving recovery evidence intact. RecoveryOnly never authorizes successful recovery. Successful same-turn witnesses retain their original semantics; the independent probe verifies legacy successful JSON without Identity and mode0400 remains usable and unchanged.

The complete retained Sol later-turn test traverses actual runtime transient-first failed observation, genuine later success, archive overflow, persisted failure, journal reopen and bounded fallback: original JSON is retained separately with one verification. Retained Astra tests cover same/empty/different turn transitions and actual active post-link permission failure. New collection_fix2 controls cover active successful AND failed repeated refusals, recovery-copy link/equal replay, restored permissions/reopen/stable effects, noncanonical JSON across later success/failure and different-turn zero-effect retention refusals. My extra probe independently covers unknown fields with empty-to-known later failure, missing recovery Identity, wrong thread, legacy successful read-only recovery and shared writer modes.

**contained_lifecycle.go:54–128:** privateJSON canonical marshaling delegates to privateBytes; exclusive Link never overwrites an existing path. Existing bytes must exactly equal the requested bytes and then reprove durability. New-file temporary data is synced/closed before linking. syncPrivateFile opens a regular file, compares descriptor/path regularity and SameFile, reads and compares exact expected bytes, syncs file and directory, and rechecks pathname identity. This closes the visible-file retry authorization gap without deleting unproven evidence. Errors stay ordinary operational failures.

I inspected ALL production privateJSON caller families: contained preparation/capture (contained_lifecycle.go:131–389), preflight (contained_preflight.go:26–83), verification (contained_verification.go:81–142), and local snapshot publication. Preparation/capture still check package/worker/launch/directory identity; preflight fixes request/workspace/bounded deadline before writing; verification additionally requires stopped provider supervision. Their writer failures return before subsequent launch/stop effects, and relevant contained/preflight/verification controls passed. Equal replay adds sync refusal without weakening identity/exclusive guards. Read-only legitimate receipts work on this Linux filesystem; unreadable receipts and unreadable directories refuse until permissions recover. Existing independent preparation/capture readers are not all newly durability-qualified by this patch; this review does not expand the repair to those pre-existing read paths.

TOCTOU assessment is bounded: the reopened descriptor's bytes must equal the earlier observed/requested bytes, descriptor/path identity must match before syncing and pathname identity must still match afterward. Stable symlink/nonregular substitution refuses. There is no exclusive inode lock against a malicious same-user concurrent in-place writer, no universal ancestor-directory anti-symlink claim, and no proof for every adversarial filesystem interleaving. These private worker paths rely on existing ownership/serialization; no new overwrite authority is introduced. No deterministic in-scope regression warranted a full gate/race rerun.

**custody.go:168–204, 630–662; backlog/result_import.go:260–279:** exact result purpose/name, package/store epochs and assignment, upload direction, worker→coordinator-outbox endpoints, complete object ordering/identity/size/digests/previous-chain bindings remain. Existing pending AND acknowledged mutation controls rehash foreign endpoints, refuse without receipt replacement/reverification/typed reclassification, and exercise real SQLite importer zero transitions/artifacts. Valid first receipts remain accepted. Actual permanent-failure importer controls produce failed sink/skipped dependent, idempotent replay and original raw retention. Receipt-authority and collection-flight/quiescence/current-attempt/epoch/pause/stop/supersession guards are unchanged by repair2.

Observation uncertainty/ErrSettleUnproven/SettlePending reopen remains separately retriable without original export/verification/publication. Typed size failures stay distinct from unknown I/O/forged-prefix errors. Tiny-budget failure persists actionable claimed attention instead of resetting original work or claiming coordinator completion. Older portable assertions and lifecycle guards are unchanged, not replaced with mocks.

## Source custody, complete retention and evidence audit

Initial HEAD was exact baseline **6b0a6798736d3c9277e7688ef3016c690bf7a9ee**, tracked/index clean. ONLY mounted collection-candidate.bundle was verified before import: **467677 bytes**, SHA256 **c6993e7f0c0cbef71a263be0447958f98684b8f6c33a4767d80a955343f46824**, sole baseline prerequisite and sole export742639f HEAD. git bundle verify/fetch/show/checkout all exited0. Exact tree/sole parent matched before detach and before the first document/overlay edit. Huyang skill/workspace ws_40225894d7db2f458d22f56e9ae51219 used for repository reads and guarded edits/removal; root remains untrusted, unchanged. Foreground Git/Go/gofmt and external artifact programs were used under authorization.

All seven changed paths reviewed: contained_lifecycle.go, local_driver.go, collection_fix2_test.go, repair1_independent_overlay_test.go, repair1_independent_astra_test.go, repair2-evidence/collection-review-sol.md and collection-review-astra.md. Full retained reports compare byte-for-byte with BOTH supplied actual reports. Independently extracted complete Go fences, passed through gofmt, and compared with candidate Git blobs:
- Sol TWO functions:3440bytes/fc26d63a8c2e120acd6b6e3b356ad9796dce1eb6c3a51a05568c3000a6d6f868.
- FINAL Astra THREE functions including directory durability:5226bytes/45317b19d774fe3ffc0b90743b8055ec7d8030bed41c0d20c20604152103e5d3.
- Older Sol FINAL importer fence:5984bytes/e872b193d15e141aee26f73f13b3bf5ac4f0a200a2629fa6267f2506226065fd.
- Older Astra fence:3563bytes/9db84fe4065eccd726ff18f584932b437330eb711638d1c1109651a573fc7d34.
Every pre-existing path except the two deliberate production helper changes has the identical mode/blob at fddd and742639f; this preserves every older portable file/assertion and report, not samples/function-count substitutes.

Verified supplied XZ **535672bytes/90d7aeb2cd04cd6defe4fb34bf9fe02737f3020bd6d9df76bb25c9b8151770c8** and full decompressed raw **15050100bytes/29b4cc60ee874c99643928fe0a9b1266c0bf42ea657a916a4e3f5bbb994fbe6f**. Retained externally at /var/tmp/r.RbPP4V/producer.log. Independently extracted and checked all42 complete outer structured blocks and all10 nested historical blocks by advertised byte length/hash. Read meaningful current chronology and programmatically indexed/read historical command/outcome/failure/correction windows; repetitive test enumeration/object rows were compared programmatically, not represented as manual review from digests.

Producer repair2 chronology: BEFORE portable probes fail1 on both blockers; narrow helper/test repair; augmented AFTER0; exact AFTER portable0; relevant selection0; ONE make check-review FAST_BASE=fddd actual0 with build/vet/full short suite, pinned Go1.25.0/staticcheckv0.7.0/format and workerruntime race55.004s; no subsequent source edits. Read-only ps --ww error1 corrected using supported ps syntax; input symlink refusal and diagnostic availability remain honestly recorded. Gate records are producer evidence, not execution here.

Historical repair1 chronology independently audited from embedded2330917bytes/048c2e7926c35aa3b965e3216f523b84f741832da79a06cc910b80abadc6b161: BEFORE original blockers fail1; initial repairs pass; expanded fixtures fail for wrong successful-capture expectation, changed AttemptID selecting a different directory, no-root fixture hitting missing workspace. Corrections retain all failed runs. Real repeated-turn regression required recovery-copy source correction. First historical gate lines621–930 exits2 at missing-turn-ID regression/short suite before lint/races (nested older audit at624–712 is not another repair gate). Source correction/new missing-turn control passes; second justified gate1065–1107 exits0 including workerruntime race53.474s. TWO historical gate invocations, distinct from ONE current producer gate.

Complete current pre==post1433 working/path/blob, logical mode/OID/stage/path index and tree rows equal candidate commit. Independently reproduced current index digest c1dd89dad1e2120dd2d1ec7ec8bfbf5d9c91e204184add6dbed8a9868019afda; hash-object verified all1433 actual working paths. Historical1428 working/index/tree rows pre==post equal fddd and historical logical index digest0b5e8cb983045ec2d5b7d186745588f9b569d8323f0cb6f5e966b92767a35b40 reproduced. Full historical typed closure10339objects/527798bytes equals independently enumerated fddd closure; source-all10708 before/after exact,369 extras. Current producer source-all10714 before/after exact,363 extras. No source-ALL==reachable-closure assertion.

Fresh independent bare import /var/tmp/r.RbPP4V/independent-fresh.git initially held only baseline ref/closure, no alternates. accepted49b99fd/prior47e53bed/parentfddd/candidate742639f each absent with actual nonzero exit1 (this Git environment differs from producer's128). Temporary source baseline ref exported then removed. Fresh bundle verify/import and source/fresh strict full fsck exited0. Full sorted typed reachable equality source==fresh==producer:
**10351objects/528414bytes/SHA25685007247b57545ab4c37eb0c114c452c38b77a64fb4c5b674acd0aa78a42a45e**, zero missing. Independent source ALL10714 unchanged,363 extras preserved. Complete raw command stdout/stderr/exit receipts and scripts are retained in /var/tmp/r.RbPP4V/independent.commands.json, independent_audit.py, independent-audit.json and full typed files. Historical unrelated inventories were actually supplied and compared, not fabricated.

## Current exact focused command — once

Private0700 GOTMPDIR /var/tmp/r.RbPP4V, GOMAXPROCS=2, GOFLAGS=-p=2:
```sh
GOTMPDIR=/var/tmp/r.RbPP4V GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerproto ./internal/workerruntime ./internal/backlog -run 'TestPermanentCollectionFailure|TestArtifactSizeError|TestCollectionFix1|TestCollectionFix2|TestRepair1Independent|TestRepair1Review|TestReviewCollection|TestIndependentCollectionReceiptAndRetention|Test.*(Collection|Custody|Importer|StopYields|DeferredStop|TaskWait|Parks|Parked|Pause|MissingWorkspace|RepeatedCollect|Unproven|Settle|Contained|Preflight|Verification)' -count=1
```
Actual exit **0**; full stdout:
```text
ok  	github.com/iryzhkov/t3-steward/internal/workerproto	0.006s
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	19.259s
ok  	github.com/iryzhkov/t3-steward/internal/backlog	11.238s
```
Full stderr: empty. Foreground session1050 completed; focused.stdout/stderr/exit retained externally. No current full gate or race rerun.

## Complete independent portable missing-coverage probe

Huyang create followed by foreground gofmt -w internal/workerruntime/repair2_independent_runtime_test.go (exit0). Full formatted source:
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

func TestRepair2IndependentLegacySuccessfulWitness(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
	original := collectedTurn{ThreadID: f.pkg.Identity.ThreadID, TurnID: f.control.thread.TurnID, Message: "legacy finished", Archive: f.control.archive}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0400); err != nil {
		t.Fatal(err)
	}
	message, archive, failure, err := f.driver.settleCollectedTurn(f.pkg, *f.control.thread, "unfinished", []byte("{}"), "not finished", "")
	after, e := os.ReadFile(path)
	t.Logf("legacy no identity read-only recovered=%v exactBytes=%v", err == nil && failure == "" && message == original.Message && bytes.Equal(archive, original.Archive), bytes.Equal(raw, after))
	if err != nil || e != nil || failure != "" || message != original.Message || !bytes.Equal(archive, original.Archive) || !bytes.Equal(raw, after) {
		t.Fatal("legacy successful witness refused", err, e)
	}
}

func TestRepair2IndependentUnknownJSONAndBinding(t *testing.T) {
	for _, mutation := range []string{"unknown-fields", "missing-identity", "wrong-thread"} {
		t.Run(mutation, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			thread := *c.thread
			thread.TurnID = ""
			if _, _, _, e := f.driver.settleCollectedTurn(f.pkg, thread, "first", []byte("exact original archive"), "failed", ""); e != nil {
				t.Fatal(e)
			}
			path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			raw, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var fields map[string]json.RawMessage
			if e = json.Unmarshal(raw, &fields); e != nil {
				t.Fatal(e)
			}
			if mutation == "missing-identity" {
				delete(fields, "identity")
			}
			if mutation == "wrong-thread" {
				fields["threadId"] = json.RawMessage(`"foreign"`)
			}
			fields["futureRecoveryMetadata"] = json.RawMessage(`{"preserve":["all", 1]}`)
			raw, e = json.MarshalIndent(fields, "", "  ")
			if e != nil {
				t.Fatal(e)
			}
			raw = append(raw, '\n')
			if e = os.WriteFile(path, raw, 0600); e != nil {
				t.Fatal(e)
			}
			if mutation == "unknown-fields" {
				thread.TurnID = "known"
				_, _, failure, e := f.driver.settleCollectedTurn(f.pkg, thread, "later failure", []byte("later archive"), "still failed", "")
				retained, re := os.ReadFile(filepath.Join(filepath.Dir(path), "collected-turn-recovery-"+shortDigest(raw)+".json"))
				t.Logf("empty-to-known later failure exact unknown JSON retained=%v", bytes.Equal(raw, retained))
				if e != nil || re != nil || failure != "still failed" || !bytes.Equal(raw, retained) {
					t.Fatal("retention", e, re)
				}
			} else {
				e = f.runtime.collect(context.Background(), "assignment-1")
				fix2NoEffects(t, f, c, e)
				after, re := os.ReadFile(path)
				t.Logf("binding=%s refused=%v unchanged=%v verification=%d settlement=%d", mutation, e != nil, bytes.Equal(raw, after), f.process.calls, c.settles)
				if re != nil || !bytes.Equal(raw, after) || f.record(t).Phase != PhaseCollecting {
					t.Fatal("binding refusal mutated evidence", re)
				}
			}
		})
	}
}

func TestRepair2IndependentSharedPrivateReplay(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("ordinary user required")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "receipt")
	data := []byte("exact private bytes")
	if e := privateBytes(path, data); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(path, 0400); e != nil {
		t.Fatal(e)
	}
	if e := privateBytes(path, data); e != nil {
		t.Fatal("read-only legitimate replay", e)
	}
	if e := os.Chmod(path, 0000); e != nil {
		t.Fatal(e)
	}
	e := privateBytes(path, data)
	if e == nil {
		t.Fatal("unreadable file accepted")
	}
	if e = os.Chmod(path, 0600); e != nil {
		t.Fatal(e)
	}
	if e = privateBytes(path, []byte("foreign")); e == nil {
		t.Fatal("conflict accepted")
	}
	if e = os.Chmod(dir, 0300); e != nil {
		t.Fatal(e)
	}
	defer os.Chmod(dir, 0700)
	for i := 0; i < 2; i++ {
		if e = privateBytes(path, data); e == nil {
			t.Fatal("unproven directory replay accepted")
		}
	}
	if e = os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e = privateBytes(path, data); e != nil {
		t.Fatal("restored replay", e)
	}
	link := filepath.Join(dir, "symlink")
	if e = os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	if e = privateBytes(link, data); e == nil {
		t.Fatal("symlink adopted")
	}
	raw, re := os.ReadFile(path)
	if re != nil || !bytes.Equal(raw, data) {
		t.Fatal("shared helper changed original", re)
	}
	t.Log("0400 legitimate replay passes; 0000/conflict/0300 repeated/symlink refuse; restored permissions replay passes with exact bytes")
}
```
Exact argv:
```sh
GOTMPDIR=/var/tmp/r.RbPP4V GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/workerruntime -run '^TestRepair2Independent' -count=1 -v
```
Actual exit **0**, no fixture/source correction or repeated probe execution. Full stdout:
```text
=== RUN   TestRepair2IndependentLegacySuccessfulWitness
2026/10/05 10:35:34 INFO the turn read as unfinished on a repeated collection; judging the turn this collection recorded when it started attempt=attempt-1 thread=thread-1 turn=turn-1 reading="not finished"
    repair2_independent_runtime_test.go:25: legacy no identity read-only recovered=true exactBytes=true
--- PASS: TestRepair2IndependentLegacySuccessfulWitness (0.03s)
=== RUN   TestRepair2IndependentUnknownJSONAndBinding
=== RUN   TestRepair2IndependentUnknownJSONAndBinding/unknown-fields
    repair2_independent_runtime_test.go:70: empty-to-known later failure exact unknown JSON retained=true
=== RUN   TestRepair2IndependentUnknownJSONAndBinding/missing-identity
    repair2_independent_runtime_test.go:78: binding=missing-identity refused=true unchanged=true verification=0 settlement=0
=== RUN   TestRepair2IndependentUnknownJSONAndBinding/wrong-thread
    repair2_independent_runtime_test.go:78: binding=wrong-thread refused=true unchanged=true verification=0 settlement=0
--- PASS: TestRepair2IndependentUnknownJSONAndBinding (0.24s)
    --- PASS: TestRepair2IndependentUnknownJSONAndBinding/unknown-fields (0.18s)
    --- PASS: TestRepair2IndependentUnknownJSONAndBinding/missing-identity (0.03s)
    --- PASS: TestRepair2IndependentUnknownJSONAndBinding/wrong-thread (0.03s)
=== RUN   TestRepair2IndependentSharedPrivateReplay
    repair2_independent_runtime_test.go:142: 0400 legitimate replay passes; 0000/conflict/0300 repeated/symlink refuse; restored permissions replay passes with exact bytes
--- PASS: TestRepair2IndependentSharedPrivateReplay (0.02s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	0.301s
```
Full stderr: empty; probe.stdout/stderr/exit retained externally. Foreground session81130 completed. Actual production helpers/runtime are called; filesystem mutation is limited to disposable fixtures. Overlay guardedly deleted with formatted document revision docrev_7726e22edf4cedcf0734282c8da2ebc9d73a7b4e2dd5d131dbd401fc6129d70a, Huyang req_12019/wsrev_6.

## Terminal boundaries and limitations

Tracked worktree/index clean; exact HEAD/tree/sole parent unchanged; temporary baseline ref and overlay removed. All commands started here ran in foreground and completed; no background processes started or task-owned pending process remains. Only .t3 and declared review.md/continuation.md are untracked, each below256KiB.

Permissions are ordinary-user Linux fixtures proving permission refusal before directory fsync and bounded recovery after restoration. No injected EIO, observed power loss, OS restart, hostile concurrent writer guarantee, live provider attachment or fleet experiment. Journal reopen is not an OS restart. Focused tests do not substitute for whole-program/platform durability qualification.

Local review only. Source publication:none; release publication:none; deployment:none. Publishing deferred/schedules disabled; no push/PR/tag/release/UpKeeper/CI/fleet/liveconfig/admin/trust/service/provider effects. No full M16/public activation/final physical HEAD/compiler/M17/diversity completion claim. Production two-actual-provider-family qualification remains mandatory.
