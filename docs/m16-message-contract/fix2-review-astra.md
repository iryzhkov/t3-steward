# Independent message-fix1 review — CHANGES REQUESTED

Exact reviewed commit: eb55e1be9c3b5b7656d3bce8641dda5541913c73
Tree: 337ef3f061617a0a3403add0d1fcea9cd5a1325b
Sole parent: 63843b4c02a77ba2ce6342e664d89dea9d358416

Reviewer: GPT-6-Astra, medium, independent Codex task. Authority jocasta:18633459f8d04a0c33d383c8796cb710@1. Message-fix1 only. No delegation, nested review, escalation, sibling review consultation/import, production edit or external effect.

Verdict: CHANGES REQUESTED. One deduplicated P2 finding (R1); no P0/P1 findings. The producer's final corrected gate passes and the assertion repair itself is sound. The full required F6 message invariant still fails on the elapsed-reset telemetry-probe path. This verdict applies to the exact candidate, not merely its last two-file diff.

## R1 — P2 — elapsed-reset probe still tells the owned agent that quota recovered

Primary changed sink: internal/workerruntime/local_pause.go:322 (and log at327).
Forwarding sink: internal/workerruntime/local_driver.go:1282.
Reason origin: internal/workerruntime/quota_guard.go:155–159, via internal/daemon/resume_policy.go:60–61 and87–100.
Coverage gap: internal/workerruntime/message_contract_test.go:51–100.

When a paused attempt's bucket reset time passes by ProbeAfterReset + ResetSettleDelay without a confirming reading, BucketsRecovered permits the existing stage-1 telemetry probe. Its callback returns true and ApplicableHealthy ignores the expired bucket, yielding allowed=true and an empty reason. HostQuotaGuard then labels that permission "<bucket> recovered". Changing the downstream prefix from "quota recovered:" to "quota resume permitted:" does not remove that false assertion: both runtime logging and LocalDriver's actual prompt retain the suffix.

The independent portable runtime probe below reproduces this with the real HostQuotaGuard, SQLite fixture and actual Runtime.Reconcile. The bucket remains PhaseStopped at97%, RecoveredAt=nil, observed timestamp unchanged; one resume occurs, and actual prompt ends:
"Resume permission: quota resume permitted: codex/codex/primary recovered".
Actual test exit1. This is not a synthetic free-text fake-guard reason: the guard computes it. The existing new "probe" fixture only exercises the other, stopped-below-threshold/stale-reading stage-2 probe, whose reason already says probe; its fresh and disabled controls pass but miss this branch.

Required fix: make the reason for elapsed-reset permission truthful at the point the guard knows which path authorized it, or use a neutral reason that does not assert recovery without evidence. Preserve the exact existing resume eligibility, thresholds, phase, receipt and runtime transition rules; do not change policy to satisfy wording and do not guess recovery by classifying arbitrary reason strings. Retain this complete failing regression and positive fresh-recovery, stage-2-probe, disabled-checks, cancellation and owned-completion controls. Verify both runtime log and emitted LocalDriver prompt. A passing narrow ownership assertion alone cannot close F6.

Limitation: this is a local synthetic reproduction of misleading prose, not a live quota observation, live Claude incident diagnosis, or a finding that the existing elapsed-reset resume policy itself should change. The reason-origin defect predates this repair; it remains within the explicitly required original F6 acceptance contract and is carried into the changed sinks.

## Declared inputs and exact source custody

Listed .t3/dependencies before consuming it; exactly one producer directory was supplied: task-cfe3170a2c879404b6f6cf50faa97f6e. Did not infer an assigned task ID. Consumed its declared repair campaign-commit plus all five outputs. The campaign-commit/v1 record has workflowRunId run-95490ef422068db15daf2cfb571424a1, matching producer taskId, name repair, repository git@github.com:iryzhkov/t3-steward.git, base6b0a6798736d3c9277e7688ef3016c690bf7a9ee, exact eb55e1b commit, and exact refs/campaigns/<run>/<producer>/repair. All fields asserted programmatically.

Full followup-plan.md equals the exact Jocasta revision/hash46dbd9f3afbd5b232bac2aa54bf71e89de9694f879bcf73636ce1152cf2fa384. Read FULL original message-repair-plan, audit, message current handoff/contract, quota-message-audit-plan/user-requirements and branch prerequisite profile-handoff/contract plus both accepted profile reviews. Those prerequisite reviews concern0131 only; they are not sibling message-fix1 reviews and do not supply this verdict. Large initially truncated deliveries were followed by bounded reads. Collection sibling files were not consumed; directory/inventory listing exposed their names only.

Programmatically verified sizes/hashes for all13 branch-specific original inputs against input-inventory.json, including historical message bundle/log. Imported ONLY declared producer repair.bundle, never original/profile/collection sibling bundles. Initial physical HEAD was exact6b0a, tracked/index clean. Bundle verify/list-heads showed sole prerequisite6b0a and sole exported HEADeb55e1b. Verified exact commit/tree/sole parent before detached checkout. Physical checkout and all1454 tracked worktree OIDs were independently compared to producer final logical index/tree.

Producer artifact byte identities (computed independently):

| Artifact | Bytes | SHA256 |
| --- | ---: | --- |
| repair campaign-commit | 496 | e002d0b99c817cedd4618a9381194bf096efe3190618bcb038357c2e7f82c295 |
| continuation.md | 1846 | fcf093ff3b7db7561d4a996ddd1b2916b24bab98650a4a63fec08f4149577587 |
| handoff.md | 7545 | 44d26d039a16e3f800d66502410a4a3af4dac715811996cb655aa73ade1f9b63 |
| repair-contract.md | 16275 | 09a4f3d7482297445ac27cbf43756f8e02c813fe49bab7fbb6a6b8ec9e1951a2 |
| repair.bundle | 540727 | 222c97b183ff262d4cd90c8877db3e4089eeab45d7e01adcd731a031c347e212 |
| verification.log.xz | 410816 | 4ce707ad0cba5688e6e35ec91a06bf2c0752c527351f98eacc5e05265671865f |

Current compressed log fully decompresses to7548527bytes,114680lines, SHA25636acbe00cbcb05a5fbcd9d7361d632f0ecfe06964ce0feab16d5413febab3c24, exactly matching the producer raw seal. Complete decoded bytes retained at /var/tmp/ar.88sqctp6/producer.log. No truncation of the artifact. Historical message log independently hashes/decompresses to10726756bytes/SHA25681a82ff95782d37ef47f156e0c41693806ca2750d468701c7dbf1270c48b67e3; its reported failed gate remains historical, not a current pass.

Huyang workspace ws_e4bb700ca5af2e27e0e7cec8a8d5198c opened before repository reads. Huyang used for source reads and guarded disposable probes/output edits; Git plumbing for diffs/object/fingerprint evidence; explicit foreground Git/Go/external-artifact authorization used. Trust unchanged/untrusted; no isolated Huyang verification or full LSP success claimed.

## Complete producer verification and closure audit

Read complete meaningful current command/failure/correction/proof chronology after classifying full structured fingerprints and all object-inventory rows separately (455 meaningful lines). All inventories and complete records were programmatically compared, not sampled or accepted from summary prose.

Current BEFORE affected ownership test exit1 with the removed unconditional provider guarantee required by old assertion. AFTER affected test exit0 and original exact seven-package selection exit0. Current gate outcomes retained:
1. make check-review FAST_BASE=63843b4... terminated intentionally during short tests for tracked historical-contract custody correction: actual-15,147.664s. No pass/lint/race claim.
2. Same gate with final source: actual2,332.982s. Unchanged t3api/TestContainedTransportPinsSocketAndRejectsReplacement failed bind with an overlong Unix socket pathname under nested TMPDIR. No lint/race reached.
3. Fresh short private0700 /var/tmp/r.MNeKXD for GOTMPDIR/TMPDIR, unchanged GOMAXPROCS2/GOFLAGS-p2, no tracked source edit. Affected socket and ownership controls pass. Corrected final gate actual0,414.321672517s: build/vet/full short suite, pinned go1.25.0 staticcheckv0.7.0, gofmt, daemon full changed-package race29.228s. Current full gate is validated; I did not rerun a full gate or race. Ordinary resource correction is independently supported by actual argv/error and unchanged fingerprints, not a hidden source fix.

All FIVE FULL_SOURCE_INDEX_TREE records parsed:1454 records each, every recorded blob size/SHA256/mode/OID independently checked through Git, full logical index reconstructed and full tree digest computed. Final-pregate/corrected-pregate/postgate/final are exactly equal, tree337ef3f. Initial interrupted-gate pregate is the expected different historical-contract tree. Full final worktree hash-object over1454 tracked paths matches exact logical index. Historical root repair-contract original12870-byte prefix preserved exactly; collected repair-contract equals committed file. Exact HEAD-parent diff has only ownership_test.go and appended repair-contract.md; all1452 other paths unchanged. Original audit and repair plan permanent copies match full input bytes.

All ELEVEN substantive inventories independently parsed: three complete reachable-ID lists, eight complete typed lists. Baseline9926 exact; producer candidate10418 exact source/fresh closures and freshALL; producer sourceALL10782 pre/post equal, containing all10779 precommit objects. SourceALL is NOT equated with reachable closure. Every available sourceALL object type/size corroborated. Two unrelated producer objects unavailable in this checkout: intermediate tree3b9956155a86b0577078b06ece5008a637e6a8be/tree1425 and bootstrapfd308731cbdfcadbbfaa7ea99f56c8b23a4327d9/commit280. They are outside required closure and remain historical preservation evidence, not fabricated imported objects or deleted evidence.

My initial external proof script exited1 because it attempted to ls-tree the unavailable producer intermediate tree. Corrected the audit to reconstruct exact Git tree digests from the full recorded mode/path/blob set without importing or writing that unrelated tree. Every recorded blob was available and verified. The corrected proof completed exit0; no Go retry/source mutation followed this audit-script correction. Both initial stderr and complete final script/receipts/output retained under /var/tmp/ar.88sqctp6.

Independent fresh witness: initialized EMPTY /var/tmp/ar.88sqctp6/fresh.git, fetched ONLY exact6b0a baseline with no tags. No alternates; complete ALL equals baseline closure9926; strict fsck0; eb55e1b absent before import. Verified/fetched ONLY declared repair.bundle; fresh and source full strict fsck0. Exact HEAD/tree/sole parent checked in fresh; full typed source candidate closure = fresh closure = freshALL10418, zero missing. Review checkout sourceALL10781 preserved before/after proof. Complete command receipts and script retained externally; no object deletion to manufacture equality.

Full compact program output:
```text
FINGERPRINTS all5 complete1454 records checked against actual Git trees; final4 equal
INVENTORY 100 10779 typed
INVENTORY 11178 10782 typed
INVENTORY 21969 9926 typed
INVENTORY 31897 9926 ids
INVENTORY 41825 9926 typed
INVENTORY 51784 10418 ids
INVENTORY 62204 10418 typed
INVENTORY 72624 10418 ids
INVENTORY 83044 10418 typed
INVENTORY 93464 10418 typed
INVENTORY 103884 10782 typed
PRODUCER CLOSURE all11 inventories consistent baseline 9926 candidate 10418 sourceALL preserved 10782 precommit 10779
UNAVAILABLE UNRELATED PRODUCER OBJECTS ['3b9956155a86b0577078b06ece5008a637e6a8be tree 1425', 'fd308731cbdfcadbbfaa7ea99f56c8b23a4327d9 commit 280']
INDEPENDENT FRESH PASS baseline 9926 candidate/freshALL 10418 review sourceALL preserved 10781
AUDIT PASS

```

## Independent changed-code and invariant assessment

Inspected all24 paths changed since accepted branch predecessor0131 (production/default/docs plus associated tests), and exact two-path latest repair. The latest ownership_test.go:192–212 corrects only prose assertions/comment: actual harness still requires warn:a then drain:a, zero stops, measured3.8%/min, projection about4m and explicit uncertainty. No fixture/threshold weakening.

F1: daemon notifyThreads still renders configured templates and appends projection only to drains; current defaults/config example/Sample agree on conditional grace, threshold/imminence and reset/runway exemptions. policy.Tick boundary inspected; selected controls actually evaluate below-stop/no-stop, stop threshold, imminent stop, near-reset and long-runway exemptions. Real warn/drain harness and disabled-control no-effect tests pass. exhaustionNote no longer promises the provider ends a session at100%. No policy/action/notice receipt mutation in diff.

Migration: exact previous shipped drain427bytes and resume341bytes equal actual0131 constants byte-for-byte, independently extracted with Git. New exact/default+single newline migration preserves older legacy handling. Portable loaded-file probe checks exact and YAML newline migrations, leading/trailing spaces, double newline, CRLF and custom suffix preservation; warn and entire on-disk YAML bytes stay unchanged. Sample/default agreement passes.

F6: actual fresh recovery, stage-2 telemetry probe and checks-disabled controls pass in original selection; runtime retains their guard reason with neutral prefix and LocalDriver honors current instructions. HOWEVER stage-1 elapsed-reset probe still falsely says recovered (R1). No eligibility change is justified by this review.

F7: native node/quota, legacy shell and task-bound wake changes are prose-only plus bounded result-command helper. Inspected actual outcome/trailer and unanswered-ask early return boundary. Original controls cover met/failed/cancelled/gave-up/timed-out/expected timeout, mixed groups, terminal failure-as-met, shell process0 with both REJECT/ACCEPT. Portable native lost-response test checks terminal failed/cancelled while outcome stays met, canonical result command and verdict warning, restart/unknown receipt no resend, positive receipt settlement once.128-byte ASCII identifier inclusion and129-byte/control/Unicode/shell punctuation refusal pass. No fabricated quota/nonterminal command. No receipt or parking/cancellation authority changes.

F9: first-turn completion exception and LocalDriver quota checkpoint keep exact done/continue markers, foreground/output/commit/task-bound-wait contract and current cancellation precedence. Inspected unchanged exact-turn/structured completion logic and real fake-driver runtime completion controls. Original selection exercises completed drain collect-once/no resume, incomplete pause, later-turn refusal and cancelled-attempt restart/no resume. Additional focused composed UTF16 boundary test passes plain/recovery/preflight variants with no provider creation for oversize input. This test was outside the original selector, so it was run separately for meaningful coverage.

## Complete independent test receipts

All Go commands ran foreground to completion, private0700 short /var/tmp GOTMPDIR and TMPDIR, GOMAXPROCS=2 GOFLAGS=-p=2. No background work. Commands ran once per stated version; no failing probe assertion was weakened or retried. Empty stderr is explicit. Required affected AFTER and original seven-package selection ran BEFORE temporary probes were created.
```json
{
  "argv": [
    "go",
    "test",
    "./internal/daemon",
    "-run",
    "^TestDrainNoticeNamesTimeToExhaustion$",
    "-count=1"
  ],
  "env": {
    "GOTMPDIR": "/var/tmp/r.2yw1ach7",
    "TMPDIR": "/var/tmp/r.2yw1ach7",
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2"
  },
  "exit": 0,
  "seconds": 2.5849825170007534
}
```
Full stdout:
```text
ok  	github.com/iryzhkov/t3-steward/internal/daemon	0.025s
```
Full stderr:
```text
```
```json
{
  "argv": [
    "go",
    "test",
    "./internal/daemon",
    "./internal/wait",
    "./internal/config",
    "./internal/domain",
    "./internal/workerruntime",
    "./internal/backlog",
    "./cmd/t3-steward",
    "-run",
    "Test.*(Message|Wake|Template|Quota|Prompt|Models|Pause|Completion)",
    "-count=1"
  ],
  "env": {
    "GOTMPDIR": "/var/tmp/r.2yw1ach7",
    "TMPDIR": "/var/tmp/r.2yw1ach7",
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2"
  },
  "exit": 0,
  "seconds": 13.779851868050173
}
```
Full stdout:
```text
ok  	github.com/iryzhkov/t3-steward/internal/daemon	0.165s
ok  	github.com/iryzhkov/t3-steward/internal/wait	0.003s
ok  	github.com/iryzhkov/t3-steward/internal/config	0.012s
ok  	github.com/iryzhkov/t3-steward/internal/domain	0.003s
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	2.908s
ok  	github.com/iryzhkov/t3-steward/internal/backlog	0.311s
ok  	github.com/iryzhkov/t3-steward/cmd/t3-steward	1.544s
```
Full stderr:
```text
```

Additional composed-input boundary check:
```json
{
  "argv": [
    "go",
    "test",
    "./internal/workerruntime",
    "-run",
    "^TestCreateThreadRefusesAFirstTurnOverT3sInputLimit$",
    "-count=1",
    "-v"
  ],
  "env": {
    "GOTMPDIR": "/var/tmp/r.22jpfyzf",
    "TMPDIR": "/var/tmp/r.22jpfyzf",
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2"
  },
  "exit": 0,
  "seconds": 0.44148051604861394
}
```
Full stdout:
```text
=== RUN   TestCreateThreadRefusesAFirstTurnOverT3sInputLimit
=== RUN   TestCreateThreadRefusesAFirstTurnOverT3sInputLimit/plain
=== RUN   TestCreateThreadRefusesAFirstTurnOverT3sInputLimit/recovery
=== RUN   TestCreateThreadRefusesAFirstTurnOverT3sInputLimit/preflight
--- PASS: TestCreateThreadRefusesAFirstTurnOverT3sInputLimit (0.06s)
    --- PASS: TestCreateThreadRefusesAFirstTurnOverT3sInputLimit/plain (0.02s)
    --- PASS: TestCreateThreadRefusesAFirstTurnOverT3sInputLimit/recovery (0.02s)
    --- PASS: TestCreateThreadRefusesAFirstTurnOverT3sInputLimit/preflight (0.02s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	0.064s
```
Full stderr:
```text
```

## Full portable probe sources and actual results

Sources use only existing fixtures at the exact reviewed commit. Huyang format=false preserved these exact executed bytes. No production files changed. Recreate each at its indicated package path, then run the exact corresponding command. Runtime R1 is expected to fail at this candidate; the other controls pass.

### internal/workerruntime/independent_message_fix1_astra_test.go

```go
package workerruntime

import (
 "context"
 "strings"
 "testing"
 "time"
 "github.com/iryzhkov/t3-steward/internal/backlog"
 "github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentMessageFix1ResetProbeReason(t *testing.T) {
 now := runtimeTestNow
 f := newRecoveryFixture(t, &now)
 st := f.stoppedBucket(t, 97, now.Add(-time.Minute))
 runtime, driver := f.pausedRuntime(t, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
 now = st.ResetsAt.Add(f.cfg.Resume.ProbeAfterReset.D()+f.cfg.Resume.ResetSettleDelay.D()+time.Second)
 renewLease(t, runtime, now.Add(time.Hour))
 if err := runtime.Reconcile(context.Background()); err != nil { t.Fatal(err) }
 after := f.bucket(t)
 if driver.resumeCalls != 1 || len(driver.resumeReasons) != 1 { t.Fatalf("reset probe did not resume: %d %v", driver.resumeCalls, driver.resumeReasons) }
 if after.Phase != domain.PhaseStopped || after.RecoveredAt != nil || !after.ObservedAt.Equal(st.ObservedAt) { t.Fatalf("fixture unexpectedly recovered: %+v", after) }
 control := &recordingT3{thread: &domain.Thread{ID:"thread-1"}}
 local := &LocalDriver{T3:control}
 pkg:=testPackage();pkg.Identity.ThreadID="thread-1"
 if err:=local.Resume(context.Background(),pkg,domain.ThrottleCommand{Reason:driver.resumeReasons[0]});err!=nil {t.Fatal(err)}
 t.Logf("phase=%s used=%.0f recoveredAt=%v observedUnchanged=%v resumeCalls=%d reason=%q prompt=%q",after.Phase,after.UsedPercent,after.RecoveredAt,after.ObservedAt.Equal(st.ObservedAt),driver.resumeCalls,driver.resumeReasons[0],control.resumes[0])
 if strings.Contains(driver.resumeReasons[0],"recovered") || !strings.Contains(driver.resumeReasons[0],"probe") {t.Error("elapsed-reset telemetry probe falsely described as confirmed recovery")}
}
```

### internal/config/independent_message_fix1_astra_test.go

```go
package config

import (
 "bytes"
 "os"
 "path/filepath"
 "strings"
 "testing"
 "gopkg.in/yaml.v3"
)

func TestIndependentMessageFix1LoadedMigration(t *testing.T) {
 for _, tc := range []struct{name,prefix,suffix string; migrate bool}{
  {"exact","","",true},{"yaml-newline","","\n",true},
  {"leading-space"," ","",false},{"double-newline","","\n\n",false},
  {"trailing-space",""," ",false},{"crlf","","\r\n",false},{"custom",""," custom",false},
 } { t.Run(tc.name,func(t *testing.T){
  drain:=tc.prefix+previousDefaultDrainMessage+tc.suffix
  resume:=tc.prefix+previousDefaultResumePrompt+tc.suffix
  warn:=" custom warning {{.UsedPercent}} \n"
  b,err:=yaml.Marshal(map[string]any{"messages":map[string]string{"warn":warn,"drain":drain},"resume":map[string]string{"prompt":resume}})
  if err!=nil {t.Fatal(err)}
  p:=filepath.Join(t.TempDir(),"config.yaml");if err:=os.WriteFile(p,b,0600);err!=nil {t.Fatal(err)}
  c,err:=loadFile(p);if err!=nil {t.Fatal(err)}
  if tc.migrate {drain=DefaultDrainMessage;resume=DefaultResumePrompt}
  if c.Messages.Drain!=drain || c.Resume.Prompt!=resume || c.Messages.Warn!=warn {t.Fatal("loaded message bytes changed outside exact migration")}
  after,err:=os.ReadFile(p);if err!=nil || !bytes.Equal(after,b) {t.Fatal("configuration rewritten")}
  t.Logf("exact migration=%v custom/warn/file bytes preserved",tc.migrate)
 })}
 p:=filepath.Join(t.TempDir(),"sample.yaml");if err:=os.WriteFile(p,[]byte(Sample()),0600);err!=nil {t.Fatal(err)}
 c,err:=loadFile(p);if err!=nil {t.Fatal(err)}
 if strings.TrimSuffix(c.Messages.Drain,"\n")!=DefaultDrainMessage || c.Resume.Prompt!=DefaultResumePrompt {t.Fatal("sample/default mismatch")}
}
```

### internal/wait/independent_message_fix1_astra_test.go

```go
package wait

import (
 "context"
 "strings"
 "testing"
 "time"
 "github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentMessageFix1NativeFailureWake(t *testing.T) {
 now:=time.Date(2030,1,1,0,0,0,0,time.UTC)
 for _, progress:=range []domain.ProgressState{domain.ProgressFailed,domain.ProgressCancelled} {
  store:=&nativeMemory{w:domain.NodeWait{Request:domain.NodeWaitRequest{ID:"nw-review",ThreadID:"thread",Target:domain.NodeRef{RunID:"run-review",TaskID:"sink"}},Host:"host",SettledAt:&now,Observation:&domain.NodeObservation{Target:domain.NodeRef{RunID:"run-review",TaskID:"sink"},Progress:progress,Outcome:domain.TaskWaitMet,ExitCode:0,Reason:"terminal"},DeliveryID:"token",Delivery:"pending"}}
  control:=&nativeControl{fail:true};runner:=New(store,control,nil);runner.NodeHost="host"
  runner.Tick(context.Background(),nil,nil)
  if control.sends!=1 || store.w.Delivery!="recovery-required" {t.Fatal("lost-response control failed")}
  sent:=control.texts[0]
  for _,want:=range []string{"outcome=met","progress="+string(progress),"task result run-review","exit 0 does not establish task success or review ACCEPT","Cancellation and pause instructions take precedence"} {
   if !strings.Contains(sent,want) {t.Fatalf("missing %q in %q",want,sent)}
  }
  // A fresh runner must not resend on an absent, inconclusive receipt.
  runner=New(store,control,nil);runner.NodeHost="host";runner.Tick(context.Background(),nil,nil)
  if control.sends!=1 {t.Fatal("unknown receipt resent")}
  control.seen=true;runner.Tick(context.Background(),nil,nil)
  if control.sends!=1 || store.w.Delivery!="delivered" {t.Fatal("confirmed receipt not settled once")}
  t.Logf("terminal %s remains met; actual result/verdict required; lost response recovered without resend",progress)
 }
 for _,id:=range []string{"a",strings.Repeat("a",128),"A0._-"} {
  w:=domain.NodeWait{Observation:&domain.NodeObservation{Target:domain.NodeRef{RunID:id},Progress:domain.ProgressSucceeded}}
  if !strings.Contains(nodeWakeResult(w),"task result "+id+"`") {t.Fatal("valid boundary identity omitted")}
 }
 for _,id:=range []string{strings.Repeat("a",129),"a/b","a b","a\x00b","aé","a`b","-x"} {
  w:=domain.NodeWait{Observation:&domain.NodeObservation{Target:domain.NodeRef{RunID:id},Progress:domain.ProgressFailed}}
  if nodeWakeResult(w)!="" {t.Fatal("unsafe or oversized identity rendered")}
 }
}
```

R1 actual receipt:
```json
{
  "argv": [
    "go",
    "test",
    "./internal/workerruntime",
    "-run",
    "^TestIndependentMessageFix1ResetProbeReason$",
    "-count=1",
    "-v"
  ],
  "env": {
    "GOTMPDIR": "/var/tmp/r.xk356c9f",
    "TMPDIR": "/var/tmp/r.xk356c9f",
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2"
  },
  "exit": 1,
  "seconds": 2.203606585972011
}
```
Full stdout:
```text
=== RUN   TestIndependentMessageFix1ResetProbeReason
2026/10/05 11:27:06 WARN quota watchdog pauses an owned attempt component=worker-runtime assignment=assignment-1 thread=thread-1 kind=drain bucket=codex/codex/primary phase=stopped used=97%
2026/10/05 11:27:06 INFO quota resume permitted; owned attempt resumed component=worker-runtime assignment=assignment-1 thread=thread-1 reason="codex/codex/primary recovered"
    independent_message_fix1_astra_test.go:27: phase=stopped used=97 recoveredAt=<nil> observedUnchanged=true resumeCalls=1 reason="quota resume permitted: codex/codex/primary recovered" prompt="Inspect current instructions and reconcile the workspace and retained checkpoint first. Continue only unfinished work that is still authorized; cancellation and pause instructions take precedence. Resume permission: quota resume permitted: codex/codex/primary recovered"
    independent_message_fix1_astra_test.go:28: elapsed-reset telemetry probe falsely described as confirmed recovery
--- FAIL: TestIndependentMessageFix1ResetProbeReason (0.12s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.123s
FAIL
```
Full stderr:
```text
```

Migration and native-wake controls actual receipt:
```json
{
  "argv": [
    "go",
    "test",
    "./internal/config",
    "./internal/wait",
    "-run",
    "^TestIndependentMessageFix1",
    "-count=1",
    "-v"
  ],
  "env": {
    "GOTMPDIR": "/var/tmp/r.nfjfzbb6",
    "TMPDIR": "/var/tmp/r.nfjfzbb6",
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2"
  },
  "exit": 0,
  "seconds": 0.5612080389983021
}
```
Full stdout:
```text
=== RUN   TestIndependentMessageFix1LoadedMigration
=== RUN   TestIndependentMessageFix1LoadedMigration/exact
    independent_message_fix1_astra_test.go:28: exact migration=true custom/warn/file bytes preserved
=== RUN   TestIndependentMessageFix1LoadedMigration/yaml-newline
    independent_message_fix1_astra_test.go:28: exact migration=true custom/warn/file bytes preserved
=== RUN   TestIndependentMessageFix1LoadedMigration/leading-space
    independent_message_fix1_astra_test.go:28: exact migration=false custom/warn/file bytes preserved
=== RUN   TestIndependentMessageFix1LoadedMigration/double-newline
    independent_message_fix1_astra_test.go:28: exact migration=false custom/warn/file bytes preserved
=== RUN   TestIndependentMessageFix1LoadedMigration/trailing-space
    independent_message_fix1_astra_test.go:28: exact migration=false custom/warn/file bytes preserved
=== RUN   TestIndependentMessageFix1LoadedMigration/crlf
    independent_message_fix1_astra_test.go:28: exact migration=false custom/warn/file bytes preserved
=== RUN   TestIndependentMessageFix1LoadedMigration/custom
    independent_message_fix1_astra_test.go:28: exact migration=false custom/warn/file bytes preserved
--- PASS: TestIndependentMessageFix1LoadedMigration (0.00s)
    --- PASS: TestIndependentMessageFix1LoadedMigration/exact (0.00s)
    --- PASS: TestIndependentMessageFix1LoadedMigration/yaml-newline (0.00s)
    --- PASS: TestIndependentMessageFix1LoadedMigration/leading-space (0.00s)
    --- PASS: TestIndependentMessageFix1LoadedMigration/double-newline (0.00s)
    --- PASS: TestIndependentMessageFix1LoadedMigration/trailing-space (0.00s)
    --- PASS: TestIndependentMessageFix1LoadedMigration/crlf (0.00s)
    --- PASS: TestIndependentMessageFix1LoadedMigration/custom (0.00s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/config	0.006s
=== RUN   TestIndependentMessageFix1NativeFailureWake
    independent_message_fix1_astra_test.go:27: terminal failed remains met; actual result/verdict required; lost response recovered without resend
    independent_message_fix1_astra_test.go:27: terminal cancelled remains met; actual result/verdict required; lost response recovered without resend
--- PASS: TestIndependentMessageFix1NativeFailureWake (0.00s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/wait	0.002s
```
Full stderr:
```text
```

## Cleanup and limits

All three temporary sources removed by exact-revision Huyang delete_file in receipt req_12242, evidence ev_bdabe2872036983f6b56634b62ba2ccf. Complete source is retained above. Temporary Go directories removed after each completed command. No production edits/commits. Git diff HEAD, cached diff and parent diff --check all exit0 after removal; physical exact HEAD/tree/sole parent unchanged. Only .t3 and declared review/continuation outputs remain untracked. Final size/process/source checks recorded in continuation.md. The first hygiene script exited1 because a broad Python-executable filter included the harness's preexisting ov-memory/server.py MCP process (PID2467602); inspected exact argv/start time and excluded that infrastructure service. It was not started by this review and was not stopped. Corrected task-command filtering found no own pending checks; the prior complete1454-path comparison had already passed.

This is one independent CHANGES REQUESTED review; BOTH independent accepts remain required before lead acceptance of a repaired candidate. No self-acceptance, sibling reconciliation, whole M16/M17 completion, restart/diversity qualification or live Claude incident resolution. Collection repair3 was not reviewed and no large/small collection probes are claimed. Synthetic local tests do not establish live provider capacity or recovery; cross-provider production qualification remains pending. Publishing deferred; schedules disabled/unchanged. No push/PR/tag/release/UpKeeper/CI/fleet/config/trust/admin/service/provider/schedule effects. Costs/token totals unavailable.
