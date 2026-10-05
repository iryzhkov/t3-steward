# Independent message-fix1 review — CHANGES REQUESTED

Verdict: CHANGES REQUESTED for exact commit eb55e1be9c3b5b7656d3bce8641dda5541913c73.
Tree: 337ef3f061617a0a3403add0d1fcea9cd5a1325b.
Sole parent: 63843b4c02a77ba2ce6342e664d89dea9d358416.

Reviewer: Codex GPT-6.1-Sol, medium. Authority: jocasta:18633459f8d04a0c33d383c8796cb710@1 and FULL followup-plan.md. Review ONLY message-fix1; no delegation, nested review, escalation, sibling current review consultation/import, production edits or external effects. BOTH independent accepts remain required; this is one review, not whole M16/M17 acceptance.

## Deduplicated finding

R1 — P2 — elapsed-reset telemetry probe still tells the owned agent that quota recovered.
Primary changed propagation sites: internal/workerruntime/local_pause.go:322 and :327; internal/workerruntime/local_driver.go:1282.
Root reason producer: internal/workerruntime/quota_guard.go:152–159; internal/daemon/resume_policy.go:56–62 and :109–112.

HostQuotaGuard.ResumeAllowed passes ProbeWindowOpen to BucketsRecovered. After the reset/probe/settle deadline, that callback may accept the still-stopped bucket without a new observation or RecoveredAt. ApplicableHealthy skips the expired window. The resulting allowed/empty reason is then labeled "<bucket> recovered". The new neutral outer prefix faithfully forwards that false inner assertion into both the runtime log and LocalDriver resume prompt. The existing new tests cover fresh recovery, stage-2 stopped-below-threshold probes and disabled checks, but omit this separate elapsed-reset probe route.

Portable reproduction below uses the real HostQuotaGuard, in-memory SQLite fixture, Runtime.Reconcile and LocalDriver.Resume with existing fake transport. Pause at synthetic97%, advance only the clock past reset+probe+settle, renew the ordinary fixture lease, and reconcile. No provider call. Actual stopped phase and RecoveredAt=nil remain, but the sent prompt ends:
"Resume permission: quota resume permitted: codex/codex/primary recovered".
Probe fails with actual Go exit1. This is a remaining F6 contract failure inherited by this exact candidate, not a regression introduced by the six-line assertion repair. The original scope explicitly requires telemetry probes to be distinguished from confirmed recovery; passing its stale-assertion repair does not satisfy that boundary.

Required fix: give the actual elapsed-reset probe path a truthful permission/probe reason and reserve recovered wording for authentic confirmed recovery, without changing allowed decisions, timing, phases, thresholds, collector or resume policy. Preserve the actual reason through the existing neutral prompt/log. Retain this full probe as a permanent regression with fresh-recovery, stage-2-probe, disabled, incomplete/completed drain and cancellation controls. Do not infer reason kind by arbitrary free-text classification. Revalidate affected checks and the required final gate after an authorized repair.
Limitations: synthetic local runtime evidence, not a live-provider incident diagnosis, freshness redesign or request to weaken the existing probe policy.

No additional P0/P1/P2 finding. No claim that the producer final gate failed: it passed, as verified below.

## Inputs, provenance and source custody

Listed .t3/dependencies (symlink to external dependencies): ONLY task-cfe3170a2c879404b6f6cf50faa97f6e exists. Its six files are repair (campaign-commit/v1) and the five declared outputs continuation.md, handoff.md, repair-contract.md, repair.bundle, verification.log.xz. Task identity was discovered from the directory and repair metadata, never guessed.

repair declares workflowRunId run-95490ef422068db15daf2cfb571424a1, taskId task-cfe3170a2c879404b6f6cf50faa97f6e, name repair, repository git@github.com:iryzhkov/t3-steward.git, base6b0a6798736d3c9277e7688ef3016c690bf7a9ee, commit eb55e1b, ref refs/campaigns/run-95490ef422068db15daf2cfb571424a1/task-cfe3170a2c879404b6f6cf50faa97f6e/repair, createdAt2026-10-05T18:23:01.542719705Z. Metadata SHA256e002d0b99c817cedd4618a9381194bf096efe3190618bcb038357c2e7f82c295.

Full producer continuation, handoff and contract read. Full followup plan equals Jocasta content hash46dbd9f3afbd5b232bac2aa54bf71e89de9694f879bcf73636ce1152cf2fa384. Full original message-repair-plan, message-handoff/contract, audit, quota-message-audit-plan, user-requirements, profile-handoff/contract and BOTH original accepted-profile reports read in bounded full followups. Those earlier reviews accept0131 only and are original inputs, not consultation with this review's sibling. No collection reports/bundle imported or consulted. All13 relevant original input size/hash pins independently match input-inventory.json. Historical logs remain historical, not fresh passes.

Five producer artifact bytes/SHA256:
- continuation.md1846 / fcf093ff3b7db7561d4a996ddd1b2916b24bab98650a4a63fec08f4149577587
- handoff.md7545 / 44d26d039a16e3f800d66502410a4a3af4dac715811996cb655aa73ade1f9b63
- repair-contract.md16275 / 09a4f3d7482297445ac27cbf43756f8e02c813fe49bab7fbb6a6b8ec9e1951a2
- repair.bundle540727 / 222c97b183ff262d4cd90c8877db3e4089eeab45d7e01adcd731a031c347e212
- verification.log.xz410816 / 4ce707ad0cba5688e6e35ec91a06bf2c0752c527351f98eacc5e05265671865f

Initial physical HEAD6b0a679, tracked/index clean. Only declared repair.bundle verified/fetched (sole prerequisite6b0a, sole exported HEADeb55e1b); detached actual physical HEAD/tree/sole parent match the above exact pins. The parent's original message changes are examined via this sole imported ancestry, not a second source import. No sibling merge. Root repair-contract is tracked historical evidence: its complete12870-byte parent prefix is unchanged; only current appendix added.

Huyang workspace ws_6692bfcec51f313f223d3bfbc3adf29e opened before source reads. A preliminary guessed metadata filename read was refused through the external input symlink; subsequent directory listing discovered the actual repair artifact. External artifact reads used explicit shell authorization; repository source and disposable overlays used Huyang. Trust remains untrusted/unchanged; no Huyang isolated-verification or full-LSP-success claim. Foreground Git/Go/external-artifact commands are expressly authorized.

## Independent source assessment

Exact repair diff has only ownership_test.go comment/assertion and the append-only contract. The test still feeds actual harness snapshots78.4/82.2/86/86.3, requires warn:a then drain:a, zero stops, measured3.8%/min, about4m projection and qualified changeable behavior, and rejects the old provider-session guarantee. It does not change policy to fit prose.

Independently inspected all F1/F6/F7/F9 production diffs from0131, associated six new test files, weekly quota assertion, collect-once strengthening and unchanged invariant boundaries:
- F1 DefaultDrainMessage/config Sample/example agree; Engine.Tick has below-stop/no-imminent, near-reset and runway no-stop branches plus threshold/imminent stop. New controls use actual Tick, not merely matching literals. exhaustionNote is qualified projection; unknown burn yields no note. Warning remains advisory.
- Exact previous drain/resume literals match their prior shipped bytes. Migration recognizes only exact prior default or one YAML block newline; older legacy migration is unchanged. Independent loadFile/YAML roundtrip verifies exact-default migration, custom whitespace/two-newline/CRLF/custom-suffix preservation and no on-disk rewrite.
- F7 node/shell/task-bound text requires outcome/result/verdict inspection, cancellation/pause precedence and authorized unfinished work. Met/exit0 never establishes success/ACCEPT. Actual shell fake Runner REJECT/ACCEPT exit0 controls, terminalfailed=met, mixed membership/trailer, expected/unexpected deadline and unanswered-ask exception remain. Terminal run command is bounded ASCII-safe; independent inclusive128/exclusive129, punctuation/control/Unicode cases pass. Quota wake never emits that command and requires authentic fresh eligibility. Machine trailer/delivery/receipt paths are unchanged.
- F9 TaskCompletionSupplement and LocalDriver checkpoint describe runtime-owned pause; exact done/continue markers remain. QuotaPauseCompleted retains exact drained turn, explicit final done, matching exported latest turn and structured completion gate. Focused controls exercise completed collect once/no resume, incomplete pause, different-turn/pending-input refusal and cancellation. No collector/transition/receipt mutation in this repair.
- F6 ordinary fresh-recovery, stage-2-probe and disabled controls pass; elapsed-reset route R1 above remains misleading.

## Complete producer evidence and closure audit

verification.log.xz fully decompresses to7548527bytes/114680lines/SHA25636acbe00cbcb05a5fbcd9d7361d632f0ecfe06964ce0feab16d5413febab3c24, matching the raw seal. Full meaningful command/failure/correction/gate chronology and retained run.py/fingerprint.py/proof.py read; repetitive inventories independently parsed/compared in full rather than sampled. Oversized deliveries were followed by bounded reads.

BEFORE affected test1/2.631s; AFTER0/0.580s; required seven-package selection0/13.491s. Gate1 actual-15/147.664s after intentional contract custody interruption; no pass. Contract prefix correction and affected revalidation0 follow. Gate2 actual2/332.982s: Unix socket path too long under nested temporary directory; build/vet pass, short suite fails, lint/race not reached. Ordinary resource correction to private0700 short /var/tmp/r.MNeKXD for both TMPDIR/GOTMPDIR; socket and affected tests0. Final gate make check-review FAST_BASE=63843b4c02a77ba2ce6342e664d89dea9d358416 actual0/414.322s: complete build/vet/short suite, pinned go1.25.0 staticcheckv0.7.0/gofmt, full daemon race29.228s. No source edit after that final gate; no reviewer full gate/race rerun.

Parsed ALL five full source/logical-index/tree fingerprints. First tree3b995615 is before contract custody correction. Remaining FOUR (final-pregate/corrected-pregate/postgate/final) compare exactly, tree337ef3f. Independently checked every one of1454 recorded Git blobs for size/SHA256 and every tracked physical worktree hash/index mode/OID against those complete records and exact final tree. Full logical index equals actual current git ls-files --stage -z. Raw index metadata byte equality is not claimed.

Parsed ALL SEVEN command-bound typed inventory blocks plus complete precommit10779-object inventory. Producer sourceALL10782 pre/post equal; all10779 precommit objects retained, only appended contract blob, final tree and repair commit added. Complete baseline typed closure9926 exact; source candidate/fresh candidate/freshALL10418 exact. All reachable IDs/type/size checked independently against imported source. Two outside-closure producer objects unavailable in this checkout: tree3b9956155a86b0577078b06ece5008a637e6a8be/tree1425 and fd308731cbdfcadbbfaa7ea99f56c8b23a4327d9/commit280. Their preservation is witnessed by internally equal producer inventories, not fabricated reviewer availability. SourceALL is never equated to reachable closure.

Independent EMPTY /var/tmp/r.VbLRhM/fresh.git seeded ONLY exact baseline6b0a, no alternates. Its ALL equals baseline closure9926, candidate absent BEFORE import, strict fsck0. Verified/fetched ONLY declared repair.bundle. Full fresh/source strict fsck0; exact candidate identity; complete fresh candidate closure=freshALL=source candidate closure10418, zero missing. Reviewer's source ALL unchanged across proof. Detailed command receipts retained locally /var/tmp/r.VbLRhM/fresh-receipts.json. These prove local closure/custody, not remote execution attestation.

## Independent actual commands and full portable probes

Private directory /var/tmp/r.VbLRhM created with mktemp -d /var/tmp/r.XXXXXX and chmod0700. BOTH GOTMPDIR/TMPDIR use that short directory; GOMAXPROCS=2 GOFLAGS=-p=2. All Go subprocesses run foreground and are awaited. Python wrappers recorded actual Go exits separately from wrapper exit; the failing portable command below is actual1, never a pass. No probe retry/assertion changes or full gate/race.

All source below is the exact executed unformatted portable source, created with Huyang format=false. Each runs against the existing exact-candidate fixture helpers. No production file modified.

Path: internal/config/independent_message_fix1_probe_test.go

````go
package config

import ("bytes"; "fmt"; "os"; "path/filepath"; "testing"; "gopkg.in/yaml.v3")

func TestIndependentMessageFix1MigrationBoundary(t *testing.T) {
 for _, suffix := range []string{"", "\n", "\n\n", " ", "\r\n", "\n custom"} {
  raw, err := yaml.Marshal(map[string]any{"messages":map[string]string{"drain":previousDefaultDrainMessage+suffix},"resume":map[string]string{"prompt":previousDefaultResumePrompt+suffix}})
  if err!=nil {t.Fatal(err)}
  path:=filepath.Join(t.TempDir(),"config.yaml");if err:=os.WriteFile(path,raw,0600);err!=nil {t.Fatal(err)}
  c,err:=loadFile(path);if err!=nil {t.Fatal(err)}
  wantDrain,wantResume:=previousDefaultDrainMessage+suffix,previousDefaultResumePrompt+suffix
  migrated:=suffix==""||suffix=="\n"
  if migrated {wantDrain,wantResume=DefaultDrainMessage,DefaultResumePrompt}
  if c.Messages.Drain!=wantDrain||c.Resume.Prompt!=wantResume {t.Fatalf("suffix=%q migration/custom mismatch",suffix)}
  after,err:=os.ReadFile(path);if err!=nil||!bytes.Equal(raw,after){t.Fatal("load rewrote operator bytes",err)}
  fmt.Printf("suffix=%q migrated=%v file-identical=true\n",suffix,migrated)
 }
}
````

Path: internal/wait/independent_message_fix1_probe_test.go

````go
package wait

import ("fmt"; "strings"; "testing"; "github.com/iryzhkov/t3-steward/internal/domain")
func TestIndependentMessageFix1ResultBoundary(t *testing.T) {
 for _, run:=range []string{"r", strings.Repeat("a",128), strings.Repeat("a",129), "run.a_b-9", "-bad","run;true","run\nnext","é","run`x","run$(x)"} {
  w:=domain.NodeWait{Observation:&domain.NodeObservation{Target:domain.NodeRef{RunID:run},Progress:domain.ProgressFailed}}
  got:=nodeWakeResult(w)
  want:=run=="r"||len(run)==128||run=="run.a_b-9"
  if (got!="")!=want {t.Fatalf("run=%q got=%q",run,got)}
  if want&&!strings.Contains(got,"t3-steward task result "+run+"`"){t.Fatal("wrong command")}
  fmt.Printf("run=%q command=%v\n",run,got!="")
 }
 for _, outcome:=range []domain.TaskWaitOutcome{domain.TaskWaitMet,domain.TaskWaitFailed,domain.TaskWaitCancelled,domain.TaskWaitGaveUp,domain.TaskWaitTimedOut} {
  w:=domain.NodeWait{Request:domain.NodeWaitRequest{ID:"quota",Name:"quota reset",Quota:&domain.QuotaWaitCondition{Pool:"pool"}},Observation:&domain.NodeObservation{Outcome:outcome,Reason:"elapsed reset",Progress:domain.ProgressFailed,Target:domain.NodeRef{RunID:"run-valid"}}}
  text:=nodeWakeProse(w)
  if strings.Contains(text,"task result")||!strings.Contains(text,"reset deadline alone does not confirm recovery")||!strings.Contains(text,"Cancellation and pause"){t.Fatal(text)}
  fmt.Printf("quota outcome=%s no-command fresh-eligibility-required=true\n",outcome)
 }
}
````

Path: internal/workerruntime/independent_message_fix1_probe_test.go

````go
package workerruntime

import ("context"; "fmt"; "strings"; "testing"; "time"; "github.com/iryzhkov/t3-steward/internal/backlog"; "github.com/iryzhkov/t3-steward/internal/domain")
func TestIndependentMessageFix1ElapsedResetProbe(t *testing.T) {
 now:=runtimeTestNow
 f:=newRecoveryFixture(t,&now)
 f.stoppedBucket(t,97,now.Add(-time.Minute))
 runtime,driver:=f.pausedRuntime(t,&now,backlog.DispatchThreadActive,backlog.DispatchThreadStopped,backlog.DispatchThreadStopped)
 // Keep the original stopped observation and phase. Advance only the clock.
 now=now.Add(3*time.Hour+f.cfg.Resume.ProbeAfterReset.D()+f.cfg.Resume.ResetSettleDelay.D()+time.Minute)
 renewLease(t,runtime,now.Add(time.Hour))
 if err:=runtime.Reconcile(context.Background());err!=nil {t.Fatal(err)}
 st:=f.bucket(t)
 if driver.resumeCalls!=1||len(driver.resumeReasons)!=1 {t.Fatalf("no allowed reset probe: resumes=%d",driver.resumeCalls)}
 reason:=driver.resumeReasons[0]
 fmt.Printf("actual phase=%s recoveredAt=%v observedAt=%s reason=%q\n",st.Phase,st.RecoveredAt,st.ObservedAt.Format(time.RFC3339),reason)
 control:=&recordingT3{thread:&domain.Thread{ID:"thread-1"}}
 local:=&LocalDriver{T3:control};pkg:=testPackage();pkg.Identity.ThreadID="thread-1"
 if err:=local.Resume(context.Background(),pkg,domain.ThrottleCommand{Reason:reason});err!=nil {t.Fatal(err)}
 fmt.Printf("actual prompt=%q\n",control.resumes[0])
 if st.Phase!=domain.PhaseStopped||st.RecoveredAt!=nil {t.Fatal("probe unexpectedly confirmed recovery")}
 if strings.Contains(control.resumes[0],"recovered") {t.Fatal("elapsed-reset telemetry probe falsely reported recovered")}
}
````

Actual argv/environment/exit:

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
    "GOTMPDIR": "/var/tmp/r.VbLRhM",
    "TMPDIR": "/var/tmp/r.VbLRhM",
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2"
  },
  "exit": 0
}
```

FULL stdout:

````text
ok  	github.com/iryzhkov/t3-steward/internal/daemon	0.024s
````

FULL stderr:

````text
````

Actual argv/environment/exit:

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
    "GOTMPDIR": "/var/tmp/r.VbLRhM",
    "TMPDIR": "/var/tmp/r.VbLRhM",
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2"
  },
  "exit": 0
}
```

FULL stdout:

````text
ok  	github.com/iryzhkov/t3-steward/internal/daemon	0.152s
ok  	github.com/iryzhkov/t3-steward/internal/wait	0.003s
ok  	github.com/iryzhkov/t3-steward/internal/config	0.012s
ok  	github.com/iryzhkov/t3-steward/internal/domain	0.003s
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	2.817s
ok  	github.com/iryzhkov/t3-steward/internal/backlog	0.353s
ok  	github.com/iryzhkov/t3-steward/cmd/t3-steward	1.473s
````

FULL stderr:

````text
````

Actual argv/environment/exit:

```json
{
  "argv": [
    "go",
    "test",
    "./internal/config",
    "./internal/wait",
    "./internal/workerruntime",
    "-run",
    "^TestIndependentMessageFix1",
    "-count=1",
    "-v"
  ],
  "env": {
    "GOTMPDIR": "/var/tmp/r.VbLRhM",
    "TMPDIR": "/var/tmp/r.VbLRhM",
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2"
  },
  "exit": 1
}
```

FULL stdout:

````text
=== RUN   TestIndependentMessageFix1MigrationBoundary
suffix="" migrated=true file-identical=true
suffix="\n" migrated=true file-identical=true
suffix="\n\n" migrated=false file-identical=true
suffix=" " migrated=false file-identical=true
suffix="\r\n" migrated=false file-identical=true
suffix="\n custom" migrated=false file-identical=true
--- PASS: TestIndependentMessageFix1MigrationBoundary (0.00s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/config	0.004s
=== RUN   TestIndependentMessageFix1ResultBoundary
run="r" command=true
run="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" command=true
run="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" command=false
run="run.a_b-9" command=true
run="-bad" command=false
run="run;true" command=false
run="run\nnext" command=false
run="é" command=false
run="run`x" command=false
run="run$(x)" command=false
quota outcome=met no-command fresh-eligibility-required=true
quota outcome=failed no-command fresh-eligibility-required=true
quota outcome=cancelled no-command fresh-eligibility-required=true
quota outcome=gave-up no-command fresh-eligibility-required=true
quota outcome=timed-out no-command fresh-eligibility-required=true
--- PASS: TestIndependentMessageFix1ResultBoundary (0.00s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/wait	0.002s
=== RUN   TestIndependentMessageFix1ElapsedResetProbe
2026/10/05 11:30:45 WARN quota watchdog pauses an owned attempt component=worker-runtime assignment=assignment-1 thread=thread-1 kind=drain bucket=codex/codex/primary phase=stopped used=97%
2026/10/05 11:30:45 INFO quota resume permitted; owned attempt resumed component=worker-runtime assignment=assignment-1 thread=thread-1 reason="codex/codex/primary recovered"
actual phase=stopped recoveredAt=<nil> observedAt=2026-09-10T19:59:00Z reason="quota resume permitted: codex/codex/primary recovered"
actual prompt="Inspect current instructions and reconcile the workspace and retained checkpoint first. Continue only unfinished work that is still authorized; cancellation and pause instructions take precedence. Resume permission: quota resume permitted: codex/codex/primary recovered"
    independent_message_fix1_probe_test.go:22: elapsed-reset telemetry probe falsely reported recovered
--- FAIL: TestIndependentMessageFix1ElapsedResetProbe (0.09s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.091s
FAIL
````

FULL stderr:

````text
````

## Cleanup and limits

All three temporary probe files removed through Huyang exact create-document revisions, receipts req_12254/req_12255/req_12256. Guarded removal leaves the original source unchanged. After removal git diff --exit-code HEAD, git diff --cached --exit-code and parent diff --check actual0. All foreground sessions completed; no own background checks started or pending Go/make/test processes. Only .t3 and declared continuation.md/review.md remain untracked. Final output size and hygiene checks passed after file creation; review.md about22KiB and continuation.md about1.3KiB, each prose<=256KiB. Exact HEAD/tree/sole parent and clean tracked/index state rechecked; /proc check found no Go/make/test process pending in this workspace.

This review requests changes for the single reproduced F6 prose-contract failure, despite the valid passing producer gate and successful focused controls. No collection review/recovery probe is claimed: this task is message-fix1 only. No live availability/session recovery, M16/M17 completion, restart/diversity/two-provider production qualification or deployment claim. Original unrelated audit F2/F3/F4/F5/F8 and provider incident remain separately scoped. BOTH independent ACCEPTs remain necessary before lead acceptance; this reviewer never self-accepts producer work. Publishing deferred, schedules disabled/unchanged; no push/PR/tag/release/UpKeeper/CI/fleet/live config/trust/admin/service/provider controls. Usage/cost unavailable.
