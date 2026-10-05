# Bounded quota/message audit

Status: complete read-only inventory for lead digestion, not a repair or profile acceptance.
Authority: jocasta:8258fdfc3ead71ed7fffc383034c0a3f@1; FULL immutable quota-message-audit-plan.md (6253 bytes, 17 lines) and user-requirements.md (1610 bytes, 8 lines) read through Huyang documents workspace. No Jocasta update was made.

## Snapshot and scope

Initial HEAD: 6b0a6798736d3c9277e7688ef3016c690bf7a9ee.
ONLY imported bundle: .t3/inputs/inputs/profile-candidate.bundle, 503439 bytes,
SHA256 93a67557ae81bbf015b31ed03f5d21d5f288ea207f913f185b430f4fc7f54062.
git bundle verify: okay; sole prerequisite 6b0a6798736d3c9277e7688ef3016c690bf7a9ee; sole export HEAD 0131f87946b24b4fc4427662d29ed3eb26a62137.
Fetched objects, checked commit/tree/sole parent BEFORE detaching:
0131f87946b24b4fc4427662d29ed3eb26a62137 / 7367723c0c6e010fd9be2ac3e9825a4d1123649b / 7751d7c29c1b4e2d74a44245d257717904a5b9a2.
No mismatch. Detached exact commit. Bundle/import is provisional evidence, not collection/profile acceptance.

All source references below name that pinned commit. Huyang project workspace ws_4c51e7021eb0ecf9b72626ab7b733f9b; reads progressed from wsrev_34 through wsrev_40 as checkpoint/probe artifacts changed. Source production files were unchanged. Huyang initially refused relative root and input symlink traversal; corrected absolute root and opened the real immutable input directory as documents, without bypassing its guards.

Baseline comparison: Git diff initial6b0a..0131 is empty for daemon messages/actions/resume policy, quota config/migration, wait node/wait/trailer/task, domain quota_wait/node/task_wait, policy, quota admission, first-turn prompt, host quota guard/local pause, models + per-window helper, campaign help/notify and task-run. LocalDriver differs only at line817 (session display title); submission.go differs in validated review-manifest admission receipt plumbing, not an implemented default fresh-quota refusal. These message/quota findings therefore predate the provisional profile snapshot. Initial6b0a is the supplied baseline; no deployed binary/service identity was independently inspected, so this is not a deployment attestation.

One prescribed focused test command and one specific additional probe only; no full gate/race. No live messages, provider sessions/calls, collector refresh, quota rearm/override, fabricated reading, quota/config/trust/admin/service changes, schedule action, CI, push/PR/tag/release/UpKeeper/fleet effect, delegation or nested review.

## Prioritized inventory and minimum acceptance

P1 means repair before relying on the affected promise as an execution decision; P2 means clarity/reliability hardening. Static proof establishes the code path, not a reproduced live incident.

| ID / milestone / priority | Classification and evidence | Proposed repair and minimum portable acceptance |
| --- | --- | --- |
| F1 M16 P1 | Confirmed prose/policy mismatch, reproduced in probe. DefaultDrainMessage config.go:836 promises “will interrupt any still-running turn in {{.GracePeriod}}”. policy.go:477-512 can clear DrainDeadline with NO stop when usage is below StopPercent and no imminent exhaustion, or reset/runway exemptions apply. Probe at90%, stop95%, deadline expired => draining/actions0. daemon/actions.go:99-106 renders this promise. | Render the actual conditional stop policy (“checkpoint and end this turn; a hard stop may follow if…”), or a truthful action-specific deadline. Table-test drain below stop, imminent exhaustion, stop threshold, near reset, long runway, and disabled control; assert prose matches decision without changing policy to fit prose. Preserve custom templates. |
| F2 M17 P1 | Confirmed reporting predicate defect, source proof (CLI tests blocked). models.go:775-787 returns “available” without testing Stale, QuotaUnknown, governing-window absence, phase or any worker Ready. scopeModelsDocument:497 uses that same status for --available; output:655-659 places it beside stale/draining/0 ready. Admission-open is not dispatch-eligible. | Define whether status means advertised or immediately eligible. Reserve “available” for fresh complete applicable quota + authorized route + ready eligible worker + current admission; show explicit unknown/stale/blocker otherwise. Test open+stale/draining, unknown, missing window, zero-ready, no binding, and fresh valid route; --available and displayed status must agree. Readiness must still not promise task-specific headroom without an estimate. |
| F3 M17 P1 | Confirmed quota-wait completeness/freshness gap, probe. quota_wait.go:83-105 counts present matched buckets; Evaluate:129-142 checks only aggregate percent/phase, no age and no comparison to all pool.Buckets. Pool declares short+weekly; only stale short7% present => below50 met although “every bucket” contract at17-18 is unsupported. | Fresh all-window conditions need explicit expected keys, age/clock checks and missing-window pending reasons; tests missing weekly, stale short, future timestamp, both fresh, account mismatch and changed selection. Distinguish an elapsed-reset timer from authentic recovered eligibility. Do not make a settled timer authorize a session. |
| F4 M16 P2 | Confirmed misleading aggregate pairing, probe. quota_wait.go:94-102 independently takes max usage97%, earliest reset (expired short), newest observedAt. models.go:376-380 uses percent/reset and a separately computed oldest time. QuotaTrailerFields:155-164 has no observedAt/freshness/window provenance. These fields need not describe the same bucket. | Label independent aggregates, or tie each percentage to its governing window/reset/time. Per-window output already exists (models_pool_observations.go:29-51). Test short7%/expired and weekly97%/future; do not imply97% will reset at the short-window date. Include observedAt, stale/unknown and exact account/window identities where actionable. |
| F5 M16 P1 | Confirmed notice-loss path by source proof, not exercised live. daemon/actions.go:109 marks notice before ControlAllowed:119 and WarnThread:124. store.go:690-695 INSERT OR IGNORE dedupes thread/bucket/epoch/kind. Known no-effect failure or disabled control consumes the only notice; subsequent same-epoch calls skip it. Drain intent saved only after successful send:129-134. | Separate claim/attempt/confirmed delivery; retain dedupe on uncertain ack but permit explicit known-no-effect retry. Fake-store/control tests: disabled then enabled, known rejected/no effect, lost ack, restart after claim, successful once. Never resend solely because bounded receipt search is empty. Treat operator custom messages identically. |
| F6 M16 P2 | Confirmed recovery-label overstatement by source proof. quota_guard.go:134-168 can allow a telemetry probe or quota_checks disabled, not confirmed recovery; local_pause.go:321-327 prefixes any allowed reason with “quota recovered”. LocalDriver.Resume:1282 transmits it as Throttle recovery. Stopped-phase probe writes only ProbedAt, explicitly does not change phase (quota_guard:181-188). | Distinct reason kinds: confirmed recovery, single telemetry probe, checks disabled. Message must say what remains unknown, identity and automatic next check. Fake guard/driver tests assert no “quota recovered” on probe/disabled; confirmed fresh recovery must use that wording. No admin bypass and no new probe policy authorized here. |
| F7 M16 P2 | Confirmed unconditional next-action wording, probe/static. nodeWakeProse:74-80 says Continue even for failed/cancelled/timeout; shell WakeMessage:437 same for gave-up; TaskWaitWakeContext.Prompt:535 says Continue for ordinary failed waits (ask failure has explicit exception:526-533). Outcomes themselves remain accurate; “Wait finished” is not inherently a false success. | Outcome-aware “inspect result/current instructions; continue only if still authorized; repair/replan/report for failure” language. Include terminal run result command in prose, not solely trailer. Test met/failed/cancelled/gave-up/deadline/expected timeout, terminal-but-failed node, review REJECT and ACCEPT with process exit0. Cancellation/pause must take precedence. |
| F8 M16 P2 | Confirmed formatting/control-text gap, probe. shell WakeMessage:431-435 inserts name/condition/output into Markdown; command output is tailed to4000 atwait.go:302, but embedded triple backticks escape the literal output block and ESC survives. domain/task_wait.go:519-520 emits output bare. | Bound assembled message and fields; safely delimit quoted data, normalize terminal controls, redact only by an explicit data policy. Tests multiline names, backticks, long group/output, UTF16 boundaries, control chars and secrets-shaped fixtures. This proves presentation leakage only; NO unsafe interpreter/injection exploit or live secret disclosure was established. |
| F9 M16 P2 | Confirmed instruction tension by static proof, not runtime failure. first_turn_prompt.go:25-28 prohibits BACKLOG STATUS: continue and says end=no wait means completion; LocalDriver.Checkpoint:1206 explicitly asks incomplete work to emit backlog status: continue and end. local_pause.go:13-20 establishes a distinct owned quota-pause lifecycle, but checkpoint text does not explain that exception. | State explicitly that quota-owned pause is handled by runtime and that the checkpoint marker is evidence, not a request for extra turns; unify ordinary completion and pause-specific contracts. Fake LocalDriver checkpoint + completion fixture should prove incomplete drain stays paused, completed turn collects once, cancellation never resumes; text must not leave the agent to infer the exception. Do not remove completion markers blindly. |

Existing current behavior / separately requested future contracts:
- R1 M17: default creation refusal on insufficient AUTHENTIC fresh spendable headroom. Current help explicitly says quota-closed is temporary and submission proceeds (campaign/help.go:323-348; task_run.go:87-93). SubmissionService applies permanent validation and returns accepted replay before new validation refusal (submission.go:166-179). This is a requested API/intent contract change, not evidence that existing documented accepted_waiting semantics accidentally malfunction. Explicit deferred opt-in may preserve intent, but must create no provider session. Minimum: fresh short+weekly budget/reserves/estimate check before new run acceptance; refusal receipt states blocker/time/identity and no run/session; opted-in deferred receipt distinguishes intent/dispatch; stable replay of an already accepted idempotency key must not fork or pretend to be a new fresh acceptance.
- R2 M17: near-weekly-reset preference is requested routing/pacing work. Use comparable normalized spendable budget minus forecast, active/paused remainder, committed + batch reservations and safety; divide by remaining usable time only after every hard window/runway gate. Never route Claude solely because Oct8 precedes CodexOct9. Test both-window gates, concurrent competing work, uncertain timestamps, reserve, estimate, deadline and equal-score deterministic fallback.
- R3 M16: bounded same-session transient network/reverse-proxy recovery is requested, not reproduced here (no incident example supplied). Separate quota pause, transport uncertainty, provider terminal error, cancellation and review failure. Proposed acceptance: authentic retryable evidence, bounded backoff/count/time, same session/effect identity, checkpoint/cancel precedence, no replay after uncertain successful completion; final message states actual terminal/recovery state. No collector/provider experiment is warranted from this audit.

## Observed discrepancy, with no root-cause attribution

User reports Claude refreshed and available. Lead input still records seven_day97%, observed2026-10-04T17:26:17.609Z/reset2026-10-08T16:00Z stale; five_hour7%, observed2026-10-04T06:13:45.558Z/reset2026-10-04T07:30Z stale; pool admission=open/phase=draining. Codex51% weekly/49% raw headroom fresh at16:44Z/resetOct9T21:30 is also INPUT evidence, not a reading collected here.
This is observed disagreement. Unknown whether collector, bridge, provider account/key, cached report or other path explains it. F2/F4 explain how contradictory facts can be displayed, not why an authentic refresh failed to arrive. Pure policy probe confirms a fresh same-window drop97->10 with unchanged future reset can rearm; the code does not universally require waiting for reset. Nothing here authorizes inventing10% as a real reading.

## Quota checklist coverage and limits

| Concern | Evidence/current behavior | Audit disposition / follow-up |
| --- | --- | --- |
| Missing/stale/out-of-order/future readings | policy.go:309-317 drops duplicate event ID, strictly older observation and expired window. quota_admission.go:172-179,217-224,240-249 blocks supplied stale/future windows and zero applied windows. MergeQuotaObservations quota_wait.go:193-237 freshest exact BucketKey wins; equal age retains existing. | Safeguards observed. F3 quota waits do not share those gates. Equal-time conflicting/new-event snapshots and future snapshots through pure policy/merge remain unproven risk; later test receipt precedence, not arbitrary timestamp overwrite. |
| Manual refresh within same window | policy.go:554-560 recognizes sufficiently low usage drop as extension; draining/stopped otherwise sticky:375-378. Probe fresh97->10 => normal/recovered/actions1. | Existing authentic recovery behavior, not universal defect. Small drops near threshold deliberately do not clear drain. Diagnose actual collector/bridge path only from pinned authentic evidence in a separate task. |
| Short+weekly governing windows | quota_admission applies every SUPPLIED matching pool window:157-216; domain matcher uses exact named keys or provider/account fallback:orchestrator.go:465-481. | F3/F4. Zero-window admission is blocked but “one supplied, another expected missing” completeness in full budget producer path not proved here; requires M17 acceptance at expected-key construction boundary. |
| Recovery versus stale readiness | models per-window stale marks expired reset and age:models_pool_observations.go:20-26; resume checks rearmed-after-stop, below threshold and settle delay:resume_policy.go:49-79, with explicit probe exceptions. | F2/F6. ApplicableHealthy:109-112 skips expired windows; normal healthy nonexpired readings are not age-gated there. Different admission/recovery policies must be explicitly reconciled; no live bypass tested. |
| Budget/forecast/concurrent reservations | quotaAvailable:285-292 subtracts current, interactive forecast, active, paused remainder, committed and safety; StartPlan copies private windows and batchReserved:107-114; Reserve:229-237; planner.go:271-273 reserves after proposal. | Current single-plan protection. Cross-plan transaction/reservation correctness and simultaneous consumption not proved by this audit; do NOT claim a concurrency race. Acceptance must check durable reservation revalidation atomically. |
| Mid-task watchdog and failure | local_pause:32-95 pauses only owned route, records request before effect, avoids repeated drain:58-63; attemptLive:254-264 enforces stop/claimed lease; reconcile:287-301 collects completed pause before resume and excludes collection. | Existing safety mechanisms. F9 text needs explicit lifecycle exception. Missing host guard never pauses; guard error keeps attempt running:36-39. This behavior is separate from creation admission and needs explicit M17 policy. |
| Ownership/provider/account | PoolBucketMatcher exact keys when resolved; legacy fallback instance+account:465-481; QuotaAdmission applies pool identity:279-283; host guard resolves routeThread provider/model. | No mismatch reproduced. Require exact account/window provenance in M17 key normalization and evidence. Legacy empty account is intentionally broad; do not infer wrong-account use without input. |
| Disabled quota checks | config.go:841-842 explicit flag; quota_admission:153-155 bypasses quota gates when disabled/admin force; HostQuotaGuard:103-104 does not pause,138-139 permits resume. | Existing operator policy, not exercised or changed. Report disabled distinctly; all other authorization/estimate/time constraints persist. No override authorized. |
| Eligibility before provider session, descendants/replay/refused ack | coordinator.go:166-174 declares admission applied after reconciliation and again before delivery; LocalDriver.CreateThread:728-778 route/env auth, preflight and composed limit precede CreateAndStartThread:815. Resume and child graphs have separate paths. | No proof all descendants/current provider-side boundary have atomic fresh quota protection. Require separately pinned full caller/ack acceptance (new-start/activation/resume/review child/replay/cancel), not rely solely on comments or absence of quota inside LocalDriver. No sessions started. |
| Raw percent vs spendable budget and near reset | modelsWindow.Headroom = max(0,100-used):37; normalized admission subtracts reserves separately. | “49% headroom” is raw display, not49% spendable task budget. R2 must integrate all hard gates/forecast, not use raw percent or earliest aggregate reset. |

## Message family / state / action matrix

Examples below are abbreviated existing literals or output from the disposable fixture; they are not live messages. Full probe stdout follows. “met” on a terminal wait means its condition settled, NOT task success; review process exit0 does NOT constitute ACCEPT. Consumers must read declared verdict/artifacts and separately pinned acceptance authority.

| Family | Source / direct caller | Durable state | Example rendered text | Correct next action / gap |
| --- | --- | --- | --- | --- |
| Interactive quota advisory | messages.go:23-43; config.go:833; daemon/actions.go:76-128 | bucket warned; notice thread/key/epoch/kind | T3 steward quota advisory: “weekly” is at97% and resets at… Continue the current user task and keep work focused… | Advisory does not request stop. Honor current cancellation/authorization. ObservedAt, account/window not in shipped template; F5 lost notice. Template fields include Provider and Window but no freshness:messages.go:14-20. |
| Interactive quota drain / exhaustion | config.go:836; actions.go:99-106; messages.go:49-57 | draining; deadline may be removed without stop | …checkpoint and pause… Then end your turn. T3 steward will interrupt any still-running turn in1m0s. | Checkpoint, finish turn; F1 conditional stop needed. Rate ETA is projection; “provider ends session at100%” is an unverified provider assertion, not a demonstrated guarantee; neutral exhaustion language preferable. |
| Interactive quota recovery | config.go:839; daemon/resume.go:164-166,191-204; daemon resume policy:49-96 | intent + rearmed/settled or explicit probe eligibility | quota is available again. Resume the unfinished user task… inspect current user instructions… | Current scope wins. Need confirmed-recovery versus probe prose; no fresh reading should be implied by elapsed reset. Custom prompt preserved by quota_defaults:18-27. |
| Owned attempt warn / drain | LocalDriver.Warn:1189, Checkpoint:1206; local_pause:32-95 | local throttle request/journal; pause survives turn | “claudeAgent/… at97%”; Write .t3/checkpoint.md… backlog status: continue… End this turn. | Honor runtime-owned pause; distinguish completion evidence from pause marker. F9; owned messages currently differ from shipped interactive templates and contain fewer reset/freshness facts. |
| Owned attempt quota resume | local_pause:306-328 -> LocalDriver.Resume:1282-1283 | stopped owned attempt, live lease, not collected; guard allows | Continue the backlog task… Throttle recovery: quota recovered: probe:… | Probe is bounded telemetry attempt, NOT confirmed recovery; F6. No resume of cancelled attempt. |
| Shell/time/GitHub wait | wait.go:372-438; settle:104-114; task.go:259-283 | met/failed/gave-up/timed-out; then woken; cancelled excluded | Wait finished… gave up(target unavailable)… Last output… Continue… | Inspect failed/gave-up/deadline outcome and latest scope; F7/F8. Shell output tail4000; local registered time lacks explicit timezone:432. Time/reset occurrence isn't successful dependent work. |
| Node / task-run notification | node.go:105-255 -> nodeTrailer + nodeWakeProse:74-99; node.go:159-189 domain outcome | settled observation + delivery pending/sending/recovery-required/delivered/rejected; terminal state may be failed | t3-steward-wait kind=node outcome=failed… Wait finished… failed(exit1). Continue… | Collect “t3-steward task result <run>” when terminal (domain node:170 supplies trailer field); examine failures/verdict. F7. Run revision and attempt clause intelligently omit absent sink attempt:89-99. Terminal success is a separate predicate. |
| Quota node wait | domain quota_wait:112-164 -> nodeWakeProse:75-77 / nodeTrailer | condition met from aggregate OR reset deadline; no automatic task success | Quota pool claude: pool claude reset at… Continue… | Revalidate authentic fresh eligibility before provider effects; F3/F4. Reset timer is documented clock-based contract atquota_wait:23-25,147; message should say deadline passed, not verified provider reset. |
| Grouped node wake | node.go:207-218,535-548 | frozen payload and exact member IDs via ClaimNodeWakeGroup | count=2… conditions of group settled; each member trailer + prose | Inspect ALL mixed outcomes; each member identity survives. No lost-membership defect established. Unknown receipt stays recovery-required; never absence-based resend:138-166. |
| Task-bound wait / ask wake | wait/task.go:237-253 -> domain/task_wait.go:491-537 | parked attempt reacquires capacity or evidence delivered mid-turn | steward is waking this task… / settled while task already running… deadline reached(timed-out,a normal outcome)… | Accurate resumption distinction. Ask failure returns before Continue:526-533. Ordinary failures need F7, bare output F8. Continuing task work means handling settled failure, not declaring success. |
| Task first-turn contract | backlog/first_turn_prompt.go:22-58 -> LocalDriver.CreateThread:768-778 | no provider session until input/preflight passes; outputs collected on unparked turn end | one turn… no BACKLOG STATUS: continue… declared outputs/commits… foreground checks… task-bound wait | Good explicit collection contract and file/commit distinction. Limit uses compat UTF16 count; creation validates full preflight+recovery envelope before provider effect. Selected tests for workerruntime not compiled; no claim they passed. Ask rules/current generated model roles were not exhaustively verified; no inconsistency established. |
| Notify-thread refusal / accepted waiting | campaign_notify:154-175; task_run:87-93; campaign/help:323-348 | unresolved caller => no submission; temporary quota => accepted_waiting intent/run | nothing was submitted; corrected --notify-thread… / ready or accepted_waiting…run exists either way | Refusal useful and resolves “current”, not assuming provider-session ID is thread ID. Accepted waiting is currently documented; R1 requested default must supersede it explicitly with opt-in semantics. |

Delivery reliability boundary: native node/task wake path freezes identity, uses receipt strengths and avoids unknown resend (node.go:138-166,216-239); existing TestNativeWakeHeldRestartLostResponseAndObservation exercises restart/lost response. Legacy shell/time/GitHub wakeThread:395-405 has send-before-save with no equivalent frozen receipt: a successful send plus failed save/crash leaves retryable settled row. This is a SOURCE-PROVEN uncertainty window, not a reproduced duplicate/spam incident. Add an M16 fake known-no-effect/unknown-ack/restart acceptance alongside F5, without claiming all native delivery is broken. A quota-warning first-mark-before-send loss and legacy-wake send-before-save uncertainty need distinct treatment.

## Acceptance ordering for the lead

M16 first: F1 truthful drain actions; F5 reliable notices/legacy wake receipt boundary; F9 explicit quota-pause completion exception; F6 recovery/probe distinction; then F7/F8 outcome-aware bounded messages, F4 keyed observations. R3 transient same-session recovery should be its own pinned contract; do not call it implemented by message changes.

M17 first: F3 fresh complete applicable quota conditions + authoritative admission/dispatch identity; F2 honest reporting. Then R1 default refusal + explicit deferred intent with fenced idempotency/reservations. Then R2 spendable-budget pacing. Preserve cancellation/lease/review authority and no-session-on-refusal across child/resume paths. Budget admission tests live in backlog and are outside the prescribed selected command, so no additional current pass is asserted.

Each repair needs a separately pinned implementation/review campaign after lead digests this inventory. No source candidate, commit or bundle produced. No implementation approval is inferred from this audit.

## Execution receipts

### Required focused command: ONCE

Full foreground shell argv passed to bash:
```bash
audit_tmp=$(mktemp -d /tmp/t3-quota-audit.XXXXXX)
chmod 0700 "$audit_tmp"
GOTMPDIR="$audit_tmp" GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/daemon ./internal/wait ./internal/config ./internal/workerruntime ./cmd/t3-steward -run 'Test.*(Message|Wake|Template|Quota|Prompt|Models)' -count=1
result=$?
rmdir "$audit_tmp"
exit "$result"
```
Combined stdout/stderr, full:
```text
ok  	github.com/iryzhkov/t3-steward/internal/daemon	0.123s
ok  	github.com/iryzhkov/t3-steward/internal/wait	0.003s
ok  	github.com/iryzhkov/t3-steward/internal/config	0.010s
# github.com/iryzhkov/t3-steward/internal/workerruntime [github.com/iryzhkov/t3-steward/internal/workerruntime.test]
compile: writing output: write $WORK/b269/_pkg_.a: disk quota exceeded
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime [build failed]
# github.com/iryzhkov/t3-steward/cmd/t3-steward [github.com/iryzhkov/t3-steward/cmd/t3-steward.test]
compile: writing output: write $WORK/b279/_pkg_.a: disk quota exceeded
FAIL	github.com/iryzhkov/t3-steward/cmd/t3-steward [build failed]
FAIL
```
Actual exit1. Three package passes, two build failures; not a passing complete selection. Shell temp cleanup succeeded. No rerun/full gate/race.

One bounded storage diagnostic argv: `df -h /tmp /dev/shm .; df -i /tmp .; quota -s`.
Combined stdout/stderr:
```text
Filesystem        Size  Used Avail Use% Mounted on
tmpfs              16G   13G  3.2G  80% /tmp
tmpfs              16G  888M   15G   6% /dev/shm
/dev/mapper/root  930G  407G  515G  45% /home
Filesystem        Inodes  IUsed  IFree IUse% Mounted on
tmpfs            1048576 363213 685363   35% /tmp
/dev/mapper/root       0      0      0     - /home
/usr/bin/bash: line 1: quota: command not found
```
Actual exit127 (quota CLI missing). Free filesystem bytes do not disprove per-user/project quota. No shared cache/admin cleanup or trust adjustment attempted. Changed only disposable probe GOTMPDIR location to existing /dev/shm for the one additional command.

### Specific additional probe

Concern: uncovered drain prose versus policy, mixed window/reset provenance, incomplete/stale quota waits, genuine fresh same-window decrease, and failed/gave-up wake presentation. In-process pure domain/policy plus existing nativeMemory/nativeControl fake; no external transports. Fixed times and mock IDs are synthetic test inputs, NOT quota reports.

Created through Huyang (format=false) at internal/wait/quota_message_audit_probe_test.go; revision docrev_d9d5b26450f3ab002b8eb6f6c13dfca7ebdfceffa7ad02ba1b9af414d55d1212. Full exact code:
```go
package wait

import (
 "context"
 "fmt"
 "strings"
 "testing"
 "time"

 "github.com/iryzhkov/t3-steward/internal/config"
 "github.com/iryzhkov/t3-steward/internal/domain"
 "github.com/iryzhkov/t3-steward/internal/policy"
)

func TestBoundedQuotaMessageAuditProbe(t *testing.T) {
 now := time.Date(2026,10,5,16,0,0,0,time.UTC)
 reset := now.Add(72*time.Hour)
 expired := now.Add(-time.Hour)
 short := domain.BucketKey{ProviderInstanceID:"claudeAgent", Window:"five_hour"}
 weekly := domain.BucketKey{ProviderInstanceID:"claudeAgent", Window:"seven_day"}
 pool := domain.QuotaPool{ID:"claude", Buckets:[]domain.BucketKey{short,weekly}, BucketSelection:domain.BucketSelectionResolved}
 states := []domain.BucketState{
  {Key:short, Phase:domain.PhaseNormal, UsedPercent:7, ResetsAt:&expired, ObservedAt:now.Add(-2*time.Hour)},
  {Key:weekly, Phase:domain.PhaseDraining, UsedPercent:97, ResetsAt:&reset, ObservedAt:now.Add(-24*time.Hour)},
 }
 agg := domain.ObserveQuotaPool(pool, states)
 fmt.Printf("aggregate percent=%.0f reset=%s observed=%s phase=%s\n",agg.Percent,agg.ResetsAt.Format(time.RFC3339),agg.ObservedAt.Format(time.RFC3339),agg.Phase)
 outcome, reason := (domain.QuotaWaitCondition{Pool:"claude",Reset:true,ResetAt:&expired}).Evaluate(agg,now)
 fmt.Printf("reset wait outcome=%s reason=%s\n",outcome,reason)
 below:=50.0
 stale := domain.ObserveQuotaPool(pool,states[:1])
 outcome,reason=(domain.QuotaWaitCondition{Pool:"claude",Below:&below}).Evaluate(stale,now)
 fmt.Printf("missing weekly/stale short wait outcome=%s reason=%s\n",outcome,reason)
 thresholds:=policy.DefaultThresholds()
 engine:=policy.New(thresholds)
 deadline:=now
 drained:=domain.BucketState{Key:weekly,Phase:domain.PhaseDraining,UsedPercent:90,DrainDeadline:&deadline,ResetsAt:&reset,ObservedAt:now.Add(-time.Minute)}
 tick:=engine.Tick(drained,now)
 fmt.Printf("drain default promises interrupt=%t; tick phase=%s actions=%d ignored=%s\n",strings.Contains(config.DefaultDrainMessage,"will interrupt"),tick.State.Phase,len(tick.Actions),tick.Ignored)
 refreshed:=engine.Evaluate(domain.QuotaSnapshot{Key:weekly,UsedPercent:10,ResetsAt:&reset,ObservedAt:now,SourceEventID:"fresh"},states[1],now)
 fmt.Printf("same-window fresh drop phase=%s recovered=%t actions=%d\n",refreshed.State.Phase,refreshed.State.RecoveredAt!=nil,len(refreshed.Actions))
 store:=&nativeMemory{w:domain.NodeWait{Request:domain.NodeWaitRequest{ID:"nw-audit",Name:"review result",ThreadID:"thread"},Host:"host",SettledAt:&now,Observation:&domain.NodeObservation{ExitCode:1,Reason:"failed"},DeliveryID:"audit-token",Delivery:"pending"}}
 control:=&nativeControl{}
 runner:=New(store,control,nil)
 runner.NodeHost="host"
 runner.Tick(context.Background(),nil,nil)
 fmt.Printf("fake node sends=%d delivery=%s\n%s\n",control.sends,store.w.Delivery,strings.Join(control.texts,"\n"))
 text:=WakeMessage([]Wait{{ID:"w-audit",Name:"check",Status:StatusGaveUp,Reason:"target unavailable",Command:[]string{"check"},LastExit:2,CreatedAt:now,LastOutput:"\x1b[31mred\n```\noutside fence"}})
 fmt.Printf("gave-up shell wake=%q\n",text)
 if agg.Percent!=97 || !agg.ResetsAt.Equal(expired) || tick.State.Phase!=domain.PhaseDraining || len(tick.Actions)!=0 || refreshed.State.Phase!=domain.PhaseNormal || control.sends!=1 { t.Fatal("unexpected audited behavior") }
}
```

Full foreground shell argv:
```bash
audit_tmp=$(mktemp -d /dev/shm/t3-quota-audit.XXXXXX)
chmod 0700 "$audit_tmp"
GOTMPDIR="$audit_tmp" GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/wait -run '^TestBoundedQuotaMessageAuditProbe$' -count=1 -v
result=$?
rmdir "$audit_tmp"
exit "$result"
```
Combined stdout/stderr, full:
```text
=== RUN   TestBoundedQuotaMessageAuditProbe
aggregate percent=97 reset=2026-10-05T15:00:00Z observed=2026-10-05T14:00:00Z phase=draining
reset wait outcome=met reason=pool claude reset at 2026-10-05T15:00:00Z
missing weekly/stale short wait outcome=met reason=pool claude is at 7%, below 50%
drain default promises interrupt=true; tick phase=draining actions=0 ignored=grace expired at 90%, below the stop threshold of 95% with no exhaustion projected; the drain request stands
same-window fresh drop phase=normal recovered=true actions=1
fake node sends=1 delivery=sending
t3-steward-wait kind=node outcome=failed wait=nw-audit revision=0

Wait finished (T3 steward): "review result". Node /: failed (exit 1).
Continue the work that was waiting on this.
gave-up shell wake="t3-steward-wait kind=shell outcome=gave-up wait=w-audit exit=2\n\nWait finished (T3 steward): \"check\" gave up (target unavailable).\n\n## check: gave up (target unavailable)\nCommand: check\nRuns: 0, last exit 2, registered 2026-10-05 09:00.\nLast output:\n```\n\x1b[31mred\n```\noutside fence\n```\n\nContinue the work that was waiting on this. Inspect the current state first; do not assume anything else changed while the thread was parked."
--- PASS: TestBoundedQuotaMessageAuditProbe (0.00s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/wait	0.004s
```
Actual exit0. This is behavior evidence, not a full regression acceptance of proposed fixes.
Probe removed through Huyang delete_file with exact above revision, receipt req_11837/wsrev_38. No own pending processes: both shell sessions completed and temporary directories removed.

Final verification: exact detached HEAD/tree/parent rechecked; Git tracked/index diff clean; only declared untracked outputs and preexisting .t3 remain. Outputs each under256KiB. Publication deferred/schedules disabled; no schedule state was changed. Findings await lead digestion and separately pinned repair/review.
