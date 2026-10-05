Independent annotation review — ACCEPT

Verdict: ACCEPT for exact commit 3c6176f7f4c7d729d967c2255ecf5ee844fc09d3.
Tree: 0c3f2c7b41d382263b482423ef2210f3b9681d57.
Sole parent: 0131f87946b24b4fc4427662d29ed3eb26a62137.
Reviewer: Codex GPT-6-Astra, medium, independent task under jocasta:715251cbbbfd43882e02587a54b70caa@1.

Deduplicated blocking findings: none (P0/P1/P2 zero). No required production fix; no finding-specific path/reproduction/fix entry applies. This is one local independent verdict, not reconciliation of both reviews or acceptance of the whole milestone.

Authority and declared inputs

Read the FULL Jocasta revision (23 logical lines, 9398 bytes, SHA256 4f9abfb32a14910e6f6bdc4081d71c34b9345229fb49d9982c0271943bd90470) and BOTH full accepted profile reviews, including their portable probes and limitations:
- profile-accepted-review-sol.md: 25910 bytes, SHA256 734216e0c95840585d2030c101c730081656e76ec2987f258bd15fb2f68bd3d2.
- profile-accepted-review-astra.md: 28943 bytes, SHA256 bde14d209191f7728e8208520277a6254643941b8b98e2f581d5844dfd9b90bf.

A combined tool delivery was truncated; bounded follow-ups completed the omitted review/probe and source portions. The profile reviewers accept exactly the annotation commit's parent; their acceptance does not substitute for annotation review.

Listed .t3/dependencies before consumption. It contains ONLY task-94f88d8cca344e98b443d09764360968, with:
- annotation (504 bytes), campaign-commit/v1;
- continuation.md (2462 bytes);
- handoff.md (9194 bytes);
- annotation-contract.md (6256 bytes);
- verification.log.xz (431488 bytes);
- annotation.bundle (522918 bytes).

Consumed only that implementation's declared commit and five artifacts, plus the controlling plan and accepted profile review inputs. A names-only input directory listing exposed the other incident/profile input names; their source/bundles/incident content were not imported or consulted. No sibling current review, sibling source, consultation, delegation, nested review or escalation.

Campaign receipt names run-8465c708e4c9209366943a40f1ecf073, producer task-94f88d8cca344e98b443d09764360968, annotation, repository git@github.com:iryzhkov/t3-steward.git, base6b0a6798736d3c9277e7688ef3016c690bf7a9ee, exact candidate3c6176f7, and refs/campaigns/run-8465c708e4c9209366943a40f1ecf073/task-94f88d8cca344e98b443d09764360968/annotation. The checkout's origin is the worker's local repository cache; that cache's origin independently matches the receipt's repository. No network fetch was used.

Initial physical HEAD was exactly6b0a6798736d3c9277e7688ef3016c690bf7a9ee, tracked/index clean. Supplied annotation.bundle SHA256 683042ddbeb2df80be1c896756c6546469b383fd2ebb01706078b53b54c0ebb3, exact522918 bytes. git bundle verify actual0; sole advertised HEAD3c6176f7 and sole prerequisite6b0a. Fetched ONLY that local bundle, detached exact candidate, verified physical HEAD/tree/sole parent. The campaign receipt's base is the original workflow baseline, while the sole commit parent is the accepted profile candidate, as the plan requires.

Huyang ws_09bd60d1153f50de16511822236c280f opened before repository reads. The mounted dependency symlink exits the repository; Huyang correctly refused those paths. External read-only artifacts were then read through their mounted location, not edited or used to bypass repository guards. Repository source reads and disposable probe creation/deletion used Huyang. Explicitly authorized foreground Git/Go/external evidence commands used the shell. Trust remained unchanged/untrusted; no Huyang isolated verification or full LSP assurance is claimed.

Source review

Inspected all nine changed files in the sole-parent diff (1336 insertions,9 deletions), including every new helper/test/doc and all existing-test argument changes. The only modified preexisting production source is internal/wait/github.go. Wait/domain serialization and delivery source are unchanged.

internal/wait/github.go:78–89 adds actual run databaseId/attempt/headSha and PR headRefOid to the original fixed gh view argv. The status evaluator's verdict mapping remains unchanged. ReadGitHub retains the exact observation that produced the verdict; registration and pending polls do not call enrichment. runGitHubOnce:312–315 enriches only terminal run or checks-passed verdicts, writes LastOutput, then settles using the original reason/fields/outcome. Missing/inaccessible diagnostics cannot turn success into failure or restart polling.

internal/wait/github_annotations.go:189–302 validates canonical GitHub target URLs and explicit/default repository identity, refuses dot-only repository segments, parses numeric check IDs from repository-bound API URLs, and constructs fixed endpoints. Run jobs are requested from the evaluated run/attempt endpoint and matched to run/head, with attempt equality when present. Check metadata binds ID/head/name/count. Repository directory semantics remain with the original gh status request; unsupported enterprise URLs return unavailable without changing the gate. API error text is categorized rather than copied from stderr.

PR association at :306–399 enumerates the evaluated head's check runs (filter=all), requires a unique match on name/details/status/conclusion/started/completed times, rejects duplicate reuse/ambiguous reruns, and excludes unrelated old checks. StatusContext entries explicitly lack annotation evidence; mixed contexts are partial. Nonterminal checks within an already failed PR gate remain missing evidence. Exact matching can conservatively refuse otherwise useful diagnostics, but it does not silently attach another workflow's annotations.

Collection at :400–459 rechecks metadata after annotation pages, rejects changed snapshots, retains accurate observed severity counts, and refuses complete-empty conclusions when enumeration is incomplete. Unknown severities remain visible and partial. Missing/invalid record fields are flagged, not counted as clean annotations. Complete metadata count0 can produce No annotations; partial zeros print unknown, and unavailable summaries do not print numerical clean counts.

internal/wait/github_annotations_recheck.go uses one constructed REST run request or fixed GraphQL PR query, not a second gh view with hidden lookups/pagination. Run ID/attempt/head/status/conclusion/URL must match. PR head/state and full bounded context snapshot must match; hasNextPage, missing structure or errors refuse consistency. A detected race discards diagnostics and leaves the initial verdict intact.

Resource limits are explicit: one <=30s child context; <=20 extra counted API invocations with the twentieth reserved for consistency; <=256KiB retained stdout per call, <=2MiB aggregate enrichment stdout; <=3 pages of50; <=20 selected checks; <=150 records; <=10 inline annotation records; <=4000 UTF8 bytes in LastOutput. Actual ExecGitHub buffers cap retained stdout and4096 stderr bytes; fixed gh api calls avoid --paginate and arbitrary URL following. Errors charge a conservative response budget. Detail rendering reserves footer room, reports display omission, and retains links/caveats ahead of details. The limits bound retained response content, not a claim that the server never transmits more bytes before truncation or that transport internals make no redirect/auth requests.

Remote annotation fields are individually quoted, clipped at UTF8 boundaries and escaped for controls, backticks, angle brackets and trailer-looking strings. Details links are validated or replaced by a constructed repository/check link; links are printed, never followed. Annotation bodies do not enter the machine trailer. The original gate reason/outcome/trailer remains unchanged.

Read the unchanged owning persistence/delivery paths: runOnce saves the settled Wait before delivery; WakeMessage renders LastOutput; taskWaitResult forwards it into TaskWaitResult.Output; TaskWaitWakeContext.Prompt renders saved output; settled replay does not repoll. Existing cancellation, retry/gone, registration argument, custom wake behavior and task delivery controls remain intact. No serialization source change means the plan's conditional worker/backlog serialization selection was not necessary.

Producer validation and full evidence

verification.log.xz SHA256 4113221fd1540135e44cfc803a3ecdf2fe78a16cb314ba2dd2a30b511e4ed79d (431488 bytes) decompresses losslessly to8380944 bytes, SHA256 08df795451fc3cc607dc7a5d99a6b782e0a45d89086200946a5aa4cd8208afc8. Full raw retained externally at /var/tmp/ar.0xdjitcw/producer.log. Parsed the complete log and its structured records, read the actual final gate output in full, and inspected the failure/correction chronology without treating repetitive object inventories or a producer summary as gate success.

The initial warning-loss regression actually exits1: successful LastOutput contains only the run-success reason. The full portable BEFORE source is retained in the log. Initial AFTER selection passes; expanded selection actually fails on the incomplete fake byte-budget fixture and stale CLI argv assertion. Both failures remain recorded and corrected in the final code. Full gate1 exits2 on the existing Unix socket path limit; source also changed during that invocation. The targeted unchanged socket test passes with shorter private temp. Full gate2 exits0 but is explicitly provisional because source changed during it. Neither is used for acceptance.

The actual final command, raw lines471–513:
make check-review FAST_BASE=0131f87946b24b4fc4427662d29ed3eb26a62137
Exit0,591.511560683s; GOMAXPROCS2, GOFLAGS-p2, private short TMPDIR/GOTMPDIR.
Full output includes build, vet, complete short suite, pinned go1.25.0/staticcheckv0.7.0/gofmt, and full changed-package race cmd/t3-steward222.109s/internal/wait1.211s. No failure is hidden by the successful task record. This producer is validated for the exact candidate; no reviewer full gate/race rerun was needed or performed.

Independently parsed the complete pre-gate-certified/post-gate-certified/final fingerprints. All three are exactly equal across ALL1451 tracked paths and tree0c3f2c7b. Batch-read EVERY recorded Git blob and verified byte size/SHA256/OID against each record, compared complete logical index (mode,OID,stage,path), and independently hash-object'ed every tracked worktree path. Every worktree OID equals the recorded index/blob OID. Raw index metadata hashes are separately historical observations, not a claim that this checkout's raw index bytes equal the producer's.

Parsed all eight full typed-inventory blocks. Current exact candidate closure10395 equals all three producer source/fresh candidate/fresh ALL inventories. Producer sourceALL10771 pre/post sets are identical, contain the closure, and preserve376 unrelated objects. The producer's fresh baseline-only proof records baseline9926, candidate absent BEFORE import, strict fsck0, only the declared annotation bundle imported, no alternates, exact tree/parent and zero missing closure objects. I corroborated the complete candidate typed closure against this checkout and independently ran current git fsck --full --strict (exit0, empty stdout/stderr). I did not claim a newly recreated fresh-repository experiment or fresh access to producer-only unrelated objects. Receipt consistency and hashes establish supplied evidence identity; they are not independent authentication of remote historical execution.

Current focused verification

Both reviewer commands ran foreground to completion. Private0700 /var/tmp/ar.0xdjitcw supplied both GOTMPDIR and TMPDIR; GOMAXPROCS2/GOFLAGS-p2. GOPROXY=off/GOTOOLCHAIN=local additionally prevented dependency/toolchain network fetch. The test seams use fake runners; no live GitHub API or provider call.

Required focused selection ran ONCE, exit0/10.312782544s:
```json
{
  "argv": [
    "go",
    "test",
    "./internal/wait",
    "./internal/domain",
    "./cmd/t3-steward",
    "-run",
    "Test.*(GitHub|Annotation|Wake|TaskWait)",
    "-count=1"
  ],
  "env": {
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2",
    "GOTMPDIR": "/var/tmp/ar.0xdjitcw",
    "TMPDIR": "/var/tmp/ar.0xdjitcw",
    "GOPROXY": "off",
    "GOTOOLCHAIN": "local"
  },
  "exit": 0,
  "seconds": 10.312782544002403
}
```
Actual stdout (complete):
```text
ok  	github.com/iryzhkov/t3-steward/internal/wait	0.017s
ok  	github.com/iryzhkov/t3-steward/internal/domain	0.002s
ok  	github.com/iryzhkov/t3-steward/cmd/t3-steward	1.741s
```
Actual stderr: empty (0 bytes).

Missing-coverage probes

Added one guarded disposable internal/wait/astra_annotation_probe_test.go, used the exact candidate's existing fixture helpers, ran once unchanged, then removed by its exact Huyang document revision. All probes passed:
- Complete two-page enumeration:50 warnings plus1 notice, accurate51-record total.
- Page2 denial and short response: retain observed50 warnings, explicitly partial with unknown missing severity counts; no stderr secret; no duplicate page fetch.
- Mixed PR CheckRun/StatusContext: retain notice, preserve met verdict, and mark status-context evidence unknown.
- Failed interactive delivery: JSON round-trip the saved Wait, create a fresh Runner whose GitHub seam fails the test if called, deliver the exact saved summary.
- Task-bound lost delivery: JSON round-trip both TaskWait/result and local check, then reconcile an observed delivery without API refetch or duplicate send.

These are actual persistence-shape/replay checks, not claims of SQLite close/open, OS restart or end-to-end production recovery. Task-bound probe reuses its test runner after restoring JSON rows; interactive probe constructs a new Runner. The permanent producer test also covers success and failure task outcomes; current focused selection ran it.

Complete portable probe source (exact executed bytes; no omitted helpers beyond existing exact-candidate test fixtures):
```go
package wait

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "strings"
 "testing"
 "time"

 "github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAstraAnnotationSecondPageEvidence(t *testing.T) {
 for _, mode := range []string{"complete", "denied", "short"} {
  t.Run(mode, func(t *testing.T) {
   a := annotationFixture(t)
   a.responses[annotationMetadata] = strings.ReplaceAll(a.responses[annotationMetadata], "\"annotations_count\":1", "\"annotations_count\":51")
   records := []map[string]any{}
   for i:=0;i<50;i++ { records=append(records,map[string]any{"annotation_level":"warning","path":"x.go","start_line":i+1,"end_line":i+1,"message":fmt.Sprintf("warning %d",i)}) }
   b,_:=json.Marshal(records); a.responses[annotationPage]=string(b)
   page2:="repos/o/r/check-runs/7/annotations?per_page=50&page=2"
   a.responses[page2]=`[{"annotation_level":"notice","path":"y.go","start_line":1,"end_line":1,"message":"last notice"}]`
   if mode=="denied" {a.errors[page2]=errors.New("HTTP 403 PRIVATE")}
   if mode=="short" {a.responses[page2]="[]"}
   w:=annotationEvaluate(t,a,GitHubTarget{Kind:"run",ID:"123",State:"completed"})
   if w.Outcome!="met" || !strings.Contains(w.LastOutput,"warning=50") {t.Fatal(w.LastOutput)}
   if mode=="complete" {
    if !strings.Contains(w.LastOutput,"Annotations complete") || !strings.Contains(w.LastOutput,"notice=1") || !strings.Contains(w.LastOutput,"records=51") {t.Fatal(w.LastOutput)}
   } else if !strings.Contains(w.LastOutput,"Annotations partial") || !strings.Contains(w.LastOutput,"notice=unknown") || !strings.Contains(w.LastOutput,"failure=unknown") || strings.Contains(w.LastOutput,"PRIVATE") {t.Fatal(w.LastOutput)}
   n:=0;for _,args:=range a.calls {if len(args)>5 && args[5]==page2 {n++}}
   if n!=1 {t.Fatalf("page 2 fetched %d times",n)}
   t.Logf("mode=%s gate=%s calls=%d; exact observed warning=50 and honest completeness",mode,w.Outcome,len(a.calls)-1)
  })
 }
}

func TestAstraAnnotationMixedContextAndNotice(t *testing.T) {
 a:=annotationPRFixture(t)
 a.status=annotationMutate(t,a.status,func(m map[string]any){m["statusCheckRollup"]=append(m["statusCheckRollup"].([]any),map[string]any{"__typename":"StatusContext","context":"legacy","state":"SUCCESS"})})
 a.responses[annotationPage]=strings.ReplaceAll(a.responses[annotationPage],"warning","notice")
 w:=annotationEvaluate(t,a,GitHubTarget{Kind:"pr",ID:"4",State:"checks-passed"})
 if w.Outcome!="met" || !strings.Contains(w.LastOutput,"Annotations partial") || !strings.Contains(w.LastOutput,"notice=1") || !strings.Contains(w.LastOutput,"warning=unknown") || !strings.Contains(w.LastOutput,"status-context-no-annotations") {t.Fatal(w.LastOutput)}
 t.Log("mixed PR keeps notice and marks inaccessible status-context counts unknown")
}

type astraAnnotationLostControl struct { *memControl; lost bool }
func (c *astraAnnotationLostControl) ResumeThread(ctx context.Context, th domain.Thread, text string) error {
 if c.lost {return errors.New("lost delivery")}
 return c.memControl.ResumeThread(ctx,th,text)
}

func TestAstraAnnotationInteractiveJSONReopen(t *testing.T) {
 a:=annotationFixture(t)
 runner,store,control,now:=gitHubRunner(t,a.dispatch)
 runner.control=&astraAnnotationLostControl{memControl:control,lost:true}
 runner.Tick(context.Background(),nil,nil)
 original:=store.waits["w1"]
 if original.Status!=StatusMet || !strings.Contains(original.LastOutput,"avoid stale value") {t.Fatal(original)}
 raw,err:=json.Marshal(original);if err!=nil {t.Fatal(err)}
 var reopened Wait;if err=json.Unmarshal(raw,&reopened);err!=nil {t.Fatal(err)}
 store.waits["w1"]=reopened
 fresh:=New(store,control,nil);fresh.SetClock(func()time.Time{return now.Add(time.Minute)})
 fresh.GitHub=func(context.Context,string,[]string)(string,error){t.Fatal("settled reopen fetched GitHub");return "",nil}
 fresh.Tick(context.Background(),nil,nil)
 if store.waits["w1"].Status!=StatusWoken || store.waits["w1"].LastOutput!=original.LastOutput || len(control.texts)!=1 || !strings.Contains(control.texts[0],"avoid stale value") {t.Fatal(store.waits["w1"],control.texts)}
 t.Log("failed interactive delivery persisted; new Runner + JSON reopen delivers exact saved diagnostics without fetch")
}

func TestAstraAnnotationTaskJSONReopen(t *testing.T) {
 a:=annotationFixture(t)
 runner,store,control,now:=taskWaitRunner(t,domain.ProgressWaitingExternal)
 w:=store.waits["w1"];w.Kind=domain.WaitKindGitHub;w.Dir="/registered/in";w.GitHub=&GitHubTarget{Kind:"run",ID:"123",State:"completed"};store.waits["w1"]=w
 runner.GitHub=a.dispatch;control.sendErr=errors.New("lost delivery");*now=now.Add(time.Minute)
 runner.Tick(context.Background(),nil,healthyBuckets())
 original:=store.taskWaits["tw-1"]
 if original.Result==nil || !strings.Contains(original.Result.Output,"avoid stale value"){t.Fatal(original)}
 raw,err:=json.Marshal(original);if err!=nil {t.Fatal(err)}
 var reopened domain.TaskWait;if err=json.Unmarshal(raw,&reopened);err!=nil {t.Fatal(err)}
 store.taskWaits["tw-1"]=reopened
 raw,err=json.Marshal(store.waits["w1"]);if err!=nil {t.Fatal(err)}
 var check Wait;if err=json.Unmarshal(raw,&check);err!=nil {t.Fatal(err)};store.waits["w1"]=check
 runner.GitHub=func(context.Context,string,[]string)(string,error){t.Fatal("task JSON reopen fetched GitHub");return "",nil}
 control.sendErr=nil;control.observed=map[string]bool{"task-wake:tw-1":true};*now=now.Add(time.Minute)
 runner.Tick(context.Background(),nil,healthyBuckets())
 if store.taskWaits["tw-1"].Result.Output!=original.Result.Output || len(control.sends)!=1 {t.Fatal("task evidence/delivery changed")}
 t.Log("task wait and check JSON reopen preserve exact diagnostics and do not fetch or duplicate delivery")
}
```

Complete actual command/stdout/stderr/exit:
```json
{
  "argv": [
    "go",
    "test",
    "./internal/wait",
    "-run",
    "^TestAstraAnnotation",
    "-count=1",
    "-v"
  ],
  "env": {
    "GOMAXPROCS": "2",
    "GOFLAGS": "-p=2",
    "GOTMPDIR": "/var/tmp/ar.0xdjitcw",
    "TMPDIR": "/var/tmp/ar.0xdjitcw",
    "GOPROXY": "off",
    "GOTOOLCHAIN": "local"
  },
  "exit": 0,
  "seconds": 0.5311541950213723
}
```
Actual stdout (complete):
```text
=== RUN   TestAstraAnnotationSecondPageEvidence
=== RUN   TestAstraAnnotationSecondPageEvidence/complete
2026/10/05 12:06:58 INFO wait settled component=wait wait=w1 name=ci thread=t1 kind=github status=met runs=1
2026/10/05 12:06:58 INFO thread woken component=wait thread=t1 waits=1
    astra_annotation_probe_test.go:34: mode=complete gate=met calls=6; exact observed warning=50 and honest completeness
=== RUN   TestAstraAnnotationSecondPageEvidence/denied
2026/10/05 12:06:58 INFO wait settled component=wait wait=w1 name=ci thread=t1 kind=github status=met runs=1
2026/10/05 12:06:58 INFO thread woken component=wait thread=t1 waits=1
    astra_annotation_probe_test.go:34: mode=denied gate=met calls=6; exact observed warning=50 and honest completeness
=== RUN   TestAstraAnnotationSecondPageEvidence/short
2026/10/05 12:06:58 INFO wait settled component=wait wait=w1 name=ci thread=t1 kind=github status=met runs=1
2026/10/05 12:06:58 INFO thread woken component=wait thread=t1 waits=1
    astra_annotation_probe_test.go:34: mode=short gate=met calls=6; exact observed warning=50 and honest completeness
--- PASS: TestAstraAnnotationSecondPageEvidence (0.00s)
    --- PASS: TestAstraAnnotationSecondPageEvidence/complete (0.00s)
    --- PASS: TestAstraAnnotationSecondPageEvidence/denied (0.00s)
    --- PASS: TestAstraAnnotationSecondPageEvidence/short (0.00s)
=== RUN   TestAstraAnnotationMixedContextAndNotice
2026/10/05 12:06:58 INFO wait settled component=wait wait=w1 name=ci thread=t1 kind=github status=met runs=1
2026/10/05 12:06:58 INFO thread woken component=wait thread=t1 waits=1
    astra_annotation_probe_test.go:45: mixed PR keeps notice and marks inaccessible status-context counts unknown
--- PASS: TestAstraAnnotationMixedContextAndNotice (0.00s)
=== RUN   TestAstraAnnotationInteractiveJSONReopen
2026/10/05 12:06:58 INFO wait settled component=wait wait=w1 name=ci thread=t1 kind=github status=met runs=1
2026/10/05 12:06:58 ERROR wake thread component=wait thread=t1 err="lost delivery"
2026/10/05 12:06:58 INFO thread woken component=wait thread=t1 waits=1
    astra_annotation_probe_test.go:68: failed interactive delivery persisted; new Runner + JSON reopen delivers exact saved diagnostics without fetch
--- PASS: TestAstraAnnotationInteractiveJSONReopen (0.00s)
=== RUN   TestAstraAnnotationTaskJSONReopen
2026/10/05 12:06:58 INFO wait settled component=wait wait=w1 name=ci thread=t1 kind=github status=met runs=2
2026/10/05 12:06:58 ERROR deliver task wake component=wait thread=t1 attempt=a1 wait=tw-1 err="lost delivery"
    astra_annotation_probe_test.go:88: task wait and check JSON reopen preserve exact diagnostics and do not fetch or duplicate delivery
--- PASS: TestAstraAnnotationTaskJSONReopen (0.00s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/wait	0.004s
```
Actual stderr: empty (0 bytes).

Cleanup and limitations

Huyang guarded create evidence ev_b1713cc9dfffa565053b18047651d560; exact probe revision docrev_b1ef871f55b490a92956b59c7e5e5c782bcb8473c7a571d162a121dece9f432a. Guarded deletion ev_ec00e1372fb26f7bde6507cd3028bc01/wsrev5. No tracked source/index edit, commit, assertion weakening or production fix. git diff --exit-code HEAD, git diff --cached --exit-code, and git diff HEAD^ HEAD --check each exit0 with empty output. Physical exact candidate/tree/parent retained. Probe absent; all own foreground executions completed; no background jobs started. Final output-size/working-tree/process audit accompanies materialization of these two outputs.

No blocker found in this bounded contract. Exact textual PR matching, page/request/size caps and unsupported enterprise hosts can report partial/unavailable even when more remote diagnostics exist; this is deliberate conservative behavior, not a clean result. Fake fixtures and supplied final gate do not prove live GitHub schema behavior, provider diversity, arbitrary remote race freedom or deployment. No API/provider/remote-control execution was used to close those gaps.

Local ACCEPT only. Both independent reviews are still lead-owned reconciliation. Publishing deferred; schedules disabled/unchanged; no push/PR/tag/release/UpKeeper/CI/fleet/service/admin/config/trust/live task effects or external messages. No whole M16/M17, production diversity, compiler/runtime restart or OS-restart acceptance. Token/cost totals unavailable.
