Independent M16 immutable execution profile review — CHANGES-REQUESTED

Reviewer: Codex gpt-6.1-sol, medium, independent declared task task-50eb19401ba7e6e0f253500a331ee9b4 in run-6ab84497032bf565a7a45b194d82ac83. No delegation, nested reviews, production fixes or effort escalation. Lens: parser/compiler legacy byte compatibility, strict malformed profiles, exact static quota grants, deep-copy isolation.

Reviewed exact commit c4287f4867bdad509032f9528c6f556646807ebb, tree a3c8d1e8b09a535abadcf352a2536620450ed705, sole parent 49b99fd2e2763ead3f68babd37c41af2783bf9c0. Initial checkout HEAD was exactly 6b0a6798736d3c9277e7688ef3016c690bf7a9ee. Read COMPLETE supplied review-plan.md, controlling plan.md (jocasta:b694668a6fba8a4e1329a658fcf6e0e5@1), handoff.md, execution-profiles-contract.md, continuation.md, recovery/log receipts and incident/cancellation evidence. Reports were treated as assertions to validate. Sibling collection repair was not imported or approved.

Two deduplicated blockers, both P2 / medium severity, blocking this review contract:

1. Integer execution scalars are silently truncated by YAML decoding. internal/backlog/review_declaration.go:27 and :65–68 decode MaxTurns directly into int; profile validation at :31–43 sees only the already-converted integer. ParseManifest accepts max_turns: 9.5 and max_turns: 32.9; compilation produces 9 and 32. The latter violates the exact 1..32 no-clamping contract. Actual BundleIngester with configured permanent declarationValidator admits/stores 32.9 as 32. Related memory_mb: 512.75 and scratch_mb: 64.75 silently compile to 512 and 64 (ManifestResources integer pointer fields at internal/backlog/manifest_resources.go:61–62). These are one scalar-decoding defect, not three independent findings. Strictly validate authored scalar type/integrality before lossy decoding at the profile boundary; preserve legacy acceptance/bytes outside this new execution block. Retain refusal tests at parsing and real permanent admission. No production fix made here.

2. Version 1's execution-presence guard misses merged explicit null. internal/backlog/review_declaration.go:71–75 only scans immediate mapping keys, while the YAML decoder at :65–68 resolves merge mappings. Replacing a legacy member's required: true} with required: true, <<: {execution: null}} makes ParseManifest succeed; the nil profile and false executionDeclared subsequently pass :132–134. Direct explicit execution: null correctly refuses, and merged nonnil profile refuses. Detect presence through resolved merge/alias mappings (or reject execution-bearing merges) so version 1 cannot silently discard execution declarations. This is specifically the contractual v1 refusal boundary, not a grant-laundering finding.

Portable evidence below was installed only as internal/backlog/independent_m16_sol_overlay_test.go via Huyang guarded create/replace operations, format=false because trust remains untrusted. It uses existing candidate test fixtures and is portable by placing the complete fence at that path in this exact candidate checkout. The first two functions ran before adding the third, and the third ran before adding the fourth. All code retained here; no assertions were weakened. Overlay removed by Huyang with its exact latest document revision before final Git hygiene.

```go
package backlog

import (
 "context"
 "reflect"
 "strings"
 "testing"
 "github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentM16StrictYAML(t *testing.T) {
 cases := map[string]string{
  "v1-merged-null": strings.Replace(declaredManifestYAML(), "required: true}", "required: true, <<: {execution: null}}", 1),
  "v1-merged-profile": strings.Replace(declaredManifestYAML(), "required: true}", "required: true, <<: {execution: {effort: medium}}}", 1),
  "v2-merged-unknown": strings.ReplaceAll(executionManifestYAML(), "effort: medium", "effort: medium, <<: {options: {unsafe: true}}"),
  "duplicate-effort": strings.ReplaceAll(executionManifestYAML(), "effort: medium", "effort: high, effort: medium"),
  "fractional-turn": strings.ReplaceAll(executionManifestYAML(), "max_turns: 9", "max_turns: 9.5"),
 }
 for name, raw := range cases { t.Run(name, func(t *testing.T) { if _, err := ParseManifest([]byte(raw)); err == nil { t.Fatal("malformed or forbidden execution declaration accepted") } }) }
}
func TestIndependentM16SharedProfileAndGrantUnion(t *testing.T) {
 m,err:=ParseManifest([]byte(executionManifestYAML()));if err!=nil {t.Fatal(err)}
 r:=m.Tasks["inspect"].ReviewRequirements
 r.Members[1].Execution=r.Members[0].Execution
 c:=compileTaskReview(r,"w","r","t",domain.Artifact{})
 original:=*c.Members[1].Execution
 c.Members[0].Execution.Resources.MemoryMB++
 if *c.Members[1].Execution!=original {t.Fatal("compiler retains shared profile")}
 detached:=cloneManifestReview(r)
 *detached.Members[0].Execution.Resources.MemoryMB++
 if *r.Members[0].Execution.Resources.MemoryMB!=512 || *detached.Members[1].Execution.Resources.MemoryMB!=512 {t.Fatal("manifest clone retains numeric alias")}
 f:=executionFixture(t)
 w:=&f.catalog.catalog.AuthoredWorkers[0]
 w.Providers[0].QuotaPoolID="wrong"
 // Pool exists on project+wrong-instance; exact instance exists on wrong-model.
 f.catalog.catalog.AuthoredWorkers=append(f.catalog.catalog.AuthoredWorkers,
 domain.WorkerInventory{ID:"union-one",Projects:[]domain.WorkerProjectInventory{{Name:"t3-steward"}},Providers:[]domain.WorkerProviderInventory{{InstanceID:"wrong",Models:[]string{"org/sol"},QuotaPoolID:"review-pool"}}},
 domain.WorkerInventory{ID:"union-two",Projects:[]domain.WorkerProjectInventory{{Name:"t3-steward"}},Providers:[]domain.WorkerProviderInventory{{InstanceID:"codex",Models:[]string{"wrong"},QuotaPoolID:"review-pool"}}})
 before,err:=f.store.LoadCoordinatorRecords(context.Background());if err!=nil {t.Fatal(err)}
 if _,err=f.service.FreezeDeclared(context.Background(),declaredRequest(f));err==nil {t.Fatal("union of inexact grants accepted")}
 after,err:=f.store.LoadCoordinatorRecords(context.Background());if err!=nil||!reflect.DeepEqual(before,after) {t.Fatal("rejection mutated records",err)}
}

func TestIndependentM16FractionalIntegerWitness(t *testing.T) {
 for _, x := range []struct{old, value string}{
  {"max_turns: 9","max_turns: 32.9"},
  {"memory_mb: 512","memory_mb: 512.75"},
  {"scratch_mb: 64","scratch_mb: 64.75"},
 } {
  t.Run(x.value,func(t *testing.T){
   m,err:=ParseManifest([]byte(strings.ReplaceAll(executionManifestYAML(),x.old,x.value)))
   if err!=nil {return}
   c:=compileTaskReview(m.Tasks["inspect"].ReviewRequirements,"w","r","t",domain.Artifact{})
   t.Fatalf("fractional integer accepted and compiled: turns=%d memory=%d scratch=%d",c.Members[0].Execution.MaxTurns,c.Members[0].Execution.Resources.MemoryMB,c.Members[0].Execution.Resources.ScratchMB)
  })
 }
}

func TestIndependentM16FractionalAdmissionRejected(t *testing.T) {
 f:=executionFixture(t)
 rewriteBundleManifest(t,f.source,strings.ReplaceAll(executionManifestYAML(),"max_turns: 9","max_turns: 32.9"))
 result,err:=(BundleIngester{Store:f.store,StorageRoot:f.service.Artifacts.SubmissionRoot,Permanent:declarationValidator{f.catalog}}).Ingest(context.Background(),f.source)
 if err!=nil {return}
 t.Fatalf("malformed profile admitted and stored with turns=%d",result.Records.Tasks[0].ReviewRequirements.Members[0].Execution.MaxTurns)
}
```

Current witnessed command receipts, with full outputs (stdout/stderr combined only in retained output capture; command and actual exit recorded separately):

Required ONCE: mktemp -d /var/tmp/r.XXXXXX produced /var/tmp/r.P1VtIT, stat asserted mode0700. GOTMPDIR=/var/tmp/r.P1VtIT GOMAXPROCS=2 GOFLAGS=-p=2 go test ./internal/review ./internal/domain ./internal/backlog ./internal/store/sqlite ./cmd/t3-steward -run '^TestReviewExecutionProfile' -count=1
Foreground exec session44665; actual exit0, polled to completion. Separate external command/env receipt /var/tmp/m16-required-command.json, exit receipt /var/tmp/m16-required-exit.json, complete output /var/tmp/m16-required-output.log. Domain has no matching tests; this selection is not domain test coverage.
```text
ok   github.com/iryzhkov/t3-steward/internal/review 0.003s
ok   github.com/iryzhkov/t3-steward/internal/domain 0.002s [no tests to run]
ok   github.com/iryzhkov/t3-steward/internal/backlog 1.061s
ok   github.com/iryzhkov/t3-steward/internal/store/sqlite 5.160s
ok   github.com/iryzhkov/t3-steward/cmd/t3-steward 0.849s
```

Same GOTMPDIR/GOMAXPROCS/GOFLAGS: go test ./internal/backlog -run '^TestIndependentM16' -count=1 -v, foreground session63344, actual exit1:
```text
=== RUN   TestIndependentM16StrictYAML
=== RUN   TestIndependentM16StrictYAML/v1-merged-null
    independent_m16_sol_overlay_test.go:19: malformed or forbidden execution declaration accepted
=== RUN   TestIndependentM16StrictYAML/v1-merged-profile
=== RUN   TestIndependentM16StrictYAML/v2-merged-unknown
=== RUN   TestIndependentM16StrictYAML/duplicate-effort
=== RUN   TestIndependentM16StrictYAML/fractional-turn
    independent_m16_sol_overlay_test.go:19: malformed or forbidden execution declaration accepted
--- FAIL: TestIndependentM16StrictYAML (0.00s)
    --- FAIL: TestIndependentM16StrictYAML/v1-merged-null (0.00s)
    --- PASS: TestIndependentM16StrictYAML/v1-merged-profile (0.00s)
    --- PASS: TestIndependentM16StrictYAML/v2-merged-unknown (0.00s)
    --- PASS: TestIndependentM16StrictYAML/duplicate-effort (0.00s)
    --- FAIL: TestIndependentM16StrictYAML/fractional-turn (0.00s)
=== RUN   TestIndependentM16SharedProfileAndGrantUnion
--- PASS: TestIndependentM16SharedProfileAndGrantUnion (0.10s)
FAIL
FAIL github.com/iryzhkov/t3-steward/internal/backlog 0.105s
FAIL
```

Same environment: go test ./internal/backlog -run '^TestIndependentM16FractionalIntegerWitness$' -count=1 -v, session76359, actual exit1. Separate external output and command/exit receipts retained:
```text
=== RUN   TestIndependentM16FractionalIntegerWitness
=== RUN   TestIndependentM16FractionalIntegerWitness/max_turns:_32.9
    independent_m16_sol_overlay_test.go:54: fractional integer accepted and compiled: turns=32 memory=512 scratch=64
=== RUN   TestIndependentM16FractionalIntegerWitness/memory_mb:_512.75
    independent_m16_sol_overlay_test.go:54: fractional integer accepted and compiled: turns=9 memory=512 scratch=64
=== RUN   TestIndependentM16FractionalIntegerWitness/scratch_mb:_64.75
    independent_m16_sol_overlay_test.go:54: fractional integer accepted and compiled: turns=9 memory=512 scratch=64
--- FAIL: TestIndependentM16FractionalIntegerWitness (0.00s)
    --- FAIL: TestIndependentM16FractionalIntegerWitness/max_turns:_32.9 (0.00s)
    --- FAIL: TestIndependentM16FractionalIntegerWitness/memory_mb:_512.75 (0.00s)
    --- FAIL: TestIndependentM16FractionalIntegerWitness/scratch_mb:_64.75 (0.00s)
FAIL
FAIL github.com/iryzhkov/t3-steward/internal/backlog 0.005s
FAIL
ACTUAL_EXIT 1
```

Same environment: go test ./internal/backlog -run '^TestIndependentM16FractionalAdmissionRejected$' -count=1 -v, session35780, actual exit1. Separate external output and command/exit receipts retained:
```text
=== RUN   TestIndependentM16FractionalAdmissionRejected
    independent_m16_sol_overlay_test.go:64: malformed profile admitted and stored with turns=32
--- FAIL: TestIndependentM16FractionalAdmissionRejected (0.06s)
FAIL
FAIL github.com/iryzhkov/t3-steward/internal/backlog 0.063s
FAIL
ACTUAL_EXIT 1
```

Source and compatibility assessment: semantic Huyang reads covered all changed production seams, profile tests, resources validation/normalization and authority/child construction. review_declaration.go custom nested strict decoder correctly rejects unknown profile fields, duplicate effort and nonnil v1 profiles; missing/mixed/unsafe profile tests pass, with the uncovered exceptions above. Domain scalar profile values have no nested maps or pointer fields; CloneReviewExecution copies them. Domain CloneTaskReview, declaredPolicy, admission policy copy, Requirements construction/Snapshot and FrozenAuthority canonicalization detach member profile pointers. Manifest clone detaches explicit numeric pointers; the independent shared-pointer probe verifies sibling isolation even when authored members share one execution pointer. Child Build derives an individual options map and exact pool/turns/resources from validated frozen authority.

admissionMemberMetadata filters grants already eligible for the exact project/instance/model by the exact quota pool, and selected grant provenance enters policy digest. Authored sole wildcard grant semantics remain supported; concrete route required. Unrelated grant changes do not alter policy; live readiness/drain observations do not enter static authority. My additional union-of-inexact-grants refusal passed and loaded records remained equal. Existing focused tests additionally compare every logical SQL/native audit table. Declared replay uses issued authority after catalog removal, and writer guards compare saved v1/v2 declaration and profile equality. Fresh/reopened/read-only terminal replay, per-field complete template/original graph/run-graph/marker tamper and rehashed authority refusal tests passed currently. These prove their isolated logical custody boundaries, not hostile-host or physical-page/distributed atomicity guarantees.

Nil omitempty execution members retain historical field ordering/JSON bytes and legacy policy/requirements golden identity. Current legacy focused tests also confirm old child MaxTurns12, empty options/pool, zero resources and replay. Git diff confirms existing review_child.go and schema unchanged; two former positional AdmissionMember literals changed to keyed form only, with assertions unchanged. No additional byte/digest, quota-authority or copy-isolation blocker found in this bounded lens.

All20 accepted portable/staging paths independently compared by candidate vs parent blob identity and recorded custody OID: the 13 independent declared/fix/repair/stage/terminal files and seven review_child_staging implementation/test files listed in raw log13597–13616. All byte-identical to49b99fd. Both FULL accepted review documents read via Huyang and independently hashed: Sol14862bytes/9237a707c3f38a1cbae31df762b60e2b501c49ad9ca34bf8030a3090d515c8db; Astra22246bytes/5857f3400ca81f1a3f048c802b5e0d03af77c57057632b37ab899cacafe556fa. Each contains exactly one complete Go fence; external gofmt of the entire extracted fence equals its committed independent_fix3 overlay exactly (Sol2090bytes; Astra5528bytes). No unchanged 2000 stress, retained expensive probes, full gate or race rerun.

Bundle/import custody: bfbaf14fe27edecb1d9f8b17aa89ba6d3badbc96acb13e6b28e92f3aed2d892f,441547bytes verified. Sole prerequisite6b0a6798736d3c9277e7688ef3016c690bf7a9ee, sole exportc4287f4867bdad509032f9528c6f556646807ebb HEAD. Initial candidate cat-file -e actual1/absent. git bundle verify/list-heads/hash/size matched. Before workspace import/detach, disposable bare /var/tmp/review-proof-dv26rhqn/proof.git imported baseline then candidate, showed exact commit/tree/sole parent above and full strict fsck actual0. The unreferenced candidate was reported dangling because only FETCH_HEAD existed; no missing objects or corruption. Only then workspace fetch/switch detached (actual0). Current fresh candidate closure and fresh ALL exactly equal source candidate closure,10345 object IDs/types/sizes; no alternates. Git object commands, import and local detach only; no publication.

Raw log custody: read-only direct worker recovery BEFORE cancellation, not coordinator-collected successful producer outcome. Producer run-0085a2f05f2d79f012df05af13f56e59 repeatedly failed aggregate custody publication with stopped executor; cancelled settled13:57:10UTC/runrevision5/attemptrevision6/noassignment. Sink success describes cancellation settlement, not producer acceptance. All five recovered artifact hash/size records match supplied recovery receipt currently. verification.log.xz382640bytes/SHA256c8e38556b9de5cea73f04abd4690609723b71fbed925a9f773d3ff6a444723a8 decompresses losslessly to COMPLETE7827075bytes/SHA256e9b0ba85a13ca8a8dff215b378c3c26f1e1034839ddf6c54f4f0455cd3819eff. Decompressed external /var/tmp/m16-raw.log; all162824 lines scanned programmatically. Meaningful chronology retained externally as1090 lines with repetitive object/fingerprint bodies replaced only after complete audit; bounded117-line chronology summary preserves commands, exits, unique failure diagnostics and corrections. Raw source artifact remains unchanged and complete.

Complete fingerprint audit: parsed all three entire structured pre-gate/post-failed/post-passing objects,1429 tracked entries/1209Go each. All three records exactly equal, including complete logical index and tree. Compared every path/mode/stage/OID/worktreeOID against exact candidate tree and logical current Git index; batch-read every blob to verify all recorded SHA256/size fields. Every tree is a3c8d1e8b09a535abadcf352a2536620450ed705. Historical raw-index hash ffa86578f77dd80843b311047ec02ca47fad443a47b8f1669c9eda4e1b24de73 is not claimed equal to this checkout's raw index bytes. The misleading post-failed label is disproved by full record equality; artifact-only label clarification is present.

Complete object-set audit: parsed ALL12 complete reachable/typed/ALL inventories, with command-exit boundaries; all set cardinalities and exact membership/type/size records independently checked. Source baseline reachable/typed and fresh baseline reachable/typed/ALL each9926; source candidate reachable/typed and fresh candidate reachable/typed/ALL each10345. Historical source ALL pre/post each10708, exactly equal and includes363 unrelated objects preserved. Of these unrelated objects362 are currently available and their recorded types/sizes independently match; one unavailable historical unrelated commit68da255985ce841a0a38f92ee9d8744ba7441079/280bytes remains only a mutually consistent historical receipt, not a current object witness. It is outside required candidate closure. No deletion and no source ALL==closure claim. Historical fresh preimport cat-file exit1 plus batch missing, strict fsck and no-alternates receipts are internally consistent. Current independent fresh closure/type-size/no-alternates/fsck checks corroborate required source closure.

Historical chronology (not current test execution): initial focused selections exited1 three times: SQLite v1 writer guard/immutable graph trigger fixtures, then whole-Task JSON reflect fixture, then mistaken configured availability premise. Diagnostics remain in complete raw log; corrections are consistent with current source. Final broad focused selection exit0 (284.196s). First make check-review FAST_BASE=49b99fd... exit2 (391.725s), Unix socket pathname118bytes bind invalid argument in unchanged t3api; stopped before lint/race. Shorter private GOTMPDIR scoped socket test exit0. Failure-driven same-source gate exit0 (1738.770s) includes build/vet/fullshort/pinned staticcheck/gofmt and one complete five-package race: cmd225.772s/backlog738.413s/domain1.020s/review1.025s/SQLite1102.330s. Historical initial custody transcript appended while race ran is explicitly labeled; not evidence of a source reset during race. Historical artifact-only copy above4MiB refused without mutation, first external split helper failed NotADirectoryError, guarded chunks recovered exact log and final receipts appended. Expected proof-ref absence exit128 is not a failed source proof. Current raw-log audit validates reports' meaningful chronology; it does not turn historical tests into current independent executions.

Huyang workspace ws_65d61e18648cf6a6bd416dedebcb89cf opened; trust unchanged/untrusted. Relative-root opening refusal corrected to absolute root; mounted input symlink refusal handled by authorized external artifact operations. Parser-backed semantic reads used; optional LSP enrichment reported partial/unavailable. Overlay edit receipts reported zero new diagnostics and edited-document coverage only; no full LSP/isolated verification success inferred. Guarded create/replace evidence ev_dc22bb0b258eae57592491f4d5fe7f5a,ev_ff9afc5dc79fb29055540d4594c18efe,ev_0c0f2d2bcab848cd8f41d47d195b53c3; guarded removal ev_36f70a742fa82691fc6c2db21da3cf00, revisionwsrev_38. Foreground Go/Git/external gofmt authorizations used.

Cleanup: overlay removed with exact docrev_7f9d8a2010f68b9f034b2ee9ab05253d8bfffa27223707a6dd7c2d3e0b115a23. AFTER removal git diff --check, git diff --exit-code HEAD, git diff --cached --exit-code, git diff49b99fd HEAD --check all actual0, empty output. Tracked worktree/index clean; initial untracked .t3 remains, review.md/continuation.md are output artifacts. HEAD/tree/sole parent unchanged; all foreground test sessions completed. No source fix, pending review process, live provider/action/admin/config/trust/service/schedule/fleet/push/PR/tag/release changes.

Limits: local-only publishing deferred and schedules disabled. This CHANGES-REQUESTED review withholds acceptance pending both fixes and lead-owned review decisions. Producer remains provisional until BOTH independent lead-owned reviews ACCEPT. Synthetic family metadata never qualifies actual two-provider production policy. No self-waiver or claim of public activation, final physical worker HEAD proof, parking/compiler/quota implementation, CI/deployment, OS restart, scalability or fullM16/M17. SQLite reopening is not OS restart. Costs/token totals unavailable.
