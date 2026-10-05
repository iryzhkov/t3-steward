Independent M16 execution profiles repair1 review — ACCEPT

Exact commit: 7751d7c29c1b4e2d74a44245d257717904a5b9a2
Tree: 32347d3cfbb42be02994f7701df9a59f90b6a0be
Sole parent: c4287f4867bdad509032f9528c6f556646807ebb
Reviewer: Codex GPT-6-Astra, medium. Independent task task-e1565e5d2f39be3b71a2201b71c5322f in run-f2ed276fb22f2b77b729f156230c9f4e. No delegation, nested review, Claude calls or escalation.

Verdict ACCEPT for this exact local candidate within the controlling contract. No new deduplicated blocking findings (P0/P1/P2: zero). Original fractional integer loss and merged-null omission blockers are resolved, with every original portable assertion retained and passing. This is this reviewer's verdict only: BOTH independent reviews must accept before lead-owned candidate acceptance. I did not read the sibling current review or import/review the collection candidate.

Controlling inputs and scope

Read the complete supplied profile-review-plan.md (jocasta:c55906d4046c0c0b99487f21b272ea19@1), profile-fix1-plan.md (jocasta:bd5820085a8808f9a424cbb86c55000b@1), profile-original-plan.md (jocasta:b694668a6fba8a4e1329a658fcf6e0e5@1), profile-handoff.md and profile-contract.md. Read BOTH complete original review documents, including all four full Go fences, from supplied inputs and permanent docs/m16-execution-profiles/fix1-review-{sol,astra}.md. Full equality of supplied/permanent documents and formatted fences was independently checked. Broad initial tool output was truncated; bounded reads supplied the missing prose instead of treating truncation as a complete read.

Huyang workspace ws_29f9e49e41364a7c489dd17fd6939a8e opened at baseline wsrev_1. Mounted inputs traverse an external symlink; Huyang refused that path, and resolved external artifact reads followed. Repository reads and disposable create/removal used Huyang; foreground Git/Go/external evidence operations used the explicitly authorized route. Trust remains untrusted and unchanged. No Huyang isolated verification or full LSP success claim.

Input identity receipts (bytes / SHA256):
- profile-review-plan.md: 4050 / dd22e4a96747bf7c1e9ad11bcc5a0a14add65f42a1d216e7ec78026b5673a872
- profile-fix1-plan.md: 6276 / 157a169733f0c3aac61c6c936d1c48338e569ed2ac3c4862c3fcf218d5c0cb8e
- profile-original-plan.md: 9972 / 0e6f0bf98a688f6cc5cf4282063c533f08267d063dd38b4ca2a1091b745c57fd
- profile-handoff.md: 9903 / 77f523d0892fc425d34d41b81dec3c355fc9e7e07b8da4fecfd7aaa675e3aaec
- profile-contract.md: 3862 / 948c13a1601ce1fb67c06bc807b1ddda165eae7a091ce7936d5c202b0b708d26
- profile-prior-review-sol.md: 21564 / cbdd60cb38c9c5642d46e8583ab66007b7a6e78a364fe8cc5648f611b946c27c
- profile-prior-review-astra.md: 26352 / 7eb98cd845cb27933ed57a6465d2aaf47bed1a01452d9f83f81b9aaae1f83df8

Bundle and independent object proof

Initial checkout HEAD was exactly 6b0a6798736d3c9277e7688ef3016c690bf7a9ee, tracked/index clean. Before import, verified profile-candidate.bundle exactly473337 bytes/SHA256 d523978203f3e1cafeee1c7a1084d47ce49b55a4aa1671747cec7e73edab69b0; header has sole prerequisite6b0a6798736d3c9277e7688ef3016c690bf7a9ee and sole export7751d7c29c1b4e2d74a44245d257717904a5b9a2 HEAD. git bundle verify actual0.

Created fresh bare /var/tmp/profile-proof-w6vvhnpr/fresh.git using git init --bare and a pack generated ONLY from baseline via git pack-objects --stdout --revs, imported using index-pack --stdin; no clone/shared object database and no alternates. Baseline ref installed; candidate cat-file -e refused before import. Baseline strict fsck passed. Bundle verify/fetch, candidate ref, full strict fsck passed. Only then workspace fetch/detach, exact tree/sole-parent assertion and source strict fsck passed. All command assertions actual0; expected candidate-absence command nonzero.

Full current source candidate reachable IDs/types/sizes equal fresh candidate reachable IDs/types/sizes and fresh ALL, exactly10363 objects, zero missing. Baseline closure9926 was also independently compared in the log audit. No equality claim between source ALL and candidate closure, no deletion of unrelated objects. The proof receipt remains /var/tmp/profile-proof-receipt.txt.

Parser and immutable-authority review

internal/backlog/review_declaration.go:59–78 validates the authored node graph and effective execution values before yaml.Marshal/strict KnownFields decode. internal/backlog/review_execution_yaml.go:9–151 bounds graph traversal, preserves explicit-null presence and follows effective aliases/merges. Direct keys beat merged keys; first merge-sequence source wins. Integer validation requires scalar !!int, at most128 spelling bytes, and successful machine-int decode before the lossy destination decoder can run. Existing profile validation subsequently enforces turns1..32 and positive declared integer resources. CPU units still allow finite positive fractional demand. Global ManifestResources/schema were not tightened.

Cycle detection uses an active path, so shared acyclic aliases remain valid and exponential repeated visits are bounded. Full member expansion stops above4096 visits/depth64; lookup bounds are32768 visits and64 alias links. The new real YAML DAG probe below accepts a small shared merge and refuses an exponentially expanded one; direct node probes verify inclusive limits. Unknown fields in later merge-sequence mappings, duplicate keys within merges, duplicate merge keys and indirect cycles refuse. The strict downstream decoder is retained rather than replaced with a permissive flattened decoder. Permanent direct-override/sequence-first tests also accept an overridden malformed integer when its effective value is a valid integer, consistent with the specified decoder precedence.

All unchanged original probes now pass, including max_turns32.9, memory512.75, scratch64.75 and actual permanent BundleIngester admission refusal. Permanent parser tests compare every logical SQL/native audit table on refusal. Legacy absent-profile canonical bytes/digests, unrelated scalar/ordinary merge behavior and v1 Build12/empty-options/pool/resources remain covered. Version1 merged effective null now refuses; version2 requires explicit validated profiles. No new defaults or serialized profile fields for legacy nil profiles.

Immutable seams read: domain/review_execution.go validates and scalar-copies normalized profiles; backlog/review_execution_admission.go filters already exact project/instance/model grants to the explicit quota pool and detaches manifest pointers; review_admission.go:165–205 clones profiles and freezes selected grants into the policy digest, with exact route grant matching at325–384. review_declared_admission.go:58–70 reuses issued authority/provenance after catalog changes. review/authority.go:41–110 clones on construction and Snapshot and rejects mixed profiles. No readiness observation becomes static authorization.

review/child_graph.go:75–159 canonicalizes authority and derives exact turns/resources/pool and a fresh effort-only options map. sqlite/review_declared_authority.go:74–94 compares saved declaration digest and complete member profiles; review_authority.go:144–178 invokes that check under the writer before initial freeze and replay. Existing independent writer probes rehash modified authority and still refuse before and after freeze without logical SQL/native audit writes. sqlite/review_child.go:43–190 builds from stored authority and compares the whole replay receipt; :565–611 checks original graph/current run graph and complete task definitions. These production custody seams are unchanged by repair1.

Current permanent tests include exact grants/union refusal, catalog removal plus separately opened SQLite, source/snapshot/graph pointer copies, every profile field in digests, fresh/reopened/terminal replay and tamper of every scalar across task/original graph/run graph/receipt. Additional returned-value mutation probe below combines profile, options, pool, turns and resources mutations, then closes/reopens read-only and verifies exact original Build and unchanged logical SQL/native audit. No new authority-laundering, copy-isolation or replay blocker found.

Original portable preservation

Each entire original Go fence was extracted, passed to external gofmt, and compared byte-for-byte to Huyang-read permanent source. All four matched:
- internal/backlog/independent_m16_sol_overlay_test.go:4220 /2740f354d3eeef1dcb19fde769a00fb1f7daa295ca266b42ebe7d5503e7a1852 (all FOUR functions).
- internal/backlog/independent_profile_astra_test.go:2105 /da53c715b5b34992493a8cabda9415d26f7dc49715e222e41b01e7abffa1a150.
- internal/store/sqlite/independent_profile_astra_test.go:2596 /6c3a7baf755542e3d4933a5f360d91ef278a3fcb1dc11978523fae87cee92b59.
- internal/backlog/independent_profile_yaml_astra_test.go:651 /7e460246a31c2ef45b3a72579412d351645feb6138929ba2c4b6e54cda4024ba.

All39 PRESERVED_OLD_PORTABLE_STAGING path/OID records in the complete raw log were checked against BOTH parent and current index blob identities; all identical. Accepted staging assertions/bounds unchanged. No unchanged stress suite rerun.

Historical evidence audit, distinctly not current reexecution

profile-verification.log.xz exactly414500 bytes/SHA25613a0b7a3037b5a9142cd9305f90a94911a10607dc6512c1f7c0c2ec432c55720 losslessly decompresses to7002652 bytes/95520 lines/SHA2566f2f0a419f7c39127451b71786dee00aad8afc6e7d951dbac48d872374d94f6b. Full raw retained externally /var/tmp/profile-review-raw.log, never truncated. Meaningful command/failure/probe/gate chronology read in full after separating repetitive test banners/object rows/full fingerprint JSON; bounded chronology /var/tmp/profile-chronology.txt has424 lines. Structured data comparisons consumed full records, not context excerpts.

Historical before-repair original probes exit1/8.680s retain fractional truncation, real admission32.9→32 and merged-null failures; writer/reopened/copy/grant positives pass. Initial after-repair full focused selection exit0/10.457s. Parser-only test exit1/5.255s retained duplicate-role fixture error; corrected fixture then expanded full focused selection exit0/9.548s; final additions focused exit0/10.032s. No assertion weakening inferred or found.

ONE historical final make check-review FAST_BASE=c4287f4867bdad509032f9528c6f556646807ebb, actual0/1429.641s, includes go build, go vet, complete short suite, staticcheck v0.7.0 under go1.25.0, gofmt and full changed-package race (backlog746.468s, SQLite1111.448s). Read-only portable/baseline proof output was appended while that foreground gate ran; its buffered full gate output follows at raw22079–22121. This is not a second gate. No current full gate/race rerun.

Parsed all THREE complete FULL_SOURCE_LOGICAL_INDEX_TREE JSON records pre-gate/post-gate/final-source. Full equality holds:1438 tracked entries,1215Go, complete logical index, tree32347d3. Batch-read every recorded Git blob and independently verified size/SHA256/indexOID/worktreeOID. Complete current logical index and final git hash-object over ALL1438 worktree paths match those full records. Raw index hash d25473120757a4fad747ca99ca56388abdbbdb5330a830c17b9db4dcecd8ff76 remains a historical observation, not a claim of current raw-index metadata byte equality.

Parsed ALL NINE full object inventory blocks. Baseline reachable/ALL9926, source/fresh candidate reachable/typed and freshALL10363 all match exact current IDs/types/sizes. Historical sourceALL10726 pre/post sets equal and include363 outside-closure objects. Of those,362 current objects corroborate recorded types/sizes; unrelated historical commit344bc126ed7a058be278d8aa90f74a0600f6045d (280 bytes) is unavailable locally and supported only by consistent historical inventories. It is outside the candidate closure. No fabricated direct read of that object or sourceALL==closure assertion.

The earlier ORIGINAL producer's full raw historical log/recovery artifacts are not supplied here; their history is described in the two complete original review reports, which I read, but I do not claim independently reading/reexecuting those unavailable historical artifacts. Original cancelled-producer recovery is not promoted to successful coordinator custody. Current review independently audits the supplied repair1 log only, plus fresh current source/test/object evidence. Hash agreement authenticates supplied byte identities, not remote journal execution.

Current witnessed commands

All Go work foreground to completion, private0700 GOTMPDIR=/var/tmp/r.t527ov54, GOMAXPROCS=2 GOFLAGS=-p=2. Required command exactly ONCE:
```sh
go test ./internal/review ./internal/domain ./internal/backlog ./internal/store/sqlite ./cmd/t3-steward -run '^TestReviewExecutionProfile|^TestIndependent(M16|Profile)' -count=1
```
Actual Go exit0,14.046335s, foreground session98026 completed. Domain is compile coverage only, no matching tests.
```text
ok  	github.com/iryzhkov/t3-steward/internal/review	0.003s
ok  	github.com/iryzhkov/t3-steward/internal/domain	0.003s [no tests to run]
ok  	github.com/iryzhkov/t3-steward/internal/backlog	1.450s
ok  	github.com/iryzhkov/t3-steward/internal/store/sqlite	3.747s
ok  	github.com/iryzhkov/t3-steward/cmd/t3-steward	0.425s
```

Additional bounded probes, actual Go exit0/5.994321s, foreground session4976 completed:
```sh
go test ./internal/backlog ./internal/store/sqlite -run '^TestRepair1Astra' -count=1 -v
```
```text
=== RUN   TestRepair1AstraAliasExpansionAndStrictMerges
=== RUN   TestRepair1AstraAliasExpansionAndStrictMerges/3
    repair1_astra_probe_test.go:19: depth=3 err=<nil>
=== RUN   TestRepair1AstraAliasExpansionAndStrictMerges/13
    repair1_astra_probe_test.go:19: depth=13 err=decode workflow manifest: review member YAML exceeds node/depth limits
=== RUN   TestRepair1AstraAliasExpansionAndStrictMerges/duplicate-merge-key
    repair1_astra_probe_test.go:30: decode workflow manifest: yaml: unmarshal errors:
          line 1: mapping key "<<" already defined at line 1
=== RUN   TestRepair1AstraAliasExpansionAndStrictMerges/mutual-effective-cycle
    repair1_astra_probe_test.go:30: decode workflow manifest: review member YAML contains a cycle
=== RUN   TestRepair1AstraAliasExpansionAndStrictMerges/duplicate-in-merge
    repair1_astra_probe_test.go:30: decode workflow manifest: yaml: unmarshal errors:
          line 1: mapping key "required" already defined at line 1
=== RUN   TestRepair1AstraAliasExpansionAndStrictMerges/unknown-after-first-sequence
    repair1_astra_probe_test.go:30: decode workflow manifest: field unknown (line 1) is not supported by this release dev; a newer t3-steward release may be required
        yaml: unmarshal errors:
          line 1: field unknown not found in this object
--- PASS: TestRepair1AstraAliasExpansionAndStrictMerges (0.00s)
    --- PASS: TestRepair1AstraAliasExpansionAndStrictMerges/3 (0.00s)
    --- PASS: TestRepair1AstraAliasExpansionAndStrictMerges/13 (0.00s)
    --- PASS: TestRepair1AstraAliasExpansionAndStrictMerges/duplicate-merge-key (0.00s)
    --- PASS: TestRepair1AstraAliasExpansionAndStrictMerges/mutual-effective-cycle (0.00s)
    --- PASS: TestRepair1AstraAliasExpansionAndStrictMerges/duplicate-in-merge (0.00s)
    --- PASS: TestRepair1AstraAliasExpansionAndStrictMerges/unknown-after-first-sequence (0.00s)
=== RUN   TestRepair1AstraExactNodeBudgets
    repair1_astra_probe_test.go:53: 4096 visits, depth64, and 32768 lookup visits exact inclusive boundaries confirmed
--- PASS: TestRepair1AstraExactNodeBudgets (0.00s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/backlog	0.006s
=== RUN   TestRepair1AstraReturnedProfileIsolationReopened
    repair1_astra_probe_test.go:30: mutated returned profile/options/pool/turns/resources detached; reopened original Build replay exact; all logical tables unchanged
--- PASS: TestRepair1AstraReturnedProfileIsolationReopened (0.09s)
PASS
ok  	github.com/iryzhkov/t3-steward/internal/store/sqlite	0.091s
```

Full portable additional witnesses

These use existing fixture helpers in this exact candidate. Exact executed bytes follow, no production edits. Recreate using guarded Huyang at the stated paths and run the additional bounded command above. Both files removed through exact original document revisions after execution. No assertions changed or retries needed.

internal/backlog/repair1_astra_probe_test.go:
```go
package backlog

import (
 "fmt"
 "strings"
 "testing"
 "gopkg.in/yaml.v3"
)

func TestRepair1AstraAliasExpansionAndStrictMerges(t *testing.T) {
 for _, depth := range []int{3, 13} {
  t.Run(fmt.Sprint(depth), func(t *testing.T) {
   nested := "&a0 {required: true}"
   for i:=1; i<=depth; i++ { nested=fmt.Sprintf("&a%d {<<: [%s, *a%d]}",i,nested,i-1) }
   raw:=strings.Replace(declaredManifestYAML(),"required: true","<<: "+nested,1)
   _,err:=ParseManifest([]byte(raw))
   if depth==3 && err!=nil {t.Fatal("small acyclic shared merge refused",err)}
   if depth==13 && (err==nil || !strings.Contains(err.Error(),"limits")) {t.Fatal("expanded alias visit budget not enforced",err)}
   t.Logf("depth=%d err=%v",depth,err)
  })
 }
 for name, fields:=range map[string]string{
  "duplicate-in-merge":"<<: {required: true, required: false}",
  "unknown-after-first-sequence":"<<: [{required: true}, {unknown: true}]",
  "duplicate-merge-key":"<<: {required: true}, <<: {required: false}",
  "mutual-effective-cycle":"<<: &a {<<: &b {<<: *a}}",
 } {
  t.Run(name,func(t *testing.T) {
   raw:=strings.Replace(declaredManifestYAML(),"required: true",fields,1)
   if _,err:=ParseManifest([]byte(raw));err==nil {t.Fatal("unsafe merge accepted")} else {t.Log(err)}
  })
 }
}

func TestRepair1AstraExactNodeBudgets(t *testing.T) {
 for _,count:=range []int{4095,4096} {
  n:=&yaml.Node{Kind:yaml.SequenceNode}
  leaf:=&yaml.Node{Kind:yaml.ScalarNode,Tag:"!!str",Value:"x"}
  for i:=0;i<count;i++ {n.Content=append(n.Content,leaf)}
  err:=validateReviewMemberNodes(n)
  if (err==nil)!=(count==4095) {t.Fatalf("count=%d err=%v",count,err)}
 }
 leaf:=&yaml.Node{Kind:yaml.ScalarNode,Tag:"!!int",Value:"9"}
 for _,depth:=range []int{64,65} {
  n:=leaf
  for i:=0;i<depth;i++ {n=&yaml.Node{Kind:yaml.AliasNode,Alias:n}}
  err:=validateReviewMemberNodes(n)
  if (err==nil)!=(depth==64) {t.Fatalf("depth=%d err=%v",depth,err)}
 }
 lookup:=reviewNodeLookup{visits:32767}
 if _,err:=lookup.resolve(leaf);err!=nil {t.Fatal(err)}
 if _,err:=lookup.resolve(leaf);err==nil {t.Fatal("lookup budget overflow accepted")}
 t.Log("4096 visits, depth64, and 32768 lookup visits exact inclusive boundaries confirmed")
}
```

internal/store/sqlite/repair1_astra_probe_test.go:
```go
package sqlite

import (
 "context"
 "reflect"
 "testing"
)

func TestRepair1AstraReturnedProfileIsolationReopened(t *testing.T) {
 s,f,cp,p,_:=executionChildFixture(t)
 ctx:=context.Background()
 first,err:=s.MaterializeReviewChild(ctx,f,cp,p);if err!=nil {t.Fatal(err)}
 original,err:=f.Canonical();if err!=nil {t.Fatal(err)}
 before:=executionSQLSnapshot(t,s)
 first.Authority.Requirements.Members[0].Execution.QuotaPoolID="returned-only"
 first.Authority.Requirements.Members[0].Execution.Resources.MemoryMB++
 first.Graph.Tasks[0].Routes[0].Options["effort"]="high"
 first.Graph.Tasks[0].Routes[0].QuotaPoolID="returned-only"
 first.Graph.Tasks[0].MaxTurns++
 first.Graph.Tasks[0].ResourceDemand.MemoryMB++
 if before!=executionSQLSnapshot(t,s) {t.Fatal("return-value mutation changed durable records")}
 path:=s.path
 if err=s.Close();err!=nil {t.Fatal(err)}
 reopened,err:=OpenReadOnly(path);if err!=nil {t.Fatal(err)}
 defer reopened.Close()
 replay,err:=reopened.MaterializeReviewChild(ctx,original,cp,p);if err!=nil {t.Fatal(err)}
 expected,err:=p.Build(original,cp,replay.Graph.Run.CreatedAt);if err!=nil {t.Fatal(err)}
 if !reflect.DeepEqual(replay.Graph,expected) {t.Fatal("reopened graph changed after returned profile mutation")}
 if before!=executionSQLSnapshot(t,reopened) {t.Fatal("read-only replay changed logical SQL/native audit")}
 t.Log("mutated returned profile/options/pool/turns/resources detached; reopened original Build replay exact; all logical tables unchanged")
}
```

Cleanup and limits

Guarded removal revisions docrev_88c82362a1fa6dbd3c546a66939d991f4f489bf2a01dc2d80b504a826b6dd6e1 and docrev_7b5987f2938392246492ff6aa168bd7fd2f9a10707492378c9cdcc1c8057912c; Huyang removal reached wsrev_42. After removal git diff --exit-code HEAD, git diff --cached --exit-code and git diff HEAD^ HEAD --check each actual0; all tracked worktree/index identities match candidate. Only mounted .t3 and declared prose outputs remain untracked. All started command sessions complete; explicit process scan found no pending own Go/test processes. No background jobs started. Output prose each below256KiB.

No production fix, source commit, publication/PR/tag/release/CI/fleet/trust/config/service/schedule mutations. Publishing deferred; schedules disabled/unchanged. Local ACCEPT does not claim fullM16, public activation, final physical worker HEAD, compiler/M17, diversity qualification, OSrestart or scalability. Reopened SQLite is not OS restart; logical SQL/native audit equality is not physical-page equality or distributed atomicity. Existing hostile-write limitations remain. Production two actual provider-family policy is unwaived by synthetic fixture metadata or two Codex reviews. Cost/token totals unavailable.
