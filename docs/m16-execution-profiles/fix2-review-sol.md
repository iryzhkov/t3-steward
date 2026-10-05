Independent M16 execution profiles repair1 review

Verdict: CHANGES-REQUESTED for exact commit 7751d7c29c1b4e2d74a44245d257717904a5b9a2.
One deduplicated P2 / medium blocking contract violation. No production fixes.

Reviewer: GPT-6.1-Sol, Codex harness, medium. Independent declared Sol lens: effective parser lookup/merge precedence, legacy canonical/scalar compatibility, and real permanent admission refusal before logical SQL/native audit writes. No delegation, nested review, Claude calls or escalation. This is this reviewer's verdict only; local acceptance requires BOTH independent reviews. No sibling review consulted.

Controlling review plan jocasta:c55906d4046c0c0b99487f21b272ea19@1, supplied profile-review-plan.md; repair plan jocasta:bd5820085a8808f9a424cbb86c55000b@1; original plan jocasta:b694668a6fba8a4e1329a658fcf6e0e5@1. Read all three FULL plans, FULL current profile-handoff.md/profile-contract.md, both FULL supplied original profile reports and both permanent original report documents. Initial broad tool deliveries were truncated and were not treated as complete reads; bounded follow-up windows completed report/source reading. Repetitive complete structured log records were compared programmatically, not dumped into context.

R1 — P2 / medium, blocking: YAML field lookup compares encoded key spelling while strict yaml.v3 decoding uses the decoded key.

Location: internal/backlog/review_execution_yaml.go:65–73 (reviewNodeLookup.field), affecting reviewExecutionNodePresence:119–148 and ManifestReviewMember.UnmarshalYAML at internal/backlog/review_declaration.go:58–76. The helper permits any ScalarNode key and compares key.Value to the desired name. A !!binary scalar contains base64 spelling in Value but decodes to a string key. Therefore !!binary bWF4X3R1cm5z is max_turns for the downstream decoder and is invisible to the integer guard. The same disagreement at execution/resources skips their nested checks or loses executionDeclared.

Actual minimal changes to valid existing fixtures:
- max_turns: 9 -> !!binary bWF4X3R1cm5z: 32.9 accepts and compiles/stores maxTurns=32.
- memory_mb: 512 -> !!binary bWVtb3J5X21i: 512.75 accepts and stores memoryMb=512.
- scratch_mb: 64 -> !!binary c2NyYXRjaF9tYg==: 64.75 accepts and stores scratchMb=64.
- Binary-tagged execution or resources key likewise hides ordinary fractional nested values.
- Replacing the turns field with <<: {!!binary bWF4X3R1cm5z: 32.9} reproduces the lossy admission through a merge.
- A version1 member with !!binary ZXhlY3V0aW9u: null is accepted as legacy absence.

ALL seven witnessed cases also pass the REAL BundleIngester with its permanent declarationValidator and configured catalog. The resulting profile/legacy member is printed below; every case changes the complete logical SQL/native audit snapshot. The snapshot helper enumerates every SQLite table from sqlite_master, serializes every row/column, sorts rows, and includes native audit. This is a demonstrated refusal-before-write failure, not merely a parser-only complaint. It is not evidence of quota grant laundering: the accepted lossy profile still satisfies normalized static grants.

The seven manifestations share one key-identity mismatch and count as ONE finding, encompassing the original integer-loss and legacy-presence invariants. Make predecode lookup agree with the decoder's effective key identity, or explicitly refuse unsafe tagged-key forms at the relevant boundary before decode/admission. Maintain explicit override/merge sequence precedence, decoded duplicate/unknown refusal, bounded traversal, and ordinary unrelated legacy scalar/merge/canonical compatibility. Do not globally tighten ManifestResources or change SQL schema. Retain these unchanged portable refusal assertions after repair, alongside all four original probes. No repair was made here.

Current source/behavior assessment:
The node walk uses active-path cycle detection, depth64 and visit4096 before marshal. Lookup has32768 resolve visits and alias64 limits; integer spellings are bounded128 bytes and scalar !!int plus machine-int Decode precedes profile range/positivity checks. Effective direct keys beat merged keys and the first merge sequence mapping wins for ordinary string keys. Null presence is distinct from value. Downstream marshal + strict KnownFields remains responsible for unknown/duplicate decoding. Original ordinary fractional and merged-null regressions all pass currently; they do not cover R1's decoded key identity.

The additional nested precedence control below passes: nested merge sequence first source wins, an explicit valid turns value overrides an earlier fractional merged value, resource explicit scalars override fractional merged scalars, CPU1.5 remains exact, and nested legacy merge canonical declaration bytes remain equal. Existing current focused golden tests preserve legacy policy/declaration/requirements bytes and digests, old Build defaults and replay. The global resources/schema seams and production profile authority/Build/replay paths have no repair1 diff.

Huyang reads covered the full repaired production files/tests and relevant admission/ingestion/domain/authority/Build seams. BundleIngester.Ingest validates ParseManifest and Permanent before creating durable records; R1 reaches SaveCoordinatorRecords with already truncated profile values. Static admission filters project/instance/model grants before exact pool selection; selected grant provenance enters immutable policy identity. Declared replay retains issued authority; profile pointer cloning in manifest conversion/domain/requirements Snapshot remains detached. Build uses the frozen profile's exact effort-only options, pool, turns and ResourceDemand. Required current selection includes unchanged original writer rehashed-override, copy/grant controls, full profile digest/isolation tests and fresh/reopened SQLite mutation-refusal/replay controls. No additional provenance, grant or clone defect demonstrated in this bounded lens.

Identity and inputs:
Initial checkout was exactly baseline 6b0a6798736d3c9277e7688ef3016c690bf7a9ee. Before workspace import, bundle SHA256, size, header prerequisite/export and bundle verify matched. Only profile-candidate.bundle imported; sibling collection bundle/source was neither imported nor reviewed. Detached exact commit7751d7c29c1b4e2d74a44245d257717904a5b9a2, tree32347d3cfbb42be02994f7701df9a59f90b6a0be, soleparentc4287f4867bdad509032f9528c6f556646807ebb. Workspace candidate absence before initial fetch was not separately witnessed; fresh independent preimport absence below WAS witnessed. No claims of unavailable historical original inputs or original cancelled producer reexecution.

Current full input hash/size receipts (these current repair handoff/contract differ from the older handoff/contract identities quoted inside the producer's historical log):

| Input | Bytes | SHA256 |
| --- | ---: | --- |
| profile-candidate.bundle | 473337 | d523978203f3e1cafeee1c7a1084d47ce49b55a4aa1671747cec7e73edab69b0 |
| profile-contract.md | 3862 | 948c13a1601ce1fb67c06bc807b1ddda165eae7a091ce7936d5c202b0b708d26 |
| profile-fix1-plan.md | 6276 | 157a169733f0c3aac61c6c936d1c48338e569ed2ac3c4862c3fcf218d5c0cb8e |
| profile-handoff.md | 9903 | 77f523d0892fc425d34d41b81dec3c355fc9e7e07b8da4fecfd7aaa675e3aaec |
| profile-original-plan.md | 9972 | 0e6f0bf98a688f6cc5cf4282063c533f08267d063dd38b4ca2a1091b745c57fd |
| profile-prior-review-astra.md | 26352 | 7eb98cd845cb27933ed57a6465d2aaf47bed1a01452d9f83f81b9aaae1f83df8 |
| profile-prior-review-sol.md | 21564 | cbdd60cb38c9c5642d46e8583ab66007b7a6e78a364fe8cc5648f611b946c27c |
| profile-review-plan.md | 4050 | dd22e4a96747bf7c1e9ad11bcc5a0a14add65f42a1d216e7ec78026b5673a872 |
| profile-verification.log.xz | 414500 | 13a0b7a3037b5a9142cd9305f90a94911a10607dc6512c1f7c0c2ec432c55720 |

Lossless log and historical audit:
XZ414500bytes/13a0b7a3037b5a9142cd9305f90a94911a10607dc6512c1f7c0c2ec432c55720 fully decompressed to raw7002652bytes/95520lines/6f2f0a419f7c39127451b71786dee00aad8afc6e7d951dbac48d872374d94f6b. Complete external raw retained /var/tmp/sol-repair-review-a64zar7o/raw.log. All lines classified; complete meaningful command/probe/failure/gate chronology read from424-line external chronology.log, with repetitive test pass banners, object rows and parsed fingerprints separated. Complete data, not an excerpt, was used for inventory/fingerprint comparisons.

Historical BEFORE unchanged portable probe selection actual1/8.680270900949836s: fractional32.9 admission stores32; fractional turns/memory/scratch assertions and legacy mapping/sequence null assertions fail. Writer rehashed override and reopened corruption positives pass. AFTER repair broad focused selection actual0/10.456929892010521s. Parser-only actual1/5.255017225979827s is retained: duplicate role introduced in the unrelated scalar fixture. The fixture was corrected, not the duplicate protection; expanded broad selection actual0/9.548084633017424s, then final expanded focused selection actual0/10.0318265070091s. Domain had no matching tests and provides compile coverage only.

ONE historical make check-review FAST_BASE=c4287f4867bdad509032f9528c6f556646807ebb actual0/1429.6410894849687s: build, vet, complete short suite, pinned go1.25.0 staticcheck v0.7.0, gofmt, full-size changed-package race backlog746.468s and SQLite1111.448s. Gate output is buffered after portable/fresh-baseline audit receipts appended while it ran. Read the full gate output and command/actual exit at raw2150 and22079–22121. No whole gate/race was reexecuted here; no historical test was promoted to a current independent witness.

All THREE complete pre-gate/post-gate/final-source fingerprints parsed and exactly equal:1438 tracked entries/1215Go, full logical index130996bytes, tree32347d3cfbb42be02994f7701df9a59f90b6a0be. Independently batch-read every Git blob and verified each recorded SHA256/byte count/worktreeOID/indexOID; complete logical index equals current git ls-files --stage. Raw index SHA256 d25473120757a4fad747ca99ca56388abdbbdb5330a830c17b9db4dcecd8ff76 remains a historical metadata observation, not a claim of raw index byte equality to this checkout.

All NINE substantive full inventories plus the tenth one-row preimport missing response compared. Baseline reachable/freshALL9926; source candidate reachable/typed and fresh candidate reachable/typed/ALL10363; historical sourceALL pre/post10726 exactly equal, including363 unrelated objects. Candidate sets/types/sizes match current complete candidate closure; full baseline typed inventory also matches current baseline and fresh database. Of the363 extras,362 are locally available with matching types/sizes. Historical unrelated commit344bc126ed7a058be278d8aa90f74a0600f6045d/280bytes is unavailable here and only supported by mutually equal historical ALL receipts. It is outside required closure and was not fabricated/imported/deleted. SOURCE ALL is not equated to candidate closure.

Initial external audit.py execution exited1 because my inventory-count assertion expected9 but also counted the valid single-row preimport missing response. Complete fingerprints were already checked; partial output was not accepted as finished audit. Corrected the assertion to count all10 and require the missing response, then complete audit.py actual0. This was audit-tool correction only, no production/probe edit or repeated Go check.

Independent fresh current proof:
Created external /var/tmp/sol-repair-review-a64zar7o/fresh.git as an empty bare Git database. Fetch ONLY baseline ref from current repository: complete ALL exactly9926 baseline objects, no alternates; strict fsck actual0. Candidate cat-file -e actual1 and batch response missing BEFORE bundle import. Bundle verify/fetch HEAD:refs/heads/candidate actual0; fresh full strict fsck and source full strict fsck actual0. Fresh candidate commit/tree/sole parent match; complete fresh reachable typed closure = freshALL = current source typed candidate closure10363 IDs/types/sizes, zero missing. This is independent current import/fsck evidence, distinct from historical fresh proof. No unrelated source objects removed.

Permanent original probes and staging:
Both entire permanent docs equal supplied original reports byte-for-byte:
Sol21564bytes/cbdd60cb38c9c5642d46e8583ab66007b7a6e78a364fe8cc5648f611b946c27c.
Astra26352bytes/7eb98cd845cb27933ed57a6465d2aaf47bed1a01452d9f83f81b9aaae1f83df8.
Extracted ALL FOUR COMPLETE Go fences and canonicalized the FULL fences with external gofmt actual0; equality with committed files asserted, with all functions/assertions retained:
- internal/backlog/independent_m16_sol_overlay_test.go4220bytes/2740f354d3eeef1dcb19fde769a00fb1f7daa295ca266b42ebe7d5503e7a1852 (all four Sol functions).
- internal/backlog/independent_profile_astra_test.go2105bytes/da53c715b5b34992493a8cabda9415d26f7dc49715e222e41b01e7abffa1a150.
- internal/store/sqlite/independent_profile_astra_test.go2596bytes/6c3a7baf755542e3d4933a5f360d91ef278a3fcb1dc11978523fae87cee92b59.
- internal/backlog/independent_profile_yaml_astra_test.go651bytes/7e460246a31c2ef45b3a72579412d351645feb6138929ba2c4b6e54cda4024ba.
All39 recorded preexisting portable/staging/evidence paths have exact unchanged parent/candidate blob identities; all37 that exist at accepted49b99fd also match that accepted commit. Thus accepted staging assertions/bounds are unchanged. No stress2000 or other expensive unchanged probe rerun.

Current commands and complete actual outputs:
Private GOTMPDIR=/var/tmp/r.3xyy__ms, mode0700 asserted; GOMAXPROCS=2 GOFLAGS=-p=2. Required focused command executed ONCE before adding this review overlay:
go test ./internal/review ./internal/domain ./internal/backlog ./internal/store/sqlite ./cmd/t3-steward -run '^TestReviewExecutionProfile|^TestIndependent(M16|Profile)' -count=1
Actual0/13.473845542001072s. Domain no matching tests means compile coverage only.

Bounded added probes ran foreground:
go test ./internal/backlog -run '^TestIndependentRepair1' -count=1 -v
Actual1/6.724820483010262s. This prefix also selected two preexisting small repair1 epoch/cleanup tests, both passed; it did not rerun gate/race or full stress. The two new functions are the full portable file below. No assertions were weakened, no pass-seeking rerun.

Complete command/exit receipts and stdout/stderr:
```text
{"argv": ["go", "test", "./internal/review", "./internal/domain", "./internal/backlog", "./internal/store/sqlite", "./cmd/t3-steward", "-run", "^TestReviewExecutionProfile|^TestIndependent(M16|Profile)", "-count=1"], "env": {"GOTMPDIR": "/var/tmp/r.3xyy__ms", "GOMAXPROCS": "2", "GOFLAGS": "-p=2"}, "exit": 0, "seconds": 13.473845542001072}ok  	github.com/iryzhkov/t3-steward/internal/review	0.003s
ok  	github.com/iryzhkov/t3-steward/internal/domain	0.003s [no tests to run]
ok  	github.com/iryzhkov/t3-steward/internal/backlog	1.166s
ok  	github.com/iryzhkov/t3-steward/internal/store/sqlite	3.124s
ok  	github.com/iryzhkov/t3-steward/cmd/t3-steward	0.303s
{"argv": ["go", "test", "./internal/backlog", "-run", "^TestIndependentRepair1", "-count=1", "-v"], "exit": 1, "seconds": 6.724820483010262}=== RUN   TestIndependentRepair1EpochReceiptBoundary
=== RUN   TestIndependentRepair1EpochReceiptBoundary/terminal-park/none
=== RUN   TestIndependentRepair1EpochReceiptBoundary/terminal-park/wrong-observation
=== RUN   TestIndependentRepair1EpochReceiptBoundary/terminal-park/pending-stop
=== RUN   TestIndependentRepair1EpochReceiptBoundary/terminal-park/rejected-stop
=== RUN   TestIndependentRepair1EpochReceiptBoundary/terminal-park/accepted-stop
=== RUN   TestIndependentRepair1EpochReceiptBoundary/terminal-park/accepted-collect
=== RUN   TestIndependentRepair1EpochReceiptBoundary/completed-park/none
=== RUN   TestIndependentRepair1EpochReceiptBoundary/completed-park/wrong-observation
=== RUN   TestIndependentRepair1EpochReceiptBoundary/completed-park/pending-stop
=== RUN   TestIndependentRepair1EpochReceiptBoundary/completed-park/rejected-stop
=== RUN   TestIndependentRepair1EpochReceiptBoundary/completed-park/accepted-stop
=== RUN   TestIndependentRepair1EpochReceiptBoundary/completed-park/accepted-collect
--- PASS: TestIndependentRepair1EpochReceiptBoundary (0.74s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/terminal-park/none (0.07s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/terminal-park/wrong-observation (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/terminal-park/pending-stop (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/terminal-park/rejected-stop (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/terminal-park/accepted-stop (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/terminal-park/accepted-collect (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/completed-park/none (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/completed-park/wrong-observation (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/completed-park/pending-stop (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/completed-park/rejected-stop (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/completed-park/accepted-stop (0.06s)
    --- PASS: TestIndependentRepair1EpochReceiptBoundary/completed-park/accepted-collect (0.06s)
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch/control-only/released
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch/control-only/accepted-stop
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch/control-only/observed-completed
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch/control-only/accepted-collect
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch/progress-and-control/released
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch/progress-and-control/accepted-stop
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch/progress-and-control/observed-completed
=== RUN   TestIndependentRepair1ParkCleanupMixedBatch/progress-and-control/accepted-collect
--- PASS: TestIndependentRepair1ParkCleanupMixedBatch (0.46s)
    --- PASS: TestIndependentRepair1ParkCleanupMixedBatch/control-only/released (0.06s)
    --- PASS: TestIndependentRepair1ParkCleanupMixedBatch/control-only/accepted-stop (0.06s)
    --- PASS: TestIndependentRepair1ParkCleanupMixedBatch/control-only/observed-completed (0.06s)
    --- PASS: TestIndependentRepair1ParkCleanupMixedBatch/control-only/accepted-collect (0.06s)
    --- PASS: TestIndependentRepair1ParkCleanupMixedBatch/progress-and-control/released (0.06s)
    --- PASS: TestIndependentRepair1ParkCleanupMixedBatch/progress-and-control/accepted-stop (0.06s)
    --- PASS: TestIndependentRepair1ParkCleanupMixedBatch/progress-and-control/observed-completed (0.06s)
    --- PASS: TestIndependentRepair1ParkCleanupMixedBatch/progress-and-control/accepted-collect (0.06s)
=== RUN   TestIndependentRepair1DecodedKeyRefusal
=== RUN   TestIndependentRepair1DecodedKeyRefusal/turn-key
    independent_repair1_sol_test.go:27: unsafe decoded key accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":32,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:33: permanent admission accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":32,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:34: admission changed logical SQL/native audit
=== RUN   TestIndependentRepair1DecodedKeyRefusal/memory-key
    independent_repair1_sol_test.go:27: unsafe decoded key accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":9,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:33: permanent admission accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":9,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:34: admission changed logical SQL/native audit
=== RUN   TestIndependentRepair1DecodedKeyRefusal/scratch-key
    independent_repair1_sol_test.go:27: unsafe decoded key accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":9,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:33: permanent admission accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":9,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:34: admission changed logical SQL/native audit
=== RUN   TestIndependentRepair1DecodedKeyRefusal/execution-key
    independent_repair1_sol_test.go:27: unsafe decoded key accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":32,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:33: permanent admission accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":32,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:34: admission changed logical SQL/native audit
=== RUN   TestIndependentRepair1DecodedKeyRefusal/resources-key
    independent_repair1_sol_test.go:27: unsafe decoded key accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":9,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:33: permanent admission accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":9,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:34: admission changed logical SQL/native audit
=== RUN   TestIndependentRepair1DecodedKeyRefusal/legacy-null-key
    independent_repair1_sol_test.go:27: unsafe decoded key accepted: {"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:33: permanent admission accepted: {"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:34: admission changed logical SQL/native audit
=== RUN   TestIndependentRepair1DecodedKeyRefusal/merged-turn-key
    independent_repair1_sol_test.go:27: unsafe decoded key accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":32,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:33: permanent admission accepted: {"execution":{"effort":"medium","quotaPoolId":"review-pool","maxTurns":32,"resources":{"minCpuClass":"medium","preferredCpuClass":"high","cpuUnits":1.5,"memoryMb":512,"scratchMb":64}},"id":"one","role":"independent","route":"codex/org/sol","required":true}
    independent_repair1_sol_test.go:34: admission changed logical SQL/native audit
--- FAIL: TestIndependentRepair1DecodedKeyRefusal (0.45s)
    --- FAIL: TestIndependentRepair1DecodedKeyRefusal/turn-key (0.07s)
    --- FAIL: TestIndependentRepair1DecodedKeyRefusal/memory-key (0.07s)
    --- FAIL: TestIndependentRepair1DecodedKeyRefusal/scratch-key (0.06s)
    --- FAIL: TestIndependentRepair1DecodedKeyRefusal/execution-key (0.06s)
    --- FAIL: TestIndependentRepair1DecodedKeyRefusal/resources-key (0.06s)
    --- FAIL: TestIndependentRepair1DecodedKeyRefusal/legacy-null-key (0.06s)
    --- FAIL: TestIndependentRepair1DecodedKeyRefusal/merged-turn-key (0.06s)
=== RUN   TestIndependentRepair1NestedPrecedenceLegacy
    independent_repair1_sol_test.go:55: nested merge and direct overrides preserve exact profiles and legacy canonical bytes
--- PASS: TestIndependentRepair1NestedPrecedenceLegacy (0.00s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/backlog	1.653s
FAIL

```

Full additional portable witness, executed exactly as shown at internal/backlog/independent_repair1_sol_test.go, using existing exact-candidate fixtures/helpers:
```go
package backlog

import (
 "context"
 "encoding/base64"
 "encoding/json"
 "strings"
 "testing"
 "github.com/iryzhkov/t3-steward/internal/domain"
)

func TestIndependentRepair1DecodedKeyRefusal(t *testing.T) {
 binary := func(s string) string { return "!!binary "+base64.StdEncoding.EncodeToString([]byte(s)) }
 cases := []struct{name,raw string}{
  {"turn-key",strings.ReplaceAll(executionManifestYAML(),"max_turns: 9",binary("max_turns")+": 32.9")},
  {"memory-key",strings.ReplaceAll(executionManifestYAML(),"memory_mb: 512",binary("memory_mb")+": 512.75")},
  {"scratch-key",strings.ReplaceAll(executionManifestYAML(),"scratch_mb: 64",binary("scratch_mb")+": 64.75")},
  {"execution-key",strings.ReplaceAll(strings.ReplaceAll(executionManifestYAML(),"execution:",binary("execution")+":"),"max_turns: 9","max_turns: 32.9")},
  {"resources-key",strings.ReplaceAll(strings.ReplaceAll(executionManifestYAML(),"resources:",binary("resources")+":"),"memory_mb: 512","memory_mb: 512.75")},
  {"legacy-null-key",strings.Replace(declaredManifestYAML(),"required: true}","required: true, "+binary("execution")+": null}",1)},
  {"merged-turn-key",strings.ReplaceAll(executionManifestYAML(),"max_turns: 9","<<: {"+binary("max_turns")+": 32.9}")},
 }
 for _,c:=range cases {t.Run(c.name,func(t *testing.T){
  m,err:=ParseManifest([]byte(c.raw))
  if err==nil {
   compiled:=compileTaskReview(m.Tasks["inspect"].ReviewRequirements,"w","r","t",domain.Artifact{})
   b,_:=json.Marshal(compiled.Members[0]);t.Errorf("unsafe decoded key accepted: %s",b)
  } else {t.Logf("parser refusal: %v",err)}
  f:=executionFixture(t); db:=stagingSQL(t,f);before:=independentDeclaredTables(t,db)
  rewriteBundleManifest(t,f.source,c.raw)
  result,err:=(BundleIngester{Store:f.store,StorageRoot:f.service.Artifacts.SubmissionRoot,Permanent:declarationValidator{f.catalog}}).Ingest(context.Background(),f.source)
  after:=independentDeclaredTables(t,db)
  if err==nil {b,_:=json.Marshal(result.Records.Tasks[0].ReviewRequirements.Members[0]);t.Errorf("permanent admission accepted: %s",b)}
  if before!=after {t.Error("admission changed logical SQL/native audit")}
 })}
}

func TestIndependentRepair1NestedPrecedenceLegacy(t *testing.T) {
 profile:="effort: medium, quota_pool: review-pool, max_turns: 9, resources: {preset: build, cpu_units: 1.5, memory_mb: 512, scratch_mb: 64}"
 for _,body:=range []string{
  "<<: [{<<: {"+profile+"}}, {max_turns: 32.9}]",
  "<<: [{max_turns: 32.9}, {"+profile+"}], max_turns: 9",
  strings.Replace(profile,"resources: {","resources: {<<: [{memory_mb: 512.75}, {scratch_mb: 64.75}], ",1),
 }{
  raw:=strings.ReplaceAll(executionManifestYAML(),profile,body);m,err:=ParseManifest([]byte(raw));if err!=nil {t.Fatal(err)}
  p:=compileTaskReview(m.Tasks["inspect"].ReviewRequirements,"w","r","t",domain.Artifact{}).Members[0].Execution
  if p.MaxTurns!=9||p.Resources.MemoryMB!=512||p.Resources.ScratchMB!=64||p.Resources.CPUUnits!=1.5 {t.Fatal("precedence changed",p)}
 }
 original,err:=ParseManifest([]byte(declaredManifestYAML()));if err!=nil {t.Fatal(err)}
 before,_:=json.Marshal(compileTaskReview(original.Tasks["inspect"].ReviewRequirements,"w","r","t",domain.Artifact{}))
 raw:=strings.ReplaceAll(declaredManifestYAML(),"required: true","<<: [{<<: {required: true}}, {required: false}]")
 m,err:=ParseManifest([]byte(raw));if err!=nil {t.Fatal(err)}
 after,_:=json.Marshal(compileTaskReview(m.Tasks["inspect"].ReviewRequirements,"w","r","t",domain.Artifact{}))
 if string(before)!=string(after) {t.Fatal("nested legacy canonical bytes changed")}
 t.Log("nested merge and direct overrides preserve exact profiles and legacy canonical bytes")
}

```

Cleanup/evidence limits:
Huyang project ws_09bbca72b08cfdd97b89b0acae73e759 opened before repository operations. Input mounted symlink refusal was honored; external evidence read at resolved external paths. Source reads/guarded overlay create/removal used Huyang; external Git/Go/gofmt were explicitly authorized. Trust stayed untrusted/unchanged, no isolated Huyang verification or full LSP success claimed. Some guessed auxiliary file paths were not found and were corrected with Huyang source lookup; no source-map overwrite occurred. Overlay creation evidence ev_2bea0e3db824770d2cc39903b94addd1; removed with exact docrev_20773c952b3c4d9da2038caeccab9a9d6f8f2ebca547be4b9985973c1db6f0de, removal evidence ev_7781223db2896d069f2251d19f76664c/wsrev_38.

After guarded removal, git diff --exit-code HEAD, git diff --cached --exit-code, git diff --check, and parent diff --check all actual0; HEAD and write-tree exact above. No production/source edits remain; tracked worktree/index clean. All foreground test/audit sessions and subprocesses completed; no pending own process or task-bound wait. Only mounted .t3 and required review outputs untracked. Final output size/hygiene check follows artifact creation; each required artifact remains below256KiB.

Local only; publishing deferred and schedules disabled/unchanged. No push/PR/tags/release/UpKeeper/CI/fleet/config/trust/service effects. publish-agent-tooling's explicit local-only exception applies. Neither this review nor synthetic fixture families waive the two real provider-family production policy. No fullM16/publicactivation/finalphysicalHEAD/compiler/M17/diversity/OSrestart/scalability claim. SQLite reopening is not OS restart; complete logical SQL/native audit comparisons are not physical-page equality or distributed atomicity. Original cancelled producer's unavailable full history/input/log was not freshly read or reexecuted here; only its complete retained original reports and current repair evidence were audited. Cost/token totals unavailable. Lead owns both-review reconciliation and any future repair/acceptance.
