# CHANGES REQUESTED — independent bounded collection review

Verdict: **CHANGES REQUESTED** for commit **47e53bed8bd1b0a3597519a789e0f0f907489a7c**, tree **94b61978230c3af2b20e11415900f70148443689**, sole parent **49b99fd2e2763ead3f68babd37c41af2783bf9c0**. Two deduplicated P2 contract blockers below. No production fix was made.

This is the Codex medium independent review with the requested lens: typed limit diagnostics, pending/acknowledged first-result preservation, actual coordinator failed-result import and original evidence retention. Lead still needs BOTH independent reviews of the exact fix. This verdict does not approve the sibling profile candidate or waive the existing two-family production contract.

## Controlling inputs and exact identity

Read BOTH COMPLETE supplied plans: collection-review-plan.md (5057 bytes), controlling review plan jocasta:8f700f70601f44c4918d8149c504712d@1; collection-original-plan.md (9451 bytes), original plan jocasta:5c0e85983abb2da58beb5a306830188d@1. Also read complete collection-handoff.md (7177 bytes), collection-contract.md (5654 bytes), and ALL 995 meaningful lines of the full raw verification stream after external lossless decompression. The lead's full handoff/contract and bounded gate/proof-window audit is provenance, not independent historical execution. Sibling profile inputs were not read or imported.

Initial HEAD was 6b0a6798736d3c9277e7688ef3016c690bf7a9ee; only .t3/ was initially untracked. Before candidate import, sha256sum and wc verified collection-candidate.bundle = 411592 bytes / 82de180803b8a22b9ce2f5dbd89092a20cc90b8c180eadd98783a2b9cf279817. git bundle verify passed; sole prerequisite 6b0a6798736d3c9277e7688ef3016c690bf7a9ee and sole export 47e53bed8bd1b0a3597519a789e0f0f907489a7c HEAD. Fetch, inspect FETCH_HEAD, and detach matched the exact commit/tree/sole parent above. No mismatch occurred.

Compressed log: 10444 bytes / SHA256 5276dd8fe8d84038649e3c8306326eb212f179ed01d6e387a234e3ccbacb4d5a. xz -t and xz -dc passed; external /var/tmp/r.Gu6jII/historical.log = 92865 bytes, 995 lines, SHA256 988cb60d628f63420520fbfe0642b7c466150512132d358a879aba822e0a2bd7. Read contiguous windows 1–250, 251–335, 336–419, 420–620, 621–800, 801–995; overlaps corrected initial transport truncation. Complete stream, not an index-only read.

## B1 — P2: require result authority before treating a receipt as durable

Primary changed location: internal/workerruntime/custody.go:177–190, especially the unconditional true at line 190. Consumer: internal/workerruntime/local_driver.go:906–917. Supporting existing validation: custody.go:631 validates against pending.Manifest.Direction itself; lines 640–647 check chain/digest but do not constrain From/To. Actual coordinator requirements are internal/backlog/result_import.go:70–74 and 271–274.

The new early shortcut accepts a validly encoded, checksummed receipt at the exact result path even when its manifest says direction=download, or its custody records name another worker/coordinator. Package and assignment epoch checks alone are insufficient. The independent probe publishes genuine success through LocalDriver/CustodyStore, alters only disposable retained metadata, returns the journal to collecting (lost terminal journal write/replay), reopens it, and collects again. BOTH malformed receipts yield durable=true, no error, PhaseCompleted, original verification=1. Their receipt bytes remain unchanged, demonstrating first-result preservation but incorrect authorization.

A second targeted probe sends each exact receipt to the real CoordinatorResultImporter with migrated SQLite storage. Download is rejected with "result import manifest authority mismatch"; wrong custody is rejected with "result import custody chain mismatch". Both have zero transitions and zero artifacts. This proves worker completion does not imply accepted coordinator custody. It can strand the result at import while the worker stops collecting. The outbox and acknowledged branches share this issue. The older broad loadPending behavior is preexisting; the reviewed change newly uses it as sufficient authority to skip collection and declare durable replay, despite the promised ambiguity refusal.

Required repair: check upload/result purpose and the expected custody endpoints before the shortcut reports true, with the same authority rules the existing importer uses. Preserve and refuse mismatched existing bytes as ambiguous; do not overwrite, recapture, reclassify as a new size error, or relax trust/limits. Add pending and acknowledged negative controls. Wrong assignment epoch and checksum controls already refuse correctly; these are not separate findings.

## B2 — P2: retain an oversized archive when the original stopped turn already failed

Primary changed location: internal/workerruntime/local_driver.go:1148 (permanent failure skips archive export), combined with new permanent transition at 1015–1017 and retained-evidence claim in internal/workerruntime/permanent_collection_failure.go:50–51. Supporting original snapshot guard: local_driver.go:1070–1087.

settleCollectedTurn persists collected-turn.json only for an originally successful completion read. A correctly stopped turn whose assistant reports BACKLOG STATUS: failed has no such snapshot. If its 2254-byte thread archive hits the real 1024-byte object bound, storeObject rejects it before storing any archive object. The permanent fallback then publishes {} and settles/completes, but no worker copy of the rejected full archive exists.

The real fixture probe uses a legitimate failed final message, 100-byte output and 2254-byte original archive, then executes actual LocalDriver, finalizer, CustodyStore, durable journal transition, reopen and bounded failure publication. Both direct and scoped-flag branches reach completed with two envelope objects / 287 bytes; collected-turn.json and the original archive's content-addressed custody object both return os.ErrNotExist. Original verification count is **0**, correctly reflecting a failed turn rather than pretending it ran once. The original archive remains only in the fake provider's memory; that is not retained worker evidence. No generic worker capture of the archive exists: original finalizer capture covers declared workspace outputs, while the full archive is supplied separately to PublishResult.

Required repair: durably retain the exact original archive for failed as well as successful terminal turns before permanent publication failure can settle the provider and switch to bounded replay. Keep it outside the small delivered envelope, preserve original bytes and recovery identity, do not recapture repeatedly or enlarge limits. Add real failed-turn archive boundary/reopen/retention coverage. The contract's narrower statement about a "successful stopped-turn archive" does not satisfy the original plan's retained raw evidence requirement for this failed-turn case. Automatic prune exemption protects only evidence actually retained.

These tests do not demonstrate a destructive cleanup action; the blocker is absent retention and a misleading "raw outputs, thread archive and capture retained" durable reason. They do not establish live provider loss, and no live provider was accessed.

## Source review and passing behavior

Reviewed the complete change across all nine Go files against HEAD^ and Huyang source windows/semantic outlines: workerproto/artifact.go, artifact_size.go, artifact_size_test.go; workerruntime/collection_flight.go, custody.go, local_driver.go, permanent_collection_failure.go, permanent_collection_failure_test.go, runtime.go. Read all 629 lines of the new runtime test file and the full new protocol tests. Inspected original sameCollection/collectionClaimed/flight release, containment quiescence, stop/superseding guards, task wait freshness and turn identity, long finalization tests, custody malformed/epoch/checksum tests, scoped driver copy/attachment behavior, cleanup/prune and actual coordinator import/custody/outcome seams.

Narrow typed classification is sound for inspected production callers: object metadata and limits are validated before ArtifactSizeError; actual aggregate overflow uses uint64 sum without signed overflow. Identity/path are digested and diagnostics are bounded for these fixed production scopes. Finalizer/I/O/transport/untyped text retain ordinary error behavior. The extra forged exact-prefix untyped probe stays collecting, Failure="", verification=1: the prefix does not forge typed classification.

failCollection keeps the finished flight until journal durability; it uses stopPreparation plus sameCollection/current epoch/attempt/phase/no-local-pause and stop-request guards. Journal/quiescence refusal retains flight. Existing lease ownership remains coordinator fenced. New tests cover synthetic and real async stale/cancel/supersede and accepted-stop refusal; task wait/pause/healthy long verifier controls were run. No retry deadline for healthy finalization was introduced.

Normal typed object, aggregate and oversized successful archive cases demonstrably import an explicit failed result through real LocalDriver/finalizer/CustodyStore/journal/SQLite/CoordinatorResultImporter/ProjectWorkflowRuns. Their original verification runs once over reconciles and journal reopen, envelopes stay 287/290 bytes, failed sink and skipped dependent settle, and original output/capture/successful archive survive beyond retention. First pending and acknowledged success remains immutable with separate unknown-settlement retry. Interrupted fallback before/after custody write retries only the small envelope. Tiny invalid budgets remain PhaseFailed, claimed, with durable actionable failure intent and preserved bytes; they do NOT falsely claim coordinator completion. Those passing cases do not cover B1/B2.

Direct/scoped probe toggles LocalDriver.scoped on otherwise isolated fixtures. It covers both local branch modes, not a real scoped provider attachment, containment namespace, service restart or provider diversity. Source scopedDriver copies Publisher/config and fails closed on unavailable attachment.

## Current required gate — ONCE

Created exactly one short private temporary directory using mktemp -d /var/tmp/r.XXXXXX; actual path /var/tmp/r.Gu6jII, stat mode 700. Root remains Huyang-untrusted; no trust/config changes or isolated Huyang verification were performed. Foreground execution was explicitly authorized.

Exact command:

```sh
GOMAXPROCS=2 GOFLAGS=-p=2 GOTMPDIR=/var/tmp/r.Gu6jII go test ./internal/workerproto ./internal/workerruntime ./internal/backlog -run 'TestPermanentCollectionFailure|TestArtifactSizeError|Test.*(Collection|Custody|Importer|StopYields|DeferredStop|TaskWait|Parks|Parked|Pause|MissingWorkspace|RepeatedCollect|Unproven|Settle)' -count=1
```

Full untruncated stdout/stderr, current-gate.log (191 bytes, SHA256 27f0e12d613aa342b59470bf5de2b285e5d62474ebdef78ea9054a8afbc964ef):

```text
ok  	github.com/iryzhkov/t3-steward/internal/workerproto	0.004s
ok  	github.com/iryzhkov/t3-steward/internal/workerruntime	8.208s
ok  	github.com/iryzhkov/t3-steward/internal/backlog	10.647s
```

Actual command exit **0**, separately retained in /var/tmp/r.Gu6jII/current-gate.exit. Tool execution session **86984**, completed (not still running). No full gate or race rerun. Current witness is distinct from historical evidence.

## Independent portable adversarial probe

Huyang guarded create of internal/workerruntime/independent_collection_review_test.go, then authorized foreground gofmt. The following complete first revision was run with the command below; it depends only on the candidate's existing test fixtures and production seams. All os writes/removals are inside disposable t.TempDir data, not repository production files.

```go
package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type independentPrefixPublisher struct{ *CustodyStore }

func (p independentPrefixPublisher) PublishResult(context.Context, workerproto.ExecutionPackage, PublishedResult) error {
	return errors.New(permanentCollectionFailurePrefix + "I/O unknown (untyped)")
}

func TestIndependentCollectionReceiptAndRetention(t *testing.T) {
	for _, mutation := range []string{"download", "wrong-custody", "wrong-epoch", "bad-digest"} {
		t.Run(mutation, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatal(err)
			}
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil {
				t.Fatalf("missing original receipt: %v", err)
			}
			receipt := *pending
			switch mutation {
			case "download":
				receipt.Manifest.Direction = "download"
			case "wrong-custody":
				previous := ""
				for i, record := range receipt.Custody {
					record.From, record.To = "worker:other-worker", "outbox:other-coordinator"
					record.PreviousSHA256 = previous
					record, err = workerproto.BuildCustodyRecord(record)
					if err != nil {
						t.Fatal(err)
					}
					receipt.Custody[i] = record
					previous = record.RecordSHA256
				}
			case "wrong-epoch":
				receipt.Manifest.AssignmentEpoch++
			case "bad-digest":
				receipt.Custody[0].RecordSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			}
			path := filepath.Join(f.custody.config.Root, "outbox", receipt.Manifest.ID+".json")
			raw, err := json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.markPhase("assignment-1", PhaseCollecting, "", f.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			durable, inspectErr := f.custody.ResultDurable(f.pkg)
			collectErr := f.runtime.collect(context.Background(), "assignment-1")
			record := f.record(t)
			t.Logf("mutation=%s durable=%v inspect=%v collect=%v phase=%s verification=%d direction=%s from=%s to=%s",
				mutation, durable, inspectErr, collectErr, record.Phase, f.process.calls,
				receipt.Manifest.Direction, receipt.Custody[0].From, receipt.Custody[0].To)
			if inspectErr == nil || durable || collectErr == nil || record.Phase != PhaseCollecting {
				t.Errorf("ambiguous receipt authorized completed result")
			}
			if f.process.calls != 1 {
				t.Error("ambiguous receipt repeated original verification")
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(raw) {
				t.Error("receipt changed")
			}
		})
	}
	for _, scoped := range []bool{false, true} {
		name := "direct-failed-archive"
		if scoped {
			name = "scoped-failed-archive"
		}
		t.Run(name, func(t *testing.T) {
			f := newCollectionFixture(t, 1024, 4096, 100, 2000)
			f.driver.scoped = scoped
			f.control.message = FailedMarker + "\noriginal task failed\n"
			firstErr := f.runtime.collect(context.Background(), "assignment-1")
			if f.record(t).Phase != PhaseFailed {
				t.Fatalf("real boundary not hit: %v", firstErr)
			}
			f.reopen(t)
			if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatal(err)
			}
			snapshot := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			_, snapshotErr := os.Stat(snapshot)
			digest := sha256.Sum256(f.control.archive)
			_, custodyErr := os.Stat(f.custody.objectPath(hex.EncodeToString(digest[:])))
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil {
				t.Fatal(err)
			}
			t.Logf("first=%v phase=%s verification=%d fullArchiveBytes=%d snapshot=%v fullArchiveCustody=%v envelopeObjects=%d envelopeBytes=%d",
				firstErr, f.record(t).Phase, f.process.calls, len(f.control.archive), snapshotErr, custodyErr,
				len(pending.Manifest.Objects), pending.Manifest.TotalBytes)
			if errors.Is(snapshotErr, os.ErrNotExist) && errors.Is(custodyErr, os.ErrNotExist) {
				t.Error("original failed-turn archive has no retained worker snapshot or custody object")
			}
		})
	}
	t.Run("forged-prefix-untyped", func(t *testing.T) {
		f := newCollectionFixture(t, 8192, 16384, 100, 100)
		f.driver.Publisher = independentPrefixPublisher{f.custody}
		err := f.runtime.collect(context.Background(), "assignment-1")
		record := f.record(t)
		t.Logf("err=%v phase=%s failure=%q verification=%d", err, record.Phase, record.Failure, f.process.calls)
		if err == nil || record.Phase != PhaseCollecting || record.Failure != "" {
			t.Error("prefix forged permanent classification")
		}
	})
}
```

```sh
GOMAXPROCS=2 GOFLAGS=-p=2 GOTMPDIR=/var/tmp/r.Gu6jII go test ./internal/workerruntime -run '^TestIndependentCollectionReceiptAndRetention$' -count=1 -v
```

Full actual stdout/stderr, /var/tmp/r.Gu6jII/probe.log (4472 bytes, SHA256 fdbb3b784a8016fdc3425af82f27b120cdefb2e1f8baf7e117ed5f11eb1e0c9c):

```text
=== RUN   TestIndependentCollectionReceiptAndRetention
=== RUN   TestIndependentCollectionReceiptAndRetention/download
    independent_collection_review_test.go:72: mutation=download durable=true inspect=<nil> collect=<nil> phase=completed verification=1 direction=download from=worker:normandy to=outbox:coordinator
    independent_collection_review_test.go:76: ambiguous receipt authorized completed result
=== RUN   TestIndependentCollectionReceiptAndRetention/wrong-custody
    independent_collection_review_test.go:72: mutation=wrong-custody durable=true inspect=<nil> collect=<nil> phase=completed verification=1 direction=upload from=worker:other-worker to=outbox:other-coordinator
    independent_collection_review_test.go:76: ambiguous receipt authorized completed result
=== RUN   TestIndependentCollectionReceiptAndRetention/wrong-epoch
    independent_collection_review_test.go:72: mutation=wrong-epoch durable=false inspect=result custody: immutable assignment binding mismatch collect=collection deferred: inspect result custody: result custody: immutable assignment binding mismatch phase=collecting verification=1 direction=upload from=worker:normandy to=outbox:coordinator
=== RUN   TestIndependentCollectionReceiptAndRetention/bad-digest
    independent_collection_review_test.go:72: mutation=bad-digest durable=false inspect=result custody receipt is unreadable: artifact custody: record checksum mismatch collect=collection deferred: inspect result custody: result custody receipt is unreadable: artifact custody: record checksum mismatch phase=collecting verification=1 direction=upload from=worker:normandy to=outbox:coordinator
=== RUN   TestIndependentCollectionReceiptAndRetention/direct-failed-archive
    independent_collection_review_test.go:112: first=collection failed permanently; bounded failure custody pending: artifact object size exceeds limit: limit=1024 rejected=2254 identity=95bc57852914c1fc phase=completed verification=0 fullArchiveBytes=2254 snapshot=stat /var/tmp/r.Gu6jII/TestIndependentCollectionReceiptAndRetentiondirect-failed-archi584269691/001/runs/run-1/task-1/attempt-1-e2/collected-turn.json: no such file or directory fullArchiveCustody=stat /var/tmp/r.Gu6jII/TestIndependentCollectionReceiptAndRetentiondirect-failed-archi584269691/001/custody/objects/40/406b49f7877f724714e7de15780616d9f8cb92445e0a90196b01018637b99022: no such file or directory envelopeObjects=2 envelopeBytes=287
    independent_collection_review_test.go:116: original failed-turn archive has no retained worker snapshot or custody object
=== RUN   TestIndependentCollectionReceiptAndRetention/scoped-failed-archive
    independent_collection_review_test.go:112: first=collection failed permanently; bounded failure custody pending: artifact object size exceeds limit: limit=1024 rejected=2254 identity=95bc57852914c1fc phase=completed verification=0 fullArchiveBytes=2254 snapshot=stat /var/tmp/r.Gu6jII/TestIndependentCollectionReceiptAndRetentionscoped-failed-archi2862523087/001/runs/run-1/task-1/attempt-1-e2/collected-turn.json: no such file or directory fullArchiveCustody=stat /var/tmp/r.Gu6jII/TestIndependentCollectionReceiptAndRetentionscoped-failed-archi2862523087/001/custody/objects/40/406b49f7877f724714e7de15780616d9f8cb92445e0a90196b01018637b99022: no such file or directory envelopeObjects=2 envelopeBytes=287
    independent_collection_review_test.go:116: original failed-turn archive has no retained worker snapshot or custody object
=== RUN   TestIndependentCollectionReceiptAndRetention/forged-prefix-untyped
    independent_collection_review_test.go:125: err=collection deferred: publish result custody: permanent collection size failure: I/O unknown (untyped) phase=collecting failure="" verification=1
--- FAIL: TestIndependentCollectionReceiptAndRetention (0.51s)
    --- FAIL: TestIndependentCollectionReceiptAndRetention/download (0.09s)
    --- FAIL: TestIndependentCollectionReceiptAndRetention/wrong-custody (0.09s)
    --- PASS: TestIndependentCollectionReceiptAndRetention/wrong-epoch (0.08s)
    --- PASS: TestIndependentCollectionReceiptAndRetention/bad-digest (0.08s)
    --- FAIL: TestIndependentCollectionReceiptAndRetention/direct-failed-archive (0.07s)
    --- FAIL: TestIndependentCollectionReceiptAndRetention/scoped-failed-archive (0.07s)
    --- PASS: TestIndependentCollectionReceiptAndRetention/forged-prefix-untyped (0.03s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.515s
FAIL
```

Actual exit **1**, separately retained in probe.exit. Execution session **64596**, completed. Expected regression failures (four subcases), not a compiler/fixture error. Positive controls wrong-epoch/bad-digest/forged-prefix passed.

To resolve the new concrete concern that worker acceptance might still import, the overlay was guardedly expanded to call the actual coordinator importer. No production file changed. Complete second portable revision:

```go
package workerruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type independentPrefixPublisher struct{ *CustodyStore }

func (p independentPrefixPublisher) PublishResult(context.Context, workerproto.ExecutionPackage, PublishedResult) error {
	return errors.New(permanentCollectionFailurePrefix + "I/O unknown (untyped)")
}

func TestIndependentCollectionReceiptAndRetention(t *testing.T) {
	for _, mutation := range []string{"download", "wrong-custody", "wrong-epoch", "bad-digest"} {
		t.Run(mutation, func(t *testing.T) {
			f := newCollectionFixture(t, 8192, 16384, 100, 100)
			if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatal(err)
			}
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil {
				t.Fatalf("missing original receipt: %v", err)
			}
			receipt := *pending
			switch mutation {
			case "download":
				receipt.Manifest.Direction = "download"
			case "wrong-custody":
				previous := ""
				for i, record := range receipt.Custody {
					record.From, record.To = "worker:other-worker", "outbox:other-coordinator"
					record.PreviousSHA256 = previous
					record, err = workerproto.BuildCustodyRecord(record)
					if err != nil {
						t.Fatal(err)
					}
					receipt.Custody[i] = record
					previous = record.RecordSHA256
				}
			case "wrong-epoch":
				receipt.Manifest.AssignmentEpoch++
			case "bad-digest":
				receipt.Custody[0].RecordSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			}
			path := filepath.Join(f.custody.config.Root, "outbox", receipt.Manifest.ID+".json")
			raw, err := json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err := f.runtime.markPhase("assignment-1", PhaseCollecting, "", f.workspace, "thread-1"); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			durable, inspectErr := f.custody.ResultDurable(f.pkg)
			collectErr := f.runtime.collect(context.Background(), "assignment-1")
			record := f.record(t)
			t.Logf("mutation=%s durable=%v inspect=%v collect=%v phase=%s verification=%d direction=%s from=%s to=%s",
				mutation, durable, inspectErr, collectErr, record.Phase, f.process.calls,
				receipt.Manifest.Direction, receipt.Custody[0].From, receipt.Custody[0].To)
			if mutation == "download" || mutation == "wrong-custody" {
				db, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "probe.db"))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				importer := backlog.CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 9,
					Store: db, Artifacts: backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "imported"), Catalog: db},
					MaxArtifactBytes: 8192, MaxTotalBytes: 16384, Now: f.runtime.config.Now}
				report, importErr := importer.Import(context.Background(), workerproto.ArtifactUploadResponse{
					Manifest: receipt.Manifest, Custody: receipt.Custody}, collectionUploadOpener{f.custody})
				t.Logf("actual coordinator import err=%v transitions=%d artifacts=%d", importErr, len(report.Transition), len(report.Artifacts))
				if importErr == nil || len(report.Transition) != 0 || len(report.Artifacts) != 0 {
					t.Error("invalid probe receipt imported")
				}
			}
			if inspectErr == nil || durable || collectErr == nil || record.Phase != PhaseCollecting {
				t.Errorf("ambiguous receipt authorized completed result")
			}
			if f.process.calls != 1 {
				t.Error("ambiguous receipt repeated original verification")
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(raw) {
				t.Error("receipt changed")
			}
		})
	}
	for _, scoped := range []bool{false, true} {
		name := "direct-failed-archive"
		if scoped {
			name = "scoped-failed-archive"
		}
		t.Run(name, func(t *testing.T) {
			f := newCollectionFixture(t, 1024, 4096, 100, 2000)
			f.driver.scoped = scoped
			f.control.message = FailedMarker + "\noriginal task failed\n"
			firstErr := f.runtime.collect(context.Background(), "assignment-1")
			if f.record(t).Phase != PhaseFailed {
				t.Fatalf("real boundary not hit: %v", firstErr)
			}
			f.reopen(t)
			if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatal(err)
			}
			snapshot := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			_, snapshotErr := os.Stat(snapshot)
			digest := sha256.Sum256(f.control.archive)
			_, custodyErr := os.Stat(f.custody.objectPath(hex.EncodeToString(digest[:])))
			pending, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || pending == nil {
				t.Fatal(err)
			}
			t.Logf("first=%v phase=%s verification=%d fullArchiveBytes=%d snapshot=%v fullArchiveCustody=%v envelopeObjects=%d envelopeBytes=%d",
				firstErr, f.record(t).Phase, f.process.calls, len(f.control.archive), snapshotErr, custodyErr,
				len(pending.Manifest.Objects), pending.Manifest.TotalBytes)
			if errors.Is(snapshotErr, os.ErrNotExist) && errors.Is(custodyErr, os.ErrNotExist) {
				t.Error("original failed-turn archive has no retained worker snapshot or custody object")
			}
		})
	}
	t.Run("forged-prefix-untyped", func(t *testing.T) {
		f := newCollectionFixture(t, 8192, 16384, 100, 100)
		f.driver.Publisher = independentPrefixPublisher{f.custody}
		err := f.runtime.collect(context.Background(), "assignment-1")
		record := f.record(t)
		t.Logf("err=%v phase=%s failure=%q verification=%d", err, record.Phase, record.Failure, f.process.calls)
		if err == nil || record.Phase != PhaseCollecting || record.Failure != "" {
			t.Error("prefix forged permanent classification")
		}
	})
}
```

Only the two affected receipt cases were rerun; this was not the required gate or a race rerun:

```sh
GOMAXPROCS=2 GOFLAGS=-p=2 GOTMPDIR=/var/tmp/r.Gu6jII go test ./internal/workerruntime -run '^TestIndependentCollectionReceiptAndRetention/(download|wrong-custody)$' -count=1 -v
```

Full actual stdout/stderr, /var/tmp/r.Gu6jII/probe-import.log (1360 bytes, SHA256 cc653693e75852274222043fbb5a877fe518469b97bb9061d0e38dd543fd400e):

```text
=== RUN   TestIndependentCollectionReceiptAndRetention
=== RUN   TestIndependentCollectionReceiptAndRetention/download
    independent_collection_review_test.go:74: mutation=download durable=true inspect=<nil> collect=<nil> phase=completed verification=1 direction=download from=worker:normandy to=outbox:coordinator
    independent_collection_review_test.go:88: actual coordinator import err=result import manifest authority mismatch transitions=0 artifacts=0
    independent_collection_review_test.go:94: ambiguous receipt authorized completed result
=== RUN   TestIndependentCollectionReceiptAndRetention/wrong-custody
    independent_collection_review_test.go:74: mutation=wrong-custody durable=true inspect=<nil> collect=<nil> phase=completed verification=1 direction=upload from=worker:other-worker to=outbox:other-coordinator
    independent_collection_review_test.go:88: actual coordinator import err=result import custody chain mismatch transitions=0 artifacts=0
    independent_collection_review_test.go:94: ambiguous receipt authorized completed result
--- FAIL: TestIndependentCollectionReceiptAndRetention (0.28s)
    --- FAIL: TestIndependentCollectionReceiptAndRetention/download (0.14s)
    --- FAIL: TestIndependentCollectionReceiptAndRetention/wrong-custody (0.14s)
FAIL
FAIL	github.com/iryzhkov/t3-steward/internal/workerruntime	0.284s
FAIL
```

Actual exit **1**, separately retained in probe-import.exit. Execution session **99956**, completed. SQLite is migrated; these failures occur at the import authority boundary before DB binding, so no artificial binding success is inferred. No failed-result/sink transition is claimed for these deliberately rejected receipts.

Both overlay versions are complete above; the executed outputs retain original line numbers for their respective versions. Huyang guarded final deletion used docrev of the formatted second revision; removal receipt req_11423, wsrev_43. Overlay was untracked throughout; no Git add was needed or performed.

## Historical full-stream audit (not execution here)

The original complete stream is retained losslessly in the supplied XZ and externally decompressed as identified above.

| Raw log lines | Witness | Actual exit / interpretation |
| --- | --- | --- |
| 1–22 | BEFORE real aggregate/object/full-thread archive | 1; remains collecting with actual plain errors and original verification=1; sealed-fixture cleanup errors are also retained |
| 23–41 | First AFTER | 1; fixture wrongly searched serialized base64 []byte as raw archive text |
| 42–88 | Expanded AFTER | 1; coordinator DB epoch 1 vs worker epoch 9 |
| 89–134 | Corrected fixture | 0; real failed import and preserved original bytes |
| 135–738 | Relevant collection/custody/import/wait/pause/long collection controls | 0 |
| 739–756 | Real dependency/sink/retention assertion | 0 |
| 795–841 | ONE make check-review FAST_BASE=49b99fd2e2763ead3f68babd37c41af2783bf9c0 | 0 |
| 843–995 | Commit/export, failed raw-SHA bundle and missing-object exit assumptions, corrected baseline-only import/closure/final checks | Corrections and final success recorded honestly |

Final historical gate includes build, vet, full repository short suite, pinned Go1.25.0 staticcheck v0.7.0 and whole-repository format gate, then full changed-package races workerproto 7.152s / workerruntime 32.588s. Source/logical-index/tree PRE/POST match: source a4f61fb0aeff92539c9a97bfaf4598dbbe07f3dce48f92a79b08266b019fbcb6, logical index 9e66fa0b209d53f6d31e92423601d185f95b60593d88aa8f3f39ad81893829f9, tree 94b61978230c3af2b20e11415900f70148443689. No source mutation after that gate in the recorded stream. Cached diff prefixes explain its different earlier digest. No clean LSP or isolated Huyang success is inferred.

Historical packaging corrections are explicit: raw-SHA-only baseline bundle failed 128 and was corrected with a temporary named ref; absent cat-file returned 128 instead of assumed 1 and was corrected to nonzero absence. No candidate import before those absence checks, no second historical gate. Historical recovery relevant/lifecycle checks are exit 0. These records do not independently prove the producer's runtime outside their supplied log.

## Independent current full closure proof

The producer's 527130-byte typed inventory was NOT supplied, so it was not read from a digest. Independently computed full reachable (OID, type, size) rows from the current source and a new baseline-only bare repository using git rev-list --objects --no-object-names candidate, sorted unique IDs, then cat-file --batch-check='%(objectname) %(objecttype) %(objectsize)' for every ID. Checked process exits, no missing rows, exact complete byte equality. No bounded HEAD-only or sample-object substitute.

A private local source ref refs/t3-evidence/independent-collection-baseline advertised baseline 6b0a679; baseline.bundle exported that ref and the source ref was removed immediately. Fresh repository /var/tmp/r.Gu6jII/fresh.git initially had only refs/heads/baseline; candidate AND parent cat-file probes were absent with actual 128. Then fresh bundle verify/import, set bare HEAD candidate, and git fsck --strict --full for fresh and source all exited 0.

Actual complete equality: **10326 objects, 527130 bytes, SHA256 68a22b34362af5e9c7a22b9863ad80c72bd6669ff8830f5554bc4a7751072374**. Rows retained externally as source-types.txt and imported-types.txt. Source all-object pre/post subset check passed: **10689** preexisting source objects retained, including **363 outside candidate closure**. No SOURCE_ALL==closure claim. Complete command/exit receipt retained externally closure.log; repetitive object rows are not copied into this report.

Portable core of the comparison, after fresh baseline/bundle import:

```python
import subprocess, hashlib
candidate = "47e53bed8bd1b0a3597519a789e0f0f907489a7c"
def typed(repo):
    ids = subprocess.check_output(["git", "-C", repo, "rev-list", "--objects",
                                  "--no-object-names", candidate])
    ids = sorted(set(ids.splitlines()))
    p = subprocess.run(["git", "-C", repo, "cat-file",
                        "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
                       input=b"\n".join(ids)+b"\n", capture_output=True, check=True)
    assert b"missing" not in p.stdout
    return b"\n".join(sorted(p.stdout.splitlines()))+b"\n"
source = typed(SOURCE_ROOT)
fresh = typed("/var/tmp/r.Gu6jII/fresh.git")
assert source == fresh
print(len(source.splitlines()), len(source), hashlib.sha256(source).hexdigest())
```

## Final limits and cleanup

Local only; publishing deferred and schedules disabled. No source push, PR, tag, release, UpKeeper, live config/admin/trust/provider/worker/service/fleet mutation or OS restart. No delegation/nested review/Claude/effort escalation. No full M16/public activation/final physical HEAD/compiler/M17/CI/deployment/scalability/real-provider-diversity claims. Final candidate remains provisional for lead-controlled repair and BOTH exact independent reviews.

Tracked production source and index were unchanged throughout this review. The disposable overlay is removed via guarded Huyang deletion. Final exact commit/tree/parent and tracked/index/diff checks are recorded in continuation.md. All three foreground Go sessions completed; no task-started background work remains. review.md and continuation.md are the declared outputs, each below 256KiB.
