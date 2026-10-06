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
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
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
				db, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "probe.db"))
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
