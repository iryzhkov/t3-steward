package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCollectionFix1ReceiptAuthority(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		for _, mutation := range []string{"valid", "download", "worker-endpoint", "coordinator-endpoint", "worker-epoch", "assignment-epoch", "coordinator-epoch", "digest", "order", "object"} {
			t.Run(fmt.Sprintf("ack=%v/%s", acknowledged, mutation), func(t *testing.T) {
				f := newCollectionFixture(t, 8192, 16384, 100, 100)
				if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
					t.Fatal(err)
				}
				p, err := f.custody.PendingUploadByPurpose("result")
				if err != nil || p == nil {
					t.Fatal(err)
				}
				directory := "outbox"
				if acknowledged {
					if err = f.custody.AcknowledgeUpload(p.Manifest.ID); err != nil {
						t.Fatal(err)
					}
					directory = "acknowledged"
				}
				switch mutation {
				case "download":
					p.Manifest.Direction = "download"
				case "worker-endpoint":
					for i := range p.Custody {
						p.Custody[i].From = "worker:foreign"
					}
				case "coordinator-endpoint":
					for i := range p.Custody {
						p.Custody[i].To = "outbox:foreign"
					}
				case "worker-epoch":
					p.Manifest.WorkerEpoch = "foreign"
				case "assignment-epoch":
					p.Manifest.AssignmentEpoch++
				case "coordinator-epoch":
					p.Manifest.CoordinatorEpoch++
				case "order":
					p.Custody[0], p.Custody[1] = p.Custody[1], p.Custody[0]
				case "object":
					p.Custody[0].ObjectID = "foreign"
				}
				// Endpoint substitutions are fully rehashed: checksum validity cannot
				// stand in for authority. Other chain mutations are also checksummed.
				previous := ""
				for i := range p.Custody {
					p.Custody[i].PreviousSHA256 = previous
					p.Custody[i], err = workerproto.BuildCustodyRecord(p.Custody[i])
					if err != nil {
						t.Fatal(err)
					}
					previous = p.Custody[i].RecordSHA256
				}
				if mutation == "digest" {
					p.Custody[0].RecordSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				}
				raw, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(f.custody.config.Root, directory, p.Manifest.ID+".json")
				if err = os.Chmod(path, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if err = f.runtime.markPhase("assignment-1", PhaseCollecting, "", f.workspace, "thread-1"); err != nil {
					t.Fatal(err)
				}
				f.reopen(t)
				durable, inspectErr := f.custody.ResultDurable(f.pkg)
				collectErr := f.runtime.collect(context.Background(), "assignment-1")
				if mutation == "valid" {
					if !durable || inspectErr != nil || collectErr != nil || f.record(t).Phase != PhaseCompleted {
						t.Fatalf("valid first receipt refused: %v %v", inspectErr, collectErr)
					}
				} else {
					var permanent *permanentCollectionFailure
					if durable || inspectErr == nil || collectErr == nil || errors.As(collectErr, &permanent) || f.record(t).Phase != PhaseCollecting || f.record(t).Failure != "" {
						t.Fatalf("ambiguous authority completed/reclassified: durable=%v inspect=%v collect=%v record=%+v", durable, inspectErr, collectErr, f.record(t))
					}
					db, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "coordinator.db"))
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					importer := backlog.CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 9, Store: db,
						Artifacts:        backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "imported"), Catalog: db},
						MaxArtifactBytes: 8192, MaxTotalBytes: 16384, Now: f.runtime.config.Now}
					report, err := importer.Import(context.Background(), workerproto.ArtifactUploadResponse{Manifest: p.Manifest, Custody: p.Custody}, collectionUploadOpener{f.custody})
					if err == nil || len(report.Transition) != 0 || len(report.Artifacts) != 0 {
						t.Fatalf("invalid actual import: %+v %v", report, err)
					}
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(raw, after) || f.process.calls != 1 {
					t.Fatalf("receipt changed or recaptured: %v calls=%d", err, f.process.calls)
				}
			})
		}
	}
}

type fix1CountingControl struct {
	*recordingT3
	unavailable      bool
	exports, settles int
}

func (c *fix1CountingControl) GetThread(ctx context.Context, id string) (*domain.Thread, error) {
	if c.unavailable {
		return nil, errors.New("observation unavailable")
	}
	return c.recordingT3.GetThread(ctx, id)
}
func (c *fix1CountingControl) ExportThread(ctx context.Context, id string) ([]byte, error) {
	c.exports++
	return c.recordingT3.ExportThread(ctx, id)
}
func (c *fix1CountingControl) SettleThread(ctx context.Context, id, token string) error {
	c.settles++
	return c.recordingT3.SettleThread(ctx, id, token)
}
func TestCollectionFix1ObservationReplay(t *testing.T) {
	for _, observation := range []string{"unknown", "missing", "settled"} {
		t.Run(observation, func(t *testing.T) {
			f := newCollectionFixture(t, 1024, 4096, 1400, 100)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			_ = f.runtime.collect(context.Background(), "assignment-1")
			if f.record(t).Phase != PhaseFailed {
				t.Fatal("size boundary absent")
			}
			originalExports := c.exports
			switch observation {
			case "unknown":
				c.unavailable = true
			case "missing":
				c.thread = nil
			case "settled":
				at := runtimeTestNow
				c.thread.SettledAt = &at
			}
			if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
				t.Fatal(err)
			}
			if f.record(t).SettlePending != (observation == "unknown") || c.settles != 0 {
				t.Fatal("observation incorrectly settled")
			}
			p, err := f.custody.PendingUploadByPurpose("result")
			if err != nil || p == nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.custody.config.Root, "outbox", p.Manifest.ID+".json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			c.unavailable = false
			for i := 0; i < 3; i++ {
				if err = f.runtime.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			after, err := os.ReadFile(path)
			wantSettles := 0
			if observation == "unknown" {
				wantSettles = 1
			}
			if err != nil || !bytes.Equal(before, after) || f.record(t).SettlePending || c.settles != wantSettles || c.exports != originalExports || f.process.calls != 1 {
				t.Fatalf("replay mutated custody/exported/reverified: err=%v settles=%d exports=%d original=%d verify=%d", err, c.settles, c.exports, originalExports, f.process.calls)
			}
		})
	}
}

func TestCollectionFix1FailedEvidenceRetention(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(fmt.Sprint(scoped), func(t *testing.T) {
			f := newCollectionFixture(t, 1024, 4096, 100, 2000)
			f.driver.scoped = scoped
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			c.message = FailedMarker + "\noriginal task failed\n"
			message := c.message
			archive := bytes.Clone(c.archive)
			_ = f.runtime.collect(context.Background(), "assignment-1")
			if f.record(t).Phase != PhaseFailed {
				t.Fatal("archive boundary absent")
			}
			originalExports := c.exports
			path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var snapshot collectedTurn
			if err = json.Unmarshal(before, &snapshot); err != nil {
				t.Fatal(err)
			}
			if !snapshot.RecoveryOnly || snapshot.Identity == nil || *snapshot.Identity != f.pkg.Identity || snapshot.ThreadID != f.pkg.Identity.ThreadID || snapshot.TurnID != "turn-1" || snapshot.Message != message || !bytes.Equal(snapshot.Archive, archive) {
				t.Fatal("failed recovery bytes/binding not exact")
			}
			f.reopen(t)
			for i := 0; i < 3; i++ {
				if err = f.runtime.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			f.runtime.config.Now = func() time.Time { return runtimeTestNow.Add(2 * DefaultRetention) }
			if err = f.runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) || f.record(t).Phase != PhaseCompleted || f.process.calls != 0 || c.exports != originalExports || c.settles != 1 {
				t.Fatalf("retention/replay: err=%v phase=%s verify=%d exports=%d settles=%d", err, f.record(t).Phase, f.process.calls, c.exports, c.settles)
			}
			// Failed finalization intentionally runs no verification or output
			// capture. Its original workspace bytes and archive remain retained.
			output, err := os.ReadFile(filepath.Join(f.workspace, "answer.txt"))
			if err != nil || !bytes.Equal(output, f.raw) {
				t.Fatalf("original failed workspace output changed: %v", err)
			}
			capture := filepath.Join(f.driver.Config.ArtifactRoot, "runs", "run-1", "task-1", "attempt-1")
			if _, err := os.Stat(capture); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed outcome unexpectedly captured/verified: %v", err)
			}
		})
	}
}

func TestCollectionFix1RecoveryEvidenceCannotWitnessSuccess(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	thread := *f.control.thread
	// Even a clean-looking archive retained from a failed observation cannot
	// launder that observation into reusable successful-turn authority.
	if _, _, _, err := f.driver.settleCollectedTurn(f.pkg, thread, "done", f.control.archive, "original failure", ""); err != nil {
		t.Fatal(err)
	}
	message, archive, failure, err := f.driver.settleCollectedTurn(f.pkg, thread, "not ready", []byte("{}"), "not finished", "")
	if err != nil || failure != "not finished" || message != "not ready" || string(archive) != "{}" {
		t.Fatal("recovery-only evidence laundered")
	}
	other := f.pkg
	other.Identity.DispatchToken = "foreign"
	if _, _, _, err = f.driver.settleCollectedTurn(other, thread, "", nil, "not finished", ""); err == nil {
		t.Fatal("foreign attempt adopted snapshot")
	}
}

func TestCollectionFix1RetentionFailureRefusesPublication(t *testing.T) {
	for _, obstacle := range []string{"directory", "symlink", "no-root"} {
		t.Run(obstacle, func(t *testing.T) {
			f := newCollectionFixture(t, 1024, 4096, 100, 2000)
			c := &fix1CountingControl{recordingT3: f.control}
			f.driver.T3 = c
			c.message = FailedMarker + "\noriginal task failed\n"
			path := filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json")
			switch obstacle {
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(t.TempDir(), "absent"), path); err != nil {
					t.Fatal(err)
				}
			case "no-root":
				f.driver.Config.RunsRoot = ""
			}
			var err error
			if obstacle == "no-root" {
				// Runtime workspace probing also needs RunsRoot; exercise the
				// retention seam directly with the original valid workspace.
				err = f.driver.Collect(context.Background(), f.pkg, f.workspace)
			} else {
				err = f.runtime.collect(context.Background(), "assignment-1")
			}
			var permanent *permanentCollectionFailure
			if err == nil || errors.As(err, &permanent) || f.record(t).Phase != PhaseCollecting || f.record(t).Failure != "" || c.settles != 0 || f.process.calls != 0 {
				t.Fatalf("unsupported retention settled/reclassified: %v %+v", err, f.record(t))
			}
			if p, e := f.custody.PendingUploadByPurpose("result"); e != nil || p != nil {
				t.Fatalf("unsupported custody: %+v %v", p, e)
			}
		})
	}
}
