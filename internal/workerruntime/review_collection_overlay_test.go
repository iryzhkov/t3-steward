package workerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type reviewUnavailableThread struct {
	*recordingT3
	unavailable bool
	settles     int
}

func (c *reviewUnavailableThread) GetThread(ctx context.Context, id string) (*domain.Thread, error) {
	if c.unavailable {
		return nil, errors.New("review: observation unavailable")
	}
	return c.recordingT3.GetThread(ctx, id)
}
func (c *reviewUnavailableThread) SettleThread(ctx context.Context, id, token string) error {
	c.settles++
	return c.recordingT3.SettleThread(ctx, id, token)
}

func TestReviewCollectionSettlementObservationUnknown(t *testing.T) {
	f := newCollectionFixture(t, 1024, 4096, 1400, 100)
	_ = f.runtime.collect(context.Background(), "assignment-1")
	if f.record(t).Phase != PhaseFailed {
		t.Fatal("real size boundary not reached")
	}
	c := &reviewUnavailableThread{recordingT3: f.control, unavailable: true}
	f.driver.T3 = c
	err := f.runtime.collect(context.Background(), "assignment-1")
	pending, pe := f.custody.PendingUploadByPurpose("result")
	if pe != nil || pending == nil {
		t.Fatalf("custody missing: %v", pe)
	}
	first := f.record(t)
	f.reopen(t)
	c.unavailable = false
	for i := 0; i < 3; i++ {
		if e := f.runtime.Reconcile(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("collectErr=%v firstPhase=%s firstSettlePending=%v recoveredSettleCalls=%d finalPending=%v verification=%d", err, first.Phase, first.SettlePending, c.settles, f.record(t).SettlePending, f.process.calls)
	if !first.SettlePending || c.settles == 0 {
		t.Fatal("unknown observation was treated as proven settlement; reopen never retries")
	}
}

func TestReviewCollectionReceiptDirection(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	if e := f.runtime.collect(context.Background(), "assignment-1"); e != nil {
		t.Fatal(e)
	}
	p, e := f.custody.PendingUploadByPurpose("result")
	if e != nil || p == nil {
		t.Fatal(e)
	}
	p.Manifest.Direction = "download"
	raw, e := json.Marshal(p)
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(f.custody.config.Root, "outbox", p.Manifest.ID+".json")
	if e = os.Chmod(path, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	durable, e := f.custody.ResultDurable(f.pkg)
	replay := f.driver.Collect(context.Background(), f.pkg, f.workspace)
	uploads, ue := f.custody.PendingUploads()
	t.Logf("wrong-direction durable=%v error=%v replay=%v discoverableUploads=%d discoveryError=%v verification=%d", durable, e, replay, len(uploads), ue, f.process.calls)
	if durable || e == nil || replay == nil {
		t.Fatal("download receipt accepted as durable result; normal upload discovery rejects it")
	}
}

func TestReviewCollectionFailedTurnArchiveRetention(t *testing.T) {
	f := newCollectionFixture(t, 1024, 4096, 1400, 2000)
	f.control.message = FailedMarker + "\noriginal provider failure"
	_ = f.runtime.collect(context.Background(), "assignment-1")
	if f.record(t).Phase != PhaseFailed {
		t.Fatal("real size boundary not reached")
	}
	if e := f.runtime.collect(context.Background(), "assignment-1"); e != nil {
		t.Fatal(e)
	}
	_, e := os.Stat(filepath.Join(f.driver.workspacePath(f.pkg), "collected-turn.json"))
	t.Logf("phase=%s originalArchiveBytes=%d retainedArchiveError=%v verification=%d", f.record(t).Phase, len(f.control.archive), e, f.process.calls)
	if e != nil {
		t.Fatal("failed original turn archive was never retained before bounded replacement and settlement")
	}
}
