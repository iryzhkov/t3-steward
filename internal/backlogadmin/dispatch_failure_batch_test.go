package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type legacyFailureReader struct {
	Reader
	store    *sqlite.Store
	failures map[string][]domain.ActivationDispatchFailure
	calls    int
}

func (r *legacyFailureReader) ListCurrentActivationDispatchFailures(ctx context.Context, runID string) ([]domain.ActivationDispatchFailure, error) {
	r.calls++
	_, err := r.store.ListCurrentActivationDispatchFailures(ctx, runID)
	return r.failures[runID], err
}

type batchFailureReader struct {
	*legacyFailureReader
	batches int
	err     error
}

func (r *batchFailureReader) ListAllCurrentActivationDispatchFailures(ctx context.Context) (map[string][]domain.ActivationDispatchFailure, error) {
	r.batches++
	_, err := r.store.ListAllCurrentActivationDispatchFailures(ctx)
	return r.failures, errors.Join(r.err, err)
}

func TestLoadViewPrefersBatchFailures(t *testing.T) {
	store := openAdminTestStore(t)
	ctx := context.Background()
	failures := map[string][]domain.ActivationDispatchFailure{
		"active": {{ID: "failure", RunID: "active"}},
		"orphan": {{ID: "hidden", RunID: "orphan"}},
	}
	legacy := &legacyFailureReader{Reader: store, store: store, failures: failures}
	batch := &batchFailureReader{legacyFailureReader: legacy}
	service, err := New(batch, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	previousBatch, previousLegacy := 0, 0
	for _, history := range []int{2, 20} {
		runs := []domain.WorkflowRun{{ID: "active", Progress: domain.ProgressActive}}
		for i := 0; i < history; i++ {
			runs = append(runs, domain.WorkflowRun{ID: fmt.Sprintf("terminal-%d", i), Progress: domain.ProgressSucceeded})
		}
		if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: runs}); err != nil {
			t.Fatal(err)
		}
		batch.batches, legacy.calls = 0, 0
		service.reader = batch
		got, err := service.loadView(ctx)
		if err != nil || legacy.calls != 0 || len(got.dispatchFailures["orphan"]) != 0 || !reflect.DeepEqual(got.dispatchFailures["active"], failures["active"]) {
			t.Fatalf("batch view=%v err=%v legacy calls=%d", got.dispatchFailures, err, legacy.calls)
		}
		service.reader = legacy
		want, err := service.loadView(ctx)
		if err != nil || !reflect.DeepEqual(got.dispatchFailures, want.dispatchFailures) || batch.batches >= legacy.calls {
			t.Fatalf("parity/work: batch=%d legacy=%d err=%v", batch.batches, legacy.calls, err)
		}
		if previousBatch > 0 && (batch.batches != previousBatch || legacy.calls <= previousLegacy) {
			t.Fatalf("history growth: batch=%d legacy=%d", batch.batches, legacy.calls)
		}
		previousBatch, previousLegacy = batch.batches, legacy.calls
	}
	service.reader = batch
	batch.err = errors.New("batch unavailable")
	legacy.calls = 0
	if _, err := service.loadView(ctx); !errors.Is(err, batch.err) || legacy.calls != 0 {
		t.Fatalf("batch error=%v fallback calls=%d", err, legacy.calls)
	}
}
