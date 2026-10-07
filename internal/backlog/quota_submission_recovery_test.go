package backlog

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type quotaCompletionFailureStore struct {
	*sqlite.Store
	fail bool
}

func (s *quotaCompletionFailureStore) CompleteSubmission(ctx context.Context, key, digest string, at time.Time) (domain.SubmissionRecord, bool, error) {
	if s.fail {
		s.fail = false
		return domain.SubmissionRecord{}, false, errors.New("injected completion failure")
	}
	return s.Store.CompleteSubmission(ctx, key, digest, at)
}
func TestQuotaSubmissionRecoversDurableAdmissionAfterCompletionFailure(t *testing.T) {
	store, gate, now, state := quotaSubmissionIntegrationFixture(t)
	service := quotaSubmissionService(t, store, gate, now)
	service.Store = &quotaCompletionFailureStore{Store: store, fail: true}
	request := DirectorySubmission{IdempotencyKey: "completion-failure", BundleDir: quotaSubmissionBundle(t, 60, "")}
	if _, err := service.SubmitDirectory(context.Background(), request); err == nil {
		t.Fatal("injected completion failure absent")
	}
	before, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(before.WorkflowRuns) != 1 {
		t.Fatalf("durable runs: %d", len(before.WorkflowRuns))
	}
	// Work may already have progressed before the caller retries. Replay must
	// retain these records, even after every governing window becomes exhausted.
	before.Attempts[0].Progress = domain.ProgressSucceeded
	before.Attempts[0].Revision = 99
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{Attempts: before.Attempts}); err != nil {
		t.Fatal(err)
	}
	state.UsedPercent = 100
	if err := store.SaveBucket(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	result, err := service.SubmitDirectory(context.Background(), request)
	if err != nil {
		t.Fatalf("retry re-admitted durable run: %v", err)
	}
	if !result.Replay || result.Record.RunID != before.WorkflowRuns[0].ID || result.Record.State != domain.SubmissionAccepted {
		t.Fatalf("recovery result: %#v", result)
	}
	if _, err := os.Stat(filepath.Join(result.StorageDir, "files", "workflow.yaml")); err != nil {
		t.Fatalf("recovery deleted committed files: %v", err)
	}
	after, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.WorkflowRuns, after.WorkflowRuns) || !reflect.DeepEqual(before.Attempts, after.Attempts) {
		t.Fatal("recovery rewrote committed run/attempt/receipt")
	}
	events, err := store.LoadAuditEvents(context.Background(), before.WorkflowRuns[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Kind == "submission-accepted" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("acceptance audit count: %d", count)
	}
}
