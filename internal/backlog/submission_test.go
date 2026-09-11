package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestSubmissionServiceSimultaneousReplayAndChangedRequest(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bundle := validBundle(t)
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	service := &SubmissionService{
		StorageRoot: storage, Store: store,
		MaxBytes: 1 << 20, MaxFiles: 16, Now: func() time.Time { return now },
	}
	request := DirectorySubmission{IdempotencyKey: "client-request-1", BundleDir: bundle}

	start := make(chan struct{})
	results := make(chan SubmissionResult, 2)
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			result, err := service.SubmitDirectory(ctx, request)
			results <- result
			errs <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("simultaneous submission: %v", err)
		}
	}
	var got []SubmissionResult
	for result := range results {
		got = append(got, result)
	}
	if len(got) != 2 || got[0].Record.WorkflowID != got[1].Record.WorkflowID ||
		got[0].Record.RunID != got[1].Record.RunID ||
		got[0].Record.State != domain.SubmissionAccepted ||
		got[1].Record.State != domain.SubmissionAccepted ||
		got[0].Replay == got[1].Replay {
		t.Fatalf("simultaneous results = %#v", got)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Workflows) != 1 || len(records.WorkflowRuns) != 1 {
		t.Fatalf("coordinator records = %d workflows, %d runs", len(records.Workflows), len(records.WorkflowRuns))
	}

	writeBundleFile(t, bundle, "prompts/inspect.md", "changed")
	_, err = service.SubmitDirectory(ctx, request)
	if !errors.Is(err, sqlite.ErrSubmissionConflict) {
		t.Fatalf("changed replay error = %v", err)
	}
}

func TestSubmissionServiceRecoversPendingPublicationAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	bundle := validBundle(t)
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	now := time.Date(2026, 9, 10, 13, 0, 0, 0, time.UTC)
	digest, err := directorySubmissionDigest(ctx, bundle, 1<<20, 16)
	if err != nil {
		t.Fatal(err)
	}
	workflowID, runID := submissionResultIDs("restart-request")
	record, _, err := store.ReserveSubmission(ctx, domain.SubmissionRecord{
		Key: "restart-request", Digest: digest, WorkflowID: workflowID, RunID: runID,
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	ingested, err := (BundleIngester{
		StorageRoot: storage, Store: store, Now: func() time.Time { return now },
		NewTypedID: submissionTypedIDGenerator(record.Key),
	}).Ingest(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := &SubmissionService{
		StorageRoot: storage, Store: store, MaxBytes: 1 << 20, MaxFiles: 16,
		Now: func() time.Time { return now.Add(time.Minute) },
	}
	result, err := service.SubmitDirectory(ctx, DirectorySubmission{
		IdempotencyKey: record.Key, BundleDir: bundle,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Replay || result.Record.State != domain.SubmissionAccepted ||
		result.Record.WorkflowID != ingested.WorkflowID || result.StorageDir != ingested.StorageDir {
		t.Fatalf("recovered result = %#v", result)
	}
}

func TestSubmissionServiceBoundsAndGeneratedKey(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bundle := validBundle(t)
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	service := &SubmissionService{
		StorageRoot: storage, Store: store,
		MaxBytes: 64, MaxFiles: 16, NewKey: func() string { return "generated-key" },
	}
	if _, err := service.SubmitDirectory(ctx, DirectorySubmission{BundleDir: bundle}); err == nil ||
		(!strings.Contains(err.Error(), "exceeds 64 bytes") && !strings.Contains(err.Error(), "no larger than 64 bytes")) {
		t.Fatalf("byte limit error = %v", err)
	}

	service.MaxBytes = 1 << 20
	result, err := service.SubmitDirectory(ctx, DirectorySubmission{BundleDir: bundle})
	if err != nil {
		t.Fatal(err)
	}
	if result.Record.Key != "generated-key" {
		t.Fatalf("generated key = %q", result.Record.Key)
	}

	unsafe := validBundle(t)
	if err := os.Remove(filepath.Join(unsafe, "prompts", "inspect.md")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(bundle, "prompts", "inspect.md"), filepath.Join(unsafe, "prompts", "inspect.md")); err != nil {
		t.Fatal(err)
	}
	service.NewKey = func() string { return "unsafe-key" }
	if _, err := service.SubmitDirectory(ctx, DirectorySubmission{BundleDir: unsafe}); err == nil {
		t.Fatal("unsafe symlink submission succeeded")
	}
}
