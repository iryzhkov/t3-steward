package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/jocasta"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// memoryJocasta keeps documents in memory with the service's create and
// revision guards.
type memoryJocasta struct {
	mu   sync.Mutex
	docs map[string]jocasta.Document
}

func (m *memoryJocasta) Get(_ context.Context, path string) (jocasta.Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	doc, ok := m.docs[path]
	if !ok {
		return jocasta.Document{}, &jocasta.Error{Op: "get", Code: jocasta.CodeNotFound}
	}
	return doc, nil
}

func (m *memoryJocasta) Create(_ context.Context, path string, content []byte, _ string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, taken := m.docs[path]; taken {
		return 0, &jocasta.Error{Op: "put", Code: jocasta.CodeConflict}
	}
	m.docs[path] = jocasta.Document{Path: path, Revision: 1, Content: append([]byte(nil), content...)}
	return 1, nil
}

func (m *memoryJocasta) Update(_ context.Context, path string, content []byte, ifRevision int64, _ string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	doc, ok := m.docs[path]
	if !ok || doc.Revision != ifRevision {
		return 0, &jocasta.Error{Op: "put", Code: jocasta.CodeConflict}
	}
	m.docs[path] = jocasta.Document{Path: path, Revision: ifRevision + 1, Content: append([]byte(nil), content...)}
	return ifRevision + 1, nil
}

// TestCoordinatorLedgerRecordsDurableReviewVerdicts runs the coordinator's own
// ledger wiring over a SQLite store: a review verdict the coordinator recorded
// in coordinator_review_rounds must appear in the ledger.
func TestCoordinatorLedgerRecordsDurableReviewVerdicts(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 1, Name: "reviewed", TaskIDs: []string{"task-review"}, CreatedAt: now,
			Ledger: &domain.WorkflowLedger{JocastaProject: "steward"},
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressSucceeded, Revision: 1,
			CreatedAt: now, UpdatedAt: now, CompletedAt: &now,
			Sink: &domain.SinkTask{ID: "sink-1", Name: "sink", Progress: domain.ProgressSucceeded, CompletedAt: &now},
		}},
		Tasks: []domain.Task{{ID: "task-review", RunID: "run-1", WorkflowID: "workflow-1", Name: "review"}},
		Attempts: []domain.Attempt{{
			ID: "attempt-review", WorkflowRunID: "run-1", TaskID: "task-review", Number: 1,
			Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, UpdatedAt: now, CompletedAt: &now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	created, err := store.CreateReviewRound(ctx, review.Round{
		ID: "round-1", WorkflowRunID: "run-1", InputManifestDigest: digest,
		Reviewers: []review.Reviewer{{ID: "sol", TaskID: "task-review", Role: "reviewer", Route: "codex/sol", Required: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordReviewResult(ctx, created.ID, "sol", created.Revision, review.Result{
		State: "succeeded", ReviewMD: "Looks good",
		VerdictJSON: []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` +
			digest + `","reviewerRoute":"codex/sol"}`),
	}); err != nil {
		t.Fatal(err)
	}

	var cfg config.Config
	cfg.BacklogV2.Storage.Workspaces = t.TempDir()
	ledger := newCoordinatorLedger(cfg, store, backlog.CoordinatorArtifactStore{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	fake := &memoryJocasta{docs: map[string]jocasta.Document{}}
	ledger.reconciler.Client = fake
	ledger.Tick(ctx)
	ledger.wait()

	document := string(fake.docs["steward/handoffs/run-1.md"].Content)
	for _, want := range []string{"round-1 reviewer sol (reviewer, codex/sol): accept", "- Review rounds: round-1: accept\n", "### Run closed: succeeded"} {
		if !strings.Contains(document, want) {
			t.Fatalf("ledger is missing %q:\n%s", want, document)
		}
	}
}
