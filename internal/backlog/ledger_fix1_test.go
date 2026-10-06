package backlog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// TestLedgerRecordsReviewVerdictsPersistedInSQLite reads the run through the
// production SQLite loaders. A verdict the coordinator recorded durably must
// reach both the reviewer task's record and the closing record, and a round of
// another run whose reviewer names the same task must not.
func TestLedgerRecordsReviewVerdictsPersistedInSQLite(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	world := newLedgerWorld(true)
	world.finishBuild("done\n")
	world.finishReview()
	records := world.records
	records.ReviewRounds = nil
	if err := s.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	recordAccept := func(id, runID string) review.Round {
		t.Helper()
		created, err := s.CreateReviewRound(ctx, review.Round{
			ID: id, WorkflowRunID: runID, InputManifestDigest: digest,
			Reviewers: []review.Reviewer{{ID: "sol", TaskID: "task-review", Role: "reviewer", Route: "codex/sol", Required: true}},
		})
		if err != nil {
			t.Fatal(err)
		}
		raw := []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` +
			digest + `","reviewerRoute":"codex/sol"}`)
		done, err := s.RecordReviewResult(ctx, created.ID, "sol", created.Revision,
			review.Result{State: "succeeded", ReviewMD: "Looks good", VerdictJSON: raw})
		if err != nil {
			t.Fatal(err)
		}
		return done
	}
	if done := recordAccept("round-1", "run-1"); done.CombinedVerdict() != "accept" {
		t.Fatalf("combined verdict = %q", done.CombinedVerdict())
	}
	recordAccept("round-other", "run-other")

	reconciler := world.reconciler()
	reconciler.Records, reconciler.States, reconciler.ReviewRounds = s.LoadCoordinatorRecords, s, s.ListReviewRoundsForRun
	if report := reconciler.Tick(ctx); len(report.Errors) != 0 || len(report.Behind) != 0 {
		t.Fatalf("tick = %#v", report)
	}
	document := world.jocasta.content(ledgerTestPath)
	for _, want := range []string{"round-1 reviewer sol (reviewer, codex/sol): accept", "- Review rounds: round-1: accept\n"} {
		if !strings.Contains(document, want) {
			t.Fatalf("ledger is missing %q:\n%s", want, document)
		}
	}
	if strings.Contains(document, "round-other") {
		t.Fatalf("another run's review round leaked into the ledger:\n%s", document)
	}
	if markerCount(document, ledgerBoundaryClose) != 1 {
		t.Fatalf("ledger did not close:\n%s", document)
	}
}

// TestLedgerReviewRoundLoadFailureLeavesTheBoundaryPending proves a verdict
// that cannot be read is retried rather than written as "none recorded": a
// record, once appended, is never revisited.
func TestLedgerReviewRoundLoadFailureLeavesTheBoundaryPending(t *testing.T) {
	ctx := context.Background()
	world := newLedgerWorld(true)
	reconciler := world.reconciler()
	reconciler.Tick(ctx)
	world.finishBuild("done\n")
	failing := true
	loads := reconciler.ReviewRounds
	reconciler.ReviewRounds = func(ctx context.Context, runID string) ([]review.Round, error) {
		if failing {
			return nil, context.DeadlineExceeded
		}
		return loads(ctx, runID)
	}
	report := reconciler.Tick(ctx)
	if len(report.Behind) != 1 {
		t.Fatalf("an unreadable verdict must mark the ledger behind: %#v", report)
	}
	if document := world.jocasta.content(ledgerTestPath); strings.Contains(document, "attempt:attempt-build") {
		t.Fatalf("a record was written without its review verdicts:\n%s", document)
	}
	failing = false
	world.advance(ledgerMaxBackoff)
	if report := reconciler.Tick(ctx); len(report.Errors) != 0 || len(report.Behind) != 0 {
		t.Fatalf("catch-up tick = %#v", report)
	}
	if document := world.jocasta.content(ledgerTestPath); markerCount(document, "attempt:attempt-build") != 1 {
		t.Fatalf("the record was not written on catch-up:\n%s", document)
	}
}

// TestLedgerCatchUpUsesTheRunGraphDefinitions creates the ledger only after a
// graph amendment removed review's dependency on build: the milestone table
// must show the run's authoritative graph, not the original task templates.
func TestLedgerCatchUpUsesTheRunGraphDefinitions(t *testing.T) {
	ctx := context.Background()
	world := newLedgerWorld(true)
	for n := range world.records.Tasks {
		task := &world.records.Tasks[n]
		task.Class, task.MaxTurns, task.PromptArtifactID = domain.TaskClassSurplus, 1, "prompt-"+task.Name
		task.Routes = []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt-6.1-sol"}}
	}
	run := &world.records.WorkflowRuns[0]
	run.GraphRevision = 1

	// Jocasta is down at submission, so the ledger is not created yet.
	world.jocasta.unavailable = true
	if report := world.reconciler().Tick(ctx); len(report.Behind) != 1 {
		t.Fatalf("submission tick = %#v", report)
	}

	tasks, err := domain.AmendTasks(domain.GraphAmendment{
		ID: "remove-edge", RunID: run.ID, ExpectedRevision: 1, Reason: "review independently",
		Operation: "edge-remove", TaskID: "task-review", Source: "build",
	}, *run, world.records.Tasks, "", "")
	if err != nil {
		t.Fatal(err)
	}
	run.Graph = &domain.GraphDefinition{RunID: run.ID, Revision: 2, Parent: 1, Tasks: tasks}
	run.GraphRevision = 2

	world.jocasta.unavailable = false
	world.advance(ledgerMaxBackoff)
	if report := world.reconciler().Tick(ctx); len(report.Errors) != 0 || len(report.Behind) != 0 {
		t.Fatalf("catch-up tick = %#v", report)
	}
	document := world.jocasta.content(ledgerTestPath)
	if !strings.Contains(document, "| review | - | review.md |") || strings.Contains(document, "| review | build |") {
		t.Fatalf("milestones do not follow the run's graph:\n%s", document)
	}

	// Attempt records resolve their task through the same graph.
	world.finishBuild("done\n")
	if report := world.reconciler().Tick(ctx); len(report.Errors) != 0 || len(report.Behind) != 0 {
		t.Fatalf("build tick = %#v", report)
	}
	if document := world.jocasta.content(ledgerTestPath); !strings.Contains(document, "### build attempt 1: succeeded") {
		t.Fatalf("build record is missing:\n%s", document)
	}
}
