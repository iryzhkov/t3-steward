package backlog

// A released overseer offer as planning sees it.
//
// rc.96 released the unclaimed offer of an activation that had ended and left
// the offer's attempt ready/unassigned. Coordinator planning and quota planning
// both read a nonterminal attempt on a settled assignment as an inconsistency,
// and reported it on every boundary. The sweep now ends the attempt with the
// release, and repairs an attempt an earlier binary left behind.

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestReleasedDeadActivationOfferLeavesPlanningConsistent(t *testing.T) {
	for _, released := range []bool{false, true} {
		name := "offered"
		if released {
			name = "released by an earlier binary"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			epoch, err := store.AcquireCoordinator(ctx, "coordinator")
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 9, 22, 3, 4, 42, 0, time.UTC)
			attempt, assignment, err := ActivationAssignment(
				// No activation row exists for it, which is one of the ways an
				// activation ends as far as its offer is concerned.
				domain.Activation{ID: "activation-gone", RunID: "run-1", Epoch: 3},
				ActivationDispatch{Identity: ActivationDispatchIdentity("run-1", 3), Epoch: 3,
					RequiredCapability: SupervisionWorkerCapability},
				ActivationPlacement{WorkerID: "homelab", WorkerEpoch: "worker-1", SnapshotSequence: 1,
					Route: domain.ProviderRoute{WorkerID: "homelab", ProviderInstanceID: "claudeAgent",
						Model: "claude-opus-5", QuotaPoolID: "claude-main"}},
				3, now)
			if err != nil {
				t.Fatal(err)
			}
			attempt.AssignmentID, attempt.Revision = assignment.ID, 1
			assignment.WorkerEpoch, assignment.CreatedAt, assignment.UpdatedAt = "worker-1", now, now
			if released {
				assignment.State = domain.AssignmentReleased
			}
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
				Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
			}); err != nil {
				t.Fatal(err)
			}

			if _, err := store.ReleaseDeadActivationOffers(ctx, epoch, now.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(records.Assignments) != 1 || records.Assignments[0].State != domain.AssignmentReleased {
				t.Fatalf("assignments = %+v, want the offer released", records.Assignments)
			}
			if len(records.Attempts) != 1 || !records.Attempts[0].Progress.Terminal() {
				t.Errorf("attempts = %+v, want the never-started attempt ended", records.Attempts)
			}

			var logged bytes.Buffer
			restore := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(restore) })

			if _, err := BuildCoordinatorPlanInput(CoordinatorPlanningStateInput{
				DisableQuotaChecks: true, Now: now.Add(time.Hour), CoordinatorEpoch: epoch,
				Attempts: records.Attempts, Assignments: records.Assignments,
				MaxWorkerSnapshotAge: time.Minute, MaxQuotaObservationAge: time.Minute,
				DeadlineRiskWindow: time.Hour,
			}); err != nil {
				t.Fatalf("BuildCoordinatorPlanInput: %v", err)
			}
			if _, err := DeriveQuotaPlanningState(QuotaPlanningStateInput{
				QuotaPools:   []domain.QuotaPool{{ID: "claude-main", MaxConcurrent: 1}},
				QuotaWindows: []QuotaWindowBudget{{QuotaPoolID: "claude-main", WindowID: "primary"}},
				Attempts:     records.Attempts, Assignments: records.Assignments, Now: now.Add(time.Hour),
			}); err != nil {
				t.Fatalf("DeriveQuotaPlanningState: %v", err)
			}
			for _, warning := range []string{
				"coordinator planning found a nonterminal attempt on a settled assignment",
				"quota planning skipped an inconsistent attempt",
			} {
				if strings.Contains(logged.String(), warning) {
					t.Fatalf("planning still warns %q:\n%s", warning, logged.String())
				}
			}
		})
	}
}
