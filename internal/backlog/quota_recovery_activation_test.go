package backlog

// An overseer activation inside quota planning.
//
// The overseer has its own quota pool and route and obeys the same automatic
// admission gates as every other work, so its activation is admitted and
// forecast through the same predicate rather than excluded from it. Reading it
// through the declared-task path instead produced, on every scheduler tick and
// for every activation attempt, a pair of warnings about an attempt that was
// perfectly consistent: it names no declared task, and its assignment carried
// no durable remaining-cost estimate.

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// activationPlanningInput is one live activation as quota planning sees it:
// the attempt and assignment ActivationAssignment builds, claimed and running.
func activationPlanningInput(t *testing.T) QuotaPlanningStateInput {
	t.Helper()
	placement := ActivationPlacement{
		WorkerID: "normandy", WorkerEpoch: "worker-epoch-1", SnapshotSequence: 1,
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "claudeAgent",
			Model: "claude-fable-5-1", QuotaPoolID: "overseer",
		},
	}
	dispatch := ActivationDispatch{
		Identity: ActivationDispatchIdentity("run-1", 1), Epoch: 1,
		RequiredCapability: SupervisionWorkerCapability,
	}
	attempt, assignment, err := ActivationAssignment(
		domain.Activation{ID: "activation-1", RunID: "run-1", Epoch: 1},
		dispatch, placement, 3, throttleDeliveryTime)
	if err != nil {
		t.Fatalf("build the activation assignment: %v", err)
	}
	attempt.AssignmentID = assignment.ID
	attempt.Progress = domain.ProgressActive
	attempt.Control = domain.ControlRunning
	attempt.ThreadID = assignment.ThreadID
	assignment.State = domain.AssignmentClaimed
	return QuotaPlanningStateInput{
		QuotaPools:   []domain.QuotaPool{{ID: "overseer", MaxConcurrent: 1}},
		QuotaWindows: []QuotaWindowBudget{{QuotaPoolID: "overseer", WindowID: "primary"}},
		Attempts:     []domain.Attempt{attempt},
		Assignments:  []domain.Assignment{assignment},
	}
}

func TestQuotaPlanningAdmitsAnActivationAttemptWithoutWarning(t *testing.T) {
	input := activationPlanningInput(t)
	if input.Assignments[0].Estimate == nil || input.Assignments[0].Estimate.RemainingCost <= 0 {
		t.Fatalf("activation assignment estimate = %#v, want a durable remaining cost",
			input.Assignments[0].Estimate)
	}

	var logged bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	state, err := DeriveQuotaPlanningState(input)
	if err != nil {
		t.Fatalf("DeriveQuotaPlanningState: %v", err)
	}
	if output := logged.String(); strings.Contains(output, "quota planning skipped an inconsistent attempt") {
		t.Fatalf("quota planning called a live activation inconsistent: %s", output)
	}
	// Admitted through the same predicate means counted by it: the activation
	// holds the pool slot it is running in.
	if len(state.QuotaPools) != 1 || state.QuotaPools[0].ActiveAssignments != 1 {
		t.Fatalf("pools = %#v, want the activation holding one slot", state.QuotaPools)
	}
	// Its cost is forecast in its own pool's window, not left out of it.
	if len(state.QuotaWindows) != 1 ||
		state.QuotaWindows[0].ActiveConsumption != input.Assignments[0].Estimate.RemainingCost {
		t.Fatalf("windows = %#v, want the activation's remaining cost consumed", state.QuotaWindows)
	}
	// It is not resumable work: a lost activation is replaced at a new epoch
	// from its durable record, never resumed from a throttle directive.
	if len(state.ResumeReservations) != 0 {
		t.Fatalf("resume reservations = %#v, want none for an activation", state.ResumeReservations)
	}
}

func TestActivationEstimateScalesRuntimeWithTheTurnBudget(t *testing.T) {
	one := ActivationEstimate(1)
	three := ActivationEstimate(3)
	if one.RemainingCost <= 0 || one.ExpectedRuntime <= 0 {
		t.Fatalf("estimate = %#v, want a positive cost and runtime", one)
	}
	if three.ExpectedRuntime != 3*one.ExpectedRuntime {
		t.Fatalf("runtime for three turns = %s, want three times %s", three.ExpectedRuntime, one.ExpectedRuntime)
	}
	if three.RemainingCost != one.RemainingCost {
		t.Fatalf("cost = %v and %v; the turn budget bounds the runtime, not the cost",
			one.RemainingCost, three.RemainingCost)
	}
	// A turn budget that was never set still yields a plannable estimate rather
	// than a zero one, which fails the route estimate validator.
	if zero := ActivationEstimate(0); zero.ExpectedRuntime != one.ExpectedRuntime {
		t.Fatalf("estimate without a turn budget = %#v, want the one-turn estimate", zero)
	}
}
