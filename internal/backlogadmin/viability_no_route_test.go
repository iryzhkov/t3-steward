package backlogadmin

import (
	"context"
	"strings"
	"testing"
)

// TestViabilityRefusesATaskWithNoRoute is the intake side of U-2: a task that
// declares no provider route is permanently impossible, and the refusal names
// the instance/model pairs the project's eligible workers advertise so that the
// caller can pick one. The coordinator never chooses a route itself.
func TestViabilityRefusesATaskWithNoRoute(t *testing.T) {
	v := viabilityView(t, nil)
	task := viabilityTaskRequest()
	task.Routes = nil
	matrix := v.viability(context.Background(), viabilityCatalog(t),
		ViabilityRequest{Tasks: []ViabilityTask{task}})
	if matrix.Outcome != "impossible" {
		t.Fatalf("outcome = %q, want impossible: %+v", matrix.Outcome, matrix.Tasks)
	}
	reason, found := candidateReason(t, matrix, "no-route")
	if !found {
		t.Fatalf("no-route was not reported: %+v", matrix.Tasks)
	}
	if !reason.Permanent {
		t.Fatal("no-route must be permanent: waiting never adds a route")
	}
	if !strings.Contains(reason.Detail, "t3-primary/opus") {
		t.Fatalf("the refusal does not list the advertised routes: %q", reason.Detail)
	}
	if len(matrix.Tasks) != 1 || len(matrix.Tasks[0].Reasons) == 0 || matrix.Tasks[0].Reasons[0].Code != "no-route" {
		t.Fatalf("the reason is not carried at the task level: %+v", matrix.Tasks)
	}
	// The same request with a route is ready, so it is the missing route and
	// nothing else that refuses it.
	if ready := v.viability(context.Background(), viabilityCatalog(t),
		ViabilityRequest{Tasks: []ViabilityTask{viabilityTaskRequest()}}); ready.Outcome != ViabilityReady {
		t.Fatalf("the routed task is %q", ready.Outcome)
	}
}
