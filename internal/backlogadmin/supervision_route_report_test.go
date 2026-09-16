package backlogadmin

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A show is the command an operator runs when a gate is not moving, so it has
// to name the reason no overseer can be dispatched. A missing supervisor admin
// client is invisible in every gate, hold and incident the run has.
func TestSupervisionShowReportsAMissingSupervisorClient(t *testing.T) {
	service, _ := supervisionFixture(t)
	service.SetSupervisorClientConfigured(false)
	response, err := service.Supervise(context.Background(), operatorPrincipal(), SupervisionRequest{
		Version: SupervisionVersion, Operation: SupervisionShow, RunID: "run-1",
	})
	if err != nil {
		t.Fatalf("show refused: %v", err)
	}
	if response.State == nil {
		t.Fatal("show returned no state")
	}
	if response.State.RouteAvailable {
		t.Fatal("show reported an available route with no supervisor client configured")
	}
	if !strings.Contains(response.State.RouteBlockReason, "no supervisor client configured") {
		t.Fatalf("show does not name the missing supervisor client: %q", response.State.RouteBlockReason)
	}
}

// The readiness matrix refuses a supervised campaign on a coordinator that
// would dispatch no overseer for it, rather than accepting one that would then
// be held forever on a gate nobody can decide.
func TestSupervisionViabilityRefusesWithoutASupervisorClient(t *testing.T) {
	supervision := ViabilitySupervision{
		Route: domain.ProviderRoute{ProviderInstanceID: "instance-a", Model: "model-b"},
	}
	capable := []domain.WorkerInventory{{
		ID:           "worker-a",
		Capabilities: []string{workerproto.CapabilityCampaignSupervision},
		Providers:    []domain.WorkerProviderInventory{{InstanceID: "instance-a", Models: []string{"model-b"}}},
	}}
	if reasons := SupervisionViabilityReasons(supervision, capable, true); len(reasons) != 0 {
		t.Fatalf("a capable fleet with a supervisor client reported reasons: %#v", reasons)
	}
	reasons := SupervisionViabilityReasons(supervision, capable, false)
	if len(reasons) != 1 || reasons[0].Code != ReasonSupervisorClientMissing {
		t.Fatalf("reasons = %#v, want one %s", reasons, ReasonSupervisorClientMissing)
	}
	if !reasons[0].Permanent {
		t.Fatal("a missing supervisor client is not fixed by waiting, so it is permanent")
	}
	if !PermanentViabilityReason(ReasonSupervisorClientMissing) {
		t.Fatal("the reason code is not in the permanent set, so submit would proceed")
	}
}
