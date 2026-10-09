package backlog

import (
	"slices"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// offerPlacement is the copy of a placement trace one worker can decode. A
// worker without resource telemetry receives no resource evaluations, and one
// without the zram split receives no zram figure in any candidate's telemetry,
// because both decode the trace strictly. The durable trace is never changed.
func offerPlacement(placement *domain.PlacementDecision, capabilities []string) *domain.PlacementDecision {
	if placement == nil {
		return nil
	}
	// Role re-resolution is coordinator evidence; workers do not consume it.
	// Keep it off the unversioned assignment wire, including to rc.119 peers.
	wire := *placement
	wire.RouteReresolution = nil
	placement = &wire
	if !slices.Contains(capabilities, workerproto.CapabilityResourceTelemetry) {
		projected := *placement
		projected.ResourceEvaluations = nil
		return &projected
	}
	if slices.Contains(capabilities, workerproto.CapabilityZramSwapTelemetry) {
		return placement
	}
	projected := *placement
	projected.ResourceEvaluations = make([]domain.ResourceEvaluation, len(placement.ResourceEvaluations))
	for i, evaluation := range placement.ResourceEvaluations {
		if evaluation.Telemetry != nil && evaluation.Telemetry.ZramSwapUsedMB != nil {
			evaluation.Telemetry = evaluation.Telemetry.Clone()
			evaluation.Telemetry.ZramSwapUsedMB = nil
		}
		projected.ResourceEvaluations[i] = evaluation
	}
	return &projected
}
