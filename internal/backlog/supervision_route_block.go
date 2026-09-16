package backlog

import (
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Route-block escalation: a gate waiting for a review nobody can perform.
//
// A gate becomes review-ready on one boundary and then waits. The route that
// would decide it can disappear at any later boundary, so route availability is
// a condition of every tick while a gate awaits review, not a property of the
// tick that made it ready. Without this, a fleet that loses its overseer worker
// after a gate opened leaves the run undecidable and tells nobody.
//
// The rule is the plan's: a persistent route block is a trigger, ordinary brief
// waiting is not. So the block must have persisted past the run's configured
// threshold, measured from the moment the incident that is waiting was opened,
// and each incident produces at most one escalation.

// RouteBlockEscalations selects the open review incidents that must be escalated
// because the supervisor route is unavailable and the block has persisted.
//
// It reports nothing when the route is available, when the run is terminal, or
// when no gate is actually waiting: an incident whose gate has since been
// decided is not blocked on the route. An incident already escalated or
// resolved is not selected again, which is the deduplication; the outbox
// deduplicates the delivery on the same incident identity a second time.
func RouteBlockEscalations(
	snapshot domain.SupervisionSnapshot,
	incidents []domain.ReviewIncident,
	threshold time.Duration,
	now time.Time,
) []domain.ReviewIncident {
	if !snapshot.Supervised || snapshot.RouteAvailable || snapshot.RunTerminal {
		return nil
	}
	awaiting := make(map[string]bool, len(snapshot.Gates))
	for _, gate := range snapshot.Gates {
		if gate.State == domain.GateReadyForReview || gate.State == domain.GateHeld {
			awaiting[gate.Definition.ID] = true
		}
	}
	var blocked []domain.ReviewIncident
	for _, incident := range incidents {
		if incident.State != domain.IncidentOpen || !awaiting[incident.GateID] {
			continue
		}
		if threshold > 0 && now.Sub(incident.OpenedAt) < threshold {
			continue
		}
		blocked = append(blocked, incident)
	}
	return blocked
}
