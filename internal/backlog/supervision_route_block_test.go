package backlog

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func routeBlockSnapshot(state domain.GateState, routeAvailable bool) domain.SupervisionSnapshot {
	return domain.SupervisionSnapshot{
		RunID: "run-1", Supervised: true, RouteAvailable: routeAvailable,
		Gates: []domain.Gate{{
			RunID: "run-1", State: state,
			Definition: domain.GateDefinition{ID: "run-1:review", Name: "review"},
		}},
	}
}

func routeBlockIncident(state domain.IncidentState, openedAt time.Time) domain.ReviewIncident {
	return domain.ReviewIncident{
		ID: "incident-1", RunID: "run-1", GateID: "run-1:review",
		SourceEventID: "event-1", State: state,
		RequiredDisposition: domain.DispositionGateDecision,
		OpenedAt:            openedAt,
	}
}

// The route can disappear long after the gate became ready, so availability is
// evaluated on every boundary while a gate awaits review. The block escalates
// once the configured threshold has elapsed, and exactly once per incident.
func TestRouteBlockEscalatesOncePerWaitingIncidentAfterTheThreshold(t *testing.T) {
	opened := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	threshold := time.Hour
	snapshot := routeBlockSnapshot(domain.GateReadyForReview, false)
	open := routeBlockIncident(domain.IncidentOpen, opened)

	// An available route is not a block, whatever the gate is waiting for.
	if blocked := RouteBlockEscalations(routeBlockSnapshot(domain.GateReadyForReview, true),
		[]domain.ReviewIncident{open}, threshold, opened.Add(2*time.Hour)); len(blocked) != 0 {
		t.Fatalf("an available route escalated %+v", blocked)
	}
	// Ordinary brief waiting is not a review incident: the threshold has to
	// elapse first.
	if blocked := RouteBlockEscalations(snapshot, []domain.ReviewIncident{open},
		threshold, opened.Add(59*time.Minute)); len(blocked) != 0 {
		t.Fatalf("a block shorter than the threshold escalated %+v", blocked)
	}
	blocked := RouteBlockEscalations(snapshot, []domain.ReviewIncident{open}, threshold, opened.Add(threshold))
	if len(blocked) != 1 || blocked[0].ID != "incident-1" {
		t.Fatalf("a persistent block escalated %+v, want incident-1", blocked)
	}

	// Once escalated, the same incident is never selected again: that is the
	// deduplication, and it is why a tick loop does not notify repeatedly.
	escalated := routeBlockIncident(domain.IncidentEscalated, opened)
	if again := RouteBlockEscalations(snapshot, []domain.ReviewIncident{escalated},
		threshold, opened.Add(10*time.Hour)); len(again) != 0 {
		t.Fatalf("an escalated incident escalated again: %+v", again)
	}

	// A gate that is no longer waiting is not blocked on the route, and a
	// terminal run escalates nothing at all.
	if decided := RouteBlockEscalations(routeBlockSnapshot(domain.GateAccepted, false),
		[]domain.ReviewIncident{open}, threshold, opened.Add(threshold)); len(decided) != 0 {
		t.Fatalf("a decided gate escalated %+v", decided)
	}
	terminal := routeBlockSnapshot(domain.GateReadyForReview, false)
	terminal.RunTerminal = true
	if done := RouteBlockEscalations(terminal, []domain.ReviewIncident{open},
		threshold, opened.Add(threshold)); len(done) != 0 {
		t.Fatalf("a terminal run escalated %+v", done)
	}

	// A run that configured no threshold escalates on the boundary that sees
	// the block, and a held gate is waiting for a review just as a ready one is.
	held := routeBlockSnapshot(domain.GateHeld, false)
	if immediate := RouteBlockEscalations(held, []domain.ReviewIncident{open}, 0, opened); len(immediate) != 1 {
		t.Fatalf("an unconfigured threshold escalated %+v, want one incident", immediate)
	}
}
