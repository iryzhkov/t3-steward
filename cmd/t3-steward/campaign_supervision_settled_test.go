package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// After run-3ae2f87b was closed, "supervision show" still told the operator
// the run waited for a reassess, that an overseer could be dispatched, and
// that a branch hold was active. A settled run's supervision is closed: every
// mutating verb is refused, no overseer is ever woken for it, and a hold on it
// holds nothing. The page now says so and offers none of it.
func TestCampaignSupervisionShowOfASettledRunAdvertisesNothing(t *testing.T) {
	state := supervisionTestState()
	state.SinkSettled = true
	state.RouteAvailable = true
	state.Activation.State = domain.ActivationSpent
	state.Activation.Outcome = domain.ActivationOutcomeNoDecision
	state.Incidents[0].Incident.State = domain.IncidentResolved
	transport := &fakeSupervisionTransport{response: backlogadmin.SupervisionResponse{
		Operation: backlogadmin.SupervisionShow, RunID: "run-1", State: &state,
	}}
	var out bytes.Buffer
	if err := supervisionTestCLI(&out, transport).runSupervision(context.Background(), []string{"show", "run-1"}); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, absent := range []string{"reassess", "an overseer can be dispatched", "no overseer can be dispatched", "  active  "} {
		if strings.Contains(text, absent) {
			t.Fatalf("show of a settled run says %q:\n%s", absent, text)
		}
	}
	for _, want := range []string{"the run is settled; supervision is closed", "hold hold-1 branch:build", "closed with the run"} {
		if !strings.Contains(text, want) {
			t.Fatalf("show of a settled run lacks %q:\n%s", want, text)
		}
	}
}
