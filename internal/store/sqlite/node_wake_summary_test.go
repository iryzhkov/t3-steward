package sqlite

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// changingSummary answers "unavailable" first and with the run afterwards, so
// every build after the first gives different bytes.
type changingSummary struct{ calls int }

func (s *changingSummary) SummaryRun(context.Context, string) (wait.SummaryRun, error) {
	s.calls++
	if s.calls == 1 {
		return wait.SummaryRun{}, wait.SummaryError{Category: "unavailable"}
	}
	return wait.SummaryRun{ID: "r2", Workflow: "wf", Progress: domain.ProgressSucceeded,
		Tasks: []wait.SummaryTask{{ID: "t2", Name: "deploy", Attempt: "a2", Progress: domain.ProgressSucceeded}}}, nil
}

func (s *changingSummary) OpenSummaryArtifact(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("none")
}

// noEffectControl refuses the first send and then reports that refusal as
// having had no effect, which is what returns a frozen wake to offline.
type noEffectControl struct {
	*fleetControl
	failFirst bool
}

func (c *noEffectControl) SendNodeWake(ctx context.Context, thread domain.Thread, id, text string) error {
	if c.failFirst {
		c.failFirst = false
		return errors.New("send refused before any effect")
	}
	return c.fleetControl.SendNodeWake(ctx, thread, id, text)
}

func (c *noEffectControl) ReconcileNodeWake(context.Context, string, string) (wait.WakeReceiptStatus, error) {
	return wait.WakeReceiptKnownNoEffect, nil
}

// A node wake whose first attempt froze its payload and had no effect is
// retried with the frozen bytes. Rebuilding the summary on the retry gave
// different bytes, the store refused the claim as a frozen delivery that
// differs, and the wake was never sent.
func TestNodeWakeRetryAfterNoEffectSendsTheFrozenSummary(t *testing.T) {
	ctx := context.Background()
	store, _, now := taskWaitFixture(t)
	otherRun(t, store, now, domain.ProgressSucceeded)
	request := domain.NodeWaitRequest{ID: "nw-a", ThreadID: "thread-1", Name: "nw-a", Target: domain.NodeRef{RunID: "r2", TaskID: "deploy"}, Timeout: time.Hour}
	if _, err := store.RegisterNodeWait(ctx, request, "operator", "host", now); err != nil {
		t.Fatal(err)
	}
	clock := now.Add(time.Minute)
	_, fleet := fleetRunner(t, store, &clock, nil)
	control := &noEffectControl{fleetControl: fleet, failFirst: true}
	source := &changingSummary{}
	runner := wait.New(store, control, nil)
	runner.SetClock(func() time.Time { return clock })
	runner.DisableQuotaChecks = true
	runner.NodeHost = "host"
	runner.NodeSummary = source
	for i := 0; i < 6 && len(fleet.sends) == 0; i++ {
		clock = clock.Add(time.Hour)
		runner.Tick(ctx, nil, nil)
	}
	waits, err := store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(fleet.sends) != 1 {
		t.Fatalf("the retried wake was never sent: delivery=%s error=%q", waits[0].Delivery, waits[0].DeliveryError)
	}
	if source.calls != 1 {
		t.Fatalf("the summary was rebuilt %d times for one frozen wake", source.calls)
	}
	if fleet.texts[0] != waits[0].DeliveryPayload || !strings.Contains(fleet.texts[0], "Summary unavailable (unavailable)") {
		t.Fatalf("the retry did not send the frozen bytes:\n%s", fleet.texts[0])
	}
}
