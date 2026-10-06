package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
	"strings"
	"testing"
	"time"
)

func TestStructuredReviewTaskBoundWake(t *testing.T) {
	ctx := context.Background()
	s, a, now := taskWaitFixture(t)
	otherRun(t, s, now, domain.ProgressActive)
	_, err := s.RegisterTaskWait(ctx, nodeRegistration(a, "review-wait", domain.NodeRef{RunID: "r2", TaskID: domain.SinkTaskName}, ""), now)
	if err != nil {
		t.Fatal(err)
	}
	otherRun(t, s, now, domain.ProgressSucceeded)
	reviewed := loadAttempt(t, s, "a2")
	reviewed.ReviewVerdict = &domain.ReviewVerdict{Verdict: "changes-requested", BlockingFindings: 2, FindingTitles: []string{"lost evidence"}}
	saveFleetAttempt(t, s, reviewed)
	if err := s.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Result == nil || !strings.Contains(rows[0].Result.Output, "lost evidence") {
		t.Fatalf("result: %+v", rows)
	}
	wakes, err := s.WakeTaskWaits(ctx, now.Add(2*time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("%+v %v", wakes, err)
	}
	clock := now.Add(3 * time.Minute)
	runner, control := fleetRunner(t, s, &clock, nil)
	runner.Tick(ctx, nil, nil)
	if len(control.texts) != 1 {
		t.Fatal(control.texts)
	}
	fields, ok := wait.ParseWakeTrailer(control.texts[0])
	if !ok || fields["review"] != "changes-requested" || fields["blocking"] != "2" || !strings.Contains(control.texts[0], "lost evidence") {
		t.Fatalf("wake: %s", control.texts[0])
	}
	if strings.Contains(strings.Split(control.texts[0], "\n")[0], "lost evidence") {
		t.Fatal("title in trailer")
	}
}
