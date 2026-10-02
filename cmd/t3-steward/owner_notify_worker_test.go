package main

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fixedWorkerView struct {
	workers []backlogadmin.Worker
	now     time.Time
}

func (v fixedWorkerView) Workers(context.Context) ([]backlogadmin.Worker, time.Time, error) {
	return v.workers, v.now, nil
}

func enrolledWorker(id string, connected bool, observed time.Time) backlogadmin.Worker {
	return backlogadmin.Worker{
		Requirement: &domain.WorkerRequirement{WorkerID: id, Connection: "persistent-ssh"},
		Enrollment:  &domain.WorkerEnrollment{Request: domain.WorkerEnrollmentRequest{WorkerID: id}, EnrolledAt: observed.Add(-time.Hour)},
		Enrolled:    true,
		Snapshot:    domain.WorkerSnapshot{WorkerID: id, Connected: connected, ObservedAt: observed},
		Stale:       !connected,
	}
}

// The coordinator hands its owner channels every worker it cannot reach, and
// marks Down only those past the threshold and outside maintenance. The
// threshold is counted from the coordinator's own start at the earliest.
func TestTheCoordinatorReportsWorkersPastTheThresholdAsDown(t *testing.T) {
	now := time.Date(2026, 10, 2, 4, 40, 0, 0, time.UTC)
	draining := enrolledWorker("homelab", false, now.Add(-time.Hour))
	draining.Requirement.Draining = true
	view := fixedWorkerView{now: now, workers: []backlogadmin.Worker{
		enrolledWorker("normandy", false, now.Add(-time.Hour)),
		enrolledWorker("omarchy-pc", false, now.Add(-3*time.Minute)),
		enrolledWorker("laptop", true, now),
		draining,
	}}
	states, err := ownerNotificationWorkers(view, 10*time.Minute, now.Add(-2*time.Hour))(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	down := map[string]bool{}
	for _, state := range states {
		down[state.WorkerID] = state.Down
	}
	if len(states) != 3 || !down["normandy"] || down["omarchy-pc"] || down["homelab"] {
		t.Fatalf("states = %+v, want normandy down, omarchy-pc in its grace period and homelab in maintenance", states)
	}
	states, _ = ownerNotificationWorkers(view, 10*time.Minute, now.Add(-time.Minute))(context.Background())
	for _, state := range states {
		if state.Down {
			t.Fatalf("%s is down a minute after the coordinator started", state.WorkerID)
		}
	}
}
