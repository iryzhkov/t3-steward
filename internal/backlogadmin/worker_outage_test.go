package backlogadmin

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// fleetWorker is one row of the workers view as the coordinator renders it.
func fleetWorker(id string, connected, stale bool, observed time.Time) Worker {
	enrolledAt := observed.Add(-24 * time.Hour)
	return Worker{
		Requirement: &domain.WorkerRequirement{WorkerID: id, Connection: "persistent-ssh"},
		Enrollment:  &domain.WorkerEnrollment{Request: domain.WorkerEnrollmentRequest{WorkerID: id}, EnrolledAt: enrolledAt},
		Enrolled:    true,
		Snapshot:    domain.WorkerSnapshot{WorkerID: id, Connected: connected, ObservedAt: observed},
		Stale:       stale,
	}
}

// Only enrolled workers are watched, and only the ones the coordinator cannot
// reach right now. The outage is measured from when the worker was last seen,
// but never from before the coordinator itself started: a coordinator that was
// down for an hour has not seen anybody for an hour either.
func TestWorkerOutagesNameEnrolledWorkersTheCoordinatorCannotReach(t *testing.T) {
	now := time.Date(2026, 10, 2, 4, 40, 0, 0, time.UTC)
	started := now.Add(-5 * time.Minute)
	longAgo := now.Add(-62 * time.Minute)
	draining := fleetWorker("homelab", false, true, longAgo)
	draining.Requirement.Draining = true
	unconfigured := fleetWorker("laptop", false, true, longAgo)
	unconfigured.Requirement, unconfigured.Enrollment = nil, nil
	never := fleetWorker("fresh", false, true, time.Time{})
	never.Enrollment.EnrolledAt = now.Add(-2 * time.Hour)
	workers := []Worker{
		fleetWorker("normandy", false, true, longAgo),
		fleetWorker("omarchy-pc", true, false, now.Add(-10*time.Second)),
		draining, unconfigured, never,
	}

	outages := WorkerOutages(workers, now, time.Time{})
	if len(outages) != 3 {
		t.Fatalf("outages = %+v, want normandy, homelab (draining) and fresh", outages)
	}
	byID := map[string]WorkerOutage{}
	for _, outage := range outages {
		byID[outage.WorkerID] = outage
	}
	if got := byID["normandy"]; got.DownFor != 62*time.Minute || !got.Since.Equal(longAgo) || got.Maintenance {
		t.Fatalf("normandy = %+v", got)
	}
	if got := byID["homelab"]; !got.Maintenance {
		t.Fatalf("a draining worker is not reported as maintenance: %+v", got)
	}
	if got := byID["fresh"]; !got.Since.Equal(now.Add(-2*time.Hour)) || got.DownFor != 2*time.Hour {
		t.Fatalf("a worker never seen is not measured from its enrollment: %+v", got)
	}

	afterRestart := WorkerOutages(workers, now, started)
	for _, outage := range afterRestart {
		if outage.DownFor != 5*time.Minute {
			t.Fatalf("%s is down for %s, measured from before the coordinator started", outage.WorkerID, outage.DownFor)
		}
		if outage.WorkerID == "normandy" && !outage.Since.Equal(longAgo) {
			t.Fatalf("the episode identity moved with the restart: %+v", outage)
		}
	}

	if down := WorkersDown(outages, 10*time.Minute); len(down) != 2 || down[0].WorkerID != "fresh" || down[1].WorkerID != "normandy" {
		t.Fatalf("down past ten minutes = %+v, want fresh and normandy (the draining worker is in maintenance)", down)
	}
	if down := WorkersDown(afterRestart, 10*time.Minute); len(down) != 0 {
		t.Fatalf("workers were down past the grace five minutes after a coordinator restart: %+v", down)
	}
}
