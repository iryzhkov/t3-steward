package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// fleetAnswer answers the two queries the fleet check makes.
func fleetAnswer(now, lastReload time.Time, workers []backlogadmin.Worker, err error) fleetQuery {
	return func(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
		if err != nil {
			return backlogadmin.Response{}, err
		}
		switch query.Kind {
		case backlogadmin.QueryStatus:
			return backlogadmin.Response{GeneratedAt: now, Status: &backlogadmin.Status{
				Runtime: backlogadmin.RuntimeStatus{LastReload: lastReload, Owner: "normandy-coordinator"}}}, nil
		case backlogadmin.QueryWorkers:
			return backlogadmin.Response{GeneratedAt: now, Workers: workers}, nil
		}
		return backlogadmin.Response{}, errors.New("unexpected query " + string(query.Kind))
	}
}

func renderCheckLines(lines []checkLine) string {
	var b strings.Builder
	for _, line := range lines {
		b.WriteString(line.level + "  " + line.text + "\n")
	}
	return b.String()
}

// "check" fails, naming the worker and the commands that bring it back, for an
// enrolled worker the coordinator has not reached for longer than the
// threshold. A worker inside the grace period or in maintenance is a warning,
// and a fleet with every worker connected is one ok line.
func TestCheckFailsForAWorkerDownPastTheThreshold(t *testing.T) {
	now := time.Date(2026, 10, 2, 4, 40, 0, 0, time.UTC)
	draining := enrolledWorker("homelab", false, now.Add(-time.Hour))
	draining.Requirement.Draining = true
	workers := []backlogadmin.Worker{
		enrolledWorker("normandy", false, now.Add(-62*time.Minute)),
		enrolledWorker("omarchy-pc", false, now.Add(-3*time.Minute)),
		enrolledWorker("laptop", true, now),
		draining,
	}
	lines := checkFleetWorkers(context.Background(), fleetAnswer(now, now.Add(-24*time.Hour), workers, nil), 10*time.Minute)
	text := renderCheckLines(lines)
	if !checkLinesFailed(lines) {
		t.Fatalf("a worker down for an hour did not fail check:\n%s", text)
	}
	for _, want := range []string{
		"FAIL  worker normandy is not connected to coordinator normandy-coordinator: last seen 2026-10-02T03:38:00Z, 1h02m ago",
		"systemctl --user restart t3-steward-worker",
		"warn  worker omarchy-pc is not connected",
		"warn  worker homelab is not connected and is in maintenance",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("check output lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "laptop") {
		t.Fatalf("a connected worker was reported:\n%s", text)
	}

	// Restarted: nothing fails, one ok line.
	lines = checkFleetWorkers(context.Background(), fleetAnswer(now, now.Add(-24*time.Hour),
		[]backlogadmin.Worker{enrolledWorker("normandy", true, now), enrolledWorker("laptop", true, now)}, nil), 10*time.Minute)
	if checkLinesFailed(lines) || !strings.Contains(renderCheckLines(lines), "ok  fleet: 2 enrolled workers connected to coordinator normandy-coordinator") {
		t.Fatalf("a connected fleet:\n%s", renderCheckLines(lines))
	}
}

// A coordinator that restarted a minute ago has not seen any worker since it
// came up; that is not an outage yet.
func TestCheckCountsAnOutageFromTheCoordinatorsStart(t *testing.T) {
	now := time.Date(2026, 10, 2, 4, 40, 0, 0, time.UTC)
	lines := checkFleetWorkers(context.Background(), fleetAnswer(now, now.Add(-time.Minute),
		[]backlogadmin.Worker{enrolledWorker("normandy", false, now.Add(-time.Hour))}, nil), 10*time.Minute)
	if checkLinesFailed(lines) {
		t.Fatalf("check failed a minute after the coordinator started:\n%s", renderCheckLines(lines))
	}
}

// A coordinator that cannot be asked is a warning with the command to retry,
// not a failure of this host's own checks.
func TestCheckWarnsWhenTheCoordinatorCannotBeRead(t *testing.T) {
	lines := checkFleetWorkers(context.Background(), fleetAnswer(time.Time{}, time.Time{}, nil, errors.New("connection refused")), 10*time.Minute)
	text := renderCheckLines(lines)
	if checkLinesFailed(lines) || !strings.Contains(text, "warn  fleet workers not read: connection refused") || !strings.Contains(text, "t3-steward worker list") {
		t.Fatalf("an unreachable coordinator:\n%s", text)
	}
}
