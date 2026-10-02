package main

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
)

// fleetQuery asks the coordinator one admin query. It is the seam the fleet
// part of "check" and "triage" read through; the daemon's transport and a
// test fixture both answer it.
type fleetQuery func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)

// checkLine is one line of "check" output: ok, warn or FAIL, and the text.
type checkLine struct{ level, text string }

func checkLinesFailed(lines []checkLine) bool {
	for _, line := range lines {
		if line.level == "FAIL" {
			return true
		}
	}
	return false
}

// coordinatorFleetQuery binds fleetQuery to this host's coordinator transport,
// and reports false when this host has neither a coordinator nor a client of
// one, which is a host with no fleet to check.
func coordinatorFleetQuery(cfg config.Config) (fleetQuery, bool) {
	if cfg.BacklogV2.Mode != "coordinator" && !cfg.BacklogV2.CoordinatorClient.Configured() {
		return nil, false
	}
	var transport *coordinatorTransport
	return func(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
		if transport == nil {
			built, err := newCoordinatorTransport(cfg)
			if err != nil {
				return backlogadmin.Response{}, err
			}
			transport = &built
		}
		query.Version = backlogadmin.Version
		query.Principal = transport.principal
		return transport.client.Query(ctx, query)
	}, true
}

// fleetOutages reads the workers the coordinator cannot reach, measured from
// the coordinator's last start or reload at the earliest: a coordinator that
// came up a minute ago has not seen anybody for longer than that, and that is
// not an outage yet. It returns the coordinator's id as well.
func fleetOutages(ctx context.Context, query fleetQuery) ([]backlogadmin.WorkerOutage, int, string, error) {
	var notBefore time.Time
	coordinator := ""
	if status, err := query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryStatus}); err == nil && status.Status != nil {
		notBefore = status.Status.Runtime.LastReload
		coordinator = status.Status.Runtime.Owner
	}
	response, err := query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkers})
	if err != nil {
		return nil, 0, coordinator, err
	}
	enrolled := 0
	for _, worker := range response.Workers {
		if worker.Requirement != nil && worker.Enrollment != nil {
			enrolled++
		}
	}
	return backlogadmin.WorkerOutages(response.Workers, response.GeneratedAt, notBefore), enrolled, coordinator, nil
}

// checkFleetWorkers is the fleet part of "check": every enrolled worker the
// coordinator has not reached for longer than after fails, with the commands
// that bring it back; a shorter outage or one in maintenance is a warning.
func checkFleetWorkers(ctx context.Context, query fleetQuery, after time.Duration) []checkLine {
	if after <= 0 {
		after = backlogadmin.DefaultWorkerDownAfter
	}
	outages, enrolled, coordinator, err := fleetOutages(ctx, query)
	if err != nil {
		return []checkLine{{"warn", fmt.Sprintf("fleet workers not read: %v; run t3-steward worker list to see the coordinator's view once it answers", err)}}
	}
	if coordinator == "" {
		coordinator = "(unnamed)"
	}
	down := map[string]bool{}
	for _, outage := range backlogadmin.WorkersDown(outages, after) {
		down[outage.WorkerID] = true
	}
	var lines []checkLine
	for _, outage := range outages {
		seen := "never seen since it was enrolled at " + outage.Since.UTC().Format(time.RFC3339)
		if !outage.LastSeen.IsZero() {
			seen = fmt.Sprintf("last seen %s, %s ago", outage.LastSeen.UTC().Format(time.RFC3339), humanDuration(outage.DownFor))
		}
		switch {
		case down[outage.WorkerID]:
			lines = append(lines, checkLine{"FAIL", fmt.Sprintf(
				"worker %s is not connected to coordinator %s: %s (threshold %s, notifications.worker_down_after). "+
					"On %s run systemctl --user status t3-steward-worker, then systemctl --user restart t3-steward-worker; "+
					"journalctl --user -u t3-steward-worker -n 50 shows why it stopped",
				outage.WorkerID, coordinator, seen, humanDuration(after), outage.WorkerID)})
		case outage.Maintenance:
			lines = append(lines, checkLine{"warn", fmt.Sprintf(
				"worker %s is not connected and is in maintenance (accept_backlog: false or connection removed in the coordinator's worker entry), so it is not alerted on: %s",
				outage.WorkerID, seen)})
		default:
			lines = append(lines, checkLine{"warn", fmt.Sprintf(
				"worker %s is not connected to coordinator %s: %s; it counts as down after %s",
				outage.WorkerID, coordinator, seen, humanDuration(after))})
		}
	}
	if len(outages) == 0 {
		lines = append(lines, checkLine{"ok", fmt.Sprintf("fleet: %d enrolled workers connected to coordinator %s", enrolled, coordinator)})
	}
	return lines
}
