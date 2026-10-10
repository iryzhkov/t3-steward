package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

const coordinatorHealthUsage = `Usage: t3-steward coordinator health [--wait-ready] [--timeout D] [--json]

Report coordinator epoch, release, health and connected workers. --wait-ready
retries temporary unavailability and waits for healthy status and every expected
worker to have a fresh connected snapshot in the current coordinator epoch.
Expected workers come from the coordinator's worker view; removed workers are
excluded. Draining workers still count. Default timeout: 60s, poll interval: 1s.
Read-only, using the same local or remote transport as coordinator identity.
--config PATH selects the configuration file (default: platform config directory).
Exit 0: ready (or a single report without --wait-ready); 6: readiness timeout.
Permanent authentication, configuration and protocol failures return immediately.
On timeout --json prints the last health document, with ready=false.
`

type coordinatorHealthDocument struct {
	Version          string   `json:"version"`
	Kind             string   `json:"kind"`
	CoordinatorID    string   `json:"coordinatorId"`
	Epoch            int64    `json:"epoch"`
	Release          string   `json:"release"`
	Health           string   `json:"health"`
	ConnectedWorkers []string `json:"connectedWorkers"`
	ExpectedWorkers  []string `json:"expectedWorkers"`
	MissingWorkers   []string `json:"missingWorkers"`
	Ready            bool     `json:"ready"`
	Error            string   `json:"error,omitempty"`
}

func cmdCoordinatorHealth(g globalFlags, args []string) error {
	fs := flag.NewFlagSet("coordinator health", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	waitReady := fs.Bool("wait-ready", false, "wait for healthy coordinator and connected expected workers")
	timeout := fs.Duration("timeout", 60*time.Second, "readiness deadline")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *timeout <= 0 {
		return errors.New("coordinator health takes no arguments and --timeout must be positive")
	}
	cfg, err := loadConfig(g)
	if err != nil {
		return err
	}
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		return err
	}
	query := func(ctx context.Context) (backlogadmin.Status, []backlogadmin.Worker, error) {
		response, err := transport.client.Query(ctx, backlogadmin.Query{Version: backlogadmin.Version, Kind: backlogadmin.QueryStatus, Principal: transport.principal})
		if err != nil {
			return backlogadmin.Status{}, nil, err
		}
		if response.Status == nil {
			return backlogadmin.Status{}, nil, errors.New("coordinator status query returned no status")
		}
		workers, err := transport.client.Query(ctx, backlogadmin.Query{Version: backlogadmin.Version, Kind: backlogadmin.QueryWorkers, Principal: transport.principal})
		return *response.Status, workers.Workers, err
	}
	return runCoordinatorHealth(context.Background(), os.Stdout, *asJSON, *waitReady, *timeout, time.Second, transport.client.Describe().CoordinatorID, query)
}

type coordinatorHealthQuery func(context.Context) (backlogadmin.Status, []backlogadmin.Worker, error)

func healthDocument(id string, status backlogadmin.Status, workers []backlogadmin.Worker) coordinatorHealthDocument {
	doc := coordinatorHealthDocument{Version: backlogadmin.Version, Kind: "coordinator-health", CoordinatorID: id,
		Epoch: status.Runtime.Epoch, Release: status.Runtime.Release, Health: status.Runtime.Health,
		ConnectedWorkers: []string{}, ExpectedWorkers: []string{}, MissingWorkers: []string{}}
	for _, worker := range workers {
		if worker.Requirement != nil && worker.Requirement.Connection == "removed" {
			continue
		}
		id := worker.Snapshot.WorkerID
		if worker.Requirement != nil {
			id = worker.Requirement.WorkerID
		}
		doc.ExpectedWorkers = append(doc.ExpectedWorkers, id)
		if worker.Snapshot.Connected && !worker.Stale && worker.Snapshot.CoordinatorEpoch == doc.Epoch {
			doc.ConnectedWorkers = append(doc.ConnectedWorkers, id)
		} else {
			doc.MissingWorkers = append(doc.MissingWorkers, id)
		}
	}
	sort.Strings(doc.ExpectedWorkers)
	sort.Strings(doc.ConnectedWorkers)
	sort.Strings(doc.MissingWorkers)
	doc.Ready = doc.Health == "healthy" && doc.Epoch > 0 && len(doc.MissingWorkers) == 0
	return doc
}

func runCoordinatorHealth(ctx context.Context, out io.Writer, asJSON, waitReady bool, timeout, poll time.Duration, id string, query coordinatorHealthQuery) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := coordinatorHealthDocument{Version: backlogadmin.Version, Kind: "coordinator-health", CoordinatorID: id, Health: "unavailable",
		ConnectedWorkers: []string{}, ExpectedWorkers: []string{}, MissingWorkers: []string{}}
	for {
		status, workers, err := query(ctx)
		if err == nil {
			last = healthDocument(id, status, workers)
			if !waitReady || last.Ready {
				return renderCoordinatorHealth(out, asJSON, last)
			}
		} else {
			class := backlogadmin.ClassOf(err)
			if !waitReady || (ctx.Err() == nil && class != backlogadmin.ClassUnavailable && class != backlogadmin.ClassTimeout) {
				return err
			}
			last.Ready = false
			last.Health = "unavailable"
			last.Error = err.Error()
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			last.Ready = false
			if err := renderCoordinatorHealth(out, asJSON, last); err != nil {
				return err
			}
			return afterDocument(&backlogadmin.TransportError{Class: backlogadmin.ClassTimeout, Operation: "coordinator health", Err: fmt.Errorf("coordinator readiness deadline: %w", ctx.Err())})
		case <-timer.C:
		}
	}
}

func renderCoordinatorHealth(out io.Writer, asJSON bool, doc coordinatorHealthDocument) error {
	if asJSON {
		return json.NewEncoder(out).Encode(doc)
	}
	_, err := fmt.Fprintf(out, "coordinator %s\nepoch %d\nrelease %s\nhealth %s\nready %t\nconnected %v\nexpected %v\nmissing %v\n", doc.CoordinatorID, doc.Epoch, doc.Release, doc.Health, doc.Ready, doc.ConnectedWorkers, doc.ExpectedWorkers, doc.MissingWorkers)
	return err
}
