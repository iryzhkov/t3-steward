package main

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAutomaticEnrollment(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	cfg, store, artifacts := busyEnrollmentWorker(t)
	worker := cfg.BacklogV2.Workers["normandy"]
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"normandy": worker}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	check := func(ok bool, format string, args ...any) {
		t.Helper()
		if !ok {
			t.Fatalf(format, args...)
		}
	}
	reload := func() {
		sessions, err := newCoordinatorWorkerSessions(cfg.BacklogV2, store, 1, persistentTestCredentials{}, nil, artifacts)
		must(err)
		t.Cleanup(func() { must(sessions.close()) })
	}
	run := func(at time.Time) error {
		return reconcileWorkerEnrollments(ctx, cfg.BacklogV2, store, coordinatorEnrollmentHandlerWith(cfg.BacklogV2, store, 1, artifacts, persistentTestCredentials{}), at)
	}
	reload()
	must(run(now))
	values, err := store.LoadWorkerEnrollments(ctx)
	must(err)
	check(len(values) == 1 && strings.Contains(values[0].Request.Reason, "automatic"), "enrollments: %+v", values)
	initial := values[0].Revision
	must(run(now.Add(time.Second)))
	values, err = store.LoadWorkerEnrollments(ctx)
	must(err)
	check(values[0].Revision == initial, "current enrollment was repeated")
	worker.Executors.CPUUnits++
	cfg.BacklogV2.Workers["normandy"] = worker
	reload()
	must(run(now.Add(automaticEnrollmentBackoff)))
	values, err = store.LoadWorkerEnrollments(ctx)
	must(err)
	check(values[0].Revision == initial+1, "stale enrollment: %+v", values)
	requirements, err := store.LoadWorkerRequirements(ctx)
	must(err)
	request := currentCatalogRequest(domain.WorkerEnrollmentRequest{Reason: "explicit"}, backlogadmin.Worker{Requirement: &requirements[0], Enrollment: &values[0]})
	_, err = coordinatorEnrollmentHandlerWith(cfg.BacklogV2, store, 1, artifacts, persistentTestCredentials{})(ctx, backlogadmin.Principal{ID: "operator"}, request)
	must(err)
	// Use a fresh busy journal: the successful snapshots above can finish its attempt.
	cfg, store, artifacts = busyEnrollmentWorker(t)
	worker = cfg.BacklogV2.Workers["normandy"]
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"normandy": worker}
	project := cfg.BacklogV2.Projects["steward"]
	project.DefaultRef = "release"
	cfg.BacklogV2.Projects["steward"] = project
	reload()
	at := now.Add(2 * automaticEnrollmentBackoff)
	err = run(at)
	check(err != nil && strings.Contains(err.Error(), "retry when idle"), "refusal: %v", err)
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	must(err)
	response, err := service.Query(ctx, backlogadmin.Query{Version: backlogadmin.AutomaticEnrollmentReadVersion, Kind: backlogadmin.QueryWorkers, Principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}})
	must(err)
	var out bytes.Buffer
	must(renderWorkers(&out, response.Workers))
	check(strings.Contains(out.String(), "retry when idle") && !response.Workers[0].Enrolled, "refusal visibility: %s %+v", out.String(), response.Workers)
	reload()
	calls := 0
	probe := func(context.Context, backlogadmin.Principal, domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error) {
		calls++
		return domain.WorkerEnrollment{}, errors.New("probe refusal")
	}
	must(reconcileWorkerEnrollments(ctx, cfg.BacklogV2, store, probe, at.Add(time.Second)))
	check(calls == 0, "reload erased backoff")
	err = reconcileWorkerEnrollments(ctx, cfg.BacklogV2, store, probe, at.Add(automaticEnrollmentBackoff))
	check(err != nil && calls == 1, "deadline retry: %d %v", calls, err)
	for _, state := range []string{"draining", "removed", "opted-out", "unconfigured"} {
		requirements, err = store.LoadWorkerRequirements(ctx)
		must(err)
		requirements[0].Draining = state == "draining"
		if state == "removed" {
			requirements[0].Connection = "removed"
		}
		must(store.SaveWorkerRequirements(ctx, requirements))
		worker.AcceptBacklog = state != "opted-out"
		cfg.BacklogV2.Workers = map[string]config.V2Worker{"normandy": worker}
		if state == "unconfigured" {
			cfg.BacklogV2.Workers = nil
		}
		before := calls
		must(reconcileWorkerEnrollments(ctx, cfg.BacklogV2, store, probe, at.Add(3*automaticEnrollmentBackoff)))
		retained, err := store.LoadWorkerRequirements(ctx)
		must(err)
		check(calls == before && retained[0].Draining == requirements[0].Draining && cfg.BacklogV2.Workers["normandy"].AcceptBacklog == (state != "opted-out" && state != "unconfigured"), "%s changed admission", state)
		requirements[0].Connection = "unix"
		must(store.SaveWorkerRequirements(ctx, requirements))
	}
}

func TestAutomaticEnrollmentTickWorkersSelection(t *testing.T) {
	var events []string
	sessions := coordinatorWorkerSessions{
		workerIDs: []string{"a", "b", "c"},
		enroll:    func(context.Context) error { events = append(events, "enroll"); return errors.New("refused") },
		handOn: func(_ context.Context, id string) (backlog.WorkerExchangeReport, error) {
			events = append(events, "hand:"+id)
			return backlog.WorkerExchangeReport{}, nil
		},
		exchange: func(_ context.Context, id string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
			events = append(events, "exchange:"+id)
			return backlog.WorkerExchangeReport{}, nil
		},
	}
	sessions.TickWorkers(context.Background(), backlog.QuotaBridgeReport{}, []string{"c", "unknown", "a", "a"})
	if !reflect.DeepEqual(events, []string{"enroll", "hand:a", "hand:c", "exchange:a", "exchange:c"}) {
		t.Fatalf("subset events: %v", events)
	}
	events = nil
	sessions.Tick(context.Background(), backlog.QuotaBridgeReport{})
	if !reflect.DeepEqual(events, []string{"enroll", "hand:a", "hand:b", "hand:c", "exchange:a", "exchange:b", "exchange:c"}) {
		t.Fatalf("delegation events: %v", events)
	}
}
