package main

import (
	"bytes"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// heterogeneousRouteFixture is availabilityFixture with a second worker that
// advertises another model of the same instance and is not ready: the ready
// worker omarchy-pc offers t3-primary/opus, the degraded worker
// unready-sonnet offers only t3-primary/sonnet. The pool is complete, fresh and
// open, so the only thing that tells the two routes apart is which worker can
// dispatch each of them.
func heterogeneousRouteFixture(now time.Time) ([]backlogadmin.Worker, []backlogadmin.Quota) {
	workers, quotas := availabilityFixture(now)
	unready := workers[0]
	unready.Health = string(domain.WorkerHealthDegraded)
	unready.Snapshot.WorkerID = "unready-sonnet"
	unready.Snapshot.QuotaObservations = nil
	unready.Snapshot.Inventory.Providers = append([]domain.WorkerProviderInventory(nil), workers[0].Snapshot.Inventory.Providers...)
	unready.Snapshot.Inventory.Providers[0].Models = []string{"sonnet"}
	return append(workers, unready), quotas
}

// R1: availability was decided per instance, so one ready worker offering
// opus made t3-primary/sonnet "available" too, although the only worker that
// offers sonnet is not ready. Each route is now judged against the ready
// workers that advertise that model, and the text row, the JSON document and
// --available all carry that one verdict.
func TestModelsAvailabilityIsDecidedPerModelRoute(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := heterogeneousRouteFixture(now)
	document := buildModelsDocument("", workers, quotas, nil, time.Hour)

	var out bytes.Buffer
	if err := renderModels(&out, document); err != nil {
		t.Fatal(err)
	}
	for route, status := range map[string]string{
		"t3-primary/opus":   `1/1 ready\s+available`,
		"t3-primary/sonnet": `0/1 ready\s+no ready worker`,
	} {
		if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(route) + `\s.*\s` + status + `\s*$`).MatchString(out.String()) {
			t.Errorf("the row for %s does not end %q:\n%s", route, status, out.String())
		}
	}

	encoded := modelsDocumentKeys(t, mustModelsJSON(t, document))
	instance := encoded["instances"].([]any)[0].(map[string]any)
	routes, _ := instance["routes"].([]any)
	codes := map[string]any{}
	for _, route := range routes {
		entry := route.(map[string]any)
		codes[entry["model"].(string)] = entry["availability"].(map[string]any)["code"]
	}
	if codes["opus"] != modelsStatusAvailable || codes["sonnet"] != modelsAvailabilityNoReadyWorker || len(codes) != 2 {
		t.Errorf("JSON route codes = %v, want opus available and sonnet %s", codes, modelsAvailabilityNoReadyWorker)
	}

	scoped := scopeModelsDocument(document, modelsScope{Available: true})
	if len(scoped.Instances) != 1 {
		t.Fatalf("--available kept %d instances, want t3-primary for its opus route", len(scoped.Instances))
	}
	if kept := scoped.Instances[0].Models; len(kept) != 1 || kept[0] != "opus" {
		t.Errorf("--available kept models %v, want only opus: the only worker advertising sonnet is not ready", kept)
	}
	if scoped.TotalRoutes != 2 {
		t.Errorf("--available reports %d routes in the unnarrowed answer, want 2", scoped.TotalRoutes)
	}
	out.Reset()
	if err := renderModels(&out, scoped); err != nil {
		t.Fatal(err)
	}
	if regexp.MustCompile(`t3-primary/sonnet`).MatchString(out.String()) {
		t.Errorf("--available printed the sonnet route:\n%s", out.String())
	}

	// With the sonnet worker ready as well, both routes are available, which is
	// the case the per-instance verdict already got right.
	workers[1].Health = string(domain.WorkerHealthReady)
	both := scopeModelsDocument(buildModelsDocument("", workers, quotas, nil, time.Hour), modelsScope{Available: true})
	if len(both.Instances) != 1 || len(both.Instances[0].Models) != 2 {
		t.Errorf("with both workers ready --available kept %+v, want opus and sonnet", both.Instances)
	}

	// With neither worker ready, no route is available and the instance says
	// so in the same words its routes do.
	workers[0].Health = string(domain.WorkerHealthDegraded)
	workers[1].Health = string(domain.WorkerHealthDegraded)
	none := buildModelsDocument("", workers, quotas, nil, time.Hour)
	if got := modelsInstanceByName(t, none, "t3-primary").Availability.Code; got != modelsAvailabilityNoReadyWorker {
		t.Errorf("with no ready worker the instance code is %q, want %q", got, modelsAvailabilityNoReadyWorker)
	}
	if scoped := scopeModelsDocument(none, modelsScope{Available: true}); len(scoped.Instances) != 0 {
		t.Errorf("with no ready worker --available kept %+v", scoped.Instances)
	}
}

// R2: ObserveQuotaPool takes percent, reset and observation time from one
// window, and models then replaced the time with the oldest reading of any
// window, publishing a tuple no observation supports. Here five_hour is the
// oldest reading and seven_day the most used, and the aggregate must be the
// seven_day reading whole; the oldest reading is reported on its own.
func TestModelsAggregateTupleComesFromOneObservation(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := availabilityFixture(now)
	fiveObserved, weekObserved := now.Add(-40*time.Minute), now.Add(-time.Minute)
	weekReset := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	workers[0].Snapshot.QuotaObservations[0].ObservedAt = fiveObserved
	workers[0].Snapshot.QuotaObservations[1].ObservedAt = weekObserved
	workers[0].Snapshot.QuotaObservations[1].ResetsAt = &weekReset

	document := buildModelsDocument("", workers, quotas, nil, time.Hour)
	instance := modelsInstanceByName(t, document, "t3-primary")
	if instance.Percent == nil || *instance.Percent != 20 || instance.ResetsAt == nil || !instance.ResetsAt.Equal(weekReset) ||
		instance.ObservedAt == nil || !instance.ObservedAt.Equal(weekObserved) {
		percent := "none"
		if instance.Percent != nil {
			percent = fmt.Sprint(*instance.Percent)
		}
		t.Fatalf("aggregate percent=%s reset=%v observed=%v, want the seven_day reading 20%% reset %v observed %v",
			percent, instance.ResetsAt, instance.ObservedAt, weekReset, weekObserved)
	}
	if instance.Stale {
		t.Errorf("both readings are fresh and the pool is marked stale")
	}

	encoded := modelsDocumentKeys(t, mustModelsJSON(t, document))
	entry := encoded["instances"].([]any)[0].(map[string]any)
	if entry["observedAt"] != weekObserved.Format(time.RFC3339) || entry["resetsAt"] != weekReset.Format(time.RFC3339) || entry["percent"] != 20.0 {
		t.Errorf("JSON aggregate percent=%v resetsAt=%v observedAt=%v, want one seven_day tuple", entry["percent"], entry["resetsAt"], entry["observedAt"])
	}
	if entry["oldestObservedAt"] != fiveObserved.Format(time.RFC3339) {
		t.Errorf("JSON oldestObservedAt = %v, want the five_hour reading at %v", entry["oldestObservedAt"], fiveObserved)
	}

	var out bytes.Buffer
	if err := renderModels(&out, document); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^t3-primary/opus\s+pool-claude\s+\S+\s+20%\s+1m\s`).MatchString(out.String()) {
		t.Errorf("the row does not pair USED 20%% with the seven_day reading's age 1m:\n%s", out.String())
	}
}
