package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// routelessCampaignFixture is the probe campaign with its routes removed: a
// manifest the parser accepts and the planner cannot place.
func routelessCampaignFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	manifest := "version: 2\nname: routeless\nenvironment:\n  project: dev-fleet\n" +
		"tasks:\n  implement:\n    prompt_file: prompts/implement.md\n"
	for name, content := range map[string]string{
		"workflow.yaml":        manifest,
		"prompts/implement.md": "implement the plan\n",
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// TestSubmissionRefusesARouteLessCampaignSoThePlannerNeverSeesIt is U-2 closed
// at intake. Before this change the submission was accepted, the task became a
// candidate on every eligible worker with a nil route, and the coordinator's
// planning report then failed for the whole fleet on every tick ("planner
// proposed attempt ... without a provider route") until somebody cancelled it.
// Now the coordinator refuses the submission permanently as no-route, names
// the routes the eligible workers advertise, and creates nothing, so the
// records the planner reads never hold a route-less task.
func TestSubmissionRefusesARouteLessCampaignSoThePlannerNeverSeesIt(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{
		"homelab": reachableWorker(t, "homelab"),
	})
	service, store := probeReadinessService(t, observer, "homelab")
	submissions := probeSubmissions(t, service, store)

	_, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "routeless-1",
		BundleDir:      routelessCampaignFixture(t),
		Principal:      "local:1000",
	})
	if err == nil {
		t.Fatal("a campaign with no route was accepted")
	}
	for _, want := range []string{"can never run as written", "no-route", "t3-primary/opus"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not say %q", err.Error(), want)
		}
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Workflows) != 0 || len(records.Tasks) != 0 {
		t.Fatalf("a refused campaign left %d workflow(s) and %d task(s) for the planner", len(records.Workflows), len(records.Tasks))
	}
	// The same campaign with a route the worker advertises is accepted, so it
	// is the missing route and nothing else that refuses it.
	if _, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "routed-1",
		BundleDir:      probeCampaignFixture(t),
		Principal:      "local:1000",
	}); err != nil {
		t.Fatalf("the routed campaign was refused: %v", err)
	}
}

// TestCheckRefusesARouteLessTaskWithTheAdvertisedRoutes is the same rule on
// the read-only path an agent runs first: check reports the task impossible
// with no-route at the task level, and the detail is the list of routes the
// caller could declare instead.
func TestCheckRefusesARouteLessTaskWithTheAdvertisedRoutes(t *testing.T) {
	service, _ := probeReadinessService(t, probeObserver(map[string]repositoryProbeClient{
		"homelab": reachableWorker(t, "homelab"),
	}), "homelab")
	request := backlogadmin.ViabilityRequest{Tasks: []backlogadmin.ViabilityTask{{
		Name: "implement", Project: "dev-fleet", Ref: "main",
		Class: domain.TaskClassRequired, Capabilities: []string{"git"},
	}}}
	response, err := service.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryViability,
		Principal: backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}},
		Viability: &request,
	})
	if err != nil {
		t.Fatal(err)
	}
	matrix := response.Viability
	if matrix == nil || matrix.Outcome != backlogadmin.ViabilityImpossible {
		t.Fatalf("matrix = %+v", matrix)
	}
	var found bool
	for _, reason := range matrix.Tasks[0].Reasons {
		if reason.Code == "no-route" {
			found = true
			if !reason.Permanent || !strings.Contains(reason.Detail, "t3-primary/opus") {
				t.Fatalf("reason = %+v", reason)
			}
		}
	}
	if !found {
		t.Fatalf("no-route is not among the task reasons: %+v", matrix.Tasks[0].Reasons)
	}
	// The refusal the CLI prints for check and submit carries the code.
	if err := campaignImpossible(*matrix); err == nil || !strings.Contains(err.Error(), "no-route") {
		t.Fatalf("campaignImpossible = %v", err)
	}
}
