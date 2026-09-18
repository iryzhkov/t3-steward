package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// isolationCampaign writes a campaign naming one project.
func isolationCampaign(t *testing.T, project string) string {
	t.Helper()
	root := t.TempDir()
	manifest := "version: 2\nname: " + project + "-work\nenvironment:\n  project: " + project +
		"\nroutes:\n  - instance: t3-primary\n    model: opus\n" +
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

// isolationReadinessService is a coordinator configured with one healthy
// project and one whose repository the catalog cannot accept. The fleet
// definitions keep both, which is what lets the matrix report the broken one as
// misconfigured rather than as unknown.
func isolationReadinessService(t *testing.T) (*backlogadmin.Service, *sqlite.Store) {
	t.Helper()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.SaveCoordinatorRecords(context.Background(), probeQuotaPools()); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(context.Background(), probeWorkerSnapshot("homelab")); err != nil {
		t.Fatal(err)
	}
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return probeNow })
	service.SetRuntimeInfo(backlogadmin.RuntimeInfo{
		Epoch: 1, Mode: "coordinator", Owner: "coordinator",
		MaxWorkerSnapshotAge: time.Minute,
		CatalogIssues: []string{
			`project:broken:invalid: project catalog: project "broken" repository: scheme "" is not allowed`,
		},
	})
	service.SetViability(backlogadmin.ViabilitySettings{
		Projects: []backlog.ProjectDefinition{
			{
				Name: "dev-fleet", Repository: probeRepository, DefaultRef: "main",
				SetupProfile: "go",
			},
			{
				// The malformed one. It stays in the definitions on purpose.
				Name: "broken", Repository: "test", DefaultRef: "main",
				SetupProfile: "go",
			},
		},
		SetupProfiles: []backlog.SetupProfile{{
			Name: "go", Commands: []string{"go build ./..."}, Timeout: time.Minute,
		}},
		Repository: probeObserver(map[string]repositoryProbeClient{
			"homelab": reachableWorker(t, "homelab"),
		}),
	})
	return service, store
}

// TestAMalformedProjectRefusesItsOwnSubmissionsOnly is the second half of the
// isolation property. The broken project's submission is refused permanently,
// by name and with the exact validation failure, and every other project on the
// same coordinator still schedules.
func TestAMalformedProjectRefusesItsOwnSubmissionsOnly(t *testing.T) {
	service, store := isolationReadinessService(t)
	submissions := probeSubmissions(t, service, store)

	_, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "broken-1",
		BundleDir:      isolationCampaign(t, "broken"),
		Principal:      "local:1000",
	})
	if err == nil {
		t.Fatal("a campaign naming a misconfigured project was accepted")
	}
	for _, want := range []string{
		backlogadmin.ReasonRepositorySyntaxInvalid, "broken", "can never run as written",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not contain %q", err.Error(), want)
		}
	}
	// It is the submission that fails, not the fleet: the healthy project on
	// the same coordinator still schedules.
	if _, err := submissions.SubmitDirectory(context.Background(), backlog.DirectorySubmission{
		IdempotencyKey: "healthy-1",
		BundleDir:      isolationCampaign(t, "dev-fleet"),
		Principal:      "local:1000",
	}); err != nil {
		t.Fatalf("one misconfigured project refused an unrelated campaign: %v", err)
	}
	if count := probeWorkflowCount(t, store); count != 1 {
		t.Fatalf("workflows = %d, want exactly the healthy one", count)
	}
}

// TestAMalformedProjectIsReportedAsMisconfiguredNotUnknown states the
// distinction the matrix has to make. "Unknown project" sends an operator to
// look for a missing configuration entry; the entry is there and is wrong.
func TestAMalformedProjectIsReportedAsMisconfiguredNotUnknown(t *testing.T) {
	service, _ := isolationReadinessService(t)
	request := backlogadmin.ViabilityRequest{Tasks: []backlogadmin.ViabilityTask{{
		Name: "implement", Project: "broken", Ref: "main",
	}}}
	response, err := service.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryViability,
		Principal: backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}},
		Viability: &request,
	})
	if err != nil {
		t.Fatal(err)
	}
	matrix := *response.Viability
	if matrix.Outcome != backlogadmin.ViabilityImpossible {
		t.Fatalf("outcome = %q", matrix.Outcome)
	}
	var codes []string
	for _, reason := range matrix.Tasks[0].Reasons {
		codes = append(codes, reason.Code)
		if reason.Code == backlogadmin.ReasonUnknownProject {
			t.Fatal("a configured but misconfigured project was reported as unknown")
		}
	}
	found := false
	for _, reason := range matrix.Tasks[0].Reasons {
		if reason.Code == backlogadmin.ReasonRepositorySyntaxInvalid &&
			strings.Contains(reason.Detail, "broken") {
			found = true
		}
	}
	if !found {
		t.Fatalf("reasons = %v, want the repository syntax failure by project name", codes)
	}
}

// TestCatalogHealthIsReportedInStatus states that an isolated failure is
// announced. Nothing else breaks to make an operator look, so the coordinator
// has to say so where health is read.
func TestCatalogHealthIsReportedInStatus(t *testing.T) {
	service, _ := isolationReadinessService(t)
	response, err := service.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryStatus,
		Principal: backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status == nil {
		t.Fatal("no status")
	}
	found := false
	for _, issue := range response.Status.Runtime.ReconciliationIssues {
		if strings.Contains(issue, "project:broken:invalid") {
			found = true
		}
	}
	if !found {
		t.Fatalf("reconciliation issues = %v", response.Status.Runtime.ReconciliationIssues)
	}
	if response.Status.Runtime.Health != "degraded" {
		t.Fatalf("health = %q, want degraded while configuration is unusable", response.Status.Runtime.Health)
	}
}
