package main

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// modelsFixtureService answers each query kind from its own fixture, because
// models joins three views and a fake that returns one response for every
// query would hide which view a field came from.
type modelsFixtureService struct {
	workers  []backlogadmin.Worker
	quotas   []backlogadmin.Quota
	projects []backlogadmin.Project
	kinds    []backlogadmin.QueryKind
	filters  []backlogadmin.Filter
}

func (s *modelsFixtureService) Query(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
	s.kinds = append(s.kinds, query.Kind)
	s.filters = append(s.filters, query.Filter)
	response := backlogadmin.Response{Version: backlogadmin.Version, Kind: query.Kind}
	switch query.Kind {
	case backlogadmin.QueryWorkers:
		response.Workers = s.workers
	case backlogadmin.QueryQuota:
		response.Quotas = s.quotas
	case backlogadmin.QueryProjects:
		for _, project := range s.projects {
			if query.Filter.Project == "" || project.Name == query.Filter.Project {
				response.Projects = append(response.Projects, project)
			}
		}
	}
	return response, nil
}

func modelsFixture() *modelsFixtureService {
	observed := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	primaryBucket := domain.BucketKey{ProviderInstanceID: "t3-primary", LimitID: "claude", Window: "seven_day"}
	worker := func(id string, providers []domain.WorkerProviderInventory, observations []domain.WorkerQuotaObservation) backlogadmin.Worker {
		return backlogadmin.Worker{
			State: "observed", Enrolled: true, Health: string(domain.WorkerHealthReady),
			Snapshot: domain.WorkerSnapshot{
				WorkerID: id, Connected: true,
				Inventory: domain.WorkerInventory{
					AcceptBacklog: true,
					Projects:      []domain.WorkerProjectInventory{{Name: "steward", Available: true}},
					Providers:     providers,
				},
				QuotaObservations: observations,
			},
		}
	}
	return &modelsFixtureService{
		workers: []backlogadmin.Worker{
			worker("omarchy-pc", []domain.WorkerProviderInventory{
				{InstanceID: "t3-primary", QuotaPoolID: "pool-claude", Available: true, Models: []string{"opus", "claude-haiku-4-5"}},
				{InstanceID: "opencode", QuotaPoolID: "", Available: true, Models: []string{"grok-code"}},
			}, []domain.WorkerQuotaObservation{{
				Key: primaryBucket, Phase: domain.PhaseWarned, UsedPercent: 82, Healthy: true, ObservedAt: observed,
			}}),
			worker("normandy", []domain.WorkerProviderInventory{
				{InstanceID: "t3-primary", QuotaPoolID: "pool-claude", Available: true, Models: []string{"opus"}},
			}, nil),
		},
		quotas: []backlogadmin.Quota{
			{Pool: domain.QuotaPool{
				ID: "pool-claude", Provider: "claude", ProviderInstanceIDs: []string{"t3-primary"},
				Buckets: []domain.BucketKey{primaryBucket}, Admission: domain.AdmissionOpen, MaxConcurrent: 2,
			}},
			{Pool: domain.QuotaPool{
				ID: "pool-codex", Provider: "codex", ProviderInstanceIDs: []string{"codex"},
				Admission: domain.AdmissionOpen, MaxConcurrent: 1,
			}},
		},
		projects: []backlogadmin.Project{{
			Name: "steward", Repository: "https://example.invalid/steward.git", DefaultRef: "main",
			Workers: []backlogadmin.ProjectWorker{
				{Worker: "omarchy-pc", Advertises: true, Enrolled: true, Ready: true, State: "observed"},
				{Worker: "normandy", Advertises: true, Enrolled: true, Ready: true, State: "observed"},
			},
		}},
	}
}

// S2: the merged claude-main observation read 98% draining for hours after
// the seven-day window had reset, and models printed it as current, with no
// age. A reading shows its age, and one that is older than an hour or was
// taken before a reset that has since passed is marked stale.
func TestModelsShowsQuotaReadingAgeAndStaleness(t *testing.T) {
	observed := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	reset := observed.Add(30 * time.Minute)
	fixture := modelsFixture()
	fixture.workers[0].Snapshot.QuotaObservations[0].ResetsAt = &reset
	for _, tc := range []struct {
		name      string
		now       time.Time
		wantStale bool
		wantAge   string
	}{
		{name: "fresh, before the reset", now: observed.Add(5 * time.Minute), wantAge: "5m"},
		{name: "young but the window has reset", now: observed.Add(40 * time.Minute), wantStale: true, wantAge: "40m stale"},
		{name: "older than an hour", now: observed.Add(3 * time.Hour), wantStale: true, wantAge: "3h stale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := modelsNow
			modelsNow = func() time.Time { return tc.now }
			t.Cleanup(func() { modelsNow = previous })
			document := buildModelsDocument("", fixture.workers, fixture.quotas, nil)
			instance := modelsInstanceByName(t, document, "t3-primary")
			if instance.ObservedAt == nil || !instance.ObservedAt.Equal(observed) || instance.Stale != tc.wantStale {
				t.Fatalf("observedAt=%v stale=%v, want %v and %v", instance.ObservedAt, instance.Stale, observed, tc.wantStale)
			}
			var out bytes.Buffer
			if err := renderModels(&out, document); err != nil {
				t.Fatal(err)
			}
			if !regexp.MustCompile(`82%\s+` + regexp.QuoteMeta(tc.wantAge) + `\s`).MatchString(out.String()) {
				t.Fatalf("table does not show age %q:\n%s", tc.wantAge, out.String())
			}
		})
	}
}

func modelsInstanceByName(t *testing.T, document modelsDocument, name string) modelsInstance {
	t.Helper()
	for _, instance := range document.Instances {
		if instance.Instance == name {
			return instance
		}
	}
	t.Fatalf("instance %q is not in %+v", name, document.Instances)
	return modelsInstance{}
}

// models answers "which routes can I ask for right now, and will they run",
// which today takes reading every worker's inventory JSON by hand. The three
// facts it joins fail separately, so each one is asserted separately: the
// fleet catalog authorises an instance, a worker advertises it with models,
// and the merged bucket observations say how much of its pool is left.
func TestModelsJoinsAuthorisationAdvertisementAndQuota(t *testing.T) {
	fixture := modelsFixture()
	var out bytes.Buffer
	cli := modelsCLI{service: fixture, principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	if err := cli.run(context.Background(), modelsScope{}, true); err != nil {
		t.Fatal(err)
	}
	var document modelsDocument
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatalf("models --json did not print one document: %v\n%s", err, out.String())
	}
	if document.SchemaVersion != modelsSchemaVersion {
		t.Fatalf("schemaVersion = %d", document.SchemaVersion)
	}
	if len(document.Instances) != 3 {
		t.Fatalf("instances = %+v, want t3-primary, codex and opencode", document.Instances)
	}

	primary := modelsInstanceByName(t, document, "t3-primary")
	if !primary.Authorized || !primary.Advertised || primary.MissingBinding {
		t.Fatalf("t3-primary = %+v, want authorized and advertised", primary)
	}
	if primary.QuotaPool != "pool-claude" || primary.Admission != string(domain.AdmissionOpen) {
		t.Fatalf("t3-primary pool = %q admission = %q", primary.QuotaPool, primary.Admission)
	}
	if primary.Phase != string(domain.PhaseWarned) || primary.Percent == nil || *primary.Percent != 82 {
		t.Fatalf("t3-primary quota = %q %v, want warned at 82", primary.Phase, primary.Percent)
	}
	if strings.Join(primary.Models, ",") != "claude-haiku-4-5,opus" {
		t.Fatalf("t3-primary models = %v, want both workers' models deduplicated and sorted", primary.Models)
	}
	if len(primary.Workers) != 2 || primary.Workers[0].Worker != "normandy" || !primary.Workers[0].Ready {
		t.Fatalf("t3-primary workers = %+v", primary.Workers)
	}

	// Authorised by the fleet catalog and offered by nobody: the row has to
	// exist, because "the fleet knows this instance but no worker is serving
	// it" is exactly the state F-17 left invisible.
	codex := modelsInstanceByName(t, document, "codex")
	if !codex.Authorized || codex.Advertised || len(codex.Models) != 0 {
		t.Fatalf("codex = %+v, want authorized and not advertised", codex)
	}

	// Advertised by a worker and in no authorised pool: the coordinator will
	// not route to it, and the row says so instead of omitting it.
	opencode := modelsInstanceByName(t, document, "opencode")
	if opencode.Authorized || !opencode.Advertised || !opencode.MissingBinding {
		t.Fatalf("opencode = %+v, want advertised with a missing binding", opencode)
	}
	if opencode.Phase != "" || opencode.Percent != nil {
		t.Fatalf("opencode reports a quota state it has no pool for: %+v", opencode)
	}
}

// The text form is what an operator reads, and the point of the table is that
// every row can be copied into "--model instance/model".
func TestModelsTextIsOneTableOfCopyableRoutes(t *testing.T) {
	fixture := modelsFixture()
	var out bytes.Buffer
	cli := modelsCLI{service: fixture, principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	if err := cli.run(context.Background(), modelsScope{}, false); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"ROUTE", "POOL", "PHASE", "USED", "ADMISSION", "WORKERS", "STATUS",
		"t3-primary/opus", "t3-primary/claude-haiku-4-5", "pool-claude", "warned", "82%",
		"opencode/grok-code", "no quota binding",
		"codex", "authorized, not advertised",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("models text does not name %q:\n%s", want, text)
		}
	}
}

// --project narrows the advertisement half to the workers that could take
// that project's work, so that a model only one ineligible worker offers is
// not presented as a route for it.
func TestModelsProjectFilterUsesTheProjectsEligibleWorkers(t *testing.T) {
	fixture := modelsFixture()
	fixture.projects[0].Workers = []backlogadmin.ProjectWorker{
		{Worker: "normandy", Advertises: true, Enrolled: true, Ready: true, State: "observed"},
	}
	var out bytes.Buffer
	cli := modelsCLI{service: fixture, principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	if err := cli.run(context.Background(), modelsScope{Project: "steward"}, true); err != nil {
		t.Fatal(err)
	}
	var document modelsDocument
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.Project != "steward" {
		t.Fatalf("project = %q", document.Project)
	}
	primary := modelsInstanceByName(t, document, "t3-primary")
	if len(primary.Workers) != 1 || primary.Workers[0].Worker != "normandy" {
		t.Fatalf("workers = %+v, want only the project's eligible worker", primary.Workers)
	}
	if strings.Join(primary.Models, ",") != "opus" {
		t.Fatalf("models = %v, want only what normandy advertises", primary.Models)
	}
	for _, instance := range document.Instances {
		if instance.Instance == "opencode" && instance.Advertised {
			t.Fatalf("opencode is advertised only by an ineligible worker: %+v", instance)
		}
	}
}

// An unknown project is a typo, and the refusal names the command that lists
// the real ones rather than printing an empty table.
func TestModelsRefusesAnUnknownProject(t *testing.T) {
	fixture := modelsFixture()
	var out bytes.Buffer
	cli := modelsCLI{service: fixture, principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	err := cli.run(context.Background(), modelsScope{Project: "nope"}, false)
	if err == nil {
		t.Fatal("an unknown project was accepted")
	}
	if !strings.Contains(err.Error(), "backlog projects") {
		t.Fatalf("the refusal does not name the listing command: %v", err)
	}
}

func TestParseModelsArguments(t *testing.T) {
	scope, asJSON, err := parseModelsArgs([]string{"--project", "steward", "--json"})
	if err != nil || scope.Project != "steward" || !asJSON {
		t.Fatalf("scope = %+v asJSON = %t err = %v", scope, asJSON, err)
	}
	if _, _, err := parseModelsArgs([]string{"--project"}); err == nil {
		t.Fatal("--project without a value was accepted")
	}
	if _, _, err := parseModelsArgs([]string{"steward"}); err == nil {
		t.Fatal("a positional argument was accepted")
	}
}
