package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// B-3. "backlog projects" answered a question nobody asked, at 90 KB.
//
// Unfiltered it printed every project with every eligible worker and all
// thirty-six advertised routes spelled out per worker: 28,088 bytes of text
// and 90,449 of JSON on the live fleet, in answer to "which projects are
// there". The fixture below is that catalog's shape -- thirteen projects,
// three eligible workers each, thirty-six routes each -- so the measurement
// here is comparable with the one the audit took against the coordinator.

// catalogRoutes is the thirty-six routes the fleet authorises, as one worker
// advertises them.
func catalogRoutes() []backlogadmin.ProjectRoute {
	instances := []struct {
		id     string
		pool   string
		models []string
	}{
		{"t3-primary", "pool-claude", []string{"opus-5", "opus-5-1m", "sonnet-4-5", "sonnet-4-5-1m", "haiku-4-5", "opus-4-1", "sonnet-4", "haiku-4", "opus-5-thinking"}},
		{"codex", "pool-codex", []string{"gpt-5-codex", "gpt-5-codex-high", "gpt-5", "gpt-5-mini", "o4-mini", "o3", "o3-pro", "gpt-4-1", "gpt-4-1-mini"}},
		{"opencode", "pool-opencode", []string{"grok-code", "grok-4", "kimi-k2", "qwen3-coder", "deepseek-v3", "glm-4-6", "minimax-m2", "llama-4", "mistral-large"}},
		{"gemini-cli", "pool-gemini", []string{"gemini-3-pro", "gemini-3-flash", "gemini-2-5-pro", "gemini-2-5-flash", "gemini-2-0-pro", "gemini-2-0-flash", "gemma-3", "gemma-3-mini", "gemini-3-deep"}},
	}
	var routes []backlogadmin.ProjectRoute
	for _, instance := range instances {
		for _, model := range instance.models {
			routes = append(routes, backlogadmin.ProjectRoute{Instance: instance.id, Model: model, QuotaPool: instance.pool})
		}
	}
	return routes
}

func projectsCatalogFixture() []backlogadmin.Project {
	names := []string{
		"t3-steward", "huyang", "upkeeper", "jocasta", "agent99", "openviking",
		"home-assistant-config", "omarchy", "dogfood-sum", "feed", "t3-code",
		"ryzhkov-dev", "homelab-infra",
	}
	// Two eligible workers on most projects and a third on a few, which is
	// the shape that made the live answer 28,088 bytes of text.
	projects := make([]backlogadmin.Project, 0, len(names))
	for index, name := range names {
		workers := []string{"omarchy-pc", "normandy"}
		if index%4 == 0 {
			workers = append(workers, "homelab")
		}
		project := backlogadmin.Project{
			Name:       name,
			Repository: "https://github.com/iryzhkov/" + name + ".git",
			DefaultRef: "main", Type: "git", SetupProfile: "go",
		}
		for _, worker := range workers {
			project.Workers = append(project.Workers, backlogadmin.ProjectWorker{
				Worker: worker, Configured: true, Advertises: true, Enrolled: true, Ready: true,
				State: "observed", Health: "ready", Routes: catalogRoutes(),
			})
		}
		projects = append(projects, project)
	}
	return projects
}

// projectsCatalogService answers the projects query from the fixture and
// honours the filter the way the coordinator does, so that --project is
// measured as the scoped read it is rather than as a filter applied here.
type projectsCatalogService struct{ projects []backlogadmin.Project }

func (s projectsCatalogService) Query(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
	response := backlogadmin.Response{
		Version: backlogadmin.Version, Kind: query.Kind,
		GeneratedAt: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC),
	}
	for _, project := range s.projects {
		if query.Filter.Project == "" || project.Name == query.Filter.Project {
			response.Projects = append(response.Projects, project)
		}
	}
	return response, nil
}

func runProjectsCommand(t *testing.T, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service:   projectsCatalogService{projects: projectsCatalogFixture()},
		principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}},
		stdout:    &out,
	}
	if err := cli.runBacklog(context.Background(), append([]string{"projects"}, args...)); err != nil {
		t.Fatalf("t3-steward backlog projects %s: %v", strings.Join(args, " "), err)
	}
	return out.String()
}

func TestBacklogProjectsSummarisesTheCatalogAndScopesBeforeIt(t *testing.T) {
	unfiltered := runProjectsCommand(t)
	if len(unfiltered) >= 4096 {
		t.Errorf("the unfiltered text form is %d bytes for thirteen projects; it is a summary and has to be under 4096:\n%s", len(unfiltered), unfiltered)
	}
	for _, project := range projectsCatalogFixture() {
		if !strings.Contains(unfiltered, project.Name) {
			t.Errorf("the summary does not name the project %q, so it is not an answer to \"which projects are there\"", project.Name)
		}
	}
	// A summarised answer says so, gives the totals, and says how to get the rest.
	for _, want := range []string{"summarised", "13 projects", "30 eligible worker rows", "468 advertised routes", "--project NAME", "--verbose"} {
		if !strings.Contains(unfiltered, want) {
			t.Errorf("the summary does not say %q:\n%s", want, unfiltered)
		}
	}
	if strings.Contains(unfiltered, "t3-primary/opus-5@pool-claude") {
		t.Errorf("the summary still spells the routes out:\n%s", unfiltered)
	}

	// The scope is applied to what is read, so the scoped answer is one
	// project in full and not a window onto the summary.
	scoped := runProjectsCommand(t, "--project", "dogfood-sum")
	for _, want := range []string{"dogfood-sum workers:", "ROUTES (instance/model@pool)", "t3-primary/opus-5@pool-claude", "gemini-cli/gemma-3-mini@pool-gemini"} {
		if !strings.Contains(scoped, want) {
			t.Errorf("the scoped answer does not contain %q:\n%s", want, scoped)
		}
	}
	if strings.Contains(scoped, "summarised") {
		t.Errorf("the scoped answer summarises something it printed in full:\n%s", scoped)
	}

	// --verbose is the escape: the whole catalog in the detailed form.
	verbose := runProjectsCommand(t, "--verbose")
	for _, want := range []string{"dogfood-sum workers:", "t3-steward workers:", "t3-primary/opus-5@pool-claude"} {
		if !strings.Contains(verbose, want) {
			t.Errorf("--verbose does not print the detail for every project: %q missing", want)
		}
	}
	if len(verbose) < 4*len(unfiltered) {
		t.Errorf("--verbose printed %d bytes and the summary %d; --verbose is meant to be the whole catalog", len(verbose), len(unfiltered))
	}
}

// --verbose belongs to the verb that summarises. Accepting it elsewhere and
// doing nothing would be a flag that lies, and the help contract derives the
// pages from the parsers, so a silently accepted flag would also be an
// undocumented one.
func TestVerboseIsRefusedByTheVerbsThatDoNotSummarise(t *testing.T) {
	for _, args := range [][]string{{"status", "--verbose"}, {"workers", "--verbose"}, {"list", "--verbose"}} {
		if _, _, err := parseBacklogAdminQuery(args); err == nil {
			t.Errorf("backlog %s was accepted", strings.Join(args, " "))
		}
	}
	if _, _, err := parseBacklogAdminQuery([]string{"projects", "--verbose", "--verbose"}); err == nil {
		t.Error("backlog projects --verbose --verbose was accepted")
	}
	// The three compose: a scope, the escape from the summary, and the
	// machine form. The typed display is deliberately not read here, so that
	// this file compiles against the commit before the fix and fails there for
	// the behaviour and not for a missing field.
	query, _, err := parseBacklogAdminQuery([]string{"projects", "--project", "dogfood-sum", "--verbose", "--json"})
	if err != nil {
		t.Fatalf("backlog projects --project dogfood-sum --verbose --json was refused: %v", err)
	}
	if query.Filter.Project != "dogfood-sum" {
		t.Fatalf("query = %+v", query)
	}
}

// TestBacklogProjectsMeasured records the sizes this finding is closed with.
// It asserts nothing the test above does not: it exists so that "go test -run
// Measured -v" prints the same numbers on any commit, including the base.
func TestBacklogProjectsMeasured(t *testing.T) {
	t.Logf("backlog projects              text %d bytes", len(runProjectsCommand(t)))
	t.Logf("backlog projects --json       %d bytes", len(runProjectsCommand(t, "--json")))
	t.Logf("backlog projects --project X  text %d bytes", len(runProjectsCommand(t, "--project", "dogfood-sum")))
	t.Logf("backlog projects --project X  json %d bytes", len(runProjectsCommand(t, "--project", "dogfood-sum", "--json")))
}
