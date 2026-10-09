package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
)

// builtInRouteCatalog is a project whose claudeAgent and codex routes carry a
// review tier but no explicit provider_family, plus an advertised route with
// no review metadata at all, as on a coordinator that classified only the
// routes it reviews with.
func builtInRouteCatalog() []backlogadmin.Project {
	return []backlogadmin.Project{{Name: "scratch", Type: "fresh", Workers: []backlogadmin.ProjectWorker{{Worker: "w", Routes: []backlogadmin.ProjectRoute{
		{Instance: "claudeAgent", Model: "claude-opus-5-5", Tier: "executor"},
		{Instance: "claudeAgent", Model: "claude-haiku-5-5"},
		{Instance: "codex", Model: "gpt-6.1-sol", Tier: "executor"},
	}}}}}
}

func reviewPlanArgs(t *testing.T, reviewers ...string) reviewArgs {
	t.Helper()
	plan := filepath.Join(reviewInputTempDir(t), "plan.md")
	if err := os.WriteFile(plan, []byte("Review this plan."), 0600); err != nil {
		t.Fatal(err)
	}
	argv := []string{"--plan", plan, "--project", "scratch", "--no-notify"}
	for _, r := range reviewers {
		argv = append(argv, "--reviewer", r)
	}
	args, err := parseReviewArgs(argv)
	if err != nil {
		t.Fatal(err)
	}
	return args
}

func TestReviewDerivesBuiltInProviderFamilies(t *testing.T) {
	projects := builtInRouteCatalog()
	routes, families, err := reviewRoutes(projects[0])
	if err != nil || families != 2 {
		t.Fatalf("families=%d err=%v", families, err)
	}
	for route, want := range map[string]string{"claudeAgent/claude-opus-5-5": "claude", "claudeAgent/claude-haiku-5-5": "claude", "codex/gpt-6.1-sol": "openai"} {
		if got := routes[route].ProviderFamily; got != want {
			t.Fatalf("%s family = %q, want %q", route, got, want)
		}
	}

	dir, err := buildReviewCampaign(reviewPlanArgs(t, "claudeAgent/claude-opus-5-5", "codex/gpt-6.1-sol"), projects, time.Now())
	if err != nil {
		t.Fatalf("cross-provider review refused: %v", err)
	}
	defer os.RemoveAll(dir)
	bundle, err := campaign.Load(dir, campaign.DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, member := range bundle.Manifest.Review.Reviewers {
		got[member.Route] = member.ProviderFamily
	}
	if got["claudeAgent/claude-opus-5-5"] != "claude" || got["codex/gpt-6.1-sol"] != "openai" {
		t.Fatalf("manifest families: %v", got)
	}
}

func TestReviewExplicitFamilyOverridesInstance(t *testing.T) {
	projects := builtInRouteCatalog()
	projects[0].Workers[0].Routes[2].ProviderFamily = "claude"
	routes, families, err := reviewRoutes(projects[0])
	if err != nil || families != 1 || routes["codex/gpt-6.1-sol"].ProviderFamily != "claude" {
		t.Fatalf("families=%d routes=%v err=%v", families, routes, err)
	}
}

func TestReviewRefusesSameProviderWithDerivedFamilies(t *testing.T) {
	projects := builtInRouteCatalog()
	_, err := buildReviewCampaign(reviewPlanArgs(t, "claudeAgent/claude-opus-5-5"), projects, time.Now())
	if err == nil || !strings.Contains(err.Error(), "provider diversity unmet") {
		t.Fatalf("same-provider review: %v", err)
	}
}

func TestReviewMissingMetadataNamesConfigurationEntry(t *testing.T) {
	projects := builtInRouteCatalog()
	_, err := buildReviewCampaign(reviewPlanArgs(t, "claudeAgent/claude-haiku-5-5", "codex/gpt-6.1-sol"), projects, time.Now())
	want := `route claudeAgent/claude-haiku-5-5 lacks catalog review tier; add backlog_v2.review_routes entry "claudeAgent/claude-haiku-5-5: {provider_family: claude, tier: executor}" on the coordinator`
	if err == nil || err.Error() != want {
		t.Fatalf("tier refusal:\n got %v\nwant %s", err, want)
	}

	projects[0].Workers[0].Routes = append(projects[0].Workers[0].Routes, backlogadmin.ProjectRoute{Instance: "opencode", Model: "deepseek/deepseek-flash", Tier: "economy"})
	_, err = buildReviewCampaign(reviewPlanArgs(t, "claudeAgent/claude-opus-5-5", "codex/gpt-6.1-sol"), projects, time.Now())
	want = `provider diversity cannot be verified: catalog route opencode/deepseek/deepseek-flash lacks provider_family and instance "opencode" has no built-in provider family; add backlog_v2.review_routes entry "opencode/deepseek/deepseek-flash: {provider_family: FAMILY, tier: economy}" on the coordinator`
	if err == nil || err.Error() != want {
		t.Fatalf("family refusal:\n got %v\nwant %s", err, want)
	}
}
