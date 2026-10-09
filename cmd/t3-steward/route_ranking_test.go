package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func rankingProject() backlogadmin.Project {
	return backlogadmin.Project{Name: "steward", DefaultRef: "main", Workers: []backlogadmin.ProjectWorker{{Worker: "omarchy-pc", Ready: true, Advertises: true, Routes: []backlogadmin.ProjectRoute{
		{Instance: "codex", Model: "sol", Tier: "executor", ProviderFamily: "openai", QuotaPool: "codex-pool"},
		{Instance: "codex", Model: "tiny", Tier: "economy", ProviderFamily: "openai", QuotaPool: "codex-pool"},
		{Instance: "t3-primary", Model: "opus", Tier: "critical", ProviderFamily: "anthropic", QuotaPool: "pool-claude"},
	}}}}
}
func rankingPolicy(t *testing.T) *routePolicy {
	t.Helper()
	p, err := parseRoutePolicy([]byte("schema: route-policy/v1\nroles:\n- name: execute\n  candidates:\n  - {route: codex/sol, effort: high, tier: standard}\n  - {route: t3-primary/opus, effort: medium, tier: premium}\n"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestM172SelectionAndVerdicts(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := availabilityFixture(now)
	reset := now.Add(18 * time.Hour)
	workers[0].Snapshot.QuotaObservations[1].ResetsAt = &reset
	view := buildRouteRankView(workers, quotas, time.Hour)
	p := rankingPolicy(t)
	project := rankingProject()
	s, err := selectPolicyRouteRanked(p, "execute", "", "", "", project, nil, view)
	if err != nil || s.Route != "t3-primary/opus" || s.Ranking != domain.RouteRankingV1 || len(s.Candidates) != 2 || s.Candidates[1].Band != "reset-soon" {
		t.Fatalf("%+v %v", s, err)
	}
	legacy, err := selectPolicyRoute(p, "execute", "", "", "", project, nil)
	if err != nil || legacy.Route != "codex/sol" || legacy.Reason != "first eligible candidate in policy order" {
		t.Fatalf("%+v %v", legacy, err)
	}
	pin, err := selectPolicyRouteRanked(p, "execute", "codex/sol", "", "", project, nil, view)
	if err != nil || pin.Route != "codex/sol" || pin.Ranking != "" {
		t.Fatalf("%+v %v", pin, err)
	}
	_, verdicts, err := evaluatePolicyCandidates(p, "execute", "", "", "", project, func(route string) bool { return route != "codex/sol" })
	if err != nil || len(verdicts) != 2 || verdicts[0].Eligible || verdicts[0].Reason == "" || !verdicts[1].Eligible {
		t.Fatalf("%+v %v", verdicts, err)
	}
	unknown, err := selectPolicyRouteRanked(p, "execute", "", "", "", project, nil, routeRankView{})
	if err != nil || unknown.Route != "codex/sol" || !strings.Contains(unknown.Reason, "quota unknown for every candidate; policy order") {
		t.Fatalf("%+v %v", unknown, err)
	}
	a, err := resolveReviewPolicy(reviewArgs{policy: p, role: "execute", risk: "routine", judge: "codex/sol", swarm: "errors", swarmModels: []string{"codex/tiny"}, rankView: view}, project)
	if err != nil || len(a.selections) != 3 || a.selections[0].Ranking != domain.RouteRankingV1 || a.reviewers[0] != "t3-primary/opus" {
		t.Fatalf("%+v %v", a, err)
	}
}
func TestM172ReviewCommandRanking(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := availabilityFixture(now)
	reset := now.Add(18 * time.Hour)
	workers[0].Snapshot.QuotaObservations[1].ResetsAt = &reset
	workers[0].Providers = []backlogadmin.WorkerProviderAuthorization{{Instance: "codex", Models: []string{"sol"}}, {Instance: "t3-primary", Models: []string{"opus"}}}
	for _, asJSON := range []bool{false, true} {
		h := newTaskRunHarness()
		h.projects = []backlogadmin.Project{rankingProject()}
		h.release = "0.11.0-rc.115"
		c := h.cli()
		p := rankingPolicy(t)
		c.policyPath = filepath.Join(t.TempDir(), "policy.yaml")
		if err := os.WriteFile(c.policyPath, p.raw, 0600); err != nil {
			t.Fatal(err)
		}
		original := c.query
		calls := 0
		c.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
			switch q.Kind {
			case backlogadmin.QueryWorkers:
				return backlogadmin.Response{Workers: workers}, nil
			case backlogadmin.QueryQuota:
				calls++
				return backlogadmin.Response{Quotas: quotas}, nil
			}
			return original(ctx, q)
		}
		plan := filepath.Join(t.TempDir(), "plan.md")
		if err := os.WriteFile(plan, []byte("review input"), 0600); err != nil {
			t.Fatal(err)
		}
		args := []string{"--role", "execute", "--judge", "codex/sol", "--swarm", "errors", "--swarm-model", "codex/tiny", "--project", "steward", "--plan", plan, "--no-notify"}
		if asJSON {
			args = append(args, "--json")
		}
		a, err := parseReviewArgs(args)
		if err != nil {
			t.Fatal(err)
		}
		if err := (reviewCLI{task: c}).run(context.Background(), a); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("quota query count %d", calls)
		}
		if !strings.Contains(h.stdout.String(), "route-ranking/v1") || !strings.Contains(h.stdout.String(), "reset-soon") || !strings.Contains(h.stdout.String(), "t3-primary/opus") {
			t.Fatal(h.stdout.String())
		}
		if asJSON {
			var doc struct{ Selections []policySelection }
			if err := json.Unmarshal(h.stdout.Bytes(), &doc); err != nil {
				t.Fatal(err)
			}
			if len(doc.Selections) == 0 || len(doc.Selections[0].Candidates) != 2 {
				t.Fatalf("%+v", doc)
			}
		}
	}
}

func TestM172TaskQuotaReplayAndFailure(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := availabilityFixture(now)
	reset := now.Add(18 * time.Hour)
	workers[0].Snapshot.QuotaObservations[1].ResetsAt = &reset
	workers[0].Providers = []backlogadmin.WorkerProviderAuthorization{{Instance: "codex", Models: []string{"sol"}}, {Instance: "t3-primary", Models: []string{"opus"}}}
	h := newTaskRunHarness()
	h.projects[0] = rankingProject()
	h.projects[0].Repository = "https://github.com/iryzhkov/t3-steward"
	c := h.cli()
	p := rankingPolicy(t)
	c.policyPath = filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(c.policyPath, p.raw, 0600); err != nil {
		t.Fatal(err)
	}
	original := c.query
	fail := false
	quotaCalls := 0
	c.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
		switch q.Kind {
		case backlogadmin.QueryWorkers:
			return backlogadmin.Response{Workers: workers}, nil
		case backlogadmin.QueryQuota:
			quotaCalls++
			if fail {
				return backlogadmin.Response{}, errors.New("unsupported quota query")
			}
			return backlogadmin.Response{Quotas: quotas}, nil
		}
		return original(ctx, q)
	}
	c.campaign.viability = func(_ context.Context, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
		selections, failures := resolveCampaignPolicy(p, request.Tasks, h.projects, func(backlogadmin.ViabilityTask, string, bool) bool { return true }, now)
		matrix := backlogadmin.ViabilityMatrix{Outcome: backlogadmin.ViabilityReady}
		for _, task := range request.Tasks {
			if reason, ok := failures[task.Name]; ok {
				t.Fatalf("role resolution: %+v", reason)
			}
			selection := selections[task.Name]
			matrix.Tasks = append(matrix.Tasks, backlogadmin.ViabilityTaskResult{Task: task.Name, RoleSelection: &selection})
		}
		return matrix, nil
	}
	args := []string{"--role", "execute", "--project", "steward", "--ref", "main", "--no-notify", "--json", "--", "hello"}
	if err := c.run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	first := h.record(t)
	_, files := h.manifest(t)
	if first.Selection == nil || first.Selection.Ranking != domain.RouteRankingV1 || len(first.Selection.Candidates) != 2 {
		t.Fatalf("%+v", first)
	}
	if strings.Contains(files["route-selection.json"], "candidates") || strings.Contains(files["route-selection.json"], "reset-soon") {
		t.Fatal(files["route-selection.json"])
	}
	workers[0].Snapshot.QuotaObservations[1].UsedPercent = 40
	h.stdout.Reset()
	if err := c.run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	second := h.record(t)
	if first.IdempotencyKey != second.IdempotencyKey || !second.Replayed {
		t.Fatalf("key/replay: %+v %+v", first, second)
	}
	h.stdout.Reset()
	fail = true
	dry := append([]string{"--dry-run"}, args...)
	if err := c.run(context.Background(), dry); err != nil {
		t.Fatal(err)
	}
	var doc taskRunDryDocument
	if err := json.Unmarshal(h.stdout.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Selection.Route != "codex/sol" || len(doc.Selection.Candidates) != 2 || !strings.Contains(doc.Selection.Reason, "policy order") {
		t.Fatalf("%+v", doc)
	}
	h.stdout.Reset()
	fail = false
	textArgs := []string{"--role", "execute", "--project", "steward", "--ref", "main", "--no-notify", "--dry-run", "--", "hello"}
	if err := c.run(context.Background(), textArgs); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(h.stdout.String(), "route-ranking/v1") || !strings.Contains(h.stdout.String(), "reset-soon") {
		t.Fatal(h.stdout.String())
	}
	before := quotaCalls
	h.stdout.Reset()
	if err := c.run(context.Background(), []string{"--role", "execute", "--model", "codex/sol", "--project", "steward", "--ref", "main", "--no-notify", "--dry-run", "--json", "--", "hello"}); err != nil {
		t.Fatal(err)
	}
	if quotaCalls != before {
		t.Fatal("explicit model queried ranking quota")
	}
}
