package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func installM13RepairPolicy(t *testing.T, raw string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	path := filepath.Join(dir, "t3-steward")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "route-policy.yaml"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestM13RepairInstalledExplicitTask(t *testing.T) {
	raw := strings.ReplaceAll(strings.ReplaceAll(m13Policy, "claude/opus", "t3-primary/opus"), "codex/sol", "t3-primary/claude-haiku-4-5")
	for _, older := range []bool{false, true} {
		t.Run(map[bool]string{false: "unready", true: "older-catalog"}[older], func(t *testing.T) {
			installM13RepairPolicy(t, raw)
			h := newTaskRunHarness()
			h.projects[0].Workers[0].Ready = false
			if older {
				h.projectsErr = errors.New("old catalog")
			}
			c := h.cli()
			query := c.query
			c.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
				if q.Kind == backlogadmin.QueryWorkers && !older {
					return backlogadmin.Response{Workers: []backlogadmin.Worker{{Providers: []backlogadmin.WorkerProviderAuthorization{{Instance: "t3-primary", Models: []string{"opus", "claude-haiku-4-5"}}}}}}, nil
				}
				if q.Kind == backlogadmin.QueryWorkers {
					t.Error("explicit route queried configured authorizations")
					return backlogadmin.Response{}, errors.New("old authorizations")
				}
				return query(ctx, q)
			}
			if err := c.run(context.Background(), []string{"--project", "steward", "--model", "t3-primary/opus", "--worker", "omarchy-pc", "--ref", "main", "--no-notify", "--json", "--", "hello"}); err != nil {
				t.Fatal(err)
			}
			record := h.record(t)
			if record.Route.Instance != "t3-primary" || record.Route.Model != "opus" || record.Route.Worker != "omarchy-pc" || record.Route.Effort != "high" || record.Selection == nil || record.Selection.Reason != "explicit model override" {
				t.Fatalf("%+v", record)
			}
			_, files := h.manifest(t)
			if files["route-policy.yaml"] != raw || !strings.Contains(files["route-selection.json"], "explicit model override") {
				t.Fatal("missing pinned provenance")
			}
		})
	}
}

func TestM13RepairDefaultReason(t *testing.T) {
	raw := strings.ReplaceAll(strings.ReplaceAll(m13Policy, "claude/opus", "t3-primary/opus"), "codex/sol", "t3-primary/claude-haiku-4-5")
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "flag"}[explicit], func(t *testing.T) {
			installM13RepairPolicy(t, raw)
			h := newTaskRunHarness()
			c := h.cli()
			c.defaultModel = "t3-primary/opus"
			query := c.query
			c.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
				if q.Kind == backlogadmin.QueryWorkers {
					return backlogadmin.Response{Workers: []backlogadmin.Worker{{Providers: []backlogadmin.WorkerProviderAuthorization{{Instance: "t3-primary", Models: []string{"opus", "claude-haiku-4-5"}}}}}}, nil
				}
				return query(ctx, q)
			}
			args := []string{"--project", "steward", "--ref", "main", "--no-notify", "--json"}
			if explicit {
				args = append(args, "--model", "t3-primary/opus")
			}
			args = append(args, "--", "hello")
			if err := c.run(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			want := "configured default model"
			if explicit {
				want = "explicit model override"
			}
			record := h.record(t)
			if record.Selection == nil || record.Selection.Reason != want {
				t.Fatalf("want %s: %+v", want, record.Selection)
			}
			_, files := h.manifest(t)
			if !strings.Contains(files["route-selection.json"], want) {
				t.Fatalf("snapshot reason: %s", files["route-selection.json"])
			}
		})
	}
}

func TestM13RepairInstalledExplicitReviewOlderCatalog(t *testing.T) {
	raw := "schema: route-policy/v1\nroles:\n - name: review\n   candidates:\n    - {route: a/full, effort: high, tier: standard}\n    - {route: b/full, effort: medium, tier: standard}\n"
	installM13RepairPolicy(t, raw)
	c := taskRunCLI{query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		t.Error("explicit review queried authorizations")
		return backlogadmin.Response{}, errors.New("old catalog")
	}}
	p, err := c.loadPolicy(context.Background(), "", "")
	if err != nil {
		t.Fatal(err)
	}
	project := reviewCatalog()[0] // Unready workers, review metadata, no configured authorization response.
	a := reviewArgs{policy: p, reviewers: []string{"a/full", "b/full"}, risk: "routine", deadline: time.Hour, noNotify: true, project: "scratch"}
	chosen, err := resolveReviewPolicy(a, project)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(chosen.reviewers, ",") != "a/full,b/full" || chosen.efforts["a/full"] != "high" || chosen.efforts["b/full"] != "medium" {
		t.Fatalf("%+v", chosen)
	}
	dir, err := buildReviewCampaign(chosen, []backlogadmin.Project{project}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	project.Workers[0].Routes[0].ProviderFamily = ""
	if _, err := resolveReviewPolicy(a, project); err == nil {
		t.Fatal("invented review family")
	}
}

func TestM13RepairRolePinIgnoresReadiness(t *testing.T) {
	raw := strings.Replace(m13Policy, "    candidates:", "    constraints: {provider_families: [claude], tiers: [standard]}\n    candidates:", 1)
	p, err := parseRoutePolicy([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	project := backlogadmin.Project{Name: "p", Workers: []backlogadmin.ProjectWorker{{Worker: "sleeping", Configured: true, Routes: []backlogadmin.ProjectRoute{{Instance: "claude", Model: "opus", ProviderFamily: "claude", Tier: "executor"}}}}}
	s, err := selectPolicyRoute(p, "execute", "claude/opus", "", "sleeping", project, nil)
	if err != nil || s.Route != "claude/opus" || s.Effort != "high" {
		t.Fatalf("%+v %v", s, err)
	}
	project.Workers[0].Routes[0].ProviderFamily = ""
	if _, err := selectPolicyRoute(p, "execute", "claude/opus", "", "sleeping", project, nil); err == nil {
		t.Fatal("missing family satisfied constraint")
	}
	project.Workers[0].Routes[0].ProviderFamily = "claude"
	project.Workers[0].Routes[0].Tier = ""
	if _, err := selectPolicyRoute(p, "execute", "claude/opus", "", "sleeping", project, nil); err == nil {
		t.Fatal("missing tier satisfied constraint")
	}
	if _, err := selectPolicyRoute(p, "unknown", "claude/opus", "", "sleeping", project, nil); err == nil {
		t.Fatal("unknown role bypass")
	}
}

func TestM13RepairProjectAvailabilityTaskAndReview(t *testing.T) {
	raw := "schema: route-policy/v1\nroles:\n - name: review\n   candidates:\n    - {route: a/full, effort: high, tier: standard}\n    - {route: b/full, effort: medium, tier: standard}\n"
	p, _ := parseRoutePolicy([]byte(raw))
	project := reviewCatalog()[0]
	routes := project.Workers[0].Routes
	project.Workers = []backlogadmin.ProjectWorker{
		{Worker: "preferred", Configured: true, Ready: true, Advertises: false, Routes: []backlogadmin.ProjectRoute{routes[0]}},
		{Worker: "fallback", Configured: true, Ready: true, Advertises: true, Routes: []backlogadmin.ProjectRoute{routes[1]}},
	}
	s, err := selectPolicyRoute(p, "review", "", "", "", project, nil)
	if err != nil || s.Route != "b/full" {
		t.Fatalf("task fallback: %+v %v", s, err)
	}
	a := reviewArgs{policy: p, role: "review", risk: "routine", swarm: "security", swarmModels: []string{"a/full"}, judge: "a/full"}
	// Use an economy swarm, preserving ordinary review tier rules.
	project.Workers[0].Routes = append(project.Workers[0].Routes, routes[2])
	a.swarmModels = []string{"a/cheap"}
	chosen, err := resolveReviewPolicy(a, project)
	if err != nil || len(chosen.reviewers) != 1 || chosen.reviewers[0] != "b/full" {
		t.Fatalf("review fallback: %+v %v", chosen, err)
	}
	project.Workers[1].Advertises = false
	if _, err := selectPolicyRoute(p, "review", "", "", "", project, nil); err == nil {
		t.Fatal("task accepted unavailable project")
	}
	if _, err := resolveReviewPolicy(a, project); err == nil {
		t.Fatal("review accepted unavailable project")
	}
}

func TestM13RepairResetFreshnessConsistency(t *testing.T) {
	now := time.Now()
	reset := now.Add(-time.Minute)
	pool := domain.QuotaPool{ID: "p", ProviderInstanceIDs: []string{"codex"}}
	for _, tc := range []struct {
		name     string
		observed time.Time
		stale    bool
	}{
		{"before", reset.Add(-time.Second), true},
		{"at", reset, false},
		{"after", now.Add(-time.Second), false},
		{"age", now.Add(-2 * time.Hour), true},
		{"unknown", time.Time{}, true},
	} {
		for _, threshold := range []time.Duration{0, time.Hour} {
			t.Run(tc.name+"/"+threshold.String(), func(t *testing.T) {
				states := []domain.BucketState{{Key: domain.BucketKey{ProviderInstanceID: "codex", Window: "five-hour"}, ObservedAt: tc.observed, ResetsAt: &reset}}
				windows := modelsPoolWindows(pool, states, now, threshold)
				_, stale := modelsPoolFreshness(pool, states, now, threshold)
				if len(windows) != 1 || windows[0].Stale != tc.stale || stale != tc.stale {
					t.Fatalf("want stale=%v: aggregate=%v windows=%+v", tc.stale, stale, windows)
				}
			})
		}
	}
}

func TestM13RepairActionableSingleRoleRefusal(t *testing.T) {
	raw := "schema: route-policy/v1\nroles:\n - name: review\n   candidates:\n    - {route: a/full, effort: high, tier: standard}\n"
	p, _ := parseRoutePolicy([]byte(raw))
	project := reviewCatalog()[0]
	project.Workers[0].Ready = true
	project.Workers[0].Advertises = true
	_, err := resolveReviewPolicy(reviewArgs{policy: p, role: "review", risk: "routine"}, project)
	if err == nil || !strings.Contains(err.Error(), "--independent a/full --independent b/full") {
		t.Fatalf("need actionable explicit provider-diverse example: %v", err)
	}
}
