package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"reflect"
	"strings"
	"testing"
	"time"
)

func configuredReviewSettings() config.BacklogV2 {
	return config.BacklogV2{
		Workers:       map[string]config.V2Worker{"w": {Address: "w", Credential: "ref:w", AcceptBacklog: true, Providers: map[string]config.V2Provider{"codex": {Models: []string{"org/sol"}, QuotaPool: "p"}, "other": {Models: []string{"model"}, QuotaPool: "p"}}}},
		Projects:      map[string]config.V2Project{"repo": {Repository: "https://example.org/repo.git", DefaultRef: "main", SetupProfile: "go", Workers: []string{"w"}}},
		SetupProfiles: map[string]config.V2SetupProfile{"go": {Commands: []string{"true"}, Timeout: config.Duration(time.Minute)}},
		QuotaPools:    map[string]config.V2QuotaPool{"p": {Provider: "fixture", MaxConcurrent: 2}},
		ReviewRoutes:  map[string]config.ReviewRouteMetadata{"codex/org/sol": {ProviderFamily: "openai", Tier: "executor"}, "other/model": {ProviderFamily: "fixture-other", Tier: "critical"}},
	}
}
func configuredReviewManifest(t *testing.T) backlog.Manifest {
	t.Helper()
	raw := "version: 2\nname: configured\nenvironment: {project: repo, ref: " + strings.Repeat("a", 40) + "}\ninputs: [criteria.md]\ntasks:\n  run:\n    prompt_file: prompt.md\n    review_requirements:\n      version: 1\n      risk: risky\n      criteria_file: criteria.md\n      required_reviewers: 2\n      min_provider_families: 2\n      members:\n        - {id: a, role: independent, route: codex/org/sol, required: true}\n        - {id: b, role: independent, route: other/model, required: true}\n"
	m, err := backlog.ParseManifest([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func TestReviewConfiguredCatalogDetachedSelectedAndControls(t *testing.T) {
	ctx := context.Background()
	cfg := configuredReviewSettings()
	c, err := NewConfiguredAdmissionCatalog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.ReviewAdmissionCatalog(ctx, "repo")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Workers["w"].Providers["codex"].Models[0] = "forged"
	delete(cfg.ReviewRoutes, "other/model")
	second, err := c.ReviewAdmissionCatalog(ctx, "repo")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("caller configuration mutation changed catalog")
	}
	first.AuthoredWorkers[0].Providers[0].Models[0] = "forged"
	third, err := c.ReviewAdmissionCatalog(ctx, "repo")
	if err != nil || !reflect.DeepEqual(second, third) {
		t.Fatal("returned catalog aliases snapshot", err)
	}
	if err = backlog.ValidateTaskReviewAdmission(ctx, configuredReviewManifest(t), c); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"missing project", "selected rejected", "missing route", "wrong tier", "same family", "exact model", "mixed wildcard", "missing quota", "invalid quota metadata"} {
		t.Run(kind, func(t *testing.T) {
			cfg := configuredReviewSettings()
			switch kind {
			case "missing project":
				delete(cfg.Projects, "repo")
			case "selected rejected":
				p := cfg.Projects["repo"]
				p.Repository = "bad repository"
				cfg.Projects["repo"] = p
			case "missing route":
				delete(cfg.ReviewRoutes, "other/model")
			case "wrong tier":
				cfg.ReviewRoutes["other/model"] = config.ReviewRouteMetadata{ProviderFamily: "fixture-other", Tier: "executor"}
			case "same family":
				cfg.ReviewRoutes["other/model"] = config.ReviewRouteMetadata{ProviderFamily: "openai", Tier: "critical"}
			case "exact model":
				cfg.Workers["w"].Providers["codex"] = config.V2Provider{Models: []string{"sol"}, QuotaPool: "p"}
			case "mixed wildcard":
				cfg.Workers["w"].Providers["codex"] = config.V2Provider{Models: []string{"*", "org/sol"}, QuotaPool: "p"}
			case "missing quota":
				delete(cfg.QuotaPools, "p")
			case "invalid quota metadata":
				cfg.QuotaPools["p"] = config.V2QuotaPool{}
			}
			c, err := NewConfiguredAdmissionCatalog(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = backlog.ValidateTaskReviewAdmission(ctx, configuredReviewManifest(t), c); err == nil {
				t.Fatal("unauthorized selected configuration accepted")
			}
		})
	}
	cfg = configuredReviewSettings()
	cfg.Workers["w"].Providers["codex"] = config.V2Provider{Models: []string{"*"}, QuotaPool: "p"}
	cfg.Projects["broken"] = config.V2Project{Repository: "bad", Workers: []string{"w"}}
	cfg.ReviewRoutes["unused/*"] = config.ReviewRouteMetadata{ProviderFamily: "", Tier: "bad"}
	cfg.Workers["w"].Providers["unused"] = config.V2Provider{Models: []string{"*", "x"}}
	w := cfg.Workers["w"]
	w.AcceptBacklog = false
	cfg.Workers["w"] = w
	c, err = NewConfiguredAdmissionCatalog(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = backlog.ValidateTaskReviewAdmission(ctx, configuredReviewManifest(t), c); err != nil {
		t.Fatal("unrelated defects/draining became permanent policy", err)
	}
}
