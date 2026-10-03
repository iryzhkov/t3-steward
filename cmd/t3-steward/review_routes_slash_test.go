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

func TestReviewSlashModelsManifestAndDiversity(t *testing.T) {
	for _, model := range []string{"deepseek/deepseek-flash", "vendor/team/model", "ollama/qwen3-coder:30b"} {
		t.Run(model, func(t *testing.T) {
			plan := filepath.Join(reviewInputTempDir(t), "plan.md")
			if err := os.WriteFile(plan, []byte("Review this plan."), 0600); err != nil {
				t.Fatal(err)
			}
			projects := reviewCatalog()
			projects[0].Workers[0].Routes = []backlogadmin.ProjectRoute{
				{Instance: "a", Model: "full", ProviderFamily: "claude", Tier: "executor"},
				{Instance: "opencode", Model: model, ProviderFamily: "deepseek", Tier: "executor"},
			}
			routes, families, err := reviewRoutes(projects[0])
			if err != nil || families != 2 || len(routes) != 2 {
				t.Fatalf("catalog families=%d routes=%v err=%v", families, routes, err)
			}
			args, err := parseReviewArgs([]string{"--plan", plan, "--project", "scratch", "--reviewer", "a/full", "--reviewer", "opencode/" + model, "--no-notify"})
			if err != nil {
				t.Fatal(err)
			}
			dir, err := buildReviewCampaign(args, projects, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			bundle, err := campaign.Load(dir, campaign.DefaultLimits)
			if err != nil {
				t.Fatal(err)
			}
			task := bundle.Manifest.Tasks["independent-2"]
			if len(task.Routes) != 1 || task.Routes[0].Instance != "opencode" || task.Routes[0].Model != model {
				t.Fatalf("model changed in manifest: %+v", task.Routes)
			}
			member := bundle.Manifest.Review.Reviewers[1]
			if member.Route != "opencode/"+model || member.ProviderFamily != "deepseek" {
				t.Fatalf("member: %+v", member)
			}

			// An unselected slash-containing route still imposes the two-family rule.
			args.reviewers = []string{"a/full"}
			if _, err := buildReviewCampaign(args, projects, time.Now()); err == nil || !strings.Contains(err.Error(), "provider diversity") {
				t.Fatalf("ignored available opencode family: %v", err)
			}
			// Instance names do not supply diversity: the declared family does.
			projects[0].Workers[0].Routes[1].ProviderFamily = "claude"
			_, families, err = reviewRoutes(projects[0])
			if err != nil || families != 1 {
				t.Fatalf("same-family catalog families=%d err=%v", families, err)
			}
			args.reviewers = []string{"a/full", "opencode/" + model}
			dir, err = buildReviewCampaign(args, projects, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
		})
	}
}
