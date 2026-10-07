package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCampaignRoleCheckOnlyOutcomeMatchesShownTasks(t *testing.T) {
	for _, other := range []backlogadmin.ViabilityOutcome{backlogadmin.ViabilityImpossible, backlogadmin.ViabilityAcceptedWaiting} {
		for _, global := range []string{"none", "temporary", "permanent"} {
			t.Run(string(other)+"/"+global, func(t *testing.T) {
				plan := campaign.Plan{Tasks: []campaign.Task{{Name: "x", Role: "execute"}, {Name: "y", Role: "execute"}}}
				selection := &domain.RoleSelection{Role: "execute", Route: "a/m", Effort: "low"}
				reasons := []backlogadmin.ViabilityReason(nil)
				want := backlogadmin.ViabilityReady
				if global != "none" {
					reasons = []backlogadmin.ViabilityReason{{Code: "bundle-limit", Detail: "request-wide obstruction", Permanent: global == "permanent"}}
					want = backlogadmin.ViabilityAcceptedWaiting
					if global == "permanent" {
						want = backlogadmin.ViabilityImpossible
					}
				}
				c := campaignCLI{viability: func(_ context.Context, request backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
					if len(request.Tasks) != 2 {
						t.Fatal("producer context was dropped")
					}
					return backlogadmin.ViabilityMatrix{Outcome: other, Reasons: reasons, Tasks: []backlogadmin.ViabilityTaskResult{
						{Task: "x", Outcome: backlogadmin.ViabilityReady, RoleSelection: selection},
						{Task: "y", Outcome: other, RoleSelection: selection, Reasons: []backlogadmin.ViabilityReason{{Code: "unknown-role", Detail: "nope", Permanent: other == backlogadmin.ViabilityImpossible}}},
					}}, nil
				}}
				matrix, err := c.checkViability(context.Background(), plan, campaign.Bundle{}, "x")
				if err != nil {
					t.Fatal(err)
				}
				if matrix.Outcome != want || len(matrix.Tasks) != 1 || matrix.Tasks[0].Task != "x" || !reflect.DeepEqual(matrix.Reasons, reasons) {
					t.Fatalf("filtered matrix = %+v; want outcome %s with global reasons retained", matrix, want)
				}
			})
		}
	}
}

func TestCampaignRoleCheckReceiptTextAndJSON(t *testing.T) {
	selection := &domain.RoleSelection{Role: "review", Route: "codex/model", Effort: "medium", PolicyDigest: "fixture-digest", Reason: "first diverse eligible candidate",
		Candidates: []domain.RoleCandidateVerdict{{Route: "unavailable/model", Reason: "no advertising worker"}, {Route: "codex/model", Eligible: true}},
		Diversity:  domain.RoleDiversity{ProducerFamilies: []string{"claude"}, CrossProvider: true, Reason: "cross-provider: codex differs from producer providers claude"}, ResolvedAt: time.Unix(1, 0).UTC()}
	for _, asJSON := range []bool{false, true} {
		t.Run(map[bool]string{false: "text", true: "json"}[asJSON], func(t *testing.T) {
			root := campaignFixture(t)
			manifest := strings.Replace(campaignFixtureManifest, "name: example-campaign", "name: example-campaign\nrole: review", 1)
			if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), []byte(manifest), 0600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			cli, requests := campaignCheckCLI(t, &out, func(request backlogadmin.ViabilityRequest) backlogadmin.ViabilityMatrix {
				matrix := campaignReadyMatrix(request)
				for i := range matrix.Tasks {
					matrix.Tasks[i].RoleSelection = selection
				}
				return matrix
			})
			args := []string{"check", root}
			if asJSON {
				args = append(args, "--json")
			}
			if err := cli.run(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			if len(*requests) != 1 || (*requests)[0].Tasks[0].Role != "review" {
				t.Fatalf("requests = %+v", *requests)
			}
			if asJSON {
				var doc campaignCheck
				if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
					t.Fatal(err)
				}
				for _, task := range doc.Matrix.Tasks {
					if !reflect.DeepEqual(task.RoleSelection, selection) {
						t.Fatalf("receipt = %+v", task.RoleSelection)
					}
				}
			} else {
				for _, want := range []string{"role: review", "selected route: codex/model", "effort: medium", "policy digest: fixture-digest", "first diverse eligible candidate", "candidate unavailable/model: no advertising worker", "diversity: cross-provider: codex differs from producer providers claude"} {
					if !strings.Contains(out.String(), want) {
						t.Fatalf("missing %q in %s", want, out.String())
					}
				}
			}
		})
	}
}

func TestCampaignRoleExternalProducersDoNotApplyDiversity(t *testing.T) {
	p := &routePolicy{Digest: "d", Roles: []policyRole{{Name: "read", Candidates: []policyCandidate{{Route: "a/m", Effort: "low", Tier: "standard"}}}}}
	project := backlogadmin.Project{Name: "p", Workers: []backlogadmin.ProjectWorker{{Worker: "w", Ready: true, Advertises: true, Enrolled: true, Routes: []backlogadmin.ProjectRoute{{Instance: "a", Model: "m", Tier: "executor"}}}}}
	for _, useInputs := range []bool{false, true} {
		task := backlogadmin.ViabilityTask{Name: "review", Project: "p", Role: "read", ReviewType: true, Needs: []string{"run1/t"}}
		if useInputs {
			task.Producers = []string{"run1/t"}
		}
		selections, failures := resolveCampaignPolicy(p, []backlogadmin.ViabilityTask{task}, []backlogadmin.Project{project}, nil, time.Unix(1, 0))
		if len(failures) != 0 {
			t.Fatal(failures)
		}
		if got := selections["review"]; got.Route != "a/m" || got.Diversity.Reason != "" || got.Diversity.CrossProvider || len(got.Diversity.ProducerFamilies) != 0 {
			t.Fatalf("external-only producer receipt = %+v", got)
		}
	}
}
