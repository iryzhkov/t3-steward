package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Strict rc.118 readers must receive their frozen receipt shape.
func TestReviewRC118RoleReceiptCompatibility(t *testing.T) {
	admin, _, _, _ := campaignRankedQuotaFixture(t)
	for _, version := range []string{"backlog.admin/v1-extended-read-rc118", backlogadmin.CurrentReadVersion} {
		response, err := admin.Query(context.Background(), backlogadmin.Query{
			Version: version, Kind: backlogadmin.QueryViability,
			Principal: backlogadmin.Principal{ID: "coordinator", Roles: []string{backlogadmin.LocalAdminRole}},
			Viability: &backlogadmin.ViabilityRequest{Tasks: []backlogadmin.ViabilityTask{{Name: "t", Project: "dev-fleet", Role: "execute", Class: domain.TaskClassRequired}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.Viability == nil || len(response.Viability.Tasks) != 1 || response.Viability.Tasks[0].RoleSelection == nil {
			t.Fatalf("receipt missing: %+v", response)
		}
		selection := response.Viability.Tasks[0].RoleSelection
		raw, err := json.Marshal(selection)
		if err != nil {
			t.Fatal(err)
		}
		if version != "backlog.admin/v1-extended-read-rc118" {
			if selection.Ranking != domain.RouteRankingV1 || len(selection.Candidates) == 0 {
				t.Fatalf("current ranking missing: %+v", selection)
			}
			for ordinal, candidate := range selection.Candidates {
				if candidate.Ordinal != ordinal || candidate.Band == "" || candidate.Pool == "" {
					t.Fatalf("current candidate ranking missing: %+v", candidate)
				}
			}
			continue
		}
		var legacy struct {
			Role         string `json:"role"`
			Route        string `json:"route"`
			Effort       string `json:"effort"`
			PolicyDigest string `json:"policyDigest"`
			Reason       string `json:"reason"`
			Candidates   []struct {
				Route    string `json:"route"`
				Eligible bool   `json:"eligible"`
				Reason   string `json:"reason,omitempty"`
			} `json:"candidates,omitempty"`
			Diversity  domain.RoleDiversity `json:"diversity"`
			ResolvedAt time.Time            `json:"resolvedAt"`
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&legacy); err != nil {
			t.Fatalf("rc118 reader rejects unchanged rc118 read version: %v", err)
		}
		if legacy.Route == "" || legacy.PolicyDigest == "" || len(legacy.Candidates) == 0 {
			t.Fatalf("legacy receipt lost fields: %+v", legacy)
		}
	}
}
