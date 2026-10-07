package main

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Submission resolves a manifest's roles by asking the coordinator's own admin
// service for viability. rc.117 projects every older read version to the
// shape its release had, and the role selection is newer than all of them, so
// asking with an older version answered every role task as unresolved.
func TestCoordinatorRoleSelectionSurvivesAdminReadProjection(t *testing.T) {
	admin, _ := probeReadinessService(t, nil, "homelab")
	selection := domain.RoleSelection{Role: "execute", Route: "t3-primary/opus", Effort: "medium", PolicyDigest: "digest", Reason: "first eligible candidate in policy order", ResolvedAt: probeNow}
	admin.SetViability(backlogadmin.ViabilitySettings{
		Projects: []backlog.ProjectDefinition{{
			Name: "dev-fleet", Repository: probeRepository, DefaultRef: "main",
			SetupProfile: "go", RequiredCredentials: []string{probeCredentialRef},
		}},
		SetupProfiles: []backlog.SetupProfile{{Name: "go", Commands: []string{"go build ./..."}, Timeout: time.Minute}},
		ResolveRoles: func(context.Context, []backlogadmin.ViabilityTask, []backlogadmin.Project, backlogadmin.RoleWorkerEligible) (map[string]domain.RoleSelection, map[string]backlogadmin.ViabilityReason) {
			return map[string]domain.RoleSelection{"implement": selection}, nil
		},
	})
	request := backlogadmin.ViabilityRequest{Tasks: []backlogadmin.ViabilityTask{{
		Name: "implement", Project: "dev-fleet", Ref: "main", Role: "execute",
		Class: domain.TaskClassRequired, Capabilities: []string{"git"},
	}}}
	selections, err := queryRoleSelections(context.Background(), admin, request)
	if err != nil {
		t.Fatal(err)
	}
	if got := selections["implement"]; got.Route != selection.Route || got.PolicyDigest != selection.PolicyDigest {
		t.Fatalf("selection = %+v, want %+v", got, selection)
	}
}
