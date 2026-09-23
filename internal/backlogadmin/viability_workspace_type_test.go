package backlogadmin

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

func TestViabilityRefusesWorkspaceTypeMismatch(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requested string
		catalog   string
	}{
		{name: "fresh manifest with Git catalog", requested: backlog.EnvironmentFresh, catalog: backlog.EnvironmentGit},
		{name: "Git manifest with fresh catalog", requested: backlog.EnvironmentGit, catalog: backlog.EnvironmentFresh},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := viabilityCatalog(t)
			settings.Projects[0].Type = tc.catalog
			if tc.catalog == backlog.EnvironmentFresh {
				settings.Projects[0].Repository = ""
				settings.Projects[0].DefaultRef = ""
			}
			task := viabilityTaskRequest()
			task.Type = tc.requested
			if tc.requested == backlog.EnvironmentFresh {
				task.Ref = ""
			}
			matrix := viabilityView(t, nil).viability(context.Background(), settings,
				ViabilityRequest{Tasks: []ViabilityTask{task}})
			if matrix.Outcome != ViabilityImpossible {
				t.Fatalf("outcome = %q, want impossible: %+v", matrix.Outcome, matrix)
			}
			reason, found := candidateReason(t, matrix, ReasonWorkspaceTypeMismatch)
			if !found || !reason.Permanent {
				t.Fatalf("permanent mismatch missing: %+v", matrix)
			}
			for _, want := range []string{task.Project, tc.requested, tc.catalog, "manifest environment.type", "catalog project type"} {
				if !strings.Contains(reason.Detail, want) {
					t.Fatalf("mismatch detail %q does not explain %q", reason.Detail, want)
				}
			}
			if len(matrix.Tasks[0].Candidates) != 0 {
				t.Fatalf("type mismatch must stop before worker evaluation: %+v", matrix.Tasks[0].Candidates)
			}
		})
	}
}

func TestViabilityAcceptsMatchingAndLegacyWorkspaceTypes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requested string
		catalog   string
	}{
		{name: "omitted request and catalog default to Git"},
		{name: "matching explicit Git", requested: backlog.EnvironmentGit, catalog: backlog.EnvironmentGit},
		{name: "matching fresh", requested: backlog.EnvironmentFresh, catalog: backlog.EnvironmentFresh},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := viabilityCatalog(t)
			settings.Projects[0].Type = tc.catalog
			task := viabilityTaskRequest()
			task.Type = tc.requested
			if tc.catalog == backlog.EnvironmentFresh {
				settings.Projects[0].Repository = ""
				settings.Projects[0].DefaultRef = ""
				task.Ref = ""
			}
			matrix := viabilityView(t, nil).viability(context.Background(), settings,
				ViabilityRequest{Tasks: []ViabilityTask{task}})
			if reason, found := candidateReason(t, matrix, ReasonWorkspaceTypeMismatch); found {
				t.Fatalf("matching workspace type refused: %+v", reason)
			}
			if matrix.Outcome != ViabilityReady {
				t.Fatalf("outcome = %q, want ready: %+v", matrix.Outcome, matrix)
			}
		})
	}
}
