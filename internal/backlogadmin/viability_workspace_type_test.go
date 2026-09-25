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

// TestViabilityNamesFreshProjects pins the one sentence a repository-free
// author needs when check refuses them: which catalog projects are fresh, or
// that there is none and how an operator adds one.
func TestViabilityNamesFreshProjects(t *testing.T) {
	for _, tc := range []struct {
		name    string
		project string
		scratch bool
		reason  string
		want    string
	}{
		{name: "Git project, no fresh project", reason: ReasonWorkspaceTypeMismatch,
			want: `no fresh project; an operator declares one with "upkeeper project add NAME --type fresh`},
		{name: "Git project, fresh project exists", scratch: true, reason: ReasonWorkspaceTypeMismatch,
			want: "fresh projects in this catalog: scratch"},
		{name: "unknown project, no fresh project", project: "missing", reason: ReasonUnknownProject,
			want: `no fresh project; an operator declares one with "upkeeper project add NAME --type fresh`},
		{name: "unknown project, fresh project exists", project: "missing", scratch: true, reason: ReasonUnknownProject,
			want: "fresh projects in this catalog: scratch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := viabilityCatalog(t)
			if tc.scratch {
				scratch := settings.Projects[0]
				scratch.Name, scratch.Type, scratch.Repository, scratch.DefaultRef = "scratch", backlog.EnvironmentFresh, "", ""
				settings.Projects = append(settings.Projects, scratch)
			}
			task := viabilityTaskRequest()
			task.Type, task.Ref = backlog.EnvironmentFresh, ""
			if tc.project != "" {
				task.Project = tc.project
			}
			matrix := viabilityView(t, nil).viability(context.Background(), settings,
				ViabilityRequest{Tasks: []ViabilityTask{task}})
			if matrix.Outcome != ViabilityImpossible {
				t.Fatalf("outcome = %q, want impossible: %+v", matrix.Outcome, matrix)
			}
			reason, found := candidateReason(t, matrix, tc.reason)
			if !found {
				t.Fatalf("reason %s missing: %+v", tc.reason, matrix)
			}
			if !strings.Contains(reason.Detail, tc.want) {
				t.Fatalf("detail %q does not contain %q", reason.Detail, tc.want)
			}
		})
	}

	// A Git manifest refused for an unknown project gets no fresh hint: it
	// would send its author towards a workspace type they did not ask for.
	task := viabilityTaskRequest()
	task.Project = "missing"
	matrix := viabilityView(t, nil).viability(context.Background(), viabilityCatalog(t),
		ViabilityRequest{Tasks: []ViabilityTask{task}})
	if reason, _ := candidateReason(t, matrix, ReasonUnknownProject); strings.Contains(reason.Detail, "fresh") {
		t.Fatalf("Git manifest got a fresh hint: %q", reason.Detail)
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
