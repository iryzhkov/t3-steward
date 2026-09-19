package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// coordinatorWithoutTheProjectsQuery answers everything the previous release
// answers and refuses the one query it does not have, the way that coordinator
// refuses it (bd5362b:internal/backlogadmin/service.go, ErrInvalidQuery).
type coordinatorWithoutTheProjectsQuery struct {
	release string
	asked   []backlogadmin.QueryKind
}

func (c *coordinatorWithoutTheProjectsQuery) Query(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
	c.asked = append(c.asked, query.Kind)
	switch query.Kind {
	case backlogadmin.QueryProjects:
		return backlogadmin.Response{}, errRC69RefusesTheProjectsQuery
	case backlogadmin.QueryStatus:
		return backlogadmin.Response{
			Version: backlogadmin.Version, Kind: query.Kind,
			Status: &backlogadmin.Status{Runtime: backlogadmin.RuntimeStatus{Release: c.release}},
		}, nil
	default:
		return backlogadmin.Response{Version: backlogadmin.Version, Kind: query.Kind}, nil
	}
}

// The explanation is not a property of "task run": every verb that asks for
// the catalog meets the same coordinator, and the rewritten t3-backlog skill
// tells an agent to run all three. The bare "invalid query: kind \"projects\""
// reads like a client bug and names nothing to do about it.
func TestEveryVerbThatNeedsTheCatalogExplainsACoordinatorWithoutIt(t *testing.T) {
	cases := []struct {
		name    string
		run     func(*testing.T, *coordinatorWithoutTheProjectsQuery) error
		instead string
	}{
		{
			name: "backlog projects",
			run: func(t *testing.T, service *coordinatorWithoutTheProjectsQuery) error {
				t.Helper()
				var out bytes.Buffer
				cli := backlogAdminCLI{
					service:   service,
					principal: backlogadmin.Principal{ID: "operator"},
					stdout:    &out,
				}
				return cli.runBacklog(context.Background(), []string{"projects"})
			},
			instead: "t3-steward backlog workers",
		},
		{
			name: "models --project",
			run: func(t *testing.T, service *coordinatorWithoutTheProjectsQuery) error {
				t.Helper()
				var out bytes.Buffer
				cli := modelsCLI{
					service:   service,
					principal: backlogadmin.Principal{ID: "operator"},
					stdout:    &out,
				}
				return cli.run(context.Background(), modelsScope{Project: "steward"}, false)
			},
			instead: `"t3-steward models" without --project`,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			service := &coordinatorWithoutTheProjectsQuery{release: "v0.11.0-rc.69"}
			err := test.run(t, service)
			if err == nil {
				t.Fatal("the verb succeeded against a coordinator that has no projects query")
			}
			for _, want := range []string{
				"v0.11.0-rc.69", `"projects" query`, projectsQueryRelease,
				"invalid query", test.instead, "upgrade the coordinator",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("the refusal does not name %q: %v", want, err)
				}
			}
		})
	}
}

// Only that one refusal is explained. Any other failure of the query is the
// caller's to read as it stands, and a verb that does not ask for the catalog
// is not touched at all.
func TestTheProjectsExplanationIsAttachedToThatOneRefusal(t *testing.T) {
	t.Run("another failure of the projects query", func(t *testing.T) {
		unreachable := errors.New("coordinator unavailable: dial unix: no such file")
		if got := explainRefusedProjectsQuery(context.Background(), nil, unreachable, projectsWithoutTheCatalog); got != unreachable {
			t.Fatalf("error = %v, want the transport failure unchanged", got)
		}
	})
	t.Run("a verb that does not ask for the catalog", func(t *testing.T) {
		service := &coordinatorWithoutTheProjectsQuery{release: "v0.11.0-rc.69"}
		var out bytes.Buffer
		cli := modelsCLI{service: service, principal: backlogadmin.Principal{ID: "operator"}, stdout: &out}
		if err := cli.run(context.Background(), modelsScope{}, false); err != nil {
			t.Fatalf("models without --project failed against a coordinator of the previous release: %v", err)
		}
		for _, kind := range service.asked {
			if kind == backlogadmin.QueryProjects {
				t.Fatalf("models without --project asked for the catalog: %v", service.asked)
			}
		}
	})
}
