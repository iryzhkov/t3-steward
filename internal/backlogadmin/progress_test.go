package backlogadmin

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"time"
)

func TestProgressQueryIncludesReviewOnlyChanges(t *testing.T) {
	for _, state := range []domain.ProgressState{domain.ProgressActive, domain.ProgressSucceeded} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			store := openAdminTestStore(t)
			old := adminTestNow.Add(-48 * time.Hour)
			records := sqlite.CoordinatorRecords{
				Workflows:    []domain.Workflow{{ID: "w", Name: "workflow", TaskIDs: []string{"t"}}},
				WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w", Progress: state, UpdatedAt: old}},
				Tasks:        []domain.Task{{ID: "t", WorkflowID: "w", Name: "task"}},
				Attempts:     []domain.Attempt{{ID: "a", TaskID: "t", WorkflowRunID: "r", Number: 1, Progress: state, UpdatedAt: old}},
				ReviewRounds: []review.Round{{ID: "round", InputManifestDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", WorkflowRunID: "r", UpdatedAt: adminTestNow, Reviewers: []review.Reviewer{{ID: "reviewer", Role: "review", Route: "codex/sol", TaskID: "t", Required: true}}}},
			}
			if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			service, err := New(store, &allowAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			q := Query{Version: Version, Kind: QueryWorkflows, ProgressMirror: &backlog.ProgressFilter{Since: old.Add(time.Hour)}}
			response, err := service.Query(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if response.ProgressMirror == nil || len(response.ProgressMirror.Runs) != 1 {
				t.Fatalf("recent durable review lost by since: %+v", response.ProgressMirror)
			}
			if response.ProgressMirror.Runs[0].LastChange.Before(q.ProgressMirror.Since) {
				t.Fatalf("wrong last change: %+v", response.ProgressMirror)
			}
		})
	}
}

func TestProgressQueryReadOnlyAndAuthorized(t *testing.T) {
	ctx := context.Background()
	store := openAdminTestStore(t)
	records := sqlite.CoordinatorRecords{Workflows: []domain.Workflow{{ID: "w", Name: "workflow", TaskIDs: []string{"t"}}}, WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w", Progress: domain.ProgressActive, UpdatedAt: adminTestNow}}, Tasks: []domain.Task{{ID: "t", WorkflowID: "w", Name: "task"}}, Attempts: []domain.Attempt{{ID: "a", TaskID: "t", WorkflowRunID: "r", Number: 1, Progress: domain.ProgressWaitingExternal, UpdatedAt: adminTestNow}}}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	auth := &allowAuthorizer{}
	service, err := New(store, auth)
	if err != nil {
		t.Fatal(err)
	}
	q := Query{Version: Version, Kind: QueryWorkflows, Principal: Principal{ID: "reader"}, ProgressMirror: &backlog.ProgressFilter{RunIDs: []string{"r"}}}
	response, err := service.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if response.ProgressMirror == nil || len(response.ProgressMirror.Runs) != 1 || response.ProgressMirror.Runs[0].Tasks[0].State != domain.ProgressWaitingExternal {
		t.Fatalf("mirror: %+v", response)
	}
	if len(response.Workflows) != 0 || len(auth.actions) != 1 || auth.actions[0].Kind != QueryWorkflows {
		t.Fatalf("query path: %+v %+v", response, auth.actions)
	}
	after, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("query wrote coordinator records")
	}
	auth.err = errors.New("denied")
	if _, err = service.Query(ctx, q); !errors.Is(err, auth.err) {
		t.Fatal(err)
	}
	auth.err = nil
	q.ProgressMirror = &backlog.ProgressFilter{Owner: "nobody"}
	response, err = service.Query(ctx, q)
	if err != nil || response.ProgressMirror == nil || len(response.ProgressMirror.Runs) != 0 {
		t.Fatalf("owner: %+v %v", response, err)
	}
	q.Kind = QueryStatus
	if _, err = service.Query(ctx, q); !errors.Is(err, ErrInvalidQuery) {
		t.Fatal(err)
	}
}
