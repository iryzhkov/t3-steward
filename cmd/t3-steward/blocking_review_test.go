package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestBlockingHelpListsInterruptExit(t *testing.T) {
	paths := map[string]bool{"backlog command show": false}
	for _, verb := range []string{"cancel", "retry", "skip", "start", "resume", "pause", "delay", "rewake"} {
		paths["backlog "+verb] = false
	}
	for _, page := range backlogHelpPages() {
		if _, ok := paths[page.Path]; !ok {
			continue
		}
		for _, exit := range page.Exits {
			if exit.Code == 130 {
				paths[page.Path] = true
			}
		}
	}
	for path, found := range paths {
		if !found {
			t.Errorf("%s has no exit 130", path)
		}
	}
}

func TestBlockingResultWrappedDeadlinePrintsLastProgress(t *testing.T) {
	f := newTaskResultFixture(t)
	f.detail.Summary.Run.Progress = domain.ProgressActive
	f.detail.Tasks[0].Attempt.Progress = domain.ProgressActive
	f.detail.Artifacts = nil
	cli := f.cli()
	query := cli.query
	calls := 0
	cli.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
		calls++
		if calls == 2 {
			return backlogadmin.Response{}, fmt.Errorf("query: %w", context.DeadlineExceeded)
		}
		return query(ctx, q)
	}
	err := cli.run(context.Background(), []string{"run-1", "--wait", "--timeout", "1s", "--json"})
	if err == nil || exitCodeFor(err) != 1 {
		t.Fatalf("err=%v", err)
	}
	if f.document(t).Outcome != string(domain.ProgressActive) {
		t.Fatal("last progress lost")
	}
}
