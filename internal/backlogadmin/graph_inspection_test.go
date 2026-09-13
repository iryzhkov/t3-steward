package backlogadmin

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type graphTransport struct {
	*localTransportService
	graph *Service
}

func (s graphTransport) AmendGraph(ctx context.Context, p Principal, r domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
	return s.graph.AmendGraph(ctx, p, r)
}

func TestGraphAmendmentSocketPeerAuthorityAndInspection(t *testing.T) {
	ctx := context.Background()
	s, store := graphFixture(t)
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), graphTransport{&localTransportService{}, s})
	defer stopLocalTransport(t, cancel, done)
	model := "new"
	r := graphRequest("socket", "task-set", "a", 1)
	r.Model = &model
	result, err := client.AmendGraph(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if result.Graph.Actor != "local:"+strings.TrimPrefix(result.Graph.Actor, "local:") || !strings.HasPrefix(result.Graph.Actor, "local:") {
		t.Fatalf("peer actor=%s", result.Graph.Actor)
	}
	replay, err := client.AmendGraph(ctx, r)
	if err != nil || !replay.Replay {
		t.Fatal(replay, err)
	}
	wait := domain.NodeWaitRequest{ID: "nw-inspection", Name: "inspect", ThreadID: "thread", Target: domain.NodeRef{RunID: "run", TaskID: "a"}, Timeout: time.Hour}
	if _, err = store.RegisterNodeWait(ctx, wait, "operator", "host", s.now()); err != nil {
		t.Fatal(err)
	}
	response, err := s.Query(ctx, Query{Version: Version, Kind: QueryDiagnose, WorkflowRunID: "run", Principal: Principal{ID: "operator"}})
	if err != nil {
		t.Fatal(err)
	}
	d := response.Diagnosis
	if d == nil || d.GraphRevision != 2 || d.Graph.GraphRevision != 2 || len(d.Explanations) != 3 || len(d.Waits) != 1 {
		t.Fatalf("diagnosis=%+v", d)
	}
	found := false
	for _, event := range d.Events {
		if event.Kind == "graph-amended" {
			found = true
		}
	}
	if !found {
		t.Fatal("amendment audit missing from diagnosis")
	}
	denied, err := New(store, &allowAuthorizer{err: errors.New("denied")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = denied.AmendGraph(ctx, Principal{}, r); err == nil {
		t.Fatal("unauthorized replay")
	}
}

func TestGraphAmendmentCrossRunCycleRollbackAndAliasReplay(t *testing.T) {
	ctx := context.Background()
	s, store := graphFixture(t)
	p := Principal{ID: "operator"}
	now := s.now()
	task := domain.Task{ID: "remote-task", WorkflowID: "remote-workflow", Name: "remote", Class: domain.TaskClassSurplus, MaxTurns: 1, PromptArtifactID: "remote-prompt",
		Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "old"}}, ExternalNeeds: []domain.NodeRef{{RunID: "run", TaskID: "a"}}}
	a, err := backlog.PrepareGraphInput(s.graphInputRoot, task.PromptArtifactID, "remote-run", task.ID, "remote", now)
	if err != nil {
		t.Fatal(err)
	}
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "remote-run", WorkflowID: task.WorkflowID, GraphRevision: 1, Revision: 1, Progress: domain.ProgressQueued, CreatedAt: now, UpdatedAt: now}, []domain.Task{task})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Workflows: []domain.Workflow{{ID: task.WorkflowID, Name: "remote"}}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Artifacts: []domain.Artifact{a}, Attempts: []domain.Attempt{{ID: "remote-attempt", WorkflowRunID: run.ID, TaskID: task.ID, Number: 1, Revision: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, UpdatedAt: now}}}); err != nil {
		t.Fatal(err)
	}
	r := graphRequest("cross-cycle", "edge-add", "a", 1)
	r.Source = "remote-run/__sink"
	if _, err = s.AmendGraph(ctx, p, r); err == nil {
		t.Fatal("cross-run cycle accepted")
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range records.WorkflowRuns {
		if run.ID == "run" && run.GraphRevision != 1 {
			t.Fatal("rejected cycle committed")
		}
	}
	r.ID = "cross-safe"
	r.TaskID = "b"
	result, err := s.AmendGraph(ctx, p, r)
	if err != nil {
		t.Fatal(err)
	}
	if result.Graph.Revision != 2 {
		t.Fatal("rejected cycle consumed revision")
	}
	replay, err := s.AmendGraph(ctx, p, r)
	if err != nil || !replay.Replay {
		t.Fatal("alias replay failed", err)
	}
}
