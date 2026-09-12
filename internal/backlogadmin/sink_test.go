package backlogadmin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestSinkVisibleWithoutExecutableAttemptAndCountsOptIn(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	run, err := domain.BindRunSink(records.WorkflowRuns[0], records.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, include := range []bool{false, true} {
		response, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryWorkflow, WorkflowRunID: run.ID, IncludeSink: include})
		if err != nil {
			t.Fatal(err)
		}
		want := len(records.Tasks)
		if include {
			want++
		}
		if response.Workflow.Summary.Progress.Total != want || len(response.Workflow.Tasks) != len(records.Tasks)+1 {
			t.Fatalf("include=%v detail=%+v", include, response.Workflow)
		}
		sink := response.Workflow.Tasks[len(response.Workflow.Tasks)-1]
		if sink.Sink == nil || sink.Attempt != nil || sink.Assignment != nil {
			t.Fatalf("sink=%+v", sink)
		}
		status, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryStatus, IncludeSink: include})
		if err != nil {
			t.Fatal(err)
		}
		total := 0
		for _, n := range status.Status.Tasks {
			total += n
		}
		if total != want {
			t.Fatalf("status total=%d want=%d", total, want)
		}
	}
	graph, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryGraph, WorkflowRunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, edge := range graph.Graph.Edges {
		if edge.ToTaskID == run.Sink.ID {
			count++
		}
	}
	if count != len(records.Tasks) {
		t.Fatalf("sink edges=%d", count)
	}
	for _, id := range []string{domain.SinkTaskName, run.Sink.ID} {
		response, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryTask, WorkflowRunID: run.ID, TaskID: id})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(response.Task)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "\"attempt\"") || response.Task.Sink == nil {
			t.Fatalf("sink JSON=%s", raw)
		}
	}
	explanation, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryExplanation, WorkflowRunID: run.ID, TaskID: domain.SinkTaskName})
	if err != nil {
		t.Fatal(err)
	}
	if explanation.Explanation.Eligible || !strings.Contains(explanation.Explanation.Summary, "coordinator sink") {
		t.Fatalf("explanation=%+v", explanation)
	}
}
