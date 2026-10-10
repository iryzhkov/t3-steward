package backlogadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestRC120ProjectionFreezesEveryNestedKey(t *testing.T) {
	var response Response
	populateForSchema(reflect.ValueOf(&response).Elem(), map[reflect.Type]bool{})
	assertRC119Projection(t, &response, rc120ExtendedResponseSchema, projectRC120ExtendedResponse)
}

func TestRC120ReadPreservesStoreAndCurrentMetadata(t *testing.T) {
	reader := explainReader{records: sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "w"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w", Sink: &domain.SinkTask{ID: "sink:r", Result: &domain.SinkResult{FixLoops: []domain.FixLoopSummary{{Name: "loop", MaxRounds: 2}}}}}},
		Tasks:        []domain.Task{{ID: "t", Name: "execute", WorkflowID: "w", NeedsVerdict: map[string]string{"review": "accept"}, FixLoop: &domain.FixLoopTask{Name: "loop", Round: 1, MaxRounds: 2}, Retry: &domain.TaskRetryPolicy{Infrastructure: 1}}},
		Attempts:     []domain.Attempt{{ID: "a", WorkflowRunID: "r", TaskID: "t", Number: 1, FailureClass: domain.FailureInfrastructure, FailureReason: domain.ReasonPreparationFailed, AutomaticRetry: &domain.AutomaticRetry{SourceAttemptID: "previous", Ordinal: 1}}},
	}}
	service, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{RC120ReadVersion, CurrentReadVersion} {
		response, err := service.Query(context.Background(), Query{Version: version, Kind: QueryWorkflow, WorkflowRunID: "r"})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		var tree any
		if err := json.Unmarshal(raw, &tree); err != nil {
			t.Fatal(err)
		}
		var refused []string
		rc120ExtendedResponseSchema.violations(tree, "", &refused)
		task := response.Workflow.Tasks[0]
		if version == RC120ReadVersion {
			if len(refused) != 0 {
				t.Fatalf("rc121 strict schema refuses: %v", refused)
			}
			if task.Task.Retry != nil || task.Task.FixLoop != nil || task.Task.NeedsVerdict != nil || task.Attempt.FailureClass != "" || task.Attempt.AutomaticRetry != nil || len(response.Workflow.Summary.Run.Sink.Result.FixLoops) != 0 {
				t.Fatal("old read carries new metadata")
			}
		} else {
			if len(refused) == 0 || task.Task.Retry == nil || task.Task.FixLoop == nil || task.Attempt.FailureClass == "" || task.Attempt.AutomaticRetry == nil || len(response.Workflow.Summary.Run.Sink.Result.FixLoops) != 1 {
				t.Fatal("current read lost metadata")
			}
		}
	}
	if reader.records.Tasks[0].Retry == nil || reader.records.Attempts[0].FailureClass == "" || len(reader.records.WorkflowRuns[0].Sink.Result.FixLoops) != 1 {
		t.Fatal("projection modified store records")
	}
}

func TestRC122ClientNegotiatesRC121Read(t *testing.T) {
	var versions []string
	got, err := queryExtended(context.Background(), Query{Version: Version, Kind: QueryTask}, func(_ context.Context, q Query) (Response, error) {
		versions = append(versions, q.Version)
		if q.Version == CurrentReadVersion {
			return Response{}, &TransportError{Class: ClassRejected, Err: fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, q.Version, Version)}
		}
		return Response{Version: q.Version}, nil
	})
	if err != nil || got.Version != RC120ReadVersion || !reflect.DeepEqual(versions, []string{CurrentReadVersion, RC120ReadVersion}) {
		t.Fatalf("negotiated %+v, %v through %v", got, err, versions)
	}
}
