package backlogadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestRC119ProjectionCompleteAndKeepsFrozenFields(t *testing.T) {
	var response Response
	populateForSchema(reflect.ValueOf(&response).Elem(), map[reflect.Type]bool{})
	assertRC119Projection(t, &response, rc119ExtendedResponseSchema, projectRC119ExtendedResponse)
	var answer localResponse
	populateForSchema(reflect.ValueOf(&answer).Elem(), map[reflect.Type]bool{})
	answer.Response = nil // Versioned read replies are not persisted in the replay cache.
	answer.Version = LocalTransportVersion
	assertRC119Projection(t, &answer, rc119LocalResponseSchema, projectRC119UnversionedAnswer)
	raw, err := json.Marshal(answer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCachedResponse(raw); err != nil {
		t.Fatalf("projected cache cannot decode: %v", err)
	}
}

func assertRC119Projection[T any](t *testing.T, value *T, schema keySchema, project func(*T) error) {
	t.Helper()
	current := parseKeySchema(strings.Join(jsonKeySchema(reflect.TypeOf(*value)), "\n"))
	for path := range schema.paths {
		if !current.paths[path] {
			t.Fatalf("rc119 field missing: %s", path)
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var original any
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	var refused []string
	schema.violations(original, "", &refused)
	if len(refused) == 0 {
		t.Fatal("sensitivity: populated rc120 answer has no rc119-incompatible fields")
	}
	schema.prune(original, "")
	if err := project(value); err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var projected any
	if err := json.Unmarshal(raw, &projected); err != nil {
		t.Fatal(err)
	}
	refused = nil
	schema.violations(projected, "", &refused)
	if len(refused) != 0 || !reflect.DeepEqual(original, projected) {
		t.Fatalf("projection changed frozen values or retained unknown fields: %v", refused)
	}
}

func TestRC119ReadVersionFreezesNewFieldsAndCurrentKeepsThem(t *testing.T) {
	selection := domain.RoleSelection{Role: "execute", Route: "codex/gpt", Effort: "medium",
		Candidates: []domain.RoleCandidateVerdict{{Route: "codex/gpt", Eligible: true, Effort: "high"}}}
	task := domain.Task{ID: "t", Name: "execute", WorkflowID: "w", RoleSelection: &selection}
	reader := explainReader{records: sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{ID: "w"}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w",
			Lineage:         &domain.RunLineage{ParentRunID: "parent", ParentTaskID: "task", ParentAttemptID: "attempt"},
			RouteSelections: map[string]domain.RoleSelection{"t": selection}}},
		Tasks: []domain.Task{task},
	}}
	service, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{RC119ReadVersion, CurrentReadVersion} {
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
		rc119ExtendedResponseSchema.violations(tree, "", &refused)
		if version == RC119ReadVersion {
			if len(refused) != 0 {
				t.Fatalf("rc119 strict decoder rejects: %v", refused)
			}
			if response.Workflow.Summary.Run.Lineage != nil {
				t.Fatal("old read carries lineage")
			}
		} else {
			if len(refused) == 0 || response.Workflow.Summary.Run.Lineage == nil {
				t.Fatal("current read lost new evidence")
			}
		}
	}
}

func TestRC122ClientNegotiatesRC119ReadVersion(t *testing.T) {
	var versions []string
	got, err := queryExtended(context.Background(), Query{Version: Version, Kind: QueryTask}, func(_ context.Context, q Query) (Response, error) {
		versions = append(versions, q.Version)
		if q.Version == CurrentReadVersion || q.Version == RC120ReadVersion {
			return Response{}, &TransportError{Class: ClassRejected, Err: fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, q.Version, Version)}
		}
		return Response{Version: q.Version}, nil
	})
	if err != nil || got.Version != RC119ReadVersion || !reflect.DeepEqual(versions, []string{CurrentReadVersion, RC120ReadVersion, RC119ReadVersion}) {
		t.Fatalf("negotiated %+v, %v through %v", got, err, versions)
	}
}
