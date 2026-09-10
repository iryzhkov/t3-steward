package domain

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestArtifactProtocolJSONRoundTrip(t *testing.T) {
	publication := ArtifactPublication{
		CoordinatorEpoch: 4, WorkerID: "normandy", WorkerEpoch: "process-2",
		AssignmentID: "assignment-1", AssignmentEpoch: 3, AttemptRevision: 7,
		Artifact: Artifact{
			ID: "artifact-1", WorkflowRunID: "run-1", TaskID: "task-1",
			AttemptID: "attempt-1", Kind: ArtifactGitState, Name: "git/diff.patch",
			MediaType: "text/x-diff", Size: 42,
			SHA256:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			StoragePath: "objects/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Producer:    "worker", CreatedAt: time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC),
		},
	}
	raw, err := json.Marshal(publication)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ArtifactPublication
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, publication) {
		t.Fatalf("round trip = %#v, want %#v", decoded, publication)
	}

	fetch := ArtifactFetchRequest{WorkflowRunID: "run-1", ArtifactIDs: []string{"artifact-1"}}
	raw, err = json.Marshal(fetch)
	if err != nil {
		t.Fatal(err)
	}
	var decodedFetch ArtifactFetchRequest
	if err := json.Unmarshal(raw, &decodedFetch); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedFetch, fetch) {
		t.Fatalf("fetch round trip = %#v, want %#v", decodedFetch, fetch)
	}
}
