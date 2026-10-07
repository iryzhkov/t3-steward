package sqlite

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestGateArtifactPublicationKind(t *testing.T) {
	hash := strings.Repeat("a", 64)
	p := domain.ArtifactPublication{CoordinatorEpoch: 1, AssignmentEpoch: 1, AttemptRevision: 1, WorkerID: "worker", WorkerEpoch: "epoch", AssignmentID: "assignment", Artifact: domain.Artifact{ID: "gate", WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt", Name: "gate", MediaType: "application/json", SHA256: hash, StoragePath: "objects/aa/" + hash, Producer: "worker", CreatedAt: time.Now(), Kind: domain.ArtifactGate}}
	if err := validateArtifactPublication(p); err != nil {
		t.Fatal(err)
	}
	p.Artifact.Name = "gate/log.txt"
	p.Artifact.MediaType = "text/plain"
	if err := validateArtifactPublication(p); err != nil {
		t.Fatal(err)
	}
}
