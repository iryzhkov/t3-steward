package workerruntime

import (
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"testing"
)

func TestGateResultObjectsKeepReportAndLogDistinct(t *testing.T) {
	result := PublishedResult{Finalized: backlog.FinalizedAttempt{Artifacts: []domain.Artifact{
		{ID: "gate-attempt", Kind: domain.ArtifactGate, Name: "gate", MediaType: "application/json"},
		{ID: "gate-log-attempt", Kind: domain.ArtifactGate, Name: "gate/log.txt", MediaType: "text/plain"},
	}}}
	objects, err := resultObjects(workerproto.ExecutionPackage{}, result)
	if err != nil {
		t.Fatal(err)
	}
	if objects[0].object.Path != "results/gate/report.json" || objects[1].object.Path != "results/gate/log.txt" {
		t.Fatalf("objects=%+v", objects)
	}
	if result.Finalized.Artifacts[0].Name != "gate" {
		t.Fatal("logical artifact identity changed")
	}
}
