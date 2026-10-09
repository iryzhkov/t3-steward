package backlog

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Immutable rc119 field set; new coordinator evidence must stay off this wire.
type rc119PlacementDecision struct {
	ResourceEvaluations []domain.ResourceEvaluation  `json:"resourceEvaluations,omitempty"`
	TaskID              string                       `json:"taskId,omitempty"`
	AttemptID           string                       `json:"attemptId,omitempty"`
	AssignmentID        string                       `json:"assignmentId,omitempty"`
	SelectedWorkerID    string                       `json:"selectedWorkerId,omitempty"`
	ReservationID       string                       `json:"reservationId,omitempty"`
	Demand              domain.ResourceDemand        `json:"demand"`
	CandidateIDs        []string                     `json:"candidateIds,omitempty"`
	Rejections          []domain.PlacementRejection  `json:"rejections,omitempty"`
	Scores              []domain.PlacementScore      `json:"scores,omitempty"`
	Snapshots           []domain.CapacitySnapshotRef `json:"snapshots,omitempty"`
	DecidedAt           time.Time                    `json:"decidedAt"`
}

func TestOfferPlacementRC119StrictDecoder(t *testing.T) {
	placement := &domain.PlacementDecision{
		TaskID: "task", SelectedWorkerID: "worker",
		RouteReresolution: &domain.RouteReresolution{Role: "execute", FromRoute: "claude/opus", ToRoute: "codex/gpt"},
	}
	for _, capabilities := range [][]string{nil, {workerproto.CapabilityResourceTelemetry}, {workerproto.CapabilityResourceTelemetry, workerproto.CapabilityZramSwapTelemetry}} {
		raw, err := json.Marshal(offerPlacement(placement, capabilities))
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		var old rc119PlacementDecision
		if err := decoder.Decode(&old); err != nil {
			t.Fatalf("rc119 cannot decode offer: %v", err)
		}
		if old.SelectedWorkerID != placement.SelectedWorkerID {
			t.Fatal("placement lost selected worker")
		}
	}
	if placement.RouteReresolution == nil {
		t.Fatal("wire projection changed durable receipt")
	}
}
