package domain

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestWorkerProtocolJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 10, 18, 0, 0, 0, time.UTC)
	fixture := struct {
		Snapshot WorkerSnapshot         `json:"snapshot"`
		Command  WorkerCommand          `json:"command"`
		Ack      WorkerAcknowledgement  `json:"ack"`
		Plan     AssignmentPlanCommit   `json:"plan"`
		Claim    AssignmentClaimRequest `json:"claim"`
	}{
		Snapshot: WorkerSnapshot{
			WorkerID: "normandy", WorkerEpoch: "worker-epoch-1",
			CoordinatorEpoch: 3, Sequence: 8, Connected: true,
			Inventory: WorkerInventory{ID: "normandy", Health: WorkerHealthReady},
			Assignments: []WorkerAssignmentObservation{{
				AssignmentID: "assignment-1", AssignmentEpoch: 2,
				State: AssignmentClaimed, ThreadID: "thread-1", ObservedAt: now,
			}},
			ObservedAt: now, ValidUntil: now.Add(time.Minute),
		},
		Command: WorkerCommand{
			ID: "command-1", Kind: WorkerCommandDispatch, WorkerID: "normandy",
			WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 3,
			AssignmentID: "assignment-1", AssignmentEpoch: 2,
			ExpectedWorkerSequence: 8, CreatedAt: now,
		},
		Ack: WorkerAcknowledgement{
			CommandID: "command-1", WorkerID: "normandy", WorkerEpoch: "worker-epoch-1",
			CoordinatorEpoch: 3, AssignmentID: "assignment-1", AssignmentEpoch: 2,
			WorkerSequence: 9, Accepted: true, AcknowledgedAt: now,
		},
		Plan: AssignmentPlanCommit{
			CoordinatorEpoch: 3, CommittedAt: now,
			Items: []AssignmentPlanItem{{
				ExpectedAttemptRevision: 4, WorkerEpoch: "worker-epoch-1",
				WorkerSnapshotSequence: 8,
				Assignment: Assignment{
					ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy",
					WorkerEpoch: "worker-epoch-1", State: AssignmentOffered, Epoch: 2,
				},
			}},
		},
		Claim: AssignmentClaimRequest{
			CoordinatorEpoch: 3, WorkerID: "normandy", WorkerEpoch: "worker-epoch-1",
			AssignmentID: "assignment-1", AssignmentEpoch: 2, LeaseToken: "lease-1",
			ClaimedAt: now, LeaseExpiresAt: now.Add(time.Minute),
		},
	}
	raw, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var got typeofWorkerProtocolFixture
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	want := typeofWorkerProtocolFixture(fixture)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\nwant %#v\n got %#v", want, got)
	}
}

type typeofWorkerProtocolFixture struct {
	Snapshot WorkerSnapshot         `json:"snapshot"`
	Command  WorkerCommand          `json:"command"`
	Ack      WorkerAcknowledgement  `json:"ack"`
	Plan     AssignmentPlanCommit   `json:"plan"`
	Claim    AssignmentClaimRequest `json:"claim"`
}
