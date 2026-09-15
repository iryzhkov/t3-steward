package workerproto

import (
	"encoding/json"
	"testing"
)

func TestSnapshotAcknowledgementWireContract(t *testing.T) {
	const fixture = `{"parkedReported":true,"observedWorkerEpoch":"epoch-2","observedSequence":17}`
	var request SnapshotRequest
	if err := json.Unmarshal([]byte(fixture), &request); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSnapshotRequest(request); err != nil {
		t.Fatal(err)
	}
	if request.ObservedWorkerEpoch != "epoch-2" || request.ObservedSequence != 17 {
		t.Fatalf("%+v", request)
	}
	for _, invalid := range []SnapshotRequest{
		{ObservedSequence: -1}, {ObservedSequence: 1}, {ObservedWorkerEpoch: "epoch-2"},
	} {
		if err := ValidateSnapshotRequest(invalid); err == nil {
			t.Fatalf("invalid ack accepted: %+v", invalid)
		}
	}
	// An older H4 coordinator can still report parks, but omission of the ack
	// provides no authority to collect a stopped task.
	if err := ValidateSnapshotRequest(SnapshotRequest{ParkedReported: true}); err != nil {
		t.Fatal(err)
	}
}
