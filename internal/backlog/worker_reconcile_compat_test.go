package backlog

import (
	"bytes"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A new coordinator receiving a snapshot from a worker built before quota
// observations (v0.11.0-rc.66): the strict decoder accepts it, the gated
// fields read as empty, and the attempt is reconciled from the observed
// control exactly as before.
func TestOlderWorkerSnapshotReconcilesWithoutGatedFields(t *testing.T) {
	records, _ := parkedReleaseRecords()
	records.Attempts[0].Progress, records.Attempts[0].Control = domain.ProgressActive, domain.ControlRunning
	at := parkedReleaseTime.UTC().Format("2006-01-02T15:04:05Z07:00")
	until := parkedReleaseTime.Add(time.Minute).UTC().Format("2006-01-02T15:04:05Z07:00")
	raw := `{"workerId":"normandy","workerEpoch":"worker-1","coordinatorEpoch":1,"sequence":4,"connected":true,` +
		`"inventory":{"id":"normandy","acceptBacklog":true,"health":"ready","observedAt":"` + at + `"},` +
		`"assignments":[{"journal":{"phase":"running","packageSha256":"","graphRevision":1,"taskRevision":1,"updatedAt":"` + at + `"},` +
		`"assignmentId":"assign-1","assignmentEpoch":2,"state":"claimed","control":"running","threadId":"thread-1","observedAt":"` + at + `"}],` +
		`"observedAt":"` + at + `","validUntil":"` + until + `"}`
	var snapshot domain.WorkerSnapshot
	if err := (workerproto.Codec{MaxBytes: 1 << 20}).Decode(bytes.NewReader([]byte(raw)), &snapshot); err != nil {
		t.Fatalf("an older worker's snapshot is refused: %v", err)
	}
	journal := snapshot.Assignments[0].Journal
	if journal == nil || journal.PauseReason != "" || journal.ThreadState != "" || snapshot.QuotaObservations != nil {
		t.Fatalf("gated fields are not empty for an older worker: journal=%+v quota=%+v", journal, snapshot.QuotaObservations)
	}
	transitions, err := PlanWorkerStateTransitions(records, snapshot, nil, parkedReleaseTime)
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range transitions {
		if transition.Attempt.Control != domain.ControlRunning || transition.Attempt.Progress != domain.ProgressActive {
			t.Fatalf("an older worker's observation changed the attempt: %+v", transition.Attempt)
		}
	}
	if merged := MergeWorkerQuotaObservations(nil, []domain.WorkerSnapshot{snapshot}); len(merged) != 0 {
		t.Fatalf("an older worker contributed quota observations: %+v", merged)
	}
}
