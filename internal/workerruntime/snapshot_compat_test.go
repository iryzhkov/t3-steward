package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The worker snapshot as a coordinator from before quota observations
// (v0.11.0-rc.66) decodes it, field for field, with unknown fields refused
// the way workerproto.Codec refuses them. Every snapshot a new worker sends
// to a coordinator that has not asked for quota observations must decode
// into this shape.
type rc66Snapshot struct {
	WorkerID         string                 `json:"workerId"`
	WorkerEpoch      string                 `json:"workerEpoch"`
	CoordinatorEpoch int64                  `json:"coordinatorEpoch"`
	Sequence         int64                  `json:"sequence"`
	Connected        bool                   `json:"connected"`
	Inventory        domain.WorkerInventory `json:"inventory"`
	Assignments      []rc66Observation      `json:"assignments,omitempty"`
	ObservedAt       time.Time              `json:"observedAt"`
	ValidUntil       time.Time              `json:"validUntil"`
}

type rc66Observation struct {
	Journal         *rc66JournalExcerpt    `json:"journal,omitempty"`
	AssignmentID    string                 `json:"assignmentId"`
	AssignmentEpoch int64                  `json:"assignmentEpoch"`
	State           domain.AssignmentState `json:"state"`
	Control         domain.ControlState    `json:"control,omitempty"`
	ThreadID        string                 `json:"threadId,omitempty"`
	WorkspacePath   string                 `json:"workspacePath,omitempty"`
	ObservedAt      time.Time              `json:"observedAt"`
}

type rc66JournalExcerpt struct {
	Phase         string    `json:"phase"`
	Failure       string    `json:"failure,omitempty"`
	PackageSHA256 string    `json:"packageSha256"`
	GraphRevision int64     `json:"graphRevision"`
	TaskRevision  int64     `json:"taskRevision"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// A new worker holding a paused attempt sends a snapshot an rc.66
// coordinator decodes: the pause reason and thread state in the journal
// excerpt are included only when the coordinator asked for quota
// observations, the same gate as the observations themselves. The pause is
// still visible to the older coordinator as the paused control state.
func TestSnapshotOmitsPauseFieldsUnlessQuotaObservationsWereAsked(t *testing.T) {
	now := runtimeTestNow
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true,
		observations: []backlog.DispatchThreadState{backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
	guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
	runtime := runningRuntime(t, driver, guard, &now)
	// The pause in force, as the worker's own quota stop records it.
	pause := stoppedPause()
	if err := runtime.journal.update(func(state *journalState) error {
		record := state.Attempts["assignment-1"]
		stopped := now
		record.LocalThrottle = &LocalThrottleRequest{Kind: domain.ThrottleCommandHardStop, Bucket: pause.Bucket, Phase: pause.Phase,
			UsedPercent: pause.UsedPercent, Reason: pause.Summary(), RequestedAt: now, StoppedAt: &stopped}
		record.Phase = PhaseStopped
		record.ObservedThreadState = string(backlog.DispatchThreadStopped)
		state.Attempts["assignment-1"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	strict := workerproto.Codec{MaxBytes: 1 << 20}

	// Not asked: the excerpt carries neither field and the rc.66 shape
	// accepts the whole snapshot.
	snapshot, err := runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if j := snapshot.Assignments[0].Journal; j == nil || j.PauseReason != "" || j.ThreadState != "" || snapshot.QuotaObservations != nil {
		t.Fatalf("unasked snapshot carries gated fields: journal=%+v quota=%+v", snapshot.Assignments[0].Journal, snapshot.QuotaObservations)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var legacy rc66Snapshot
	if err := strict.Decode(bytes.NewReader(data), &legacy); err != nil {
		t.Fatalf("an rc.66 coordinator refuses the unasked snapshot: %v\n%s", err, data)
	}
	if len(legacy.Assignments) != 1 || legacy.Assignments[0].Control != domain.ControlPaused || legacy.Assignments[0].Journal.Phase != string(PhaseStopped) {
		t.Fatalf("legacy view = %+v", legacy.Assignments)
	}

	// Asked: the fields are present, and the rc.66 shape refuses them, which
	// is why the gate exists.
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true, QuotaObservationsWanted: true}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = runtime.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if j := snapshot.Assignments[0].Journal; j == nil || j.PauseReason != "codex/codex/primary at 97%" || j.ThreadState != "stopped" {
		t.Fatalf("asked snapshot journal = %+v", snapshot.Assignments[0].Journal)
	}
	data, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := strict.Decode(bytes.NewReader(data), &rc66Snapshot{}); err == nil {
		t.Fatal("the asked snapshot decodes into the rc.66 shape; the gate is not exercising anything")
	}
	// A coordinator that stops asking (a downgrade) stops receiving them.
	if err := runtime.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true}); err != nil {
		t.Fatal(err)
	}
	snapshot, err = runtime.Snapshot(context.Background())
	if err != nil || snapshot.Assignments[0].Journal.PauseReason != "" || snapshot.Assignments[0].Journal.ThreadState != "" {
		t.Fatalf("snapshot after the ask was withdrawn = %+v err=%v", snapshot.Assignments[0].Journal, err)
	}
}
