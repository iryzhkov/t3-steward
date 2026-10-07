package workerruntime

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"testing"
)

func TestResourceTelemetryCountsPreparingAndResumingAttempts(t *testing.T) {
	assignments := []domain.WorkerAssignmentObservation{
		{Control: domain.ControlPreparing}, {Control: domain.ControlRunning}, {Control: domain.ControlResuming},
		{Control: domain.ControlPaused}, {Control: domain.ControlStopped},
	}
	if got := activeAttempts(assignments); got != 3 {
		t.Fatalf("active attempts = %d, want 3", got)
	}
}

func TestResourceTelemetrySnapshotGate(t *testing.T) {
	runtime := newTestRuntime(t, t.TempDir(), &fakeDriver{})
	calls := 0
	runtime.config.CollectResourceTelemetry = func(workspace, temp string, running int) domain.WorkerTelemetry {
		calls++
		if workspace != "/work" || temp == "" || running != 0 {
			t.Fatalf("collector args %q %q %d", workspace, temp, running)
		}
		return domain.WorkerTelemetry{ObservedAt: runtimeTestNow}
	}
	runtime.config.WorkspaceRoot = "/work"
	e := Exchange{Runtime: runtime}
	for _, asked := range []bool{false, true} {
		payload, _ := json.Marshal(workerproto.SnapshotRequest{ReportResourceTelemetry: asked})
		_, value, err := e.handle(context.Background(), workerproto.Envelope{Type: workerproto.MessageSnapshot, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		observations := value.(workerproto.Observations)
		if (observations.Telemetry != nil) != asked {
			t.Fatalf("asked %v observations %+v", asked, observations)
		}
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
	if !containsString(AdvertisedCapabilities(nil), workerproto.CapabilityResourceTelemetry) {
		t.Fatal("missing capability")
	}
}
