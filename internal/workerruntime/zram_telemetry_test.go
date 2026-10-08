package workerruntime

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The zram split reaches only a coordinator that asks for it, because an
// older coordinator decodes observations strictly.
func TestZramSwapTelemetryIsGated(t *testing.T) {
	runtime := newTestRuntime(t, t.TempDir(), &fakeDriver{})
	zram := int64(4300)
	runtime.config.CollectResourceTelemetry = func(string, string, int) domain.WorkerTelemetry {
		return domain.WorkerTelemetry{ObservedAt: runtimeTestNow, ZramSwapUsedMB: &zram}
	}
	e := Exchange{Runtime: runtime}
	for _, wanted := range []bool{false, true} {
		payload, _ := json.Marshal(workerproto.SnapshotRequest{ReportResourceTelemetry: true, ZramSwapWanted: wanted})
		_, value, err := e.handle(context.Background(), workerproto.Envelope{Type: workerproto.MessageSnapshot, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		observations := value.(workerproto.Observations)
		if observations.Telemetry == nil || (observations.Telemetry.ZramSwapUsedMB != nil) != wanted {
			t.Fatalf("wanted %v telemetry %+v", wanted, observations.Telemetry)
		}
	}
	if !containsString(AdvertisedCapabilities(nil), workerproto.CapabilityZramSwapTelemetry) {
		t.Fatal("missing zram capability")
	}
}
