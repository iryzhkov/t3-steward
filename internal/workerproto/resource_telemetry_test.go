package workerproto

import (
	"bytes"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestResourceTelemetryOptionalWireFields(t *testing.T) {
	data, err := json.Marshal(Observations{})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("telemetry")) {
		t.Fatalf("old response gained field: %s", data)
	}
	var older struct {
		Snapshot domain.WorkerSnapshot `json:"snapshot"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&older); err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(SnapshotRequest{})
	if strings.Contains(string(request), "reportResourceTelemetry") {
		t.Fatalf("old request gained field: %s", request)
	}
	cpu := 4
	data, err = json.Marshal(Observations{Telemetry: &domain.WorkerTelemetry{ObservedAt: time.Now(), CPUCount: &cpu}})
	if err != nil {
		t.Fatal(err)
	}
	var got Observations
	if err := (Codec{MaxBytes: 4096}).Decode(bytes.NewReader(data), &got); err != nil {
		t.Fatal(err)
	}
	if got.Telemetry == nil || got.Telemetry.CPUCount == nil || *got.Telemetry.CPUCount != 4 || got.Telemetry.MemoryAvailableMB != nil {
		t.Fatalf("decoded telemetry = %+v", got)
	}
}
