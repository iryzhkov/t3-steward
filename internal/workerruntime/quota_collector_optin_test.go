package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"path/filepath"
	"testing"
	"time"
)

func TestCollectorProvenanceNegotiatesWithoutBreakingStrictOldPeer(t *testing.T) {
	now := runtimeTestNow
	reset := now.Add(time.Hour)
	p := &domain.QuotaProvenance{SourceID: "native", AcquisitionID: "one", OriginWorkerID: "collector", AcquiredAt: now.Add(-300 * time.Second)}
	guard := HostQuotaGuard{Buckets: runwayBuckets{{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", AccountID: "account", LimitID: "claude", Window: "five_hour"}, ObservedAt: p.AcquiredAt, Phase: domain.PhaseNormal, Healthy: true, UsedPercent: 20, ResetsAt: &reset, Provenance: p}}}
	for _, supported := range []bool{false, true} {
		t.Run(map[bool]string{false: "strict legacy peer", true: "negotiated native peer"}[supported], func(t *testing.T) {
			driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true}
			r := runningRuntime(t, driver, guard, &now)
			var caps []string
			for _, cap := range AdvertisedCapabilities(nil) {
				if cap != workerproto.CapabilityQuotaCollectorProvenance || supported {
					caps = append(caps, cap)
				}
			}
			previous := domain.WorkerSnapshot{WorkerID: r.config.WorkerID, WorkerEpoch: r.config.WorkerEpoch, Sequence: 1, Inventory: domain.WorkerInventory{Capabilities: caps}}
			request, snapshot, raw := exchangeSnapshot(t, r, previous)
			if request.QuotaCollectorProvenanceWanted != supported || len(snapshot.QuotaObservations) != 1 {
				t.Fatalf("negotiation %+v %+v", request, snapshot.QuotaObservations)
			}
			got := snapshot.QuotaObservations[0]
			if (got.Provenance != nil) != supported {
				t.Fatalf("provenance=%+v supported=%v", got.Provenance, supported)
			}
			if supported && (!got.Provenance.AcquiredAt.Equal(p.AcquiredAt) || !got.ObservedAt.Equal(p.AcquiredAt) || got.Provenance == p) {
				t.Fatal("source time restamped or shared mutable provenance")
			}
			if !supported {
				var envelope struct {
					QuotaObservations []json.RawMessage `json:"quotaObservations"`
				}
				if err := json.Unmarshal(raw, &envelope); err != nil {
					t.Fatal(err)
				}
				decoder := json.NewDecoder(bytes.NewReader(envelope.QuotaObservations[0]))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&rc115WorkerQuotaObservation{}); err != nil {
					t.Fatalf("old strict observation rejects unnegotiated metadata: %v", err)
				}
			}
			if err := r.ApplyParkedAssignments(workerproto.SnapshotRequest{ParkedReported: true, QuotaCollectorProvenanceWanted: true}); err != nil {
				t.Fatal(err)
			}
			snapshot, err := r.Snapshot(context.Background())
			if err != nil || len(snapshot.QuotaObservations) != 0 {
				t.Fatalf("flag alone emitted quota %v %+v", err, snapshot)
			}
		})
	}
	if p.OriginWorkerID != "collector" {
		t.Fatal("projection mutated stored metadata")
	}
}
