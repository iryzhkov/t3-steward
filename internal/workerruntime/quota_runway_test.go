package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type runwayBuckets []domain.BucketState

func (b runwayBuckets) ListBuckets(context.Context) ([]domain.BucketState, error) { return b, nil }

// rc115WorkerQuotaObservation is the observation shape an rc.115 coordinator
// decodes. Its snapshot decoder rejects unknown fields, so a worker that asks
// for no new capability must never emit anything outside this set.
type rc115WorkerQuotaObservation struct {
	Key           domain.BucketKey `json:"key"`
	Phase         domain.Phase     `json:"phase"`
	UsedPercent   float64          `json:"usedPercent"`
	Healthy       bool             `json:"healthy"`
	ObservedAt    time.Time        `json:"observedAt"`
	ResetsAt      *time.Time       `json:"resetsAt,omitempty"`
	Epoch         string           `json:"epoch,omitempty"`
	LimitName     string           `json:"limitName,omitempty"`
	ModelSelector string           `json:"modelSelector,omitempty"`
}

// An older coordinator that asks for quota observations must still accept
// every snapshot from this worker, including the incident's warned bucket
// with a positive burn rate and a draining bucket with a drain deadline.
func TestHostObservationsDecodeOnOlderCoordinator(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	deadline := now.Add(time.Minute)
	key := domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	cfg := config.Default()
	cfg.Policy.DrainPercent = 90
	guard := HostQuotaGuard{Config: cfg, Buckets: runwayBuckets{
		{Key: key, Phase: domain.PhaseWarned, UsedPercent: 86, RatePerMinute: 0.8, ObservedAt: now, AppliedThresholds: &domain.ThresholdSet{WarnPercent: 85, DrainPercent: 90, StopPercent: 95}},
		{Key: domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "five_hour"}, Phase: domain.PhaseDraining, UsedPercent: 91, RatePerMinute: 0.5, ObservedAt: now, DrainDeadline: &deadline},
	}}
	observations, err := guard.Observations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 2 {
		t.Fatalf("observations = %+v", observations)
	}
	for _, observation := range observations {
		raw, err := json.Marshal(observation)
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		var old rc115WorkerQuotaObservation
		if err := decoder.Decode(&old); err != nil {
			t.Fatalf("rc.115 coordinator rejects worker observation %s: %v", raw, err)
		}
		if old.Key != observation.Key || old.UsedPercent != observation.UsedPercent || !old.ObservedAt.Equal(now) {
			t.Fatalf("decoded %+v from %s", old, raw)
		}
	}
}
