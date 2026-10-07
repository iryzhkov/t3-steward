package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMergeQuotaObservationDrainMetadata(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	crossing, deadline := now.Add(5*time.Minute), now.Add(8*time.Minute)
	observation := WorkerQuotaObservation{Key: BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}, ObservedAt: now, RatePerMinute: 0.8, DrainPercent: 90, DrainsAt: &crossing, DrainDeadline: &deadline}
	merged := MergeQuotaObservations(nil, []WorkerSnapshot{{QuotaObservations: []WorkerQuotaObservation{observation}}})
	if len(merged) != 1 || merged[0].RatePerMinute != 0.8 || merged[0].DrainsAt == nil || !merged[0].DrainsAt.Equal(crossing) || merged[0].DrainDeadline == nil || !merged[0].DrainDeadline.Equal(deadline) || merged[0].AppliedThresholds == nil || merged[0].AppliedThresholds.DrainPercent != 90 {
		t.Fatalf("metadata lost: %+v", merged)
	}
	raw, err := json.Marshal(WorkerQuotaObservation{})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ratePerMinute", "drainPercent", "drainsAt", "drainDeadline"} {
		if _, exists := fields[name]; exists {
			t.Fatalf("optional %s emitted: %s", name, raw)
		}
	}
}
