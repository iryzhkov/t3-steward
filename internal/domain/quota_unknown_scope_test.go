package domain

import (
	"testing"
	"time"
)

func TestCollectorUnmappedRestrictionCannotBeResolvedByNewerTimestamp(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, states := collectorWindowFixture(now)
	unknown := states[0]
	unknown.Key.AccountID = ""
	unknown.Key.Window = "unmapped-account-window"
	unknown.Provenance = nil
	unknown.Phase = PhaseStopped
	unknown.Healthy = false
	unknown.ObservedAt = now.Add(-350 * time.Second)
	all := append(append([]BucketState{}, states...), unknown)
	if ReadQuotaWindows(pool, all, now, time.Minute).Fresh() {
		t.Fatal("later native timestamp invented equivalence to an unknown constraint")
	}
	unrelated := unknown
	unrelated.Key.ProviderInstanceID = "different-pool"
	if !ReadQuotaWindows(pool, append(states, unrelated), now, time.Minute).Fresh() {
		t.Fatal("unrelated pool restriction vetoed coverage")
	}
	unrelated = unknown
	unrelated.ModelSelector = "opus"
	if !ReadQuotaWindows(pool, append(states, unrelated), now, time.Minute).Fresh() {
		t.Fatal("unrelated model restriction vetoed coverage")
	}
	unknown.ModelSelector = "fable"
	if ReadQuotaWindows(pool, append(states, unknown), now, time.Minute).Fresh() {
		t.Fatal("relevant unmapped model restriction disappeared")
	}
}
