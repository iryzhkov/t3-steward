package policy

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestNativeProvenancePersistsUntilNewOrdinaryReading(t *testing.T) {
	engine := New(DefaultThresholds())
	reset := base.Add(time.Hour)
	native := snap(20, base, &reset, "native")
	native.Provenance = &domain.QuotaProvenance{SourceID: "source", AcquisitionID: "batch", OriginWorkerID: "worker", AcquiredAt: base}
	first := engine.Evaluate(native, domain.BucketState{}, base).State
	if first.Provenance == nil || first.Provenance == native.Provenance {
		t.Fatal("native metadata absent or mutable alias")
	}
	stale := snap(10, base.Add(-time.Second), &reset, "old-log")
	if got := engine.Evaluate(stale, first, base).State; got.Provenance == nil || got.Provenance.AcquisitionID != "batch" {
		t.Fatal("older log erased newest acquisition")
	}
	newer := snap(21, base.Add(time.Second), &reset, "new-log")
	got := engine.Evaluate(newer, first, base.Add(time.Second)).State
	if got.Provenance != nil || got.OriginValidUntil != nil {
		t.Fatal("ordinary newer log inherited source authority")
	}
}
