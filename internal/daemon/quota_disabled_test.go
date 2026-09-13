package daemon

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/config"
	"testing"
	"time"
)

func TestDisabledQuotaRetainsReadingsWithoutActions(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { enabled := false; c.QuotaChecks = &enabled; c.Policy.DryRun = false })
	h.fake.add("active", "codex", "gpt", true)
	reset := h.clock.Add(time.Hour)
	h.snap(codexPrimary, 99, reset, "disabled-1")
	h.advance(2 * time.Minute)
	h.poll()
	if len(h.fake.warnings)+len(h.fake.stops)+len(h.fake.resumes) != 0 {
		t.Fatalf("disabled quota acted: warnings=%v stops=%v resumes=%v", h.fake.warnings, h.fake.stops, h.fake.resumes)
	}
	buckets, err := h.store.ListBuckets(context.Background())
	if err != nil || len(buckets) != 1 || buckets[0].UsedPercent != 99 {
		t.Fatalf("readings=%v err=%v", buckets, err)
	}
}
