package main

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestBucketLineShowsStaleWhenTheWindowRolledOver(t *testing.T) {
	key := domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	resets := time.Date(2026, 9, 14, 17, 0, 0, 0, time.Local)
	observed := resets.Add(-90 * time.Minute)
	now := resets.Add(15 * time.Minute)
	b := domain.BucketState{Key: key, Phase: domain.PhaseWarned, UsedPercent: 86, ResetsAt: &resets, ObservedAt: observed}

	line := bucketLine(b, now)
	if phase := strings.Fields(line)[2]; phase != "stale" {
		t.Fatalf("a window that reset still presents %q as current: %s", phase, line)
	}
	// The observation age and the phase it belongs to are still reported.
	if !strings.Contains(line, "observed 1h45m0s ago") || !strings.Contains(line, "warned was the phase of the window that ended") {
		t.Fatalf("stale line lost its evidence: %s", line)
	}

	// A reading inside the current window is current fact again.
	b.ObservedAt = resets.Add(5 * time.Minute)
	line = bucketLine(b, now)
	if phase := strings.Fields(line)[2]; phase != "warned" || strings.Contains(line, "stale") {
		t.Fatalf("a bucket with a reading since the reset = %s", line)
	}

	// A bucket the provider gave no reset time for is never stale by the
	// clock: there is no window end to pass.
	b.ResetsAt, b.ObservedAt = nil, observed
	if line := bucketLine(b, now); strings.Contains(line, "stale") {
		t.Fatalf("bucket without a reset time called stale: %s", line)
	}
}
