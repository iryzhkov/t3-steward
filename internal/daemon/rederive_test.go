package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// storeBucket writes a bucket state straight into the store, as a previous
// daemon run under other thresholds would have left it.
func (h *harness) storeBucket(st domain.BucketState) {
	h.t.Helper()
	if err := h.store.SaveBucket(context.Background(), st); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) loadBucket(key domain.BucketKey) domain.BucketState {
	h.t.Helper()
	st, err := h.store.LoadBucket(context.Background(), key)
	if err != nil {
		h.t.Fatal(err)
	}
	return st
}

// The homelab sequence of 2026-09-17 19:06 (F-1): the operator tightened the
// thresholds, the bucket stopped at 44%, the thresholds were restored to
// 85/90/95 and the daemon restarted. Nothing runs on a host whose threads are
// all paused attempts, so no reading arrives and the stored phase stayed
// stopped until the reset. At load the daemon re-derives the phase from the
// stored percentage under the loaded thresholds, lowers it, and records one
// rearm naming both threshold sets.
func TestLoadRederivesStoredPhaseUnderRelaxedThresholds(t *testing.T) {
	h := newHarness(t, nil)
	reset := h.clock.Add(3 * time.Hour)
	stopped := h.clock.Add(-time.Hour)
	h.storeBucket(domain.BucketState{
		Key: codexPrimary, Phase: domain.PhaseStopped, Epoch: domain.EpochFor(&reset), LimitName: "codex",
		UsedPercent: 44, ResetsAt: &reset, ObservedAt: stopped, StoppedAt: &stopped, ETAStrikes: 2,
		AppliedThresholds: &domain.ThresholdSet{WarnPercent: 40, DrainPercent: 42, StopPercent: 44},
	})
	h.d.Rederive(context.Background())
	st := h.loadBucket(codexPrimary)
	if st.Phase != domain.PhaseNormal {
		t.Fatalf("phase after load = %s, want normal", st.Phase)
	}
	if st.RecoveredAt == nil || !st.RecoveredAt.Equal(h.clock) {
		t.Fatalf("RecoveredAt = %v, want %v", st.RecoveredAt, h.clock)
	}
	if st.StoppedAt != nil || st.DrainDeadline != nil || st.ETAStrikes != 0 {
		t.Fatalf("stop bookkeeping not cleared: %+v", st)
	}
	if st.AppliedThresholds == nil || st.AppliedThresholds.String() != "85/90/95" {
		t.Fatalf("AppliedThresholds = %v, want 85/90/95", st.AppliedThresholds)
	}
	if !st.Healthy {
		t.Fatalf("bucket at 44%% under 85/90/95 is not healthy: %+v", st)
	}
	actions, _ := h.store.RecentActions(context.Background(), 10)
	if len(actions) != 1 || actions[0].Kind != domain.ActionRearm || actions[0].Bucket != codexPrimary.String() {
		t.Fatalf("actions = %+v, want one rearm for %s", actions, codexPrimary)
	}
	for _, want := range []string{"stopped", "normal", "44%", "40/42/44", "85/90/95"} {
		if !strings.Contains(actions[0].Detail, want) {
			t.Fatalf("rearm detail %q does not name %q", actions[0].Detail, want)
		}
	}
	// A second start changes nothing and records nothing.
	h.d.Rederive(context.Background())
	actions, _ = h.store.RecentActions(context.Background(), 10)
	if len(actions) != 1 {
		t.Fatalf("second load recorded again: %+v", actions)
	}
}

// A stored phase the loaded thresholds still produce is left alone: draining
// at 92% under 85/90/95 stays draining, with no action.
func TestLoadKeepsAPhaseTheThresholdsStillProduce(t *testing.T) {
	h := newHarness(t, nil)
	reset := h.clock.Add(3 * time.Hour)
	deadline := h.clock.Add(-time.Minute)
	h.storeBucket(domain.BucketState{
		Key: codexPrimary, Phase: domain.PhaseDraining, Epoch: domain.EpochFor(&reset), LimitName: "codex",
		UsedPercent: 92, ResetsAt: &reset, ObservedAt: h.clock.Add(-time.Hour), DrainDeadline: &deadline,
	})
	h.d.Rederive(context.Background())
	st := h.loadBucket(codexPrimary)
	if st.Phase != domain.PhaseDraining || st.DrainDeadline == nil || st.RecoveredAt != nil {
		t.Fatalf("draining bucket changed at load: %+v", st)
	}
	if actions, _ := h.store.RecentActions(context.Background(), 10); len(actions) != 0 {
		t.Fatalf("actions = %+v, want none", actions)
	}
}

// Load-time re-derivation never raises: a bucket at warned with 96% stays
// warned until a reading says otherwise.
func TestLoadNeverRaisesAStoredPhase(t *testing.T) {
	h := newHarness(t, nil)
	reset := h.clock.Add(3 * time.Hour)
	h.storeBucket(domain.BucketState{
		Key: codexPrimary, Phase: domain.PhaseWarned, Epoch: domain.EpochFor(&reset), LimitName: "codex",
		UsedPercent: 96, ResetsAt: &reset, ObservedAt: h.clock.Add(-time.Hour),
	})
	h.d.Rederive(context.Background())
	st := h.loadBucket(codexPrimary)
	if st.Phase != domain.PhaseWarned {
		t.Fatalf("phase after load = %s, want warned", st.Phase)
	}
	if actions, _ := h.store.RecentActions(context.Background(), 10); len(actions) != 0 {
		t.Fatalf("actions = %+v, want none", actions)
	}
	if len(h.fake.stops) != 0 || len(h.fake.warnings) != 0 {
		t.Fatalf("load touched threads: stops=%v warnings=%v", h.fake.stops, h.fake.warnings)
	}
}

// A load-time lowering keeps the epoch, so it keeps the epoch's thread
// notices: the user-resumed record and the warn notice survive a restart that
// lowers a projection stop at 87% to warned under unchanged thresholds.
// Otherwise the next raise to stopped would stop the user-resumed thread and
// warn it a second time in the same window. A real reset still clears them
// (TestUserResumedExemptionEndsWithTheEpoch).
func TestLoadKeepsThreadNoticesWhenTheEpochIsUnchanged(t *testing.T) {
	h := newHarness(t, nil)
	stopped := h.clock.Add(-time.Hour)
	st := h.stoppedBucket(codexPrimary, 87, stopped)
	for _, kind := range []domain.ActionKind{domain.NoticeUserResumed, domain.ActionWarn} {
		if _, err := h.store.MarkThreadNotice(context.Background(), "user", codexPrimary, st.Epoch, kind, stopped.Add(30*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	h.d.Rederive(context.Background())
	after := h.loadBucket(codexPrimary)
	if after.Phase != domain.PhaseWarned || after.Epoch != st.Epoch {
		t.Fatalf("after load: phase %s epoch %q, want warned in epoch %q", after.Phase, after.Epoch, st.Epoch)
	}
	if actions, _ := h.store.RecentActions(context.Background(), 10); len(actions) != 1 || actions[0].Kind != domain.ActionRearm {
		t.Fatalf("actions = %+v, want one rearm", actions)
	}
	for _, kind := range []domain.ActionKind{domain.NoticeUserResumed, domain.ActionWarn} {
		notices, err := h.store.ThreadNotices(context.Background(), codexPrimary, st.Epoch, kind)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := notices["user"]; !ok {
			t.Fatalf("the %s notice was dropped by the load-time re-derivation", kind)
		}
	}
}

// A bucket of a past epoch is not re-derived: its phase belongs to a window
// that is over, and a fresh reading rearms or re-stops it.
func TestLoadSkipsExpiredEpochs(t *testing.T) {
	h := newHarness(t, nil)
	reset := h.clock.Add(-time.Hour)
	stopped := h.clock.Add(-2 * time.Hour)
	h.storeBucket(domain.BucketState{
		Key: codexPrimary, Phase: domain.PhaseStopped, Epoch: domain.EpochFor(&reset), LimitName: "codex",
		UsedPercent: 44, ResetsAt: &reset, ObservedAt: stopped, StoppedAt: &stopped,
	})
	h.d.Rederive(context.Background())
	if st := h.loadBucket(codexPrimary); st.Phase != domain.PhaseStopped {
		t.Fatalf("expired bucket re-derived: %+v", st)
	}
}

// The engine records the thresholds it evaluated under, so a later load can
// name the old set against the new one.
func TestEvaluateRecordsTheAppliedThresholds(t *testing.T) {
	h := newHarness(t, nil)
	h.snap(codexPrimary, 10, h.clock.Add(5*time.Hour), "1")
	st := h.loadBucket(codexPrimary)
	if st.AppliedThresholds == nil || st.AppliedThresholds.String() != "85/90/95" {
		t.Fatalf("AppliedThresholds = %v, want 85/90/95", st.AppliedThresholds)
	}
}
