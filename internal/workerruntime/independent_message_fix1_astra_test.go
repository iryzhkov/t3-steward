package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestIndependentMessageFix1ResetProbeReason(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	st := f.stoppedBucket(t, 97, now.Add(-time.Minute))
	runtime, driver := f.pausedRuntime(t, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
	now = st.ResetsAt.Add(f.cfg.Resume.ProbeAfterReset.D() + f.cfg.Resume.ResetSettleDelay.D() + time.Second)
	renewLease(t, runtime, now.Add(time.Hour))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := f.bucket(t)
	if driver.resumeCalls != 1 || len(driver.resumeReasons) != 1 {
		t.Fatalf("reset probe did not resume: %d %v", driver.resumeCalls, driver.resumeReasons)
	}
	if after.Phase != domain.PhaseStopped || after.RecoveredAt != nil || !after.ObservedAt.Equal(st.ObservedAt) {
		t.Fatalf("fixture unexpectedly recovered: %+v", after)
	}
	control := &recordingT3{thread: &domain.Thread{ID: "thread-1"}}
	local := &LocalDriver{T3: control}
	pkg := testPackage()
	pkg.Identity.ThreadID = "thread-1"
	if err := local.Resume(context.Background(), pkg, domain.ThrottleCommand{Reason: driver.resumeReasons[0]}); err != nil {
		t.Fatal(err)
	}
	t.Logf("phase=%s used=%.0f recoveredAt=%v observedUnchanged=%v resumeCalls=%d reason=%q prompt=%q", after.Phase, after.UsedPercent, after.RecoveredAt, after.ObservedAt.Equal(st.ObservedAt), driver.resumeCalls, driver.resumeReasons[0], control.resumes[0])
	if strings.Contains(driver.resumeReasons[0], "recovered") || !strings.Contains(driver.resumeReasons[0], "probe") {
		t.Error("elapsed-reset telemetry probe falsely described as confirmed recovery")
	}
}
