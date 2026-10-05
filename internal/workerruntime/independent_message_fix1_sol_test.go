package workerruntime

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestIndependentMessageFix1ElapsedResetProbe(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.stoppedBucket(t, 97, now.Add(-time.Minute))
	runtime, driver := f.pausedRuntime(t, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
	// Keep the original stopped observation and phase. Advance only the clock.
	now = now.Add(3*time.Hour + f.cfg.Resume.ProbeAfterReset.D() + f.cfg.Resume.ResetSettleDelay.D() + time.Minute)
	renewLease(t, runtime, now.Add(time.Hour))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := f.bucket(t)
	if driver.resumeCalls != 1 || len(driver.resumeReasons) != 1 {
		t.Fatalf("no allowed reset probe: resumes=%d", driver.resumeCalls)
	}
	reason := driver.resumeReasons[0]
	fmt.Printf("actual phase=%s recoveredAt=%v observedAt=%s reason=%q\n", st.Phase, st.RecoveredAt, st.ObservedAt.Format(time.RFC3339), reason)
	control := &recordingT3{thread: &domain.Thread{ID: "thread-1"}}
	local := &LocalDriver{T3: control}
	pkg := testPackage()
	pkg.Identity.ThreadID = "thread-1"
	if err := local.Resume(context.Background(), pkg, domain.ThrottleCommand{Reason: reason}); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("actual prompt=%q\n", control.resumes[0])
	if st.Phase != domain.PhaseStopped || st.RecoveredAt != nil {
		t.Fatal("probe unexpectedly confirmed recovery")
	}
	if strings.Contains(control.resumes[0], "recovered") {
		t.Fatal("elapsed-reset telemetry probe falsely reported recovered")
	}
}
