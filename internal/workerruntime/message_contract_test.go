package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQuotaMessageContractResumePermission(t *testing.T) {
	for _, why := range []string{"codex/codex/primary recovered", "probe: stale reading; one telemetry probe", "quota checks are disabled"} {
		t.Run(why, func(t *testing.T) {
			now := runtimeTestNow
			base := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true, observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped}}
			guard := &fakeQuotaGuard{pause: stoppedPause(), pauseNeeded: true}
			runtime := runningRuntime(t, base, guard, &now)
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			// Observe the stop after sending the non-blocking drain notice.
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			guard.pauseNeeded = false
			guard.resumeOK = true
			guard.resumeWhy = why
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if base.resumeCalls != 1 || len(base.resumeReasons) != 1 {
				t.Fatal("allowed control did not resume")
			}
			t.Log(base.resumeReasons[0])
			if strings.Contains(base.resumeReasons[0], "quota recovered:") || !strings.HasSuffix(base.resumeReasons[0], why) {
				t.Error("permission reason misrepresented")
			}
			control := &recordingT3{thread: &domain.Thread{ID: "thread-1"}}
			driver := &LocalDriver{T3: control}
			pkg := testPackage()
			pkg.Identity.ThreadID = "thread-1"
			if err := driver.Resume(context.Background(), pkg, domain.ThrottleCommand{Reason: base.resumeReasons[0]}); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(control.resumes[0], "Throttle recovery:") || !strings.Contains(control.resumes[0], "current instructions") {
				t.Error("resume prompt implies recovery", control.resumes[0])
			}
		})
	}
}
func TestQuotaMessageContractActualGuardReasons(t *testing.T) {
	for _, mode := range []string{"recovered", "probe", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			now := runtimeTestNow
			f := newRecoveryFixture(t, &now)
			f.stoppedBucket(t, 60, now.Add(-time.Minute))
			runtime, driver := f.pausedRuntime(t, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
			now = now.Add(15 * time.Minute)
			renewLease(t, runtime, now.Add(time.Hour))
			switch mode {
			case "recovered":
				st := f.bucket(t)
				decision := f.guard.engine(sevenDay, "codex", 0).Evaluate(domain.QuotaSnapshot{Key: sevenDay, UsedPercent: 10, ResetsAt: st.ResetsAt, ObservedAt: now, SourceEventID: "synthetic-fresh"}, st, now)
				if decision.State.Phase != domain.PhaseNormal {
					t.Fatal("fresh control did not rearm")
				}
				if err := f.store.SaveBucket(context.Background(), decision.State); err != nil {
					t.Fatal(err)
				}
				now = now.Add(f.cfg.Resume.ResetSettleDelay.D() + time.Second)
			case "disabled":
				disabled := false
				f.guard.Config.QuotaChecks = &disabled
			}
			runtime.config.Quota = f.guard
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if driver.resumeCalls != 1 {
				t.Fatal("guard did not allow", mode)
			}
			reason := driver.resumeReasons[0]
			t.Log(mode, reason)
			want := mode
			if mode == "disabled" {
				want = "quota checks are disabled"
			}
			if !strings.Contains(reason, want) || strings.Contains(reason, "quota recovered:") {
				t.Fatal("guard reason misrepresented", reason)
			}
			if mode == "probe" {
				st := f.bucket(t)
				if st.Phase != domain.PhaseStopped || st.ProbedAt == nil {
					t.Fatal("probe changed recovery policy")
				}
			}
		})
	}
}

func TestQuotaMessageContractCheckpointException(t *testing.T) {
	pkg := testPackage()
	pkg.Identity.ThreadID = "thread-1"
	control := &recordingT3{thread: &domain.Thread{ID: "thread-1"}}
	pub := &recordingPublisher{}
	driver := &LocalDriver{Config: LocalDriverConfig{RunsRoot: t.TempDir()}, T3: control, Publisher: pub}
	path := filepath.Join(driver.workspacePath(pkg), "workspace", ".t3", "checkpoint.md")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("checkpoint"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Checkpoint(context.Background(), pkg, domain.ThrottleCommand{Reason: "quota drain"}); err != nil {
		t.Fatal(err)
	}
	prompt := backlog.TaskCompletionSupplement(pkg.Outputs)
	for _, text := range []string{prompt, control.warns[0].Text} {
		if !strings.Contains(text, "owned quota pause") || !strings.Contains(text, "not a request for extra turns") {
			t.Error("missing runtime exception", text)
		}
	}
	for _, marker := range []string{"backlog status: done", "backlog status: continue"} {
		if !strings.Contains(control.warns[0].Text, marker) {
			t.Fatal("lost completion marker")
		}
	}
	if pub.checkpoints != 1 {
		t.Fatal("checkpoint not published")
	}
}
