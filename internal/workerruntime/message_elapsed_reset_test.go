package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestQuotaMessageElapsedResetLogAndPrompt(t *testing.T) {
	for _, mode := range []string{"elapsed-reset", "recovered", "stage2", "disabled", "denied"} {
		t.Run(mode, func(t *testing.T) {
			now := runtimeTestNow
			f := newRecoveryFixture(t, &now)
			used := 97.0
			if mode == "stage2" {
				used = 60
			}
			original := f.stoppedBucket(t, used, now.Add(-time.Minute))
			runtime, driver := f.pausedRuntime(t, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
			var logs bytes.Buffer
			runtime.log = slog.New(slog.NewJSONHandler(&logs, nil))
			switch mode {
			case "elapsed-reset":
				now = original.ResetsAt.Add(f.cfg.Resume.ProbeAfterReset.D() + f.cfg.Resume.ResetSettleDelay.D() + time.Second)
			case "recovered":
				now = now.Add(15 * time.Minute)
				decision := f.guard.engine(sevenDay, "codex", 0).Evaluate(domain.QuotaSnapshot{Key: sevenDay, UsedPercent: 10, ResetsAt: original.ResetsAt, ObservedAt: now, SourceEventID: "synthetic-message-recovery"}, original, now)
				if decision.State.Phase != domain.PhaseNormal || decision.State.RecoveredAt == nil {
					t.Fatal("fresh reading did not confirm recovery")
				}
				if err := f.store.SaveBucket(context.Background(), decision.State); err != nil {
					t.Fatal(err)
				}
				now = now.Add(f.cfg.Resume.ResetSettleDelay.D() + time.Second)
			case "stage2":
				now = now.Add(15 * time.Minute)
			case "disabled":
				disabled := false
				f.guard.Config.QuotaChecks = &disabled
			}
			runtime.config.Quota = f.guard
			renewLease(t, runtime, now.Add(time.Hour))
			before := f.bucket(t)
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			after := f.bucket(t)
			if mode == "denied" {
				if driver.resumeCalls != 0 || !reflect.DeepEqual(before, after) || strings.Contains(logs.String(), "owned attempt resumed") {
					t.Fatal("denied control resumed or changed bucket")
				}
				return
			}
			if driver.resumeCalls != 1 || len(driver.resumeReasons) != 1 {
				t.Fatal("permission did not resume exactly once")
			}
			record := journalRecord(t, runtime)
			if record.Phase != PhaseRunning || record.LocalThrottle != nil || record.LastLocalThrottle == nil {
				t.Fatal("resume transition changed")
			}
			if mode == "stage2" {
				if after.ProbedAt == nil || after.Phase != before.Phase || after.RecoveredAt != nil || !after.ObservedAt.Equal(before.ObservedAt) {
					t.Fatal("stage2 policy changed")
				}
			} else if !reflect.DeepEqual(before, after) {
				t.Fatal("reason selection mutated quota state")
			}
			control := &recordingT3{thread: &domain.Thread{ID: "thread-1"}}
			local := &LocalDriver{T3: control}
			pkg := testPackage()
			pkg.Identity.ThreadID = "thread-1"
			reason := driver.resumeReasons[0]
			if err := local.Resume(context.Background(), pkg, domain.ThrottleCommand{Reason: reason}); err != nil {
				t.Fatal(err)
			}
			if len(control.resumes) != 1 || !strings.Contains(control.resumes[0], reason) {
				t.Fatal("actual prompt lost reason")
			}
			log := logs.String()
			if !strings.Contains(log, "quota resume permitted; owned attempt resumed") {
				t.Fatal("missing actual runtime resume log")
			}
			want := "probe:"
			if mode == "recovered" {
				want = "recovered"
			}
			if mode == "disabled" {
				want = "quota checks are disabled"
			}
			for _, text := range []string{reason, control.resumes[0], log} {
				if !strings.Contains(text, want) {
					t.Fatalf("missing truthful %s reason: %s", mode, text)
				}
				if mode != "recovered" && strings.Contains(text, "recovered") {
					t.Fatalf("unconfirmed permission claims recovery: %s", text)
				}
				if mode == "elapsed-reset" && (!strings.Contains(text, "recovery unconfirmed") || !strings.Contains(text, "reset deadline elapsed")) {
					t.Fatal("elapsed-reset distinction lost", text)
				}
			}
			t.Logf("mode=%s one resume; actual log=%s actual prompt=%q", mode, log, control.resumes[0])
		})
	}
}

type messageFailingBuckets struct{}

func (messageFailingBuckets) ListBuckets(context.Context) ([]domain.BucketState, error) {
	return nil, errors.New("synthetic bucket read failure")
}

func TestQuotaMessageResumeUnavailableState(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	pause := LocalThrottleRequest{Bucket: sevenDay, RequestedAt: now}
	for _, missing := range []bool{true, false} {
		guard := f.guard
		if missing {
			guard.Buckets = nil
		} else {
			guard.Buckets = messageFailingBuckets{}
		}
		allowed, reason, err := guard.ResumeAllowed(context.Background(), pause, domain.ProviderRoute{})
		if allowed || (missing && (reason != "no watchdog state on this host" || err != nil)) || (!missing && err == nil) {
			t.Fatal("unavailable state permission changed", allowed, reason, err)
		}
	}
}
