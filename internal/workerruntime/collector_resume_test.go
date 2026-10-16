package workerruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func acceptedClaudeStop(t *testing.T, guard HostQuotaGuard, now *time.Time) (*Runtime, *fakeDriver, domain.ThrottleCommand) {
	t.Helper()
	runtime, driver := coveredClaudeRuntime(t, guard, now)
	stop := testThrottle(runtime, domain.ThrottleCommandHardStop, "quota-stop")
	stop.Route = journalRecord(t, runtime).Package.Package.Route
	stop.QuotaPoolID = stop.Route.QuotaPoolID
	acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{stop})
	if err != nil || len(acks) != 1 || !acks[0].Accepted {
		t.Fatalf("stop ack=%+v err=%v", acks, err)
	}
	stoppedAt := acks[0].AcknowledgedAt
	*now = now.Add(5 * time.Minute)
	renewLease(t, runtime, now.Add(time.Hour))
	command := testThrottle(runtime, domain.ThrottleCommandResume, "quota-resume")
	command.Route = stop.Route
	command.QuotaPoolID = stop.QuotaPoolID
	command.CreatedAt = now.Add(-30 * time.Second)
	acquired := now.Add(-3 * time.Minute)
	command.QuotaRecovery = &domain.QuotaRecoveryGrant{
		OriginProviderInstanceID: "claudeAgent", CoverageDigest: guard.CollectorCoverageDigest(command.Route), QuotaPoolID: command.QuotaPoolID, AccountID: "account-a",
		StopCommandID: stop.ID, StoppedAt: stoppedAt, ExpiresAt: acquired.Add(10 * time.Minute),
		Provenance: domain.QuotaProvenance{SourceID: "native", AcquisitionID: "joint-batch", OriginWorkerID: "normandy", AcquiredAt: acquired},
	}
	pool, _ := guard.Config.CollectorCoverage(command.Route)
	command.QuotaRecovery.EvidenceScope = domain.RecoveryEvidenceScope(pool)
	command.QuotaRecovery.EvidenceDigest = "test-qualified-source-authority"
	return runtime, driver, command
}

func TestCoveredClaudeCredentialFreeWorkerAcceptsOnlyBoundCurrentRecoveryGrant(t *testing.T) {
	now := runtimeTestNow
	guard := HostQuotaGuard{Config: coveredClaudeConfig(), Now: func() time.Time { return now }}
	runtime, driver, command := acceptedClaudeStop(t, guard, &now)
	acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{command})
	if err != nil || len(acks) != 1 || !acks[0].Accepted || driver.resumeCalls != 1 {
		t.Fatalf("valid grant acks=%+v resumes=%d err=%v", acks, driver.resumeCalls, err)
	}
	replay, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{command})
	if err != nil || len(replay) != 1 || !replay[0].Accepted || driver.resumeCalls != 1 {
		t.Fatalf("grant replay duplicated native resume: acks=%+v resumes=%d err=%v", replay, driver.resumeCalls, err)
	}
}

func TestCoveredClaudeRecoveryGrantRejectsDelayedAndChangedAuthority(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Runtime, *domain.ThrottleCommand, *time.Time)
	}{
		{"missing grant", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) { c.QuotaRecovery = nil }},
		{"wrong assignment epoch", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) { c.AssignmentEpoch++ }},
		{"unknown stop acknowledgement", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) { c.QuotaRecovery.StopCommandID = "unknown" }},
		{"wrong stop time", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) {
			c.QuotaRecovery.StoppedAt = c.QuotaRecovery.StoppedAt.Add(time.Second)
		}},
		{"acquisition before stop ack", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) {
			c.QuotaRecovery.Provenance.AcquiredAt = c.QuotaRecovery.StoppedAt
		}},
		{"command before acquisition", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) {
			c.CreatedAt = c.QuotaRecovery.Provenance.AcquiredAt.Add(-time.Second)
		}},
		{"future command", func(_ *Runtime, c *domain.ThrottleCommand, n *time.Time) { c.CreatedAt = n.Add(time.Second) }},
		{"expired grant", func(_ *Runtime, c *domain.ThrottleCommand, n *time.Time) { c.QuotaRecovery.ExpiresAt = *n }},
		{"restamped expiry", func(_ *Runtime, c *domain.ThrottleCommand, n *time.Time) {
			c.QuotaRecovery.ExpiresAt = n.Add(time.Hour)
		}},
		{"wrong account", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) { c.QuotaRecovery.AccountID = "other-account" }},
		{"unbound origin", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) {
			c.QuotaRecovery.Provenance.OriginWorkerID = "other-origin"
		}},
		{"wrong source", func(_ *Runtime, c *domain.ThrottleCommand, _ *time.Time) {
			c.QuotaRecovery.Provenance.SourceID = "old-source"
		}},
		{"coverage changed", func(r *Runtime, _ *domain.ThrottleCommand, _ *time.Time) {
			g := r.config.Quota.(HostQuotaGuard)
			p := g.Config.BacklogV2.QuotaPools["claude-main"]
			p.Collector.RequiredWindows = append(p.Collector.RequiredWindows, domain.QuotaCollectorWindow{Window: "weekly-model", Models: []string{"claude-fable-5-1"}})
			g.Config.BacklogV2.QuotaPools["claude-main"] = p
			r.config.Quota = g
		}},
		{"coverage removed", func(r *Runtime, _ *domain.ThrottleCommand, _ *time.Time) {
			g := r.config.Quota.(HostQuotaGuard)
			p := g.Config.BacklogV2.QuotaPools["claude-main"]
			p.Collector = nil
			g.Config.BacklogV2.QuotaPools["claude-main"] = p
			r.config.Quota = g
		}},
		{"resume policy tightened", func(r *Runtime, _ *domain.ThrottleCommand, _ *time.Time) {
			g := r.config.Quota.(HostQuotaGuard)
			g.Config.Resume.BelowPercent--
			r.config.Quota = g
		}},
		{"expired lease", func(r *Runtime, _ *domain.ThrottleCommand, n *time.Time) { renewLease(t, r, n.Add(-time.Second)) }},
		{"newer local pause", func(r *Runtime, c *domain.ThrottleCommand, n *time.Time) {
			at := n.Add(-time.Second)
			if err := r.journal.update(func(s *journalState) error {
				v := s.Attempts["assignment-1"]
				v.LocalThrottle = &LocalThrottleRequest{RequestedAt: at, StoppedAt: &at}
				s.Attempts["assignment-1"] = v
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			c.CreatedAt = n.Add(-2 * time.Second)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := runtimeTestNow
			guard := HostQuotaGuard{Config: coveredClaudeConfig(), Now: func() time.Time { return now }}
			runtime, driver, command := acceptedClaudeStop(t, guard, &now)
			tc.mutate(runtime, &command, &now)
			before := journalRecord(t, runtime)
			acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{command})
			if err != nil || len(acks) != 1 || acks[0].Accepted || driver.resumeCalls != 0 {
				t.Fatalf("invalid grant acks=%+v resumes=%d err=%v", acks, driver.resumeCalls, err)
			}
			after := journalRecord(t, runtime)
			if before.PendingContinuation != nil && after.PendingContinuation == nil {
				t.Fatal("rejected grant consumed owed paused-turn checkpoint")
			}
		})
	}
}

func TestCoveredClaudeCoordinatorGrantCannotEraseNewerLocalRestriction(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.guard.Config = coveredClaudeConfig()
	runtime, driver, command := acceptedClaudeStop(t, f.guard, &now)
	local := claudeAnonymousStop(now)
	// It arrived before command creation, but after the acquisition the grant cites.
	local.ObservedAt = command.QuotaRecovery.Provenance.AcquiredAt.Add(time.Second)
	local.UpdatedAt = local.ObservedAt
	if err := f.store.SaveBucket(context.Background(), local); err != nil {
		t.Fatal(err)
	}
	acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{command})
	if err != nil || len(acks) != 1 || acks[0].Accepted || driver.resumeCalls != 0 || !strings.Contains(acks[0].Error, "restrictive") {
		t.Fatalf("local veto acks=%+v resumes=%d err=%v", acks, driver.resumeCalls, err)
	}
}

func TestCoveredClaudeLocalPauseKeepsCoverageBoundaryAcrossRemovalAndRestart(t *testing.T) {
	for _, change := range []string{"removed", "policy changed"} {
		t.Run(change, func(t *testing.T) {
			now := runtimeTestNow
			f := newRecoveryFixture(t, &now)
			f.cfg = coveredClaudeConfig()
			f.guard.Config = f.cfg
			old := claudeAnonymousStop(now)
			if err := f.store.SaveBucket(context.Background(), old); err != nil {
				t.Fatal(err)
			}
			runtime, driver := coveredClaudeRuntime(t, f.guard, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			record := journalRecord(t, runtime)
			if record.LocalThrottle == nil || !record.LocalThrottle.CollectorCovered || record.LocalThrottle.CollectorCoverageDigest == "" || record.StopObservedSequence == 0 {
				t.Fatalf("pause lost durable collector boundary: %+v", record)
			}
			now = now.Add(10 * time.Minute)
			renewLease(t, runtime, now.Add(time.Hour))
			saveClaudeAcquisition(t, f, now)
			changed := f.guard
			if change == "removed" {
				p := changed.Config.BacklogV2.QuotaPools["claude-main"]
				p.Collector = nil
				changed.Config.BacklogV2.QuotaPools["claude-main"] = p
			} else {
				changed.Config.Resume.ResetSettleDelay += 1
			}
			runtime.config.Quota = changed
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if driver.resumeCalls != 0 {
				t.Fatal("changed coverage fell back to legacy resume")
			}
			if allowed, _, err := changed.ResumeAllowed(context.Background(), *record.LocalThrottle, record.Package.Package.Route); err != nil || allowed {
				t.Fatalf("guard allowed coverage removal: allowed=%v err=%v", allowed, err)
			}
			// Reopen the actual persisted journal, then repeat the same veto.
			statePath := runtime.journal.root
			reopened := reopenTestRuntime(t, statePath, driver)
			reopened.config.Now = func() time.Time { return now }
			reopened.config.Quota = changed
			driver.observations = []backlog.DispatchThreadState{backlog.DispatchThreadStopped}
			if err := reopened.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if driver.resumeCalls != 0 || !journalRecord(t, reopened).LocalThrottle.CollectorCovered {
				t.Fatal("restart erased the covered pause boundary")
			}
		})
	}
}
