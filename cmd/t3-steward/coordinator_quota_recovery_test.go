package main

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func collectorGrantFixture(now time.Time) (config.Config, domain.QuotaPool, domain.ThrottleAttemptRecord, []domain.BucketState) {
	cfg := config.Default()
	cfg.BacklogV2.Workers = map[string]config.V2Worker{
		"normandy":      {Epoch: "worker-1", Providers: map[string]config.V2Provider{"claudeExec": {Models: []string{"claude-fable-5-1"}, QuotaPool: "claude-main"}}},
		"source-worker": {Epoch: "source-1", Providers: map[string]config.V2Provider{"claudeSource": {Models: []string{"claude-fable-5-1", "claude-opus-5-5"}, QuotaPool: "claude-main"}}},
	}
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"claude-main": {Provider: "claudeAgent", AccountID: "account-a", MaxConcurrent: 2, ObservationMaxAge: config.Duration(10 * time.Minute),
		Collector: &config.V2QuotaCollectorCoverage{SourceID: "native", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "source-worker", ProviderInstanceID: "claudeSource"}},
			RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}, {Window: "weekly-opus", Models: []string{"claude-opus-5-5"}}}}}}
	route := domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "claudeExec", Model: "claude-fable-5-1", QuotaPoolID: "claude-main"}
	pool, _ := cfg.CollectorCoverage(route)
	stopped := now.Add(-5 * time.Minute)
	oldReset := now.Add(-4 * time.Minute)
	record := domain.ThrottleAttemptRecord{DirectiveID: "quota-stop", AttemptID: "attempt", Revision: 1, Delivery: domain.ThrottleDeliveryAcknowledged,
		Control: domain.ControlPaused, Result: domain.ThrottleResultStopped, AcknowledgedAt: &stopped, UpdatedAt: stopped,
		Command: domain.ThrottleCommand{ID: "stop-command", DirectiveID: "quota-stop", AttemptID: "attempt", AssignmentID: "assignment", AssignmentEpoch: 1,
			WorkerID: "normandy", ThreadID: "thread", WorkspacePath: "/owned/workspace", Route: route, Kind: domain.ThrottleCommandHardStop,
			QuotaPoolID: "claude-main", CreatedAt: stopped,
			BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: domain.BucketKey{ProviderInstanceID: "claudeExec", LimitID: "legacy", Window: "seven_day"}, Epoch: domain.EpochFor(&oldReset)}}}}
	acquired := now.Add(-3 * time.Minute)
	reset := now.Add(time.Hour)
	var states []domain.BucketState
	for _, window := range []string{"five_hour", "seven_day", "weekly-opus"} {
		key := domain.BucketKey{ProviderInstanceID: "claudeSource", AccountID: "account-a", LimitID: "native", Window: window}
		states = append(states, domain.BucketState{Key: key, Phase: domain.PhaseNormal, Healthy: true, UsedPercent: 20, ObservedAt: acquired, UpdatedAt: acquired,
			ResetsAt: &reset, Epoch: domain.EpochFor(&reset),
			Provenance: &domain.QuotaProvenance{SourceID: "native", AcquisitionID: "joint-read", OriginWorkerID: "source-worker", AcquiredAt: acquired}})
	}
	return cfg, pool, record, states
}

func issuedCollectorResume(cfg config.Config, pool domain.QuotaPool, record domain.ThrottleAttemptRecord, states []domain.BucketState, now time.Time) []domain.ThrottleCommand {
	folded, valid := backlog.FoldQuotaRecoveryStates(pool, states)
	authorize := func(p domain.QuotaPool, r domain.ThrottleAttemptRecord, at time.Time) *domain.QuotaRecoveryGrant {
		if !valid || r.AcknowledgedAt == nil {
			return nil
		}
		return daemon.IssueCollectorRecoveryGrant(cfg, p, r.Command.Route, r.Command.BucketEpochs, *r.AcknowledgedAt, r.Command.ID, 0, folded, at)
	}
	_, commands, err := backlog.PlanThrottleResumes([]domain.ThrottleAttemptRecord{record}, []domain.QuotaAdmissionRecord{{QuotaPoolID: pool.ID, Admission: domain.AdmissionOpen}}, []domain.QuotaPool{pool}, now, authorize)
	if err != nil {
		panic(err)
	}
	return commands
}

func TestCollectorGrantProducerRequiresWholePoolVerifiedBatch(t *testing.T) {
	now := time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC)
	cfg, pool, record, states := collectorGrantFixture(now)
	commands := issuedCollectorResume(cfg, pool, record, states, now)
	if len(commands) != 1 || commands[0].QuotaRecovery == nil || commands[0].QuotaRecovery.OriginProviderInstanceID != "claudeSource" || commands[0].Route.ProviderInstanceID != "claudeExec" {
		t.Fatalf("shared pool producer/consumer binding: %+v", commands)
	}
	cases := []struct {
		name   string
		mutate func(*domain.ThrottleAttemptRecord, []domain.BucketState) []domain.BucketState
	}{
		{"unavailable", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState { return nil }},
		{"partial required model window", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState { return s[:2] }},
		{"missing base window", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState { return s[1:] }},
		{"stale", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			for i := range s {
				s[i].ObservedAt = now.Add(-11 * time.Minute)
				s[i].Provenance.AcquiredAt = s[i].ObservedAt
			}
			return s
		}},
		{"missing source tag", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			s[0].Provenance = nil
			return s
		}},
		{"mixed batch", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			s[0].Provenance.AcquisitionID = "other"
			return s
		}},
		{"unconfigured producer instance", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			for i := range s {
				s[i].Key.ProviderInstanceID = "unconfigured"
			}
			return s
		}},
		{"expired source envelope", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			at := now
			for i := range s {
				s[i].OriginValidUntil = &at
			}
			return s
		}},
		{"above recovery threshold", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			s[2].UsedPercent = cfg.Resume.BelowPercent
			return s
		}},
		{"unconfirmed reset", func(r *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			at := now.Add(time.Hour)
			r.Command.BucketEpochs[0].Epoch = domain.EpochFor(&at)
			return s
		}},
		{"pre-stop acquisition", func(r *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			at := now.Add(-time.Minute)
			r.AcknowledgedAt = &at
			return s
		}},
		{"unsettled acquisition", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			for i := range s {
				s[i].ObservedAt = now.Add(-time.Minute)
				s[i].Provenance.AcquiredAt = s[i].ObservedAt
			}
			return s
		}},
		{"newer anonymous restriction", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			at := now.Add(-time.Minute)
			reset := now.Add(time.Hour)
			return append(s, domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeExec", LimitID: "legacy", Window: "seven_day"}, Phase: domain.PhaseStopped, UsedPercent: 100, ObservedAt: at, ResetsAt: &reset, Epoch: domain.EpochFor(&reset)})
		}},
		{"anonymous history barrier", func(_ *domain.ThrottleAttemptRecord, s []domain.BucketState) []domain.BucketState {
			return append(s, domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeSource", LimitID: "legacy", Window: "seven_day"}, Phase: domain.PhaseStopped, UsedPercent: 100, ObservedAt: now.Add(-time.Hour)})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, p, r, s := collectorGrantFixture(now)
			s = tc.mutate(&r, s)
			if got := issuedCollectorResume(c, p, r, s, now); len(got) != 0 {
				t.Fatalf("invalid source authority emitted resume: %+v", got)
			}
		})
	}
	for _, conflict := range []string{"100 percent", "stopped", "invalid percent", "reset conflict", "different acquisition"} {
		t.Run("equal time "+conflict, func(t *testing.T) {
			c, p, r, s := collectorGrantFixture(now)
			high := s[0]
			copyP := *high.Provenance
			high.Provenance = &copyP
			switch conflict {
			case "100 percent":
				high.UsedPercent = 100
			case "stopped":
				high.Phase = domain.PhaseStopped
				high.Healthy = false
			case "invalid percent":
				high.UsedPercent = -1
			case "reset conflict":
				at := now.Add(2 * time.Hour)
				high.ResetsAt = &at
				high.Epoch = domain.EpochFor(&at)
			case "different acquisition":
				high.Provenance.AcquisitionID = "other"
			}
			s = append(s, high)
			for range 2 {
				if got := issuedCollectorResume(c, p, r, s, now); len(got) != 0 {
					t.Fatalf("equal-time competitor lost for order: %+v", got)
				}
				slices.Reverse(s)
			}
		})
	}
}

func TestCoordinatorGrantCallbackFencesActualCustodyAndStop(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC)
	cfg, pool, record, states := collectorGrantFixture(now)
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.SetClock(func() time.Time { return now })
	epoch, err := store.CoordinatorEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assignment := domain.Assignment{ID: "assignment", AttemptID: "attempt", WorkerID: "normandy", WorkerEpoch: "worker-1", Route: record.Command.Route, State: domain.AssignmentClaimed, Epoch: 1, ThreadID: "thread", LeaseToken: "lease", LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now}
	attempt := domain.Attempt{ID: "attempt", WorkflowRunID: "run", TaskID: "task", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlPaused, Revision: 1, AssignmentID: assignment.ID, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow", Version: 1, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task"}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "workflow", GraphRevision: 1, Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now}},
		Tasks:        []domain.Task{{ID: "task", WorkflowID: "workflow", Name: "task", Class: domain.TaskClassRequired}},
		Attempts:     []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if err := store.SaveBucket(ctx, state); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := domain.WorkerSnapshot{WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: epoch, Sequence: 10, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		Assignments: []domain.WorkerAssignmentObservation{{AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, State: domain.AssignmentClaimed, Control: domain.ControlPaused, ThreadID: "thread", WorkspacePath: record.Command.WorkspacePath, ObservedAt: now}}}
	producer := newCoordinatorQuotaRecoveryGrant(cfg, store)
	grant, err := producer(ctx, pool, record, snapshot, now)
	if err != nil || grant == nil {
		t.Fatalf("current authority not issued: %+v %v", grant, err)
	}
	localSnapshot := snapshot
	localSnapshot.Assignments = append([]domain.WorkerAssignmentObservation(nil), snapshot.Assignments...)
	localSnapshot.Assignments[0].QuotaStop = &domain.QuotaStopObservation{Bucket: record.Command.BucketEpochs[0].Bucket, Epoch: record.Command.BucketEpochs[0].Epoch, StoppedAt: *record.AcknowledgedAt, ObservedSequence: 9}
	authorize := func(p domain.QuotaPool, r domain.ThrottleAttemptRecord, at time.Time) *domain.QuotaRecoveryGrant {
		grant, err := producer(ctx, p, r, localSnapshot, at)
		if err != nil {
			t.Fatal(err)
		}
		return grant
	}
	bindings := []backlog.ThrottleAttemptBinding{{Attempt: attempt, Assignment: assignment, WorkspacePath: record.Command.WorkspacePath}}
	admissions := []domain.QuotaAdmissionRecord{{QuotaPoolID: pool.ID, Admission: domain.AdmissionOpen}}
	transitions, commands := backlog.PlanLocalCollectorResumes(localSnapshot, bindings, nil, admissions, []domain.QuotaPool{pool}, authorize, now)
	if len(commands) != 1 || commands[0].QuotaRecovery.StopCommandID != "" || commands[0].QuotaRecovery.StopObservedSequence != 9 {
		t.Fatalf("local stop authority not issued: %+v", commands)
	}
	localSnapshot.Inventory.ID = localSnapshot.WorkerID
	if err := store.SaveWorkerSnapshot(ctx, localSnapshot); err != nil {
		t.Fatal(err)
	}
	for _, race := range []string{"stop observation changed", "lease expired", "attempt cancelled", "central source batch changed"} {
		t.Run("commit race "+race, func(t *testing.T) {
			changedSnapshot := localSnapshot
			changedSnapshot.Assignments = append([]domain.WorkerAssignmentObservation(nil), localSnapshot.Assignments...)
			changedStop := *localSnapshot.Assignments[0].QuotaStop
			changedSnapshot.Assignments[0].QuotaStop = &changedStop
			changedAssignment := assignment
			changedAttempt := attempt
			switch race {
			case "stop observation changed":
				changedStop.ObservedSequence++
			case "lease expired":
				changedAssignment.LeaseExpiresAt = now
			case "attempt cancelled":
				changedAttempt.Progress = domain.ProgressCancelled
			case "central source batch changed":
				high := states[0]
				high.UsedPercent = 100
				high.Phase = domain.PhaseStopped
				high.Healthy = false
				high.ObservedAt = now
				high.Provenance = nil
				if err := store.SaveBucket(ctx, high); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(time.Nanosecond)
			changedSnapshot.ObservedAt = now
			changedSnapshot.Sequence += 1
			if err := store.SaveWorkerSnapshot(ctx, changedSnapshot); err != nil {
				t.Fatal(err)
			}
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{changedAssignment}, Attempts: []domain.Attempt{changedAttempt}}); err != nil {
				t.Fatal(err)
			}
			if err := store.CommitThrottleAttemptTransitions(ctx, transitions); err == nil {
				t.Fatal("changed custody committed a resume intent")
			}
			pending, err := store.LoadThrottleAttemptRecordsFor(ctx, snapshot.WorkerID)
			if err != nil || len(pending) != 0 {
				t.Fatalf("refused transaction leaked intent: %+v %v", pending, err)
			}
			now = now.Add(time.Nanosecond)
			localSnapshot.ObservedAt = now
			if err := store.SaveBucket(ctx, states[0]); err != nil {
				t.Fatal(err)
			}
			localSnapshot.Sequence = changedSnapshot.Sequence + 1
			if err := store.SaveWorkerSnapshot(ctx, localSnapshot); err != nil {
				t.Fatal(err)
			}
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}, Attempts: []domain.Attempt{attempt}}); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := store.CommitThrottleAttemptTransitions(ctx, transitions); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.LoadThrottleAttemptRecordsFor(ctx, snapshot.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, replay := backlog.PlanLocalCollectorResumes(localSnapshot, bindings, persisted, admissions, []domain.QuotaPool{pool}, authorize, now); len(replay) != 0 {
		t.Fatal("local stop observation generated a duplicate resume intent")
	}
	if _, denied := backlog.PlanLocalCollectorResumes(localSnapshot, bindings, nil, admissions, []domain.QuotaPool{pool}, nil, now); len(denied) != 0 {
		t.Fatal("local observation alone minted recovery authority")
	}
	for _, change := range []string{"expired snapshot", "old coordinator epoch", "changed workspace", "changed assignment epoch", "missing stop acknowledgement"} {
		t.Run(change, func(t *testing.T) {
			s := snapshot
			s.Assignments = append([]domain.WorkerAssignmentObservation(nil), snapshot.Assignments...)
			r := record
			switch change {
			case "expired snapshot":
				s.ValidUntil = now
			case "old coordinator epoch":
				s.CoordinatorEpoch--
			case "changed workspace":
				s.Assignments[0].WorkspacePath = "/other"
			case "changed assignment epoch":
				r.Command.AssignmentEpoch++
			case "missing stop acknowledgement":
				r.AcknowledgedAt = nil
			}
			if g, e := producer(ctx, pool, r, s, now); e != nil || g != nil {
				t.Fatalf("stale custody emitted grant: %+v %v", g, e)
			}
		})
	}
}
