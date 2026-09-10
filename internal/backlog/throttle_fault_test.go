package backlog

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestM5DrainCompletionAndHardStopAcknowledgementRaces(t *testing.T) {
	attempt := turnOutcomeAttempt("attempt")
	stopping := turnOutcomeThrottle("attempt", "directive-stop", domain.ThrottleCommandHardStop, domain.ControlDraining)
	done := turnOutcome("done", "attempt", domain.TurnOutcomeDone)
	done.VerificationPassed = true

	completed, err := PlanTurnOutcomes(
		[]domain.Attempt{attempt},
		[]domain.ThrottleAttemptRecord{stopping},
		[]domain.TurnOutcome{done},
		turnOutcomeTestTime,
	)
	if err != nil || len(completed) != 1 || completed[0].Throttle == nil {
		t.Fatalf("completion-first race = %#v, err = %v", completed, err)
	}
	if completed[0].Attempt.Progress != domain.ProgressSucceeded ||
		completed[0].Throttle.Record.Delivery != domain.ThrottleDeliveryCancelled {
		t.Fatalf("completion did not win hard-stop race: %#v", completed[0])
	}

	lateStop := domain.ThrottleAcknowledgement{
		CommandID: stopping.Command.ID, AttemptID: stopping.AttemptID,
		Accepted: true, Result: domain.ThrottleResultStopped,
		AcknowledgedAt: turnOutcomeTestTime.Add(time.Second),
	}
	late, err := PlanThrottleAcknowledgements(
		[]domain.ThrottleAttemptRecord{completed[0].Throttle.Record},
		[]domain.ThrottleAcknowledgement{lateStop},
		turnOutcomeTestTime.Add(time.Second),
	)
	if err != nil || len(late) != 0 {
		t.Fatalf("late hard-stop acknowledgement = %#v, err = %v", late, err)
	}

	stopped, err := PlanThrottleAcknowledgements(
		[]domain.ThrottleAttemptRecord{stopping},
		[]domain.ThrottleAcknowledgement{lateStop},
		turnOutcomeTestTime.Add(time.Second),
	)
	if err != nil || len(stopped) != 1 ||
		stopped[0].Record.Control != domain.ControlPausedUncheckpointed {
		t.Fatalf("stop-first race = %#v, err = %v", stopped, err)
	}
	completedAfterStop, err := PlanTurnOutcomes(
		[]domain.Attempt{attempt},
		[]domain.ThrottleAttemptRecord{stopped[0].Record},
		[]domain.TurnOutcome{done},
		turnOutcomeTestTime.Add(2*time.Second),
	)
	if err != nil || len(completedAfterStop) != 1 ||
		completedAfterStop[0].Attempt.Progress != domain.ProgressSucceeded ||
		completedAfterStop[0].Attempt.Control != domain.ControlStopped {
		t.Fatalf("explicit completion after stop did not win: %#v, err = %v", completedAfterStop, err)
	}
}

func TestM5MultipleBucketsAndPoolsRemainIsolated(t *testing.T) {
	aWeekly := admissionBucket("codex", "weekly")
	aShort := admissionBucket("codex", "five-hour")
	bWeekly := admissionBucket("claude", "weekly")
	states := []domain.BucketState{
		admissionBucketState(aWeekly, domain.PhaseNormal, true),
		admissionBucketState(aShort, domain.PhaseDraining, false),
		admissionBucketState(bWeekly, domain.PhaseStopped, false),
	}
	snapshots, err := DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{
		Now: admissionDerivationTime, MaxObservationAge: time.Hour,
		Pools: []domain.QuotaPool{
			{ID: "pool-b", Buckets: []domain.BucketKey{bWeekly}},
			{ID: "pool-a", Buckets: []domain.BucketKey{aWeekly, aShort}},
		},
		BucketStates: states,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 ||
		snapshots[0].QuotaPoolID != "pool-a" || snapshots[0].Admission != domain.AdmissionDraining ||
		snapshots[1].QuotaPoolID != "pool-b" || snapshots[1].Admission != domain.AdmissionClosed {
		t.Fatalf("multi-pool admissions = %#v", snapshots)
	}
	transitions, err := PlanQuotaAdmissionTransitions(nil, snapshots, admissionTransitionTime)
	if err != nil || len(transitions) != 2 {
		t.Fatalf("multi-pool transitions = %#v, err = %v", transitions, err)
	}
	directives := []domain.ThrottleDirective{*transitions[1].Directive, *transitions[0].Directive}
	bindings := []ThrottleAttemptBinding{
		throttleBindingFor("attempt-b", "worker-b", "pool-b", domain.ControlRunning),
		throttleBindingFor("attempt-a", "worker-a", "pool-a", domain.ControlRunning),
	}
	_, commands, err := PlanThrottleDeliveries(nil, directives, bindings, throttleDeliveryTime)
	if err != nil || len(commands) != 2 {
		t.Fatalf("multi-pool deliveries = %#v, err = %v", commands, err)
	}
	got := map[string]domain.ThrottleCommandKind{}
	for _, command := range commands {
		got[command.AttemptID] = command.Kind
	}
	want := map[string]domain.ThrottleCommandKind{
		"attempt-a": domain.ThrottleCommandDrain,
		"attempt-b": domain.ThrottleCommandHardStop,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pool-scoped commands = %#v, want %#v", got, want)
	}
}

func TestM5RecoveryProbesAdmitOneSlotAndReplayInFlightResume(t *testing.T) {
	fixture := quotaRecoveryFixture()
	records := []domain.ThrottleAttemptRecord{
		recoveryThrottle(fixture.ThrottleRecords, "surplus"),
		recoveryThrottle(fixture.ThrottleRecords, "paused"),
		recoveryThrottle(fixture.ThrottleRecords, "forced"),
	}
	admissions := []domain.QuotaAdmissionRecord{{
		QuotaPoolID: "shared", Revision: 3, Admission: domain.AdmissionRecovering,
	}}
	probePool := []domain.QuotaPool{{ID: "shared", MaxConcurrent: 2, ActiveAssignments: 1}}

	first, commands, err := PlanThrottleResumes(records, admissions, probePool, throttleDeliveryTime.Add(time.Hour))
	if err != nil || len(first) != 1 || len(commands) != 1 || commands[0].AttemptID != "forced" {
		t.Fatalf("first recovery probe = %#v %#v, err = %v", first, commands, err)
	}
	for i := range records {
		if records[i].AttemptID == "forced" {
			records[i] = first[0].Record
		}
	}
	fullPool := []domain.QuotaPool{{ID: "shared", MaxConcurrent: 2, ActiveAssignments: 2}}
	replay, replayCommands, err := PlanThrottleResumes(records, admissions, fullPool, throttleDeliveryTime.Add(2*time.Hour))
	if err != nil || len(replay) != 0 || len(replayCommands) != 1 ||
		replayCommands[0].ID != commands[0].ID {
		t.Fatalf("in-flight resume replay = %#v %#v, err = %v", replay, replayCommands, err)
	}

	resumedAck := domain.ThrottleAcknowledgement{
		CommandID: commands[0].ID, AttemptID: "forced", Accepted: true,
		Result: domain.ThrottleResultResumed, AcknowledgedAt: throttleDeliveryTime.Add(3 * time.Hour),
	}
	resumed, err := PlanThrottleAcknowledgements(records, []domain.ThrottleAcknowledgement{resumedAck}, throttleDeliveryTime.Add(3*time.Hour))
	if err != nil || len(resumed) != 1 {
		t.Fatalf("resume acknowledgement = %#v, err = %v", resumed, err)
	}
	for i := range records {
		if records[i].AttemptID == "forced" {
			records[i] = resumed[0].Record
		}
	}
	next, nextCommands, err := PlanThrottleResumes(records, admissions, probePool, throttleDeliveryTime.Add(4*time.Hour))
	if err != nil || len(next) != 1 || len(nextCommands) != 1 || nextCommands[0].AttemptID != "paused" {
		t.Fatalf("next recovery probe = %#v %#v, err = %v", next, nextCommands, err)
	}
}

func TestM5InteractiveRecoveryContentionIsDeterministic(t *testing.T) {
	input := quotaRecoveryPolicyFixture(t, "paused", "forced")
	input.InteractiveResumeAttemptIDs = []string{"paused", "forced"}
	first, err := PlanQuotaRecovery(input)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(input.InteractiveResumeAttemptIDs)
	slices.Reverse(input.PlanningState.ResumeReservations)
	slices.Reverse(input.ThrottleRecords)
	second, err := PlanQuotaRecovery(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || len(first.ResumeCommands) != 1 ||
		first.ResumeCommands[0].AttemptID != "forced" {
		t.Fatalf("interactive contention = %#v then %#v", first, second)
	}
}

func TestM5LateStaleEpochCannotSupersedeCurrentThrottleIntent(t *testing.T) {
	attempt := turnOutcomeAttempt("attempt")
	stale := turnOutcomeThrottle("attempt", "directive-old", domain.ThrottleCommandDrain, domain.ControlPaused)
	stale.Delivery = domain.ThrottleDeliveryAcknowledged
	stale.Command.CreatedAt = turnOutcomeTestTime.Add(-2 * time.Hour)
	stale.UpdatedAt = turnOutcomeTestTime.Add(time.Hour)

	current := turnOutcomeThrottle("attempt", "directive-current", domain.ThrottleCommandHardStop, domain.ControlDraining)
	current.Command.CreatedAt = turnOutcomeTestTime.Add(-time.Hour)
	current.UpdatedAt = turnOutcomeTestTime

	outcome := turnOutcome("continue", "attempt", domain.TurnOutcomeContinue)
	transitions, err := PlanTurnOutcomes(
		[]domain.Attempt{attempt},
		[]domain.ThrottleAttemptRecord{stale, current},
		[]domain.TurnOutcome{outcome},
		turnOutcomeTestTime.Add(2*time.Hour),
	)
	if err != nil || len(transitions) != 1 || transitions[0].Throttle == nil {
		t.Fatalf("stale-epoch outcome = %#v, err = %v", transitions, err)
	}
	if transitions[0].Throttle.Record.DirectiveID != current.DirectiveID ||
		transitions[0].Attempt.Control != domain.ControlDraining {
		t.Fatalf("stale epoch superseded current intent: %#v", transitions[0])
	}

	resumes, commands, err := PlanThrottleResumes(
		[]domain.ThrottleAttemptRecord{current, stale},
		[]domain.QuotaAdmissionRecord{{QuotaPoolID: "pool", Admission: domain.AdmissionRecovering}},
		[]domain.QuotaPool{{ID: "pool", MaxConcurrent: 1}},
		turnOutcomeTestTime.Add(3*time.Hour),
	)
	if err != nil || len(resumes) != 0 || len(commands) != 0 {
		t.Fatalf("stale paused epoch produced resume: %#v %#v, err = %v", resumes, commands, err)
	}
}
