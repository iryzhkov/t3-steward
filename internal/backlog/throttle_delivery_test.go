package backlog

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var throttleDeliveryTime = time.Date(2026, time.September, 10, 23, 0, 0, 0, time.UTC)

type throttleDeliveryStoreFake struct {
	records []domain.ThrottleAttemptRecord
	commits [][]domain.ThrottleAttemptTransition
}

func (s *throttleDeliveryStoreFake) LoadThrottleAttemptRecords(context.Context) ([]domain.ThrottleAttemptRecord, error) {
	return append([]domain.ThrottleAttemptRecord(nil), s.records...), nil
}

func (s *throttleDeliveryStoreFake) CommitThrottleAttemptTransitions(_ context.Context, transitions []domain.ThrottleAttemptTransition) error {
	s.commits = append(s.commits, append([]domain.ThrottleAttemptTransition(nil), transitions...))
	for _, transition := range transitions {
		found := false
		for index := range s.records {
			if s.records[index].DirectiveID == transition.Record.DirectiveID &&
				s.records[index].AttemptID == transition.Record.AttemptID {
				if s.records[index].Revision != transition.ExpectedRevision {
					return errors.New("stale fake revision")
				}
				s.records[index] = cloneThrottleAttemptRecord(transition.Record)
				found = true
			}
		}
		if !found {
			if transition.ExpectedRevision != 0 {
				return errors.New("missing fake revision")
			}
			s.records = append(s.records, cloneThrottleAttemptRecord(transition.Record))
		}
	}
	return nil
}

type throttleTransportFunc func(context.Context, string, []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error)

func (f throttleTransportFunc) DeliverThrottleCommands(
	ctx context.Context,
	workerID string,
	commands []domain.ThrottleCommand,
) ([]domain.ThrottleAcknowledgement, error) {
	return f(ctx, workerID, commands)
}

func TestReconcileThrottleDeliveriesPersistsBeforePartialAcknowledgement(t *testing.T) {
	store := &throttleDeliveryStoreFake{}
	bindings := []ThrottleAttemptBinding{
		throttleBinding("attempt-b", "worker-a", domain.ControlRunning),
		throttleBinding("attempt-a", "worker-a", domain.ControlRunning),
	}
	directive := throttleDirectiveFixture(domain.ThrottleDrain)
	transport := throttleTransportFunc(func(_ context.Context, workerID string, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
		if workerID != "worker-a" || len(commands) != 2 {
			t.Fatalf("delivery = %q %#v", workerID, commands)
		}
		if len(store.records) != 2 {
			t.Fatalf("commands became eligible before intent commit: %#v", store.records)
		}
		for _, record := range store.records {
			if record.Delivery != domain.ThrottleDeliveryPending || record.Control != domain.ControlDraining {
				t.Fatalf("persisted intent = %#v", record)
			}
		}
		return []domain.ThrottleAcknowledgement{{
			CommandID: commands[0].ID, AttemptID: commands[0].AttemptID,
			Accepted: true, Result: domain.ThrottleResultCheckpointed,
			Checkpoint: checkpointFixture(), AcknowledgedAt: throttleDeliveryTime.Add(time.Second),
		}}, errors.New("worker response interrupted")
	})

	report, err := ReconcileThrottleDeliveries(
		context.Background(), store, transport,
		[]domain.ThrottleDirective{directive}, bindings, throttleDeliveryTime,
	)
	if err == nil || len(report.Commands) != 2 || len(report.Acknowledgements) != 1 {
		t.Fatalf("report = %#v, err = %v", report, err)
	}
	slices.SortFunc(store.records, func(a, b domain.ThrottleAttemptRecord) int {
		if a.AttemptID < b.AttemptID {
			return -1
		}
		if a.AttemptID > b.AttemptID {
			return 1
		}
		return 0
	})
	if store.records[0].Delivery != domain.ThrottleDeliveryAcknowledged ||
		store.records[0].Revision != 2 ||
		store.records[1].Delivery != domain.ThrottleDeliveryPending ||
		store.records[1].Revision != 1 {
		t.Fatalf("partial acknowledgement records = %#v", store.records)
	}
}

func TestReconcilePendingThrottleCommandsReplaysAdminPauseAfterRestart(t *testing.T) {
	binding := throttleBinding("attempt-a", "worker-a", domain.ControlRunning)
	planned, err := PlanAdminPauseDelivery(
		"admin-pause", "operator pause", false, binding, throttleDeliveryTime,
	)
	if err != nil {
		t.Fatal(err)
	}
	store := &throttleDeliveryStoreFake{records: []domain.ThrottleAttemptRecord{planned.Record}}
	deliveries := 0
	transport := throttleTransportFunc(func(_ context.Context, workerID string, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
		deliveries++
		if workerID != binding.Assignment.WorkerID || len(commands) != 1 ||
			commands[0].AssignmentID != binding.Assignment.ID ||
			commands[0].ThreadID != binding.Assignment.ThreadID ||
			commands[0].WorkspacePath != binding.WorkspacePath ||
			!reflect.DeepEqual(commands[0].Route, binding.Assignment.Route) {
			t.Fatalf("replayed delivery changed identity: %q %#v", workerID, commands)
		}
		return []domain.ThrottleAcknowledgement{{
			CommandID: commands[0].ID, AttemptID: binding.Attempt.ID, Accepted: true,
			Result: domain.ThrottleResultCheckpointed, Checkpoint: checkpointFixture(),
			AcknowledgedAt: throttleDeliveryTime.Add(time.Minute),
		}}, nil
	})
	report, err := ReconcilePendingThrottleCommands(
		context.Background(), store, transport, throttleDeliveryTime.Add(time.Minute),
	)
	if err != nil || len(report.Commands) != 1 || len(report.Acknowledgements) != 1 ||
		store.records[0].Delivery != domain.ThrottleDeliveryAcknowledged ||
		store.records[0].Control != domain.ControlPaused {
		t.Fatalf("pending replay = %#v, records = %#v, err = %v", report, store.records, err)
	}
	report, err = ReconcilePendingThrottleCommands(
		context.Background(), store, transport, throttleDeliveryTime.Add(2*time.Minute),
	)
	if err != nil || len(report.Commands) != 0 || deliveries != 1 {
		t.Fatalf("settled replay = %#v, deliveries = %d, err = %v", report, deliveries, err)
	}
}

func TestReconcileThrottleDeliveriesReplaysLostDeliveryWithStableCommand(t *testing.T) {
	store := &throttleDeliveryStoreFake{}
	binding := throttleBinding("attempt-a", "worker-a", domain.ControlRunning)
	directive := throttleDirectiveFixture(domain.ThrottleWarn)
	var delivered []string
	fail := true
	transport := throttleTransportFunc(func(_ context.Context, _ string, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
		delivered = append(delivered, commands[0].ID)
		if fail {
			return nil, errors.New("connection lost")
		}
		return []domain.ThrottleAcknowledgement{{
			CommandID: commands[0].ID, AttemptID: commands[0].AttemptID,
			Accepted: true, Result: domain.ThrottleResultWarned,
			AcknowledgedAt: throttleDeliveryTime.Add(time.Minute),
		}}, nil
	})

	first, err := ReconcileThrottleDeliveries(context.Background(), store, transport,
		[]domain.ThrottleDirective{directive}, []ThrottleAttemptBinding{binding}, throttleDeliveryTime)
	if err == nil || len(first.Commands) != 1 || store.records[0].Delivery != domain.ThrottleDeliveryPending {
		t.Fatalf("lost delivery report = %#v records = %#v err = %v", first, store.records, err)
	}
	fail = false
	second, err := ReconcileThrottleDeliveries(context.Background(), store, transport,
		[]domain.ThrottleDirective{directive}, []ThrottleAttemptBinding{binding}, throttleDeliveryTime.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Commands) != 1 || len(delivered) != 2 || delivered[0] != delivered[1] {
		t.Fatalf("replayed command IDs = %#v, report = %#v", delivered, second)
	}
	if store.records[0].Delivery != domain.ThrottleDeliveryAcknowledged ||
		store.records[0].Control != domain.ControlRunning {
		t.Fatalf("warn acknowledgement = %#v", store.records[0])
	}

	third, err := ReconcileThrottleDeliveries(context.Background(), store, transport,
		[]domain.ThrottleDirective{directive}, []ThrottleAttemptBinding{binding}, throttleDeliveryTime.Add(2*time.Minute))
	if err != nil || len(third.Commands) != 0 || len(delivered) != 2 {
		t.Fatalf("acknowledged replay = %#v delivered = %#v err = %v", third, delivered, err)
	}
}

func TestPlanThrottleDeliveriesMultipleWorkersAndInputOrder(t *testing.T) {
	directiveZ := throttleDirectiveFixtureFor("directive-z", "pool-z", domain.ThrottleDrain)
	directiveZ.BucketEpochs = append(directiveZ.BucketEpochs, domain.QuotaBucketEpoch{
		Bucket: domain.BucketKey{ProviderInstanceID: "codex", LimitID: "requests", Window: domain.WindowSecondary},
		Epoch:  "epoch-2",
	})
	directives := []domain.ThrottleDirective{
		directiveZ,
		throttleDirectiveFixtureFor("directive-a", "pool-a", domain.ThrottleWarn),
	}
	bindings := []ThrottleAttemptBinding{
		throttleBindingFor("attempt-z", "worker-z", "pool-z", domain.ControlRunning),
		throttleBindingFor("attempt-a", "worker-a", "pool-a", domain.ControlPreparing),
		throttleBindingFor("paused", "worker-a", "pool-a", domain.ControlPaused),
	}
	forwardTransitions, forwardCommands, err := PlanThrottleDeliveries(nil, directives, bindings, throttleDeliveryTime)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(directives)
	slices.Reverse(bindings)
	reverseTransitions, reverseCommands, err := PlanThrottleDeliveries(nil, directives, bindings, throttleDeliveryTime)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(forwardTransitions, reverseTransitions) || !reflect.DeepEqual(forwardCommands, reverseCommands) {
		t.Fatalf("reordered plan mismatch:\nforward %#v %#v\nreverse %#v %#v",
			forwardTransitions, forwardCommands, reverseTransitions, reverseCommands)
	}
	if len(forwardCommands) != 2 ||
		forwardCommands[0].WorkerID != "worker-a" ||
		forwardCommands[1].WorkerID != "worker-z" ||
		len(forwardCommands[1].BucketEpochs) != 2 {
		t.Fatalf("commands = %#v", forwardCommands)
	}
	if forwardTransitions[0].Record.Control != domain.ControlPreparing ||
		forwardTransitions[1].Record.Control != domain.ControlDraining {
		t.Fatalf("controls = %#v", forwardTransitions)
	}
	for _, command := range forwardCommands {
		binding := bindings[0]
		for _, candidate := range bindings {
			if candidate.Attempt.ID == command.AttemptID {
				binding = candidate
			}
		}
		if command.ThreadID != binding.Assignment.ThreadID ||
			command.WorkspacePath != binding.WorkspacePath ||
			!reflect.DeepEqual(command.Route, binding.Assignment.Route) {
			t.Fatalf("command changed execution identity: %#v binding %#v", command, binding)
		}
	}
}

func TestPlanThrottleAcknowledgementsDuplicateSettledResponseIsNoOp(t *testing.T) {
	binding := throttleBinding("attempt-a", "worker-a", domain.ControlRunning)
	directive := throttleDirectiveFixture(domain.ThrottleWarn)
	transitions, _, err := PlanThrottleDeliveries(nil, []domain.ThrottleDirective{directive},
		[]ThrottleAttemptBinding{binding}, throttleDeliveryTime)
	if err != nil {
		t.Fatal(err)
	}
	record := transitions[0].Record
	ack := domain.ThrottleAcknowledgement{
		CommandID: record.Command.ID, AttemptID: record.AttemptID,
		Accepted: true, Result: domain.ThrottleResultWarned,
		AcknowledgedAt: throttleDeliveryTime.Add(time.Second),
	}
	settled, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{record},
		[]domain.ThrottleAcknowledgement{ack, ack}, throttleDeliveryTime.Add(time.Second))
	if err != nil || len(settled) != 1 {
		t.Fatalf("settle = %#v, err = %v", settled, err)
	}
	replay, err := PlanThrottleAcknowledgements(
		[]domain.ThrottleAttemptRecord{settled[0].Record},
		[]domain.ThrottleAcknowledgement{ack},
		throttleDeliveryTime.Add(2*time.Second),
	)
	if err != nil || len(replay) != 0 {
		t.Fatalf("duplicate acknowledgement = %#v, err = %v", replay, err)
	}
}

func TestThrottleCheckpointDeadlineHardStopAndResumeLifecycle(t *testing.T) {
	binding := throttleBinding("attempt-a", "worker-a", domain.ControlRunning)
	directive := throttleDirectiveFixture(domain.ThrottleDrain)
	planned, commands, err := PlanThrottleDeliveries(nil, []domain.ThrottleDirective{directive},
		[]ThrottleAttemptBinding{binding}, throttleDeliveryTime)
	if err != nil || len(planned) != 1 || commands[0].Kind != domain.ThrottleCommandDrain {
		t.Fatalf("drain plan = %#v %#v, err = %v", planned, commands, err)
	}
	drain := planned[0].Record
	failed, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{drain},
		[]domain.ThrottleAcknowledgement{{
			CommandID: drain.Command.ID, AttemptID: drain.AttemptID, Accepted: true,
			Result: domain.ThrottleResultCheckpointFailed, Error: "checkpoint upload failed",
			AcknowledgedAt: throttleDeliveryTime.Add(time.Minute),
		}}, throttleDeliveryTime.Add(time.Minute))
	if err != nil || len(failed) != 1 || failed[0].Record.Control != domain.ControlDraining {
		t.Fatalf("checkpoint failure = %#v, err = %v", failed, err)
	}

	before, beforeCommands, err := PlanThrottleDeadlineExpirations(
		[]domain.ThrottleAttemptRecord{failed[0].Record}, throttleDeliveryTime.Add(4*time.Minute))
	if err != nil || len(before) != 0 || len(beforeCommands) != 0 {
		t.Fatalf("early hard stop = %#v %#v, err = %v", before, beforeCommands, err)
	}
	expired, stopCommands, err := PlanThrottleDeadlineExpirations(
		[]domain.ThrottleAttemptRecord{failed[0].Record}, throttleDeliveryTime.Add(5*time.Minute))
	if err != nil || len(expired) != 1 || stopCommands[0].Kind != domain.ThrottleCommandHardStop {
		t.Fatalf("hard stop = %#v %#v, err = %v", expired, stopCommands, err)
	}
	stopping := expired[0].Record
	if stopping.Command.ID == drain.Command.ID || len(stopping.PriorCommandIDs) != 1 ||
		stopping.PriorCommandIDs[0] != drain.Command.ID {
		t.Fatalf("hard-stop command history = %#v", stopping)
	}
	late, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{stopping},
		[]domain.ThrottleAcknowledgement{{
			CommandID: drain.Command.ID, AttemptID: drain.AttemptID, Accepted: true,
			Result: domain.ThrottleResultCheckpointed, Checkpoint: checkpointFixture(),
			AcknowledgedAt: throttleDeliveryTime.Add(6 * time.Minute),
		}}, throttleDeliveryTime.Add(6*time.Minute))
	if err != nil || len(late) != 0 {
		t.Fatalf("late drain acknowledgement = %#v, err = %v", late, err)
	}
	stopped, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{stopping},
		[]domain.ThrottleAcknowledgement{{
			CommandID: stopping.Command.ID, AttemptID: stopping.AttemptID, Accepted: true,
			Result: domain.ThrottleResultStopped, AcknowledgedAt: throttleDeliveryTime.Add(6 * time.Minute),
		}}, throttleDeliveryTime.Add(6*time.Minute))
	if err != nil || stopped[0].Record.Control != domain.ControlPausedUncheckpointed ||
		stopped[0].Record.Control.HoldsProviderSlot() {
		t.Fatalf("stopped record = %#v, err = %v", stopped, err)
	}

	closed := domain.QuotaAdmissionRecord{QuotaPoolID: "pool", Revision: 3, Admission: domain.AdmissionClosed}
	noResume, noCommands, err := PlanThrottleResumes(
		[]domain.ThrottleAttemptRecord{stopped[0].Record}, []domain.QuotaAdmissionRecord{closed}, []domain.QuotaPool{{ID: "pool", MaxConcurrent: 1}},
		throttleDeliveryTime.Add(7*time.Minute))
	if err != nil || len(noResume) != 0 || len(noCommands) != 0 {
		t.Fatalf("closed resume = %#v %#v, err = %v", noResume, noCommands, err)
	}
	recovering := closed
	recovering.Admission = domain.AdmissionRecovering
	recovering.Reason = "probe admitted"
	resumes, resumeCommands, err := PlanThrottleResumes(
		[]domain.ThrottleAttemptRecord{stopped[0].Record}, []domain.QuotaAdmissionRecord{recovering}, []domain.QuotaPool{{ID: "pool", MaxConcurrent: 1}},
		throttleDeliveryTime.Add(8*time.Minute))
	if err != nil || len(resumes) != 1 || resumeCommands[0].Kind != domain.ThrottleCommandResume {
		t.Fatalf("resume plan = %#v %#v, err = %v", resumes, resumeCommands, err)
	}
	resume := resumes[0].Record
	if resume.Command.ThreadID != binding.Assignment.ThreadID ||
		resume.Command.WorkspacePath != binding.WorkspacePath ||
		!reflect.DeepEqual(resume.Command.Route, binding.Assignment.Route) {
		t.Fatalf("resume changed execution identity: %#v", resume.Command)
	}
	resumed, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{resume},
		[]domain.ThrottleAcknowledgement{{
			CommandID: resume.Command.ID, AttemptID: resume.AttemptID, Accepted: true,
			Result: domain.ThrottleResultResumed, AcknowledgedAt: throttleDeliveryTime.Add(9 * time.Minute),
		}}, throttleDeliveryTime.Add(9*time.Minute))
	if err != nil || resumed[0].Record.Control != domain.ControlRunning {
		t.Fatalf("resumed record = %#v, err = %v", resumed, err)
	}
}

func TestThrottleCheckpointSuccessSkipsHardStopAndResumesWithMetadata(t *testing.T) {
	binding := throttleBinding("attempt-a", "worker-a", domain.ControlRunning)
	directive := throttleDirectiveFixture(domain.ThrottleDrain)
	planned, _, err := PlanThrottleDeliveries(nil, []domain.ThrottleDirective{directive},
		[]ThrottleAttemptBinding{binding}, throttleDeliveryTime)
	if err != nil {
		t.Fatal(err)
	}
	record := planned[0].Record
	checkpoint := checkpointFixture()
	unsafeCheckpoint := *checkpoint
	unsafeCheckpoint.Path = "../checkpoint.md"
	if _, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{record},
		[]domain.ThrottleAcknowledgement{{
			CommandID: record.Command.ID, AttemptID: record.AttemptID, Accepted: true,
			Result: domain.ThrottleResultCheckpointed, Checkpoint: &unsafeCheckpoint,
			AcknowledgedAt: throttleDeliveryTime.Add(time.Minute),
		}}, throttleDeliveryTime.Add(time.Minute)); err == nil {
		t.Fatal("unsafe checkpoint path accepted")
	}
	paused, err := PlanThrottleAcknowledgements([]domain.ThrottleAttemptRecord{record},
		[]domain.ThrottleAcknowledgement{{
			CommandID: record.Command.ID, AttemptID: record.AttemptID, Accepted: true,
			Result: domain.ThrottleResultCheckpointed, Checkpoint: checkpoint,
			AcknowledgedAt: throttleDeliveryTime.Add(time.Minute),
		}}, throttleDeliveryTime.Add(time.Minute))
	if err != nil || paused[0].Record.Control != domain.ControlPaused ||
		!reflect.DeepEqual(paused[0].Record.Checkpoint, checkpoint) {
		t.Fatalf("checkpointed record = %#v, err = %v", paused, err)
	}
	expired, commands, err := PlanThrottleDeadlineExpirations(
		[]domain.ThrottleAttemptRecord{paused[0].Record}, throttleDeliveryTime.Add(10*time.Minute))
	if err != nil || len(expired) != 0 || len(commands) != 0 {
		t.Fatalf("checkpointed attempt hard-stopped = %#v %#v, err = %v", expired, commands, err)
	}
	admission := domain.QuotaAdmissionRecord{
		QuotaPoolID: "pool", Revision: 3, Admission: domain.AdmissionOpen, Reason: "healthy",
	}
	resumes, commands, err := PlanThrottleResumes(
		[]domain.ThrottleAttemptRecord{paused[0].Record}, []domain.QuotaAdmissionRecord{admission}, []domain.QuotaPool{{ID: "pool", MaxConcurrent: 1}},
		throttleDeliveryTime.Add(11*time.Minute))
	if err != nil || len(resumes) != 1 || !reflect.DeepEqual(commands[0].Checkpoint, checkpoint) {
		t.Fatalf("checkpoint resume = %#v %#v, err = %v", resumes, commands, err)
	}
}

func TestPlanThrottleResumesIgnoresObsoletePausedProjection(t *testing.T) {
	binding := throttleBinding("attempt-a", "worker-a", domain.ControlRunning)
	drainDirective := throttleDirectiveFixtureFor("directive-old", "pool", domain.ThrottleDrain)
	drainTransitions, _, err := PlanThrottleDeliveries(nil, []domain.ThrottleDirective{drainDirective},
		[]ThrottleAttemptBinding{binding}, throttleDeliveryTime)
	if err != nil {
		t.Fatal(err)
	}
	drain := drainTransitions[0].Record
	pausedTransitions, err := PlanThrottleAcknowledgements(
		[]domain.ThrottleAttemptRecord{drain},
		[]domain.ThrottleAcknowledgement{{
			CommandID: drain.Command.ID, AttemptID: drain.AttemptID, Accepted: true,
			Result: domain.ThrottleResultCheckpointed, Checkpoint: checkpointFixture(),
			AcknowledgedAt: throttleDeliveryTime.Add(time.Minute),
		}},
		throttleDeliveryTime.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}

	warnDirective := throttleDirectiveFixtureFor("directive-new", "pool", domain.ThrottleWarn)
	warnTransitions, _, err := PlanThrottleDeliveries(nil, []domain.ThrottleDirective{warnDirective},
		[]ThrottleAttemptBinding{binding}, throttleDeliveryTime.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	warn := warnTransitions[0].Record
	runningTransitions, err := PlanThrottleAcknowledgements(
		[]domain.ThrottleAttemptRecord{warn},
		[]domain.ThrottleAcknowledgement{{
			CommandID: warn.Command.ID, AttemptID: warn.AttemptID, Accepted: true,
			Result:         domain.ThrottleResultWarned,
			AcknowledgedAt: throttleDeliveryTime.Add(3 * time.Minute),
		}},
		throttleDeliveryTime.Add(3*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}

	admission := domain.QuotaAdmissionRecord{
		QuotaPoolID: "pool", Revision: 4, Admission: domain.AdmissionOpen,
	}
	transitions, commands, err := PlanThrottleResumes(
		[]domain.ThrottleAttemptRecord{pausedTransitions[0].Record, runningTransitions[0].Record},
		[]domain.QuotaAdmissionRecord{admission}, []domain.QuotaPool{{ID: "pool", MaxConcurrent: 1}},
		throttleDeliveryTime.Add(4*time.Minute),
	)
	if err != nil || len(transitions) != 0 || len(commands) != 0 {
		t.Fatalf("obsolete paused projection resumed = %#v %#v, err = %v", transitions, commands, err)
	}
}

func TestPlanThrottleAcknowledgementsRejectsPriorCommandAttemptMismatch(t *testing.T) {
	binding := throttleBinding("attempt-a", "worker-a", domain.ControlRunning)
	directive := throttleDirectiveFixture(domain.ThrottleDrain)
	planned, _, err := PlanThrottleDeliveries(nil, []domain.ThrottleDirective{directive},
		[]ThrottleAttemptBinding{binding}, throttleDeliveryTime)
	if err != nil {
		t.Fatal(err)
	}
	drain := planned[0].Record
	expired, _, err := PlanThrottleDeadlineExpirations(
		[]domain.ThrottleAttemptRecord{drain}, throttleDeliveryTime.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	transitions, err := PlanThrottleAcknowledgements(
		[]domain.ThrottleAttemptRecord{expired[0].Record},
		[]domain.ThrottleAcknowledgement{{
			CommandID: drain.Command.ID, AttemptID: "attempt-other", Accepted: true,
			Result: domain.ThrottleResultCheckpointed, Checkpoint: checkpointFixture(),
			AcknowledgedAt: throttleDeliveryTime.Add(6 * time.Minute),
		}},
		throttleDeliveryTime.Add(6*time.Minute),
	)
	if err == nil || len(transitions) != 0 {
		t.Fatalf("mismatched prior acknowledgement = %#v, err = %v", transitions, err)
	}
}

func TestReconcileThrottleDeadlineAndResumePersistsBeforeDelivery(t *testing.T) {
	binding := throttleBinding("attempt-a", "worker-a", domain.ControlRunning)
	directive := throttleDirectiveFixture(domain.ThrottleDrain)
	planned, _, err := PlanThrottleDeliveries(nil, []domain.ThrottleDirective{directive},
		[]ThrottleAttemptBinding{binding}, throttleDeliveryTime)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := PlanThrottleAcknowledgements(
		[]domain.ThrottleAttemptRecord{planned[0].Record},
		[]domain.ThrottleAcknowledgement{{
			CommandID: planned[0].Record.Command.ID, AttemptID: binding.Attempt.ID,
			Accepted: true, Result: domain.ThrottleResultCheckpointFailed,
			Error: "checkpoint failed", AcknowledgedAt: throttleDeliveryTime.Add(time.Minute),
		}},
		throttleDeliveryTime.Add(time.Minute),
	)
	if err != nil || len(failed) != 1 {
		t.Fatalf("checkpoint failure = %#v, err = %v", failed, err)
	}

	store := &throttleDeliveryStoreFake{records: []domain.ThrottleAttemptRecord{failed[0].Record}}
	transport := throttleTransportFunc(func(_ context.Context, _ string, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
		if len(commands) != 1 || len(store.records) != 1 ||
			store.records[0].Delivery != domain.ThrottleDeliveryPending ||
			store.records[0].Command.ID != commands[0].ID {
			t.Fatalf("command was delivered before durable intent: commands %#v records %#v", commands, store.records)
		}
		result := domain.ThrottleResultStopped
		if commands[0].Kind == domain.ThrottleCommandResume {
			result = domain.ThrottleResultResumed
		}
		return []domain.ThrottleAcknowledgement{{
			CommandID: commands[0].ID, AttemptID: commands[0].AttemptID,
			Accepted: true, Result: result, AcknowledgedAt: commands[0].CreatedAt.Add(time.Second),
		}}, nil
	})

	stopReport, err := ReconcileThrottleDeadlineExpirations(
		context.Background(), store, transport, throttleDeliveryTime.Add(5*time.Minute),
	)
	if err != nil || len(stopReport.Commands) != 1 ||
		stopReport.Commands[0].Kind != domain.ThrottleCommandHardStop ||
		store.records[0].Control != domain.ControlPausedUncheckpointed {
		t.Fatalf("hard-stop reconciliation = %#v records %#v, err = %v", stopReport, store.records, err)
	}

	admission := domain.QuotaAdmissionRecord{
		QuotaPoolID: "pool", Revision: 4, Admission: domain.AdmissionRecovering,
		Reason: "recovery probe",
	}
	resumeReport, err := ReconcileThrottleResumes(
		context.Background(), store, transport,
		[]domain.QuotaAdmissionRecord{admission}, []domain.QuotaPool{{ID: "pool", MaxConcurrent: 1}}, throttleDeliveryTime.Add(6*time.Minute),
	)
	if err != nil || len(resumeReport.Commands) != 1 ||
		resumeReport.Commands[0].Kind != domain.ThrottleCommandResume ||
		store.records[0].Control != domain.ControlRunning {
		t.Fatalf("resume reconciliation = %#v records %#v, err = %v", resumeReport, store.records, err)
	}
	if resumeReport.Commands[0].ThreadID != binding.Assignment.ThreadID ||
		resumeReport.Commands[0].WorkspacePath != binding.WorkspacePath ||
		!reflect.DeepEqual(resumeReport.Commands[0].Route, binding.Assignment.Route) {
		t.Fatalf("resume changed execution identity: %#v", resumeReport.Commands[0])
	}
}

func checkpointFixture() *domain.CheckpointMetadata {
	return &domain.CheckpointMetadata{
		ArtifactID: "checkpoint-attempt-a",
		Path:       ".t3/checkpoint.md",
		SHA256:     "abc123",
		Size:       42,
		CapturedAt: throttleDeliveryTime.Add(time.Minute),
	}
}

func throttleBinding(attemptID, workerID string, control domain.ControlState) ThrottleAttemptBinding {
	return throttleBindingFor(attemptID, workerID, "pool", control)
}

func throttleBindingFor(attemptID, workerID, poolID string, control domain.ControlState) ThrottleAttemptBinding {
	assignmentID := "assignment-" + attemptID
	return ThrottleAttemptBinding{
		Attempt: domain.Attempt{
			ID: attemptID, AssignmentID: assignmentID,
			Progress: domain.ProgressActive, Control: control,
		},
		Assignment: domain.Assignment{
			ID: assignmentID, AttemptID: attemptID, WorkerID: workerID,
			Route: domain.ProviderRoute{
				WorkerID: workerID, ProviderInstanceID: "codex", Model: "gpt-5.6-sol",
				Options: map[string]string{"effort": "medium"}, QuotaPoolID: poolID,
			},
			State: domain.AssignmentClaimed, Epoch: 3,
			ThreadID: "thread-" + attemptID,
		},
		WorkspacePath: "/runs/" + attemptID + "/workspace",
	}
}

func throttleDirectiveFixture(severity domain.ThrottleSeverity) domain.ThrottleDirective {
	return throttleDirectiveFixtureFor("directive", "pool", severity)
}

func throttleDirectiveFixtureFor(id, poolID string, severity domain.ThrottleSeverity) domain.ThrottleDirective {
	deadline := throttleDeliveryTime.Add(5 * time.Minute)
	return domain.ThrottleDirective{
		ID: id, QuotaPoolID: poolID, AdmissionRevision: 2, Severity: severity,
		BucketEpochs: []domain.QuotaBucketEpoch{{
			Bucket: domain.BucketKey{
				ProviderInstanceID: "codex", LimitID: "tokens", Window: domain.WindowPrimary,
			},
			Epoch: "epoch-1",
		}},
		Reason: "quota " + string(severity), Deadline: &deadline, CreatedAt: throttleDeliveryTime,
	}
}
