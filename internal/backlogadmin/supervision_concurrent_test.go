package backlogadmin

// One supervisor client, several runs under review at the same time.
//
// This is the deployment the fleet actually runs: exactly one supervisor admin
// client serves every supervised run, and the coordinator dispatches one
// activation per run to it. Resolving that client's capability by principal
// alone made the second concurrent review refuse every command of both, show
// and escalate included, so an overseer ended its turn with no decision and the
// run was stranded with its incident open. The capability is one activation,
// which is a pair of a principal and a run, and every supervision request names
// its run.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const fleetSupervisorPrincipal = "remote:supervisor:fleet"

// fleetSupervisionReader records several supervised runs and the live
// activation each one has dispatched to one shared supervisor principal.
type fleetSupervisionReader struct {
	runs     map[string]SupervisionState
	receipts map[string]SupervisionReceipt
	commits  []SupervisionCommit
}

func (r *fleetSupervisionReader) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return sqlite.CoordinatorRecords{}, nil
}

func (r *fleetSupervisionReader) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return nil, nil
}

func (r *fleetSupervisionReader) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return nil, nil
}

func (r *fleetSupervisionReader) LoadSupervision(_ context.Context, runID string) (SupervisionState, error) {
	state, found := r.runs[runID]
	if !found {
		return SupervisionState{}, ErrNotFound
	}
	return state, nil
}

func (r *fleetSupervisionReader) ResolveSupervisionBranch(context.Context, string, string) (SupervisionBranch, error) {
	return SupervisionBranch{}, nil
}

func (r *fleetSupervisionReader) SupervisionReplay(_ context.Context, runID, key string) (SupervisionReceipt, bool, error) {
	receipt, found := r.receipts[runID+"/"+key]
	return receipt, found, nil
}

func (r *fleetSupervisionReader) CommitSupervision(_ context.Context, commit SupervisionCommit) (SupervisionReceipt, error) {
	r.commits = append(r.commits, commit)
	receipt := SupervisionReceipt{
		PayloadDigest: commit.PayloadDigest,
		Response: SupervisionResponse{
			Version: SupervisionVersion, Operation: commit.Operation, RunID: commit.RunID,
			GeneratedAt: commit.Now, Actor: commit.Actor,
			GateID: commit.GateID, GateState: commit.GateState, Decision: commit.Decision,
		},
	}
	r.receipts[commit.RunID+"/"+commit.RequestKey] = receipt
	return receipt, nil
}

// SupervisorScope is the per-activation answer: this principal holds a live
// activation on the named run, or it does not. Holding one on another run is
// neither authority here nor a reason to refuse here.
func (r *fleetSupervisionReader) SupervisorScope(_ context.Context, principal, runID string) (SupervisorScope, error) {
	state, found := r.runs[runID]
	if !found || principal != state.Activation.Principal {
		return SupervisorScope{}, nil
	}
	return SupervisorScope{RunID: runID, ActivationEpoch: state.Activation.Epoch}, nil
}

func (r *fleetSupervisionReader) ArtifactRun(context.Context, string) (string, error) {
	return "", nil
}

// fleetSupervisionRun is one supervised run with one gate ready for review and
// one live activation held by the shared supervisor principal.
func fleetSupervisionRun(runID string, epoch int64) SupervisionState {
	return SupervisionState{
		Record: domain.SupervisionRecord{
			RunID: runID, ActivationEpoch: epoch, Revision: 5,
			Config: domain.SupervisionConfig{MaxActivations: 3},
		},
		Activation: domain.Activation{
			ID: "activation:" + runID, RunID: runID, Epoch: epoch,
			State: domain.ActivationActive, Principal: fleetSupervisorPrincipal,
		},
		Gates: []SupervisionGateView{{
			Gate: domain.Gate{
				Definition: domain.GateDefinition{
					ID: runID + ":gate", Name: "review",
					ObservedTaskIDs: []string{"task-build"}, ProtectedTaskIDs: []string{"task-publish"},
				},
				RunID: runID, State: domain.GateReadyForReview, GraphRevision: 4,
				EvidenceSnapshotID: runID + ":snapshot", Revision: 2,
			},
			ProducersVerified: true,
			Evidence: &domain.EvidenceSnapshot{
				ID: runID + ":snapshot", GraphRevision: 4, TakenAt: supervisionTestNow,
			},
		}},
	}
}

func fleetSupervisionFixture(t *testing.T) (*Service, *fleetSupervisionReader) {
	t.Helper()
	reader := &fleetSupervisionReader{
		runs: map[string]SupervisionState{
			"run-claude": fleetSupervisionRun("run-claude", 2),
			"run-codex":  fleetSupervisionRun("run-codex", 1),
		},
		receipts: map[string]SupervisionReceipt{},
	}
	service, err := New(reader, SupervisorAuthorizer{Scope: reader, Delegate: allowAll{}})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return supervisionTestNow })
	return service, reader
}

func fleetSupervisor() Principal {
	return Principal{ID: fleetSupervisorPrincipal, Roles: []string{SupervisorRole}}
}

func fleetShowRequest(runID string, epoch int64) SupervisionRequest {
	return SupervisionRequest{
		Version: SupervisionVersion, Operation: SupervisionShow, RunID: runID, ActivationEpoch: epoch,
	}
}

func fleetAcceptRequest(runID string, epoch int64, key string) SupervisionRequest {
	return SupervisionRequest{
		Version: SupervisionVersion, Operation: SupervisionDecide, RunID: runID,
		ActivationEpoch: epoch, RequestKey: key, ExpectedRevision: 2,
		Reason: "the rubric is satisfied",
		Gate: &SupervisionGateDecision{
			GateID: runID + ":gate", Outcome: domain.GateDecisionAccept,
			EvidenceSnapshotID: runID + ":snapshot", ExpectedGraphRevision: 4,
		},
	}
}

// Each of the two concurrent activations reads and decides its own run.
func TestConcurrentActivationsOfOneSupervisorEachDecideTheirOwnRun(t *testing.T) {
	service, reader := fleetSupervisionFixture(t)
	ctx := context.Background()
	for runID, epoch := range map[string]int64{"run-claude": 2, "run-codex": 1} {
		show, err := service.Supervise(ctx, fleetSupervisor(), fleetShowRequest(runID, epoch))
		if err != nil {
			t.Fatalf("show of %s refused: %v", runID, err)
		}
		if show.State == nil || show.State.Record.RunID != runID {
			t.Fatalf("show of %s answered %+v", runID, show.State)
		}
		decision, err := service.Supervise(ctx, fleetSupervisor(),
			fleetAcceptRequest(runID, epoch, "key-"+runID))
		if err != nil {
			t.Fatalf("decision on %s refused: %v", runID, err)
		}
		if decision.GateState != domain.GateAccepted {
			t.Fatalf("gate of %s is %q after an acceptance", runID, decision.GateState)
		}
	}
	if len(reader.commits) != 2 {
		t.Fatalf("committed %d decisions, want one per run", len(reader.commits))
	}
}

// Each activation is refused on the other's run, at that run's own epoch: the
// capability is bound to the activation the coordinator dispatched, and holding
// one activation is not authority over a run it does not name.
func TestOneActivationIsRefusedTheOtherRun(t *testing.T) {
	service, reader := fleetSupervisionFixture(t)
	ctx := context.Background()
	foreign := reader.runs["run-codex"]
	foreign.Activation.Principal = "remote:supervisor:other"
	reader.runs["run-codex"] = foreign

	_, showErr := service.Supervise(ctx, fleetSupervisor(), fleetShowRequest("run-codex", 1))
	if class := ClassifySupervisionError(showErr); class != SupervisionErrorUnauthorizedScope {
		t.Fatalf("show of another activation's run: class = %q (%v)", class, showErr)
	}
	_, decideErr := service.Supervise(ctx, fleetSupervisor(), fleetAcceptRequest("run-codex", 1, "key-foreign"))
	if class := ClassifySupervisionError(decideErr); class != SupervisionErrorUnauthorizedScope {
		t.Fatalf("decision on another activation's run: class = %q (%v)", class, decideErr)
	}
	// Its own run is untouched by the refusal above.
	if _, err := service.Supervise(ctx, fleetSupervisor(), fleetShowRequest("run-claude", 2)); err != nil {
		t.Fatalf("a refusal on one run refused the other: %v", err)
	}
}

// An epoch the activation is no longer at is stale, not unauthorized: the class
// is what tells an operator takeover apart from a credential with no scope.
func TestConcurrentActivationRefusesAStaleEpoch(t *testing.T) {
	service, _ := fleetSupervisionFixture(t)
	_, err := service.Supervise(context.Background(), fleetSupervisor(),
		fleetAcceptRequest("run-claude", 1, "key-stale"))
	if class := ClassifySupervisionError(err); class != SupervisionErrorStaleEvidence {
		t.Fatalf("stale epoch: class = %q (%v)", class, err)
	}
	if !strings.Contains(err.Error(), "epoch") {
		t.Fatalf("stale epoch refusal = %v, want it to name the epoch", err)
	}
}

// The refusal that stranded the live run named two runs in one message and
// claimed a supervisor capability is bound to one run. Nothing may say that
// again: it is bound to one activation, and one client holds many.
func TestNoRefusalClaimsACapabilityIsBoundToOneRun(t *testing.T) {
	service, _ := fleetSupervisionFixture(t)
	ctx := context.Background()
	for _, request := range []SupervisionRequest{
		fleetShowRequest("run-claude", 2),
		fleetShowRequest("run-codex", 1),
		fleetShowRequest("run-absent", 1),
	} {
		_, err := service.Supervise(ctx, fleetSupervisor(), request)
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), "a supervisor capability is bound to one run") ||
			strings.Contains(err.Error(), "holds live activations on runs") {
			t.Fatalf("the over-broad per-principal refusal is still reachable: %v", err)
		}
	}
}

func fleetReassessRequest(runID, key string) SupervisionRequest {
	return SupervisionRequest{
		Version: SupervisionVersion, Operation: SupervisionReassess, RunID: runID,
		RequestKey: key, ExpectedRevision: 5, Reason: "the overseer ended without deciding",
	}
}

// Reassessment is the operator's re-arming verb. An overseer asking for it is
// the automatic spin the plan forbids wearing an operator's verb.
func TestReassessIsRefusedToAnOverseer(t *testing.T) {
	service, _ := fleetSupervisionFixture(t)
	request := fleetReassessRequest("run-claude", "key-reassess")
	request.ActivationEpoch = 2
	_, err := service.Supervise(context.Background(), fleetSupervisor(), request)
	if class := ClassifySupervisionError(err); class != SupervisionErrorUnauthorizedScope {
		t.Fatalf("overseer reassessment: class = %q (%v)", class, err)
	}
}

func TestReassessByAnOperatorIsAccepted(t *testing.T) {
	service, reader := fleetSupervisionFixture(t)
	response, err := service.Supervise(context.Background(), operatorPrincipal(),
		fleetReassessRequest("run-claude", "key-reassess"))
	if err != nil {
		t.Fatalf("operator reassessment refused: %v", err)
	}
	if response.Operation != SupervisionReassess || response.Actor.Kind != domain.ActorOperator {
		t.Fatalf("reassessment answered %+v", response)
	}
	if len(reader.commits) != 1 || reader.commits[0].Operation != SupervisionReassess {
		t.Fatalf("commits = %+v, want one reassessment", reader.commits)
	}
}

// A run that has spent its declared activations is not re-armed by asking
// again. Raising the budget is a separate, receipted decision.
func TestReassessIsRefusedOnceTheActivationBudgetIsExhausted(t *testing.T) {
	service, reader := fleetSupervisionFixture(t)
	state := reader.runs["run-claude"]
	state.Record.ActivationsUsed = state.Record.Config.MaxActivations
	reader.runs["run-claude"] = state
	_, err := service.Supervise(context.Background(), operatorPrincipal(),
		fleetReassessRequest("run-claude", "key-reassess"))
	if err == nil || !strings.Contains(err.Error(), "activations") {
		t.Fatalf("reassessment with no budget = %v, want a budget refusal", err)
	}
	if len(reader.commits) != 0 {
		t.Fatalf("a refused reassessment committed %+v", reader.commits)
	}
}

// A reassessment carries no gate, hold or incident payload: one wearing a
// payload is a decision under the re-arming verb's name.
func TestReassessCarriesNoPayload(t *testing.T) {
	request := fleetReassessRequest("run-claude", "key-reassess")
	request.Incident = &SupervisionIncidentRequest{IncidentID: "incident-1"}
	if err := request.Validate(); err == nil {
		t.Fatal("a reassessment carrying an incident payload validated")
	}
}
