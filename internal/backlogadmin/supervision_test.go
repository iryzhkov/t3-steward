package backlogadmin

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var supervisionTestNow = time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

// supervisionReader is a Reader that also records supervision, which is how a
// coordinator store reaches the service.
type supervisionReader struct {
	state     SupervisionState
	branch    SupervisionBranch
	receipts  map[string]SupervisionReceipt
	commits   []SupervisionCommit
	loadErr   error
	artifacts map[string]string
}

func newSupervisionReader() *supervisionReader {
	return &supervisionReader{
		receipts:  map[string]SupervisionReceipt{},
		artifacts: map[string]string{},
	}
}

func (r *supervisionReader) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return sqlite.CoordinatorRecords{}, nil
}
func (r *supervisionReader) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return nil, nil
}
func (r *supervisionReader) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return nil, nil
}

func (r *supervisionReader) LoadSupervision(_ context.Context, runID string) (SupervisionState, error) {
	if r.loadErr != nil {
		return SupervisionState{}, r.loadErr
	}
	if runID != r.state.Record.RunID {
		return SupervisionState{}, ErrNotFound
	}
	return r.state, nil
}

func (r *supervisionReader) ResolveSupervisionBranch(context.Context, string, string) (SupervisionBranch, error) {
	return r.branch, nil
}

func (r *supervisionReader) SupervisionReplay(_ context.Context, runID, key string) (SupervisionReceipt, bool, error) {
	receipt, found := r.receipts[runID+"/"+key]
	return receipt, found, nil
}

func (r *supervisionReader) CommitSupervision(_ context.Context, commit SupervisionCommit) (SupervisionReceipt, error) {
	r.commits = append(r.commits, commit)
	receipt := SupervisionReceipt{
		PayloadDigest: commit.PayloadDigest,
		Response: SupervisionResponse{
			Version:       SupervisionVersion,
			Operation:     commit.Operation,
			RunID:         commit.RunID,
			GeneratedAt:   commit.Now,
			Actor:         commit.Actor,
			GateID:        commit.GateID,
			GateState:     commit.GateState,
			Decision:      commit.Decision,
			Hold:          commit.Hold,
			IncidentID:    commit.IncidentID,
			IncidentState: commit.IncidentState,
			Resolution:    commit.Resolution,
		},
	}
	r.receipts[commit.RunID+"/"+commit.RequestKey] = receipt
	return receipt, nil
}

// SupervisorScope and ArtifactRun make the same fake the coordinator's scope
// source, so a test states one truth about who may act on what.
func (r *supervisionReader) SupervisorScope(_ context.Context, principal string) (SupervisorScope, error) {
	if principal != "remote:overseer" {
		return SupervisorScope{}, nil
	}
	return SupervisorScope{RunID: r.state.Record.RunID, ActivationEpoch: r.state.Record.ActivationEpoch}, nil
}

func (r *supervisionReader) ArtifactRun(_ context.Context, artifactID string) (string, error) {
	run, found := r.artifacts[artifactID]
	if !found {
		return "", errors.New("no such artifact")
	}
	return run, nil
}

// allowAll is the operator side of the authorizer: the owner-only socket
// grants its peer everything, exactly as localAdminAuthorizer does.
type allowAll struct{}

func (allowAll) Authorize(context.Context, Principal, Action) error { return nil }

func supervisionFixture(t *testing.T) (*Service, *supervisionReader) {
	t.Helper()
	reader := newSupervisionReader()
	reader.state = SupervisionState{
		Record: domain.SupervisionRecord{
			RunID:           "run-1",
			ActivationEpoch: 4,
			Revision:        7,
		},
		Activation: domain.Activation{
			ID: "activation-1", RunID: "run-1", Epoch: 4, State: domain.ActivationActive,
		},
		Gates: []SupervisionGateView{{
			Gate: domain.Gate{
				Definition: domain.GateDefinition{
					ID: "gate-1", Name: "review",
					ObservedTaskIDs:  []string{"task-build"},
					ProtectedTaskIDs: []string{"task-publish"},
				},
				RunID:              "run-1",
				State:              domain.GateReadyForReview,
				GraphRevision:      12,
				EvidenceSnapshotID: "snapshot-1",
				Revision:           3,
			},
			ProducersVerified: true,
			Evidence: &domain.EvidenceSnapshot{
				ID: "snapshot-1", GraphRevision: 12, TakenAt: supervisionTestNow,
			},
		}},
		Holds: []domain.Hold{{
			ID:    "hold-operator",
			RunID: "run-1",
			Scope: domain.HoldScope{Kind: domain.HoldScopeRun},
			Owner: domain.Actor{Kind: domain.ActorOperator, Principal: "local:1000"},
			State: domain.HoldActive,
		}},
		Incidents: []SupervisionIncidentView{{
			Incident: domain.ReviewIncident{
				ID: "incident-1", RunID: "run-1", GateID: "gate-1",
				Revision:            2,
				RequiredDisposition: domain.DispositionGateDecision,
				State:               domain.IncidentOpen,
			},
		}},
	}
	reader.artifacts["artifact-mine"] = "run-1"
	reader.artifacts["artifact-theirs"] = "run-2"
	service, err := New(reader, SupervisorAuthorizer{Scope: reader, Delegate: allowAll{}})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return supervisionTestNow })
	return service, reader
}

func supervisorPrincipal() Principal {
	return Principal{ID: "remote:overseer", Roles: []string{SupervisorRole}}
}

func operatorPrincipal() Principal {
	return Principal{ID: "local:1000", Roles: []string{LocalAdminRole}}
}

func acceptRequest() SupervisionRequest {
	return SupervisionRequest{
		Version:          SupervisionVersion,
		Operation:        SupervisionDecide,
		RunID:            "run-1",
		ActivationEpoch:  4,
		RequestKey:       "key-1",
		ExpectedRevision: 3,
		Reason:           "the rubric is satisfied",
		Gate: &SupervisionGateDecision{
			GateID:                "gate-1",
			Outcome:               domain.GateDecisionAccept,
			EvidenceSnapshotID:    "snapshot-1",
			ExpectedGraphRevision: 12,
		},
	}
}

func TestSupervisorAcceptsItsOwnGate(t *testing.T) {
	service, reader := supervisionFixture(t)
	response, err := service.Supervise(context.Background(), supervisorPrincipal(), acceptRequest())
	if err != nil {
		t.Fatalf("accept refused: %v", err)
	}
	if response.Version != SupervisionVersion || response.GateState != domain.GateAccepted {
		t.Fatalf("response = %+v", response)
	}
	if response.Actor.Kind != domain.ActorOverseer || response.Actor.ActivationEpoch != 4 {
		t.Fatalf("actor = %+v", response.Actor)
	}
	if len(reader.commits) != 1 || reader.commits[0].Decision == nil {
		t.Fatalf("commits = %+v", reader.commits)
	}
	if reader.commits[0].Decision.Evidence.ID != "snapshot-1" {
		t.Fatalf("decision records no evidence identity: %+v", reader.commits[0].Decision)
	}
}

// The refusal must name the scope, not the run, and it must be an
// unauthorized-scope class rather than something a caller would retry.
func TestSupervisorIsRefusedEveryOperationOutsideItsRun(t *testing.T) {
	service, _ := supervisionFixture(t)
	for _, operation := range SupervisionOperations() {
		request := acceptRequest()
		request.Operation = operation
		request.RunID = "run-2"
		switch operation {
		case SupervisionShow:
			request = SupervisionRequest{
				Version: SupervisionVersion, Operation: SupervisionShow, RunID: "run-2",
			}
		case SupervisionHold:
			request.Gate = nil
			request.Hold = &SupervisionHoldRequest{Scope: domain.HoldScope{Kind: domain.HoldScopeRun}}
		case SupervisionRelease:
			request.Gate = nil
			request.Release = &SupervisionReleaseRequest{HoldID: "hold-operator"}
		case SupervisionEscalate:
			request.Gate = nil
			request.Incident = &SupervisionIncidentRequest{IncidentID: "incident-1"}
		case SupervisionResolve:
			request.Gate = nil
			request.Incident = &SupervisionIncidentRequest{
				IncidentID: "incident-1", Outcome: domain.IncidentOutcomeConcludeFailure,
			}
		}
		_, err := service.Supervise(context.Background(), supervisorPrincipal(), request)
		if err == nil {
			t.Fatalf("%s on another run was permitted", operation)
		}
		if class := ClassifySupervisionError(err); class != SupervisionErrorUnauthorizedScope {
			t.Fatalf("%s on another run: class = %q (%v)", operation, class, err)
		}
	}
}

func TestSupervisorIsRefusedOutsideItsActivationEpoch(t *testing.T) {
	service, _ := supervisionFixture(t)
	request := acceptRequest()
	request.ActivationEpoch = 3
	_, err := service.Supervise(context.Background(), supervisorPrincipal(), request)
	if class := ClassifySupervisionError(err); class != SupervisionErrorStaleEvidence {
		t.Fatalf("stale epoch: class = %q (%v)", class, err)
	}
	request = acceptRequest()
	request.ActivationEpoch = 0
	_, err = service.Supervise(context.Background(), supervisorPrincipal(), request)
	if class := ClassifySupervisionError(err); class != SupervisionErrorUnauthorizedScope {
		t.Fatalf("epoch-less decision: class = %q (%v)", class, err)
	}
}

// A takeover revokes the activation and raises the epoch. The decision the old
// supervisor had already formed then arrives too late, and arriving late must
// not reverse the takeover.
func TestOperatorTakeoverRevokesLateSupervisorDecisions(t *testing.T) {
	service, reader := supervisionFixture(t)
	reader.state.Record.ActivationEpoch = 5
	reader.state.Activation = domain.Activation{
		ID: "activation-1", RunID: "run-1", Epoch: 4, State: domain.ActivationRevoked,
	}
	_, err := service.Supervise(context.Background(), supervisorPrincipal(), acceptRequest())
	if class := ClassifySupervisionError(err); class != SupervisionErrorStaleEvidence {
		t.Fatalf("late decision: class = %q (%v)", class, err)
	}
	if len(reader.commits) != 0 {
		t.Fatalf("a revoked activation committed %d decisions", len(reader.commits))
	}
	// The operator itself is not fenced by the activation and may accept with
	// the same evidence checks.
	operatorAccept := acceptRequest()
	operatorAccept.ActivationEpoch = 0
	if _, err := service.Supervise(context.Background(), operatorPrincipal(), operatorAccept); err != nil {
		t.Fatalf("operator accept refused: %v", err)
	}
}

func TestSupervisionReplaysOneKeyAndRefusesAChangedPayload(t *testing.T) {
	service, reader := supervisionFixture(t)
	first, err := service.Supervise(context.Background(), supervisorPrincipal(), acceptRequest())
	if err != nil {
		t.Fatal(err)
	}
	if first.Replay {
		t.Fatal("the first answer is a replay")
	}
	replay, err := service.Supervise(context.Background(), supervisorPrincipal(), acceptRequest())
	if err != nil {
		t.Fatalf("replay refused: %v", err)
	}
	if !replay.Replay || replay.GateState != first.GateState {
		t.Fatalf("replay = %+v, first = %+v", replay, first)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("a replay committed a second effect: %d commits", len(reader.commits))
	}
	// A different reason under the same key is the same decision and still
	// replays; a different outcome is a different decision and is refused.
	sameDecision := acceptRequest()
	sameDecision.Reason = "restating the same acceptance"
	if _, err := service.Supervise(context.Background(), supervisorPrincipal(), sameDecision); err != nil {
		t.Fatalf("a re-worded replay was refused: %v", err)
	}
	changed := acceptRequest()
	changed.Gate.Outcome = domain.GateDecisionReject
	_, err = service.Supervise(context.Background(), supervisorPrincipal(), changed)
	if !errors.Is(err, ErrSupervisionRequestConflict) {
		t.Fatalf("a changed payload under the same key: %v", err)
	}
	if class := ClassifySupervisionError(err); class != SupervisionErrorStaleEvidence {
		t.Fatalf("conflict class = %q", class)
	}
	if len(reader.commits) != 1 {
		t.Fatalf("commits = %d", len(reader.commits))
	}
}

func TestSupervisionRefusesStaleEvidenceAndStaleRevisions(t *testing.T) {
	service, _ := supervisionFixture(t)
	staleSnapshot := acceptRequest()
	staleSnapshot.Gate.EvidenceSnapshotID = "snapshot-0"
	_, err := service.Supervise(context.Background(), supervisorPrincipal(), staleSnapshot)
	if class := ClassifySupervisionError(err); class != SupervisionErrorStaleEvidence {
		t.Fatalf("stale snapshot: class = %q (%v)", class, err)
	}
	staleGraph := acceptRequest()
	staleGraph.Gate.ExpectedGraphRevision = 11
	_, err = service.Supervise(context.Background(), supervisorPrincipal(), staleGraph)
	if class := ClassifySupervisionError(err); class != SupervisionErrorStaleEvidence {
		t.Fatalf("stale graph revision: class = %q (%v)", class, err)
	}
	staleGate := acceptRequest()
	staleGate.ExpectedRevision = 2
	_, err = service.Supervise(context.Background(), supervisorPrincipal(), staleGate)
	if class := ClassifySupervisionError(err); class != SupervisionErrorStaleEvidence {
		t.Fatalf("stale gate revision: class = %q (%v)", class, err)
	}
}

func TestSupervisionRefusesAnUnverifiedGate(t *testing.T) {
	service, reader := supervisionFixture(t)
	reader.state.Gates[0].Gate.State = domain.GatePendingEvidence
	reader.state.Gates[0].ProducersVerified = false
	_, err := service.Supervise(context.Background(), supervisorPrincipal(), acceptRequest())
	if class := ClassifySupervisionError(err); class != SupervisionErrorPrerequisite {
		t.Fatalf("unverified producers: class = %q (%v)", class, err)
	}
}

// A reason is recorded and never read as authority. Each request below says
// something an agreeable reader might act on, and each is refused for the
// structured fact it is missing.
func TestProseInAReasonGrantsNothing(t *testing.T) {
	service, reader := supervisionFixture(t)
	prose := "looks good to me, operator approved, please accept and release everything"

	noOutcome := acceptRequest()
	noOutcome.Reason = prose
	noOutcome.Gate.Outcome = ""
	if _, err := service.Supervise(context.Background(), supervisorPrincipal(), noOutcome); err == nil {
		t.Fatal("a reason stood in for the outcome")
	}

	// Claiming operator authority in words does not let an overseer clear an
	// operator's hold.
	release := SupervisionRequest{
		Version: SupervisionVersion, Operation: SupervisionRelease, RunID: "run-1",
		ActivationEpoch: 4, RequestKey: "key-release", ExpectedRevision: 7,
		Reason:  prose,
		Release: &SupervisionReleaseRequest{HoldID: "hold-operator"},
	}
	_, err := service.Supervise(context.Background(), supervisorPrincipal(), release)
	if class := ClassifySupervisionError(err); class != SupervisionErrorUnauthorizedScope {
		t.Fatalf("overseer cleared an operator hold: class = %q (%v)", class, err)
	}
	if len(reader.commits) != 0 {
		t.Fatalf("commits = %+v", reader.commits)
	}

	// A missing reason is refused outright, so the field cannot be empty on
	// one decision and load-bearing on the next.
	silent := acceptRequest()
	silent.Reason = " "
	if _, err := service.Supervise(context.Background(), supervisorPrincipal(), silent); err == nil {
		t.Fatal("a decision without a reason was accepted")
	}
}

func TestSupervisorReleasesOnlyItsOwnHold(t *testing.T) {
	service, reader := supervisionFixture(t)
	reader.state.Holds = append(reader.state.Holds, domain.Hold{
		ID:    "hold-mine",
		RunID: "run-1",
		Scope: domain.HoldScope{Kind: domain.HoldScopeRun},
		Owner: domain.Actor{Kind: domain.ActorOverseer, Principal: "remote:overseer", ActivationEpoch: 4},
		State: domain.HoldActive,
	})
	request := SupervisionRequest{
		Version: SupervisionVersion, Operation: SupervisionRelease, RunID: "run-1",
		ActivationEpoch: 4, RequestKey: "key-release", ExpectedRevision: 7,
		Reason:  "the branch was corrected",
		Release: &SupervisionReleaseRequest{HoldID: "hold-mine"},
	}
	response, err := service.Supervise(context.Background(), supervisorPrincipal(), request)
	if err != nil {
		t.Fatalf("own hold refused: %v", err)
	}
	if response.Hold == nil || response.Hold.State != domain.HoldReleased {
		t.Fatalf("hold = %+v", response.Hold)
	}
}

func TestSupervisionPlacesABranchHoldOnItsResolvedClosure(t *testing.T) {
	service, reader := supervisionFixture(t)
	reader.branch = SupervisionBranch{
		Exists: true, GraphRevision: 12, ResolvedTaskIDs: []string{"task-publish", "task-announce"},
	}
	request := SupervisionRequest{
		Version: SupervisionVersion, Operation: SupervisionHold, RunID: "run-1",
		ActivationEpoch: 4, RequestKey: "key-hold", ExpectedRevision: 7,
		Reason: "the publish branch waits for the rubric",
		Hold: &SupervisionHoldRequest{Scope: domain.HoldScope{
			Kind: domain.HoldScopeBranch, BranchRootTaskID: "task-publish",
		}},
	}
	response, err := service.Supervise(context.Background(), supervisorPrincipal(), request)
	if err != nil {
		t.Fatalf("branch hold refused: %v", err)
	}
	if response.Hold == nil || response.Hold.State != domain.HoldActive {
		t.Fatalf("hold = %+v", response.Hold)
	}
	// One request key is one hold, and the closure it recorded is the one the
	// store resolved in the same call.
	if response.Hold.ID != "key-hold" || len(response.Hold.ResolvedTaskIDs) != 2 ||
		response.Hold.GraphRevision != 12 {
		t.Fatalf("hold = %+v", response.Hold)
	}
	if reader.commits[0].Request.Operation != SupervisionHold {
		t.Fatalf("the commit carries no structured request: %+v", reader.commits[0].Request)
	}
	// A branch root the store cannot resolve is an unmet prerequisite, not a
	// hold over nothing.
	missing := request
	missing.RequestKey = "key-hold-2"
	reader.branch = SupervisionBranch{Exists: false}
	_, err = service.Supervise(context.Background(), supervisorPrincipal(), missing)
	if class := ClassifySupervisionError(err); class != SupervisionErrorPrerequisite {
		t.Fatalf("missing branch root: class = %q (%v)", class, err)
	}
}

func TestSupervisionResolvesOneIncidentAndRefusesABulkClose(t *testing.T) {
	service, reader := supervisionFixture(t)
	reader.state.Incidents[0].Incident.RequiredDisposition = domain.DispositionConcludeFailure
	reader.state.Incidents[0].SourceTaskTerminallyFailed = true
	single := SupervisionRequest{
		Version: SupervisionVersion, Operation: SupervisionResolve, RunID: "run-1",
		ActivationEpoch: 4, RequestKey: "key-resolve", ExpectedRevision: 2,
		Reason:   "the producer failed terminally and is acknowledged for settlement",
		Incident: &SupervisionIncidentRequest{IncidentID: "incident-1", Outcome: domain.IncidentOutcomeConcludeFailure},
	}
	response, err := service.Supervise(context.Background(), supervisorPrincipal(), single)
	if err != nil {
		t.Fatalf("single resolve refused: %v", err)
	}
	if response.IncidentState != domain.IncidentResolved || response.Resolution == nil {
		t.Fatalf("response = %+v", response)
	}
	bulk := single
	bulk.RequestKey = "key-bulk"
	bulk.Incident = &SupervisionIncidentRequest{
		IncidentIDs: []string{"incident-1", "incident-2"},
		Outcome:     domain.IncidentOutcomeConcludeFailure,
	}
	_, err = service.Supervise(context.Background(), supervisorPrincipal(), bulk)
	if class := ClassifySupervisionError(err); class != SupervisionErrorPrerequisite {
		t.Fatalf("bulk close: class = %q (%v)", class, err)
	}
}

func TestSupervisionShowIsReadOnlyAndCarriesNoDecision(t *testing.T) {
	service, reader := supervisionFixture(t)
	show := SupervisionRequest{Version: SupervisionVersion, Operation: SupervisionShow, RunID: "run-1"}
	response, err := service.Supervise(context.Background(), supervisorPrincipal(), show)
	if err != nil {
		t.Fatalf("show refused: %v", err)
	}
	if response.State == nil || len(response.State.Gates) != 1 || len(response.State.Incidents) != 1 {
		t.Fatalf("state = %+v", response.State)
	}
	if len(reader.commits) != 0 {
		t.Fatal("a read committed an effect")
	}
	smuggled := show
	smuggled.Gate = &SupervisionGateDecision{GateID: "gate-1", Outcome: domain.GateDecisionAccept}
	if _, err := service.Supervise(context.Background(), supervisorPrincipal(), smuggled); err == nil {
		t.Fatal("a read carried a decision")
	}
}

// The capability must not inherit cross-run artifact access through the
// ordinary admin APIs, which authorize an artifact by its ID alone.
func TestSupervisorCannotReadAnotherRunsArtifacts(t *testing.T) {
	_, reader := supervisionFixture(t)
	authorizer := SupervisorAuthorizer{Scope: reader, Delegate: allowAll{}}
	ctx := context.Background()
	if err := authorizer.Authorize(ctx, supervisorPrincipal(),
		Action{Kind: QueryArtifact, ArtifactID: "artifact-mine"}); err != nil {
		t.Fatalf("own artifact refused: %v", err)
	}
	err := authorizer.Authorize(ctx, supervisorPrincipal(),
		Action{Kind: QueryArtifact, ArtifactID: "artifact-theirs"})
	if class := ClassifySupervisionError(err); class != SupervisionErrorUnauthorizedScope {
		t.Fatalf("another run's artifact: class = %q (%v)", class, err)
	}
	// Naming its own run does not launder a foreign artifact: the handler reads
	// the artifact by ID and never looks at the run, so the artifact's own run
	// is what the capability is compared against.
	err = authorizer.Authorize(ctx, supervisorPrincipal(),
		Action{Kind: QueryArtifact, WorkflowRunID: "run-1", ArtifactID: "artifact-theirs"})
	if class := ClassifySupervisionError(err); class != SupervisionErrorUnauthorizedScope {
		t.Fatalf("foreign artifact under its own run: class = %q (%v)", class, err)
	}
	// An artifact no run owns is refused too, rather than being read because a
	// permitted run was named beside it.
	if err := authorizer.Authorize(ctx, supervisorPrincipal(),
		Action{Kind: QueryArtifact, WorkflowRunID: "run-1", ArtifactID: "artifact-unknown"}); err == nil {
		t.Fatal("an unresolvable artifact was permitted under its own run")
	}
}

// The allowlist is a closed set. Everything outside it is refused, including
// every coordinator command kind, so a supervisor can never route to backlog
// start and around quota admission.
func TestSupervisorCapabilityRefusesEverythingOutsideItsAllowlist(t *testing.T) {
	_, reader := supervisionFixture(t)
	authorizer := SupervisorAuthorizer{Scope: reader, Delegate: allowAll{}}
	ctx := context.Background()
	for _, kind := range []domain.AdminCommandKind{
		domain.AdminCommandStart, domain.AdminCommandSkip, domain.AdminCommandRetry,
		domain.AdminCommandCancel, domain.AdminCommandPause, domain.AdminCommandResume,
	} {
		err := authorizer.Authorize(ctx, supervisorPrincipal(),
			Action{Kind: "mutation", CommandKind: kind, WorkflowRunID: "run-1"})
		if class := ClassifySupervisionError(err); class != SupervisionErrorUnauthorizedScope {
			t.Fatalf("%s: class = %q (%v)", kind, class, err)
		}
	}
	for _, kind := range []QueryKind{
		QueryWorkflows, QuerySchedules, QueryWorkers, QueryQuota, QueryCommands,
		QueryReservations, QueryLocks, QueryRecovery, QueryQuarantine, QueryStatus,
		QuarantineReleaseKind, "worker-enrollment", "graph-amendment", "node-wait",
	} {
		err := authorizer.Authorize(ctx, supervisorPrincipal(), Action{Kind: kind, WorkflowRunID: "run-1"})
		if err == nil {
			t.Fatalf("a supervisor was permitted %q", kind)
		}
	}
	// A run-scoped read of its own run is permitted; the same read naming no
	// run at all is not, because a run-less read is a fleet-wide one.
	if err := authorizer.Authorize(ctx, supervisorPrincipal(),
		Action{Kind: QueryGraph, WorkflowRunID: "run-1"}); err != nil {
		t.Fatalf("own run graph refused: %v", err)
	}
	if err := authorizer.Authorize(ctx, supervisorPrincipal(), Action{Kind: QueryGraph}); err == nil {
		t.Fatal("a run-less read was permitted")
	}
}

// A capability that carries a second role is not a narrow capability, and a
// principal with no live scope holds nothing at all.
func TestSupervisorCapabilityIsNotWidenedByASecondRole(t *testing.T) {
	_, reader := supervisionFixture(t)
	authorizer := SupervisorAuthorizer{Scope: reader, Delegate: allowAll{}}
	ctx := context.Background()
	widened := Principal{ID: "remote:overseer", Roles: []string{SupervisorRole, LocalAdminRole}}
	if err := authorizer.Authorize(ctx, widened, Action{Kind: QueryGraph, WorkflowRunID: "run-1"}); err == nil {
		t.Fatal("a second role widened a supervisor capability")
	}
	unknown := Principal{ID: "remote:stranger", Roles: []string{SupervisorRole}}
	err := authorizer.Authorize(ctx, unknown, Action{Kind: SupervisionShowKind, WorkflowRunID: "run-1"})
	if class := ClassifySupervisionError(err); class != SupervisionErrorUnauthorizedScope {
		t.Fatalf("principal without a scope: class = %q (%v)", class, err)
	}
	// An operator principal is delegated unchanged.
	if err := authorizer.Authorize(ctx, operatorPrincipal(), Action{Kind: QueryWorkflows}); err != nil {
		t.Fatalf("operator delegation refused: %v", err)
	}
}

func TestSupervisionIsUnavailableWithoutAStore(t *testing.T) {
	service, err := New(&countingReader{}, allowAll{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Supervise(context.Background(), operatorPrincipal(),
		SupervisionRequest{Version: SupervisionVersion, Operation: SupervisionShow, RunID: "run-1"})
	if class := ClassifySupervisionError(err); class != SupervisionErrorUnavailable {
		t.Fatalf("missing store: class = %q (%v)", class, err)
	}
}

func TestSupervisionRefusesAMutationAfterSettlement(t *testing.T) {
	service, reader := supervisionFixture(t)
	reader.state.SinkSettled = true
	_, err := service.Supervise(context.Background(), supervisorPrincipal(), acceptRequest())
	if !errors.Is(err, domain.ErrSupervisionTerminal) {
		t.Fatalf("decision after settlement: %v", err)
	}
}

func TestSupervisionRequestValidation(t *testing.T) {
	for name, request := range map[string]SupervisionRequest{
		"no version":   {Operation: SupervisionShow, RunID: "run-1"},
		"bad version":  {Version: "backlog.admin.supervision/v0", Operation: SupervisionShow, RunID: "run-1"},
		"no operation": {Version: SupervisionVersion, RunID: "run-1"},
		"no run":       {Version: SupervisionVersion, Operation: SupervisionShow},
		"no key": {
			Version: SupervisionVersion, Operation: SupervisionHold, RunID: "run-1",
			ExpectedRevision: 7, Reason: "because",
			Hold: &SupervisionHoldRequest{Scope: domain.HoldScope{Kind: domain.HoldScopeRun}},
		},
		"no revision": {
			Version: SupervisionVersion, Operation: SupervisionHold, RunID: "run-1",
			RequestKey: "k", Reason: "because",
			Hold: &SupervisionHoldRequest{Scope: domain.HoldScope{Kind: domain.HoldScopeRun}},
		},
		"two payloads": {
			Version: SupervisionVersion, Operation: SupervisionHold, RunID: "run-1",
			RequestKey: "k", ExpectedRevision: 7, Reason: "because",
			Hold:    &SupervisionHoldRequest{Scope: domain.HoldScope{Kind: domain.HoldScopeRun}},
			Release: &SupervisionReleaseRequest{HoldID: "hold-1"},
		},
		"branch without a root": {
			Version: SupervisionVersion, Operation: SupervisionHold, RunID: "run-1",
			RequestKey: "k", ExpectedRevision: 7, Reason: "because",
			Hold: &SupervisionHoldRequest{Scope: domain.HoldScope{Kind: domain.HoldScopeBranch}},
		},
		"resolve as gate acceptance": {
			Version: SupervisionVersion, Operation: SupervisionResolve, RunID: "run-1",
			RequestKey: "k", ExpectedRevision: 2, Reason: "because",
			Incident: &SupervisionIncidentRequest{
				IncidentID: "incident-1", Outcome: domain.IncidentOutcomeGateAccepted,
			},
		},
	} {
		if err := request.Validate(); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestSupervisionOperationWordsAreDeclaredAndClassified(t *testing.T) {
	if !ValidOperation(localOperationSupervisionShow) || !ValidOperation(localOperationSupervisionDecision) {
		t.Fatal("a supervision operation word is not in the declared vocabulary")
	}
	if mutatingOperation(localOperationSupervisionShow) {
		t.Fatal("showing supervision is replay-protected as a mutation")
	}
	if !mutatingOperation(localOperationSupervisionDecision) {
		t.Fatal("a supervision decision is not replay-protected")
	}
	for _, operation := range SupervisionOperations() {
		if operation == SupervisionShow {
			if operation.Mutating() || operation.ActionKind() != SupervisionShowKind {
				t.Fatalf("%s is classified as a mutation", operation)
			}
			continue
		}
		if !operation.Mutating() || operation.ActionKind() != SupervisionDecisionKind {
			t.Fatalf("%s is classified as a read", operation)
		}
	}
	// Neither word is a query kind: every declared query kind is granted to
	// the remote-admin role automatically, and deciding a gate is not a read.
	if IsQueryKind(SupervisionDecisionKind) {
		t.Fatal("a supervision decision is authorized as a read")
	}
}

// The admin credential namespace is unchanged by supervision: a worker
// protocol credential is still refused where an admin one is expected, and is
// named as such rather than reported as a generic malformed reference.
func TestWorkerCredentialsStayRefusedOnTheAdminNamespace(t *testing.T) {
	err := ValidateAdminCredentialReference(WorkerCredentialPrefix + "omarchy-pc")
	if err == nil {
		t.Fatal("a worker credential was accepted as an admin credential")
	}
	resolver := FileAdminCredentialResolver{Home: t.TempDir()}
	if _, err := resolver.ResolveAdmin(WorkerCredentialPrefix + "omarchy-pc"); err == nil {
		t.Fatal("a worker credential resolved on the admin path")
	}
	if err := ValidateAdminCredentialReference(AdminCredentialPrefix + "overseer"); err != nil {
		t.Fatalf("a supervisor admin reference was refused: %v", err)
	}
}

// The remote carrier decides the role from the coordinator's own
// configuration, never from anything the client sent. A client listed as a
// supervisor is relayed under the narrower role; every other client keeps the
// ordinary remote-admin role.
func TestRemoteCarrierRelaysASupervisorUnderTheNarrowerRole(t *testing.T) {
	credentials := testAdminCredentials()
	build := func(supervisors map[string]bool) *RemoteAdminAssertion {
		t.Helper()
		server, err := NewRemoteServer(RemoteServerConfig{
			CoordinatorID:   testCoordinatorID,
			Clients:         map[string]AdminCredentials{credentials.ClientPrincipal: credentials},
			Supervisors:     supervisors,
			MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		})
		if err != nil {
			t.Fatal(err)
		}
		request := localRequest{
			Version: LocalTransportVersion, Operation: localOperationSupervisionDecision,
			Supervision: &SupervisionRequest{
				Version: SupervisionVersion, Operation: SupervisionDecide, RunID: "run-1",
			},
		}
		frame, err := newRemoteFrame(request.Operation, "session-1", "req/1",
			credentials.ClientPrincipal, testCoordinatorID, 1, time.Now(), time.Now().Add(time.Minute), request)
		if err != nil {
			t.Fatal(err)
		}
		if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
			t.Fatal(err)
		}
		relayed, _, verified, protocolErr := server.validate("", frame)
		if protocolErr != nil || !verified {
			t.Fatalf("frame refused: %v", protocolErr)
		}
		if relayed.RemoteAdmin == nil {
			t.Fatal("the relayed request carries no assertion")
		}
		return relayed.RemoteAdmin
	}
	if role := build(nil).role(); role != RemoteAdminRole {
		t.Fatalf("an ordinary client was relayed as %q", role)
	}
	supervisor := build(map[string]bool{credentials.ClientPrincipal: true})
	if supervisor.Role != SupervisorRole || supervisor.role() != SupervisorRole {
		t.Fatalf("assertion = %+v", supervisor)
	}
}

// supervisionTransportService is a LocalService that also answers
// supervision, which is how the coordinator's own service reaches the carrier.
type supervisionTransportService struct {
	*localTransportService
	seen      []SupervisionRequest
	principal Principal
}

func (s *supervisionTransportService) Supervise(_ context.Context, principal Principal, request SupervisionRequest) (SupervisionResponse, error) {
	s.seen = append(s.seen, request)
	s.principal = principal
	return SupervisionResponse{
		Version: SupervisionVersion, Operation: request.Operation, RunID: request.RunID,
		GeneratedAt: supervisionTestNow,
	}, nil
}

// The operation word has to carry the request end to end, and the word and
// the request must agree: a key pinned to supervision-show must not be able to
// carry a decision.
func TestSupervisionTravelsOverTheLocalCarrier(t *testing.T) {
	service := &supervisionTransportService{localTransportService: &localTransportService{}}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
	defer stopLocalTransport(t, cancel, done)
	ctx := context.Background()

	show := SupervisionRequest{Version: SupervisionVersion, Operation: SupervisionShow, RunID: "run-1"}
	if _, err := client.Supervise(ctx, show); err != nil {
		t.Fatalf("show over the local carrier: %v", err)
	}
	if _, err := client.Supervise(ctx, acceptRequest()); err != nil {
		t.Fatalf("decide over the local carrier: %v", err)
	}
	if len(service.seen) != 2 || service.seen[1].Operation != SupervisionDecide {
		t.Fatalf("service saw %+v", service.seen)
	}
	// The principal is the one the carrier authenticated, never the one the
	// request claimed.
	if service.principal.ID == "" || len(service.principal.Roles) != 1 ||
		service.principal.Roles[0] != LocalAdminRole {
		t.Fatalf("principal = %+v", service.principal)
	}

	mismatched := localRequest{
		Version: LocalTransportVersion, Operation: localOperationSupervisionShow,
		Supervision: &SupervisionRequest{
			Version: SupervisionVersion, Operation: SupervisionDecide, RunID: "run-1",
		},
	}
	var response localResponse
	err := client.call(ctx, mismatched, &response)
	if err == nil {
		t.Fatal("a decision travelled under the read word")
	}
	if len(service.seen) != 2 {
		t.Fatalf("the mismatched request reached the service: %+v", service.seen)
	}

	// A supervision payload riding another operation is refused too.
	stowaway := localRequest{
		Version: LocalTransportVersion, Operation: localOperationQuery,
		Query:       &Query{Version: Version, Kind: QueryStatus},
		Supervision: &SupervisionRequest{Version: SupervisionVersion, Operation: SupervisionDecide, RunID: "run-1"},
	}
	if err := client.call(ctx, stowaway, &response); err == nil {
		t.Fatal("a supervision payload rode a query")
	}
}

// The relayed assertion may narrow authority and never widen it.
func TestRemoteAssertionNarrowsToSupervisorAndNeverToLocalAdmin(t *testing.T) {
	supervisor := &RemoteAdminAssertion{
		Principal: "admin:overseer", Coordinator: "c1", RequestID: "r1", Role: SupervisorRole,
	}
	if !supervisor.valid("c1") || supervisor.role() != SupervisorRole {
		t.Fatalf("supervisor assertion = %+v", supervisor)
	}
	ordinary := &RemoteAdminAssertion{Principal: "admin:omarchy-pc", Coordinator: "c1", RequestID: "r1"}
	if !ordinary.valid("c1") || ordinary.role() != RemoteAdminRole {
		t.Fatalf("ordinary assertion = %+v", ordinary)
	}
	for _, role := range []string{LocalAdminRole, "operator", "admin"} {
		widened := &RemoteAdminAssertion{
			Principal: "admin:overseer", Coordinator: "c1", RequestID: "r1", Role: role,
		}
		if widened.valid("c1") {
			t.Fatalf("an assertion claiming %q was accepted", role)
		}
	}
}
