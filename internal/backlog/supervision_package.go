package backlog

// The execution package one overseer activation is started from.
//
// It is the same immutable package a task gets, with three differences that
// follow from an activation not being a task: it names a fresh task-scoped
// workspace so it holds no lock the reviewed tasks need, it carries the bounded
// activation snapshot and the exact scoped commands instead of outputs and
// verification, and it requires the campaign supervision capability so an older
// worker refuses it by name rather than running a review as work.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ActivationSetupProfile is the setup profile name an activation package
// carries. Nothing prepares a repository for an activation, so the profile
// exists to satisfy the package's own identity requirements and to make an
// activation workspace recognisable on a worker host.
const ActivationSetupProfile = "supervision-activation"

// ActivationCLI is the program the overseer's scoped commands name. It is the
// coordinator-admin CLI the worker host already has, because the plan forbids
// inventing a second decision channel.
const ActivationCLI = "t3-steward"

// ActivationGateFacts is one gate as the coordinator knows it, with the exact
// revisions a decision about it must name.
type ActivationGateFacts struct {
	Gate     domain.Gate
	Evidence *domain.EvidenceSnapshot
}

// SupervisionOfferSource is the supervision state an activation offer is
// rendered from. It is two reads, both of one run, and neither of them decides
// anything: the activation and its inbox say what this overseer was woken for,
// and the admin state says which gates and incidents it may act on.
type SupervisionOfferSource interface {
	LoadSupervisionActivationState(ctx context.Context, runID string) (SupervisionActivationState, error)
	LoadSupervisionAdminState(ctx context.Context, runID string) (sqlite.SupervisionAdminState, error)
}

// buildActivationOffer renders the offer of one activation assignment.
//
// It reads the activation rather than being told about it, because the offer is
// built at delivery time and the durable record is the only thing that is still
// true then: a lease renewed since the dispatch was planned, or an epoch raised
// by an operator takeover, must reach the worker as it now stands.
func (b CoordinatorOfferBuilder) buildActivationOffer(
	ctx context.Context,
	records sqlite.CoordinatorRecords,
	attempt domain.Attempt,
	assignment domain.Assignment,
	expiresAt time.Time,
) (workerproto.AssignmentOffer, error) {
	if b.Supervision == nil {
		return workerproto.AssignmentOffer{}, errors.New(
			"execution package builder: this coordinator reads no supervision state, so it cannot offer an activation")
	}
	advertised, known, err := b.advertisedCapabilities(ctx, assignment.WorkerID)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	if !known {
		return workerproto.AssignmentOffer{}, fmt.Errorf(
			"supervision activation: worker %q capabilities are unknown", assignment.WorkerID)
	}
	if err := RequireSupervisionCapability(assignment.WorkerID, advertised); err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	state, err := b.Supervision.LoadSupervisionActivationState(ctx, attempt.WorkflowRunID)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	if state.Activation.ID != attempt.SupervisionActivationID ||
		state.Activation.Epoch != attempt.SupervisionActivationEpoch {
		// The run has moved to another activation epoch. Delivering this one
		// would start a second overseer whose decisions are already fenced out.
		return workerproto.AssignmentOffer{}, fmt.Errorf(
			"supervision activation: assignment %q carries activation %q at epoch %d, the run is at %q epoch %d",
			assignment.ID, attempt.SupervisionActivationID, attempt.SupervisionActivationEpoch,
			state.Activation.ID, state.Activation.Epoch)
	}
	admin, err := b.Supervision.LoadSupervisionAdminState(ctx, attempt.WorkflowRunID)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	var run domain.WorkflowRun
	for _, candidate := range records.WorkflowRuns {
		if candidate.ID == attempt.WorkflowRunID {
			run = candidate
		}
	}
	if run.ID == "" {
		return workerproto.AssignmentOffer{}, fmt.Errorf(
			"supervision activation: run %q is not a coordinator record", attempt.WorkflowRunID)
	}
	var workflow domain.Workflow
	for _, candidate := range records.Workflows {
		if candidate.ID == run.WorkflowID {
			workflow = candidate
		}
	}
	gates := make([]ActivationGateFacts, 0, len(admin.Gates))
	for _, facts := range admin.Gates {
		gates = append(gates, ActivationGateFacts{Gate: facts.Gate, Evidence: facts.Evidence})
	}
	incidents := make([]domain.ReviewIncident, 0, len(admin.Incidents))
	for _, facts := range admin.Incidents {
		incidents = append(incidents, facts.Incident)
	}
	var artifacts []domain.Artifact
	for _, artifact := range records.Artifacts {
		if artifact.WorkflowRunID == run.ID {
			artifacts = append(artifacts, artifact)
		}
	}
	var attempts []domain.Attempt
	for _, candidate := range records.Attempts {
		if candidate.WorkflowRunID == run.ID {
			attempts = append(attempts, candidate)
		}
	}
	inbox := CoalesceSupervisionEvents(run.ID, state.Record.EventCursor, state.Pending)
	if len(inbox.Triggers) == 0 {
		return workerproto.AssignmentOffer{}, fmt.Errorf(
			"supervision activation: run %q has nothing pending for activation %q to review",
			run.ID, state.Activation.ID)
	}
	dispatch := ActivationDispatch{
		Identity: state.Activation.DispatchIdentity, Epoch: state.Activation.Epoch,
		LeaseToken: state.Activation.LeaseToken, RequiredCapability: SupervisionWorkerCapability,
	}
	if state.Activation.LeaseExpiresAt != nil {
		dispatch.LeaseExpiresAt = state.Activation.LeaseExpiresAt.UTC()
	}
	if state.Activation.Deadline != nil {
		dispatch.Deadline = state.Activation.Deadline.UTC()
	}
	input := ActivationPackageInput{
		Workflow: workflow, Run: run, Record: state.Record, Activation: state.Activation,
		Dispatch: dispatch, Attempt: attempt, Assignment: assignment,
		Triggers: inbox.Triggers,
		Tasks:    domain.TasksForRun(run, records.Tasks),
		Attempts: attempts, Gates: gates, Incidents: incidents, Artifacts: artifacts,
		SupervisorPrincipal:           b.SupervisorPrincipal,
		SupervisorCredentialReference: b.SupervisorCredentialReference,
		CoordinatorID:                 b.CoordinatorID,
		CoordinatorEpoch:              b.CoordinatorEpoch,
		CatalogRevision:               b.CatalogRevision,
		PrepareTimeout:                b.VerificationTimeout,
		VerificationTimeout:           b.VerificationTimeout,
		MaxArtifactBytes:              b.MaxArtifactBytes,
		MaxTotalBytes:                 b.MaxTotalBytes,
		Now:                           assignment.CreatedAt,
	}
	if b.ActivationEvidence == nil {
		return workerproto.AssignmentOffer{}, &ActivationPackageError{
			Code:  ActivationPackageErrorEvidenceMissing,
			Cause: errors.New("coordinator activation evidence custody is not configured"),
		}
	}
	retained, reader, found, err := b.ActivationEvidence.OpenActivationEvidenceSnapshot(
		ctx, run.ID, state.Activation.ID)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	if found {
		defer reader.Close()
		data, err := io.ReadAll(reader)
		if err != nil {
			return workerproto.AssignmentOffer{}, fmt.Errorf("read retained activation evidence: %w", err)
		}
		input.Evidence, err = DecodeActivationEvidenceSnapshot(data)
		if err != nil {
			return workerproto.AssignmentOffer{}, err
		}
		input.EvidenceArtifact = retained
	} else {
		object, err := BuildActivationEvidenceForPackage(input)
		if err != nil {
			return workerproto.AssignmentOffer{}, err
		}
		artifact := ActivationEvidenceArtifact(run.ID, attempt.TaskID, attempt.ID, object, "", assignment.CreatedAt)
		retained, err := b.ActivationEvidence.EnsureActivationEvidenceSnapshot(ctx,
			sqlite.ActivationEvidencePublication{
				CoordinatorEpoch: b.CoordinatorEpoch,
				ActivationID:     state.Activation.ID, RunID: run.ID,
				ActivationEpoch: state.Activation.Epoch, GraphRevision: run.GraphRevision,
				Artifact: artifact,
			}, bytes.NewReader(object.Data))
		if err != nil {
			return workerproto.AssignmentOffer{}, err
		}
		input.Evidence, err = DecodeActivationEvidenceSnapshot(object.Data)
		if err != nil {
			return workerproto.AssignmentOffer{}, err
		}
		input.EvidenceArtifact = retained
	}
	return BuildActivationOffer(input, expiresAt)
}

// ActivationPackageInput is everything the package builder reads. It is plain
// values rather than a store handle so the whole build is a function: the same
// inputs always produce the same package, which is what lets an undelivered
// dispatch be retried byte for byte.
type ActivationPackageInput struct {
	Workflow   domain.Workflow
	Run        domain.WorkflowRun
	Record     domain.SupervisionRecord
	Activation domain.Activation
	Dispatch   ActivationDispatch
	Attempt    domain.Attempt
	Assignment domain.Assignment
	// Triggers are the coalesced events this activation was woken for. An
	// activation with none of them would be a wake with nothing to review.
	Triggers  []CoalescedTrigger
	Tasks     []domain.Task
	Attempts  []domain.Attempt
	Gates     []ActivationGateFacts
	Incidents []domain.ReviewIncident
	// Artifacts are the run's retained artifacts. Only their IDs and digests
	// reach the overseer; the bytes are fetched on demand through the artifact
	// reads the supervisor capability already allows.
	Artifacts []domain.Artifact
	// Evidence is the first immutable inventory frozen for this activation.
	// EvidenceArtifact is its retained coordinator artifact metadata.
	Evidence         ActivationEvidenceSnapshot
	EvidenceArtifact domain.Artifact
	// SupervisorPrincipal is the admin principal the CLI on the worker host
	// authenticates as. It is configuration rather than a derived value: the
	// coordinator has to be told which client it will see, and it records the
	// principal on the activation so its own authorizer can bind that client to
	// this run and this epoch.
	SupervisorPrincipal string
	// SupervisorCredentialReference names the credential the CLI resolves on the
	// worker host, in the existing admin credential convention. See the
	// co-tenancy limitation recorded on workerproto.SupervisionActivation.
	SupervisorCredentialReference string
	CoordinatorID                 string
	CoordinatorEpoch              int64
	CatalogRevision               string
	PrepareTimeout                time.Duration
	VerificationTimeout           time.Duration
	MaxArtifactBytes              int64
	MaxTotalBytes                 int64
	ByteCap                       int
	Now                           time.Time
}

// BuildActivationOffer renders one activation into the offer its worker is
// sent.
func BuildActivationOffer(input ActivationPackageInput, expiresAt time.Time) (workerproto.AssignmentOffer, error) {
	pkg, err := BuildActivationPackage(input)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		return workerproto.AssignmentOffer{}, err
	}
	return workerproto.AssignmentOffer{Assignment: input.Assignment, Package: manifest, ExpiresAt: expiresAt}, nil
}

// BuildActivationEvidenceForPackage creates the complete immutable inventory
// that must be persisted before the first dispatch of an activation. A retry
// opens and decodes the retained object instead of calling this helper again.
func BuildActivationEvidenceForPackage(input ActivationPackageInput) (ActivationEvidenceObject, error) {
	artifacts := activationArtifactDigests(input.Artifacts)
	var overseerPrompt domain.ArtifactDigest
	for _, artifact := range artifacts {
		if artifact.ArtifactID == input.Record.Config.PromptArtifactID {
			overseerPrompt = artifact
			break
		}
	}
	gateEvidence := make([]ActivationGateEvidence, 0, len(input.Gates))
	for _, facts := range input.Gates {
		var evidence *domain.EvidenceSnapshot
		if facts.Evidence != nil {
			copy := *facts.Evidence
			copy.Producers = append([]domain.ProducerEvidence(nil), facts.Evidence.Producers...)
			evidence = &copy
		}
		gateEvidence = append(gateEvidence, ActivationGateEvidence{Gate: facts.Gate, Evidence: evidence})
	}
	actions := ActivationScopedActions(input.Run.ID, input.Activation.Epoch)
	return BuildActivationEvidenceSnapshot(ActivationSnapshot{
		ActivationID: input.Activation.ID, RunID: input.Run.ID,
		Epoch: input.Activation.Epoch, GraphRevision: input.Run.GraphRevision,
		RecordRevision: input.Record.Revision, Deadline: input.Dispatch.Deadline,
		TurnsRemaining: input.Record.Config.MaxTurnsPerActivation - input.Activation.TurnsUsed,
		Tasks:          activationTaskViews(input.Tasks, input.Attempts),
		Gates:          activationGateViews(input.Gates),
		Incidents:      activationIncidentViews(input.Incidents),
		Triggers:       input.Triggers, Artifacts: artifacts,
		TaskContracts: append([]domain.Task(nil), input.Tasks...),
		Attempts:      append([]domain.Attempt(nil), domain.DeclaredTaskAttempts(input.Attempts)...),
		GateEvidence:  gateEvidence, OverseerPrompt: overseerPrompt,
		Actions:         activationPromptActions(actions),
		Constraints:     ActivationPromptConstraints(input),
		ConsumedThrough: input.Activation.ConsumedEventCursor,
	})
}

// BuildActivationPackage assembles the activation package.
func BuildActivationPackage(input ActivationPackageInput) (workerproto.ExecutionPackage, error) {
	switch {
	case input.CoordinatorID == "" || input.CoordinatorEpoch < 1 || input.CatalogRevision == "":
		return workerproto.ExecutionPackage{}, errors.New("supervision activation package: coordinator identity and catalog revision are required")
	case input.PrepareTimeout <= 0 || input.VerificationTimeout <= 0 ||
		input.MaxArtifactBytes <= 0 || input.MaxTotalBytes < input.MaxArtifactBytes:
		return workerproto.ExecutionPackage{}, errors.New("supervision activation package: complete limits are required")
	case strings.TrimSpace(input.SupervisorPrincipal) == "":
		return workerproto.ExecutionPackage{}, errors.New("supervision activation package: a supervisor principal is required")
	case input.Assignment.ID == "" || input.Assignment.AttemptID != input.Attempt.ID:
		return workerproto.ExecutionPackage{}, errors.New("supervision activation package: the assignment is not attached to the activation attempt")
	case !input.Attempt.IsSupervisionActivation():
		return workerproto.ExecutionPackage{}, errors.New("supervision activation package: the attempt does not carry an activation")
	}
	turns := input.Record.Config.MaxTurnsPerActivation
	if turns < 1 {
		return workerproto.ExecutionPackage{}, errors.New("supervision activation package: max_turns_per_activation must be positive")
	}
	actions := ActivationScopedActions(input.Run.ID, input.Activation.Epoch)
	if input.Evidence.ActivationID != input.Activation.ID ||
		input.Evidence.RunID != input.Run.ID ||
		input.Evidence.Epoch != input.Activation.Epoch ||
		input.Evidence.GraphRevision != input.Run.GraphRevision {
		return workerproto.ExecutionPackage{}, &ActivationPackageError{
			Code: ActivationPackageErrorEvidenceMismatch,
			Cause: fmt.Errorf("frozen evidence identity does not match activation %s epoch %d graph %d",
				input.Activation.ID, input.Activation.Epoch, input.Run.GraphRevision),
		}
	}
	if err := validateActivationEvidenceArtifact(input.Evidence, input.EvidenceArtifact); err != nil {
		return workerproto.ExecutionPackage{}, err
	}
	evidenceObject, err := packageArtifact(input.EvidenceArtifact, "inputs/supervision-evidence.json", "input")
	if err != nil {
		return workerproto.ExecutionPackage{}, &ActivationPackageError{Code: ActivationPackageErrorEvidenceMismatch, Cause: err}
	}
	evidenceDigest := domain.ArtifactDigest{ArtifactID: input.EvidenceArtifact.ID, Digest: input.EvidenceArtifact.SHA256}
	activation := &workerproto.SupervisionActivation{
		ActivationID: input.Activation.ID, RunID: input.Run.ID,
		Epoch: input.Activation.Epoch, RecordRevision: input.Record.Revision,
		GraphRevision:       input.Run.GraphRevision,
		Principal:           input.SupervisorPrincipal,
		CredentialReference: input.SupervisorCredentialReference,
		LeaseToken:          input.Dispatch.LeaseToken,
		LeaseExpiresAt:      input.Dispatch.LeaseExpiresAt.UTC(),
		MaxTurns:            turns,
		Actions:             actions,
	}
	if !input.Dispatch.Deadline.IsZero() {
		activation.Deadline = input.Dispatch.Deadline.UTC()
	}
	finalCap := input.ByteCap
	if finalCap <= 0 {
		finalCap = workerproto.SupervisionPromptByteCap
	}
	overhead := len(workerproto.RenderSupervisionPrompt(*activation))
	if overhead >= finalCap {
		return workerproto.ExecutionPackage{}, &ActivationPackageError{
			Code:  ActivationPackageErrorPromptTooLarge,
			Cause: fmt.Errorf("mandatory activation authority is %d bytes for a %d byte final prompt cap", overhead, finalCap),
		}
	}
	snapshot := ActivationSnapshot{
		ActivationID:     input.Activation.ID,
		RunID:            input.Run.ID,
		Epoch:            input.Activation.Epoch,
		GraphRevision:    input.Run.GraphRevision,
		RecordRevision:   input.Record.Revision,
		Deadline:         input.Dispatch.Deadline,
		TurnsRemaining:   turns - input.Activation.TurnsUsed,
		Tasks:            input.Evidence.Tasks,
		Gates:            input.Evidence.Gates,
		Incidents:        input.Evidence.Incidents,
		Triggers:         input.Evidence.Triggers,
		Artifacts:        input.Evidence.Artifacts,
		EvidenceSnapshot: &evidenceDigest,
		Actions:          activationPromptActions(actions),
		Constraints:      ActivationPromptConstraints(input),
		ConsumedThrough:  input.Activation.ConsumedEventCursor,
		ByteCap:          finalCap - overhead,
	}
	envelope, err := BuildActivationPromptEnvelope(snapshot)
	if err != nil {
		return workerproto.ExecutionPackage{}, err
	}
	rendered := envelope.Render()
	if strings.TrimSpace(rendered) == "" {
		return workerproto.ExecutionPackage{}, errors.New("supervision activation package: the activation snapshot rendered empty")
	}
	prompt, err := activationPromptArtifact(input)
	if err != nil {
		return workerproto.ExecutionPackage{}, err
	}
	project := input.Workflow.Project
	if project == "" {
		project = input.Run.ID
	}
	deadline := input.Dispatch.Deadline
	pkg := workerproto.ExecutionPackage{
		Version:          workerproto.ExecutionPackageVersion,
		ID:               stableCoordinatorID("package", input.Assignment.ID),
		CoordinatorID:    input.CoordinatorID,
		CoordinatorEpoch: input.CoordinatorEpoch,
		WorkerID:         input.Assignment.WorkerID,
		WorkerEpoch:      input.Assignment.WorkerEpoch,
		Identity: workerproto.ExecutionIdentity{
			WorkflowID: input.Workflow.ID, WorkflowRunID: input.Run.ID,
			TaskID: input.Attempt.TaskID, AttemptID: input.Attempt.ID,
			AttemptRevision: input.Attempt.Revision,
			AssignmentID:    input.Assignment.ID, AssignmentEpoch: input.Assignment.Epoch,
			DispatchToken: input.Assignment.DispatchToken, ThreadID: input.Assignment.ThreadID,
		},
		// An activation is required work: it is the thing a blocked run is
		// waiting for, so running it only out of forecast surplus would let a
		// busy fleet leave every gate undecided.
		Class:        domain.TaskClassRequired,
		Prompt:       prompt,
		StaticInputs: []workerproto.ArtifactObject{evidenceObject},
		Route:        cloneProviderRoute(input.Assignment.Route),
		Environment: workerproto.EnvironmentReference{
			Type: "fresh", CatalogRevision: input.CatalogRevision, Project: project,
			Scope: "task", SetupProfile: ActivationSetupProfile,
		},
		RequiredCapabilities: []string{
			workerproto.CapabilityCampaignSupervision,
			workerproto.PackageCapabilitySupervisionEvidence,
		},
		Supervision: activation,
		Limits: workerproto.ExecutionLimits{
			MaxTurns: turns, PrepareTimeout: input.PrepareTimeout,
			VerificationTimeout: input.VerificationTimeout,
			MaxArtifactBytes:    input.MaxArtifactBytes, MaxTotalBytes: input.MaxTotalBytes,
		},
		CreatedAt: input.Now.UTC(),
	}
	if !deadline.IsZero() {
		bounded := deadline.UTC()
		pkg.Deadline = &bounded
		pkg.Supervision.Deadline = bounded
		// The package expiry is the activation's own maximum elapsed time. A
		// review that outlives its deadline has already lost its authority, so
		// letting the package outlive it would only produce a turn nobody may
		// act on.
		expiry := bounded
		pkg.ExpiresAt = &expiry
	}
	pkg.Supervision.Prompt = rendered
	finalPrompt := workerproto.RenderSupervisionPrompt(*pkg.Supervision)
	if len(finalPrompt) > finalCap {
		return workerproto.ExecutionPackage{}, &ActivationPackageError{
			Code:  ActivationPackageErrorPromptTooLarge,
			Cause: fmt.Errorf("final activation prompt is %d bytes over its %d byte cap", len(finalPrompt)-finalCap, finalCap),
		}
	}
	return pkg, nil
}

// ActivationPromptConstraints states the limits that are not already rendered
// from the snapshot's own fields: who the overseer is, what it may not do, and
// the one thing about credentials that must not be overstated.
func ActivationPromptConstraints(input ActivationPackageInput) []string {
	constraints := []string{
		"supervisor principal: " + input.SupervisorPrincipal,
		"run under review: " + input.Run.ID,
		"every mutating command needs --request-id KEY; repeating a key with the same payload returns the first answer, and the same key with a different payload is refused",
		"a decision exists only when one of the commands below returns success; this turn ending is not a decision",
		"do not start, retry, skip, cancel or amend any task, and do not clear an operator hold",
		"do not delegate this review to a native subagent; every separately scheduled session is a campaign task the manifest declared",
	}
	if input.SupervisorCredentialReference != "" {
		constraints = append(constraints,
			"admin credential reference: "+input.SupervisorCredentialReference,
			// The CLI discovers this identity by itself, from the record the worker
			// wrote into this activation's workspace. The flag is stated in full
			// because the discovery depends on the working directory: a command run
			// from outside the workspace, or in a shell that changed directory, has
			// to name the credential or it will be signed as this host's own admin
			// client and recorded as an operator decision rather than yours.
			"run every command below from this activation's workspace, where the steward wrote "+
				workerproto.SupervisorIdentityFile+", so the CLI authenticates as the supervisor; "+
				"from anywhere else add the exact flag "+
				"--supervisor-credential "+input.SupervisorCredentialReference,
			"check the actor on the receipt: a decision recorded with actor kind operator was not "+
				"recorded as yours, and the activation records that an operator decided it")
	}
	return constraints
}

// ActivationScopedActions is the exact action set an activation is given, with
// the exact command line for each.
//
// The commands are the ones "t3-steward campaign supervision --help" documents,
// with the run and the activation epoch already filled in and every remaining
// placeholder in upper case. The coordinator authorizes each invocation on its
// own side against the activation's live lease, epoch and record revision; this
// list tells the overseer what to type, and grants nothing.
func ActivationScopedActions(runID string, epoch int64) []workerproto.SupervisionAction {
	activation := strconv.FormatInt(epoch, 10)
	base := func(verb string) []string {
		return []string{ActivationCLI, "campaign", "supervision", verb, runID}
	}
	mutating := []string{"--activation", activation, "--request-id", "KEY", "--reason", "TEXT"}
	with := func(verb string, arguments ...string) []string {
		return append(append(base(verb), arguments...), mutating...)
	}
	return []workerproto.SupervisionAction{{
		Name:    "show",
		Command: append(base("show"), "--json"),
		Constraints: []string{
			"read the current gate, hold and incident revisions here before every decision",
		},
	}, {
		Name: "decide",
		Command: with("decide", "--gate", "GATE_ID", "--accept", "--evidence", "EVIDENCE_SNAPSHOT_ID",
			"--expected-revision", "GATE_REVISION", "--graph-revision", "GRAPH_REVISION", "--incident", "INCIDENT_ID"),
		Constraints: []string{
			"exactly one of --accept and --reject",
			"--evidence is the snapshot the gate is ready against, and --expected-revision is that gate's revision",
			"a rejection records the required corrections in --reason; it never retries or rewrites a task",
		},
	}, {
		Name:    "hold",
		Command: with("hold", "--scope", "run|branch:TASK_ID", "--expected-revision", "RECORD_REVISION"),
		Constraints: []string{
			"--expected-revision is the supervision record's revision",
			"a hold withholds future dispatch; it never interrupts work that already started",
		},
	}, {
		Name:    "release",
		Command: with("release", "--hold", "HOLD_ID", "--expected-revision", "RECORD_REVISION"),
		Constraints: []string{
			"only a hold this activation placed; an operator hold is not yours to clear",
		},
	}, {
		Name:    "escalate",
		Command: with("escalate", "--incident", "INCIDENT_ID", "--expected-revision", "INCIDENT_REVISION"),
		Constraints: []string{
			"escalate whenever the evidence does not support a decision you are scoped to make",
		},
	}, {
		Name: "resolve",
		Command: with("resolve", "--incident", "INCIDENT_ID", "--expected-revision", "INCIDENT_REVISION",
			"--outcome", "conclude-failure"),
		Constraints: []string{
			"conclude-failure acknowledges an observed terminal task failure for settlement; it changes no task and cancels nothing",
			"a gate acceptance closes its own incident through decide --accept, never through resolve",
		},
	}}
}

// activationPromptActions projects the scoped commands into the prompt
// envelope's action shape, with the command line itself as the first
// constraint so a truncated read still sees what to type.
func activationPromptActions(actions []workerproto.SupervisionAction) []ActivationAction {
	prompt := make([]ActivationAction, 0, len(actions))
	for _, action := range actions {
		prompt = append(prompt, ActivationAction{
			Name:        action.Name,
			Constraints: append([]string{strings.Join(action.Command, " ")}, action.Constraints...),
		})
	}
	return prompt
}

// activationPromptArtifact resolves the run's declared overseer prompt. It is
// the operator's own standing instruction to the overseer, retained at
// ingestion, and it is separate from the per-activation snapshot for the same
// reason a task's prompt is separate from its inputs: one is authored once, the
// other is assembled per dispatch.
func activationPromptArtifact(input ActivationPackageInput) (workerproto.ArtifactObject, error) {
	id := input.Record.Config.PromptArtifactID
	for _, artifact := range input.Artifacts {
		if artifact.ID != id {
			continue
		}
		if artifact.WorkflowRunID != input.Run.ID || artifact.Kind != domain.ArtifactInput {
			return workerproto.ArtifactObject{}, fmt.Errorf(
				"supervision activation package: overseer prompt %q is not an input of this run", id)
		}
		return packageArtifact(artifact, "prompt/overseer.md", "prompt")
	}
	return workerproto.ArtifactObject{}, fmt.Errorf(
		"supervision activation package: overseer prompt %q is not a retained artifact", id)
}

// activationArtifactDigests names every retained artifact of the run by ID and
// digest. The bytes stay in coordinator custody and are read on demand.
func activationArtifactDigests(artifacts []domain.Artifact) []domain.ArtifactDigest {
	digests := make([]domain.ArtifactDigest, 0, len(artifacts))
	for _, artifact := range artifacts {
		if artifact.ID == "" || artifact.SHA256 == "" {
			continue
		}
		digests = append(digests, domain.ArtifactDigest{ArtifactID: artifact.ID, Digest: artifact.SHA256})
	}
	sort.Slice(digests, func(left, right int) bool { return digests[left].ArtifactID < digests[right].ArtifactID })
	return digests
}

// activationTaskViews reports each declared task's latest attempt. The
// coordinator's own progress is the only statement about success an activation
// may rely on, so nothing a worker wrote appears here.
func activationTaskViews(tasks []domain.Task, attempts []domain.Attempt) []ActivationTaskView {
	latest := make(map[string]domain.Attempt, len(tasks))
	for _, attempt := range domain.DeclaredTaskAttempts(attempts) {
		if current, seen := latest[attempt.TaskID]; !seen || attempt.Number > current.Number {
			latest[attempt.TaskID] = attempt
		}
	}
	views := make([]ActivationTaskView, 0, len(tasks))
	for _, task := range tasks {
		attempt := latest[task.ID]
		view := ActivationTaskView{
			TaskID: task.ID, State: string(attempt.Progress),
			AttemptID: attempt.ID, AttemptRevision: attempt.Revision,
		}
		if attempt.Progress == domain.ProgressSucceeded {
			view.Verification = "passed"
		} else if attempt.Progress.Terminal() {
			view.Verification = "not passed"
		}
		views = append(views, view)
	}
	sort.Slice(views, func(left, right int) bool { return views[left].TaskID < views[right].TaskID })
	return views
}

func activationGateViews(gates []ActivationGateFacts) []ActivationGateView {
	views := make([]ActivationGateView, 0, len(gates))
	for _, facts := range gates {
		view := ActivationGateView{
			GateID: facts.Gate.Definition.ID, State: facts.Gate.State,
			GraphRevision:    facts.Gate.GraphRevision,
			ObservedTaskIDs:  facts.Gate.Definition.ObservedTaskIDs,
			ProtectedTaskIDs: facts.Gate.Definition.ProtectedTaskIDs,
		}
		if facts.Evidence != nil {
			view.EvidenceSnapshotID = facts.Evidence.ID
		}
		views = append(views, view)
	}
	sort.Slice(views, func(left, right int) bool { return views[left].GateID < views[right].GateID })
	return views
}

func activationIncidentViews(incidents []domain.ReviewIncident) []ActivationIncidentView {
	views := make([]ActivationIncidentView, 0, len(incidents))
	for _, incident := range incidents {
		if incident.State == domain.IncidentResolved {
			continue
		}
		views = append(views, ActivationIncidentView{
			IncidentID: incident.ID, State: incident.State,
			RequiredDisposition: incident.RequiredDisposition,
			Revision:            incident.Revision, Reason: incident.Reason,
		})
	}
	sort.Slice(views, func(left, right int) bool { return views[left].IncidentID < views[right].IncidentID })
	return views
}
