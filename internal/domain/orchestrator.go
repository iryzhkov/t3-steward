package domain

import (
	"encoding/json"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

// TaskClass controls whether work consumes reserved capacity or only forecast
// surplus. Required work is still subject to hard quota admission closures.
type TaskClass string

const (
	TaskClassRequired TaskClass = "required"
	TaskClassSurplus  TaskClass = "surplus"
)

// ProgressState describes durable workflow progress. It intentionally excludes
// quota-control states such as draining and paused.
type ProgressState string

const (
	ProgressQueued     ProgressState = "queued"
	ProgressBlocked    ProgressState = "blocked"
	ProgressReady      ProgressState = "ready"
	ProgressActive     ProgressState = "active"
	ProgressNeedsInput ProgressState = "needs-input"
	// ProgressWaitingExternal is an attempt whose current turn parked on a
	// machine condition that has not settled: a build, a review, a deploy.
	//
	// It is deliberately not ProgressNeedsInput. Needs-input means a human must
	// answer before the work can continue, and an operator reading a queue acts
	// on those two facts differently: one is a question addressed to them, the
	// other is a condition nobody has to do anything about yet.
	ProgressWaitingExternal ProgressState = "waiting-external"
	ProgressVerifying       ProgressState = "verifying"
	ProgressSucceeded       ProgressState = "succeeded"
	ProgressFailed          ProgressState = "failed"
	ProgressCancelled       ProgressState = "cancelled"
	ProgressSkipped         ProgressState = "skipped"
)

// Terminal reports whether no more execution can advance this progress state.
func (s ProgressState) Terminal() bool {
	switch s {
	case ProgressSucceeded, ProgressFailed, ProgressCancelled, ProgressSkipped:
		return true
	default:
		return false
	}
}

// ControlState describes how an attempt is being executed. It is independent of
// ProgressState so an active attempt can be paused without appearing complete.
type ControlState string

const (
	ControlUnassigned           ControlState = "unassigned"
	ControlPreparing            ControlState = "preparing"
	ControlRunning              ControlState = "running"
	ControlDraining             ControlState = "draining"
	ControlPaused               ControlState = "paused"
	ControlPausedUncheckpointed ControlState = "paused-uncheckpointed"
	ControlResuming             ControlState = "resuming"
	ControlStopped              ControlState = "stopped"
	// ControlWaitingExternal is an attempt parked on a task-bound wait. The
	// thread, workspace and assignment are still owned by the coordinator, but
	// no provider slot and no executor capacity are held, because a wait on a
	// build or a review lasts minutes to hours and holding a worker slot that
	// long turns one parked task into a stalled queue.
	//
	// It is explicitly not a quiescent state: see RunExecutionsQuiescent.
	ControlWaitingExternal ControlState = "waiting-external"
)

// HoldsProviderSlot reports whether the attempt occupies a provider concurrency
// slot. A paused attempt retains its reservation but releases the runtime slot.
func (s ControlState) HoldsProviderSlot() bool {
	switch s {
	case ControlPreparing, ControlRunning, ControlDraining, ControlResuming:
		return true
	default:
		return false
	}
}

// AdmissionState is the scheduler permission currently applied to a quota pool.
type AdmissionState string

const (
	AdmissionOpen        AdmissionState = "open"
	AdmissionConstrained AdmissionState = "constrained"
	AdmissionDraining    AdmissionState = "draining"
	AdmissionClosed      AdmissionState = "closed"
	AdmissionRecovering  AdmissionState = "recovering"
)

// ExecutionEnvironment describes the immutable project workspace requested by a workflow.
type ExecutionEnvironment struct {
	Type  string `json:"type"`
	Scope string `json:"scope"`
	Ref   string `json:"ref,omitempty"`
}

// Workflow is an immutable workflow definition after submission.
type Workflow struct {
	ID               string               `json:"id"`
	Version          int                  `json:"version"`
	Name             string               `json:"name"`
	Project          string               `json:"project,omitempty"`
	Environment      ExecutionEnvironment `json:"environment"`
	Class            TaskClass            `json:"class"`
	TaskIDs          []string             `json:"taskIds"`
	InputArtifactIDs []string             `json:"inputArtifactIds,omitempty"`
	CreatedAt        time.Time            `json:"createdAt"`
}

// WorkflowRun is one execution of a workflow definition.
type WorkflowRun struct {
	Graph            *GraphDefinition `json:"graph,omitempty"`
	ID               string           `json:"id"`
	WorkflowID       string           `json:"workflowId"`
	GraphRevision    int64            `json:"graphRevision,omitempty"`
	Sink             *SinkTask        `json:"sink,omitempty"`
	ScheduleID       string           `json:"scheduleId,omitempty"`
	TriggerID        string           `json:"triggerId,omitempty"`
	Progress         ProgressState    `json:"progress"`
	InputArtifactIDs []string         `json:"inputArtifactIds,omitempty"`
	Revision         int64            `json:"revision"`
	CreatedAt        time.Time        `json:"createdAt"`
	UpdatedAt        time.Time        `json:"updatedAt"`
	CompletedAt      *time.Time       `json:"completedAt,omitempty"`
}

// ArtifactDeclaration names an output a task promises to retain.
type ArtifactDeclaration struct {
	Name      string `json:"name"`
	MediaType string `json:"mediaType,omitempty"`
	// Commit declares that this output is a Git commit rather than a file in
	// the workspace. The retained artifact is the commit's provenance record,
	// and the commit itself is kept reachable under a campaign-scoped ref.
	Commit *CommitOutput `json:"commit,omitempty"`
}

// CommitOutput declares a Git commit a task produces for a downstream task.
type CommitOutput struct {
	// Revision is resolved in the producing workspace and defaults to HEAD.
	Revision string `json:"revision,omitempty"`
}

// Placement constrains the workers eligible to execute a task.
type Placement struct {
	Hosts        []string `json:"hosts,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
}

// ProviderRoute is one ordered worker and provider candidate for a task.
type ProviderRoute struct {
	WorkerID           string            `json:"workerId,omitempty"`
	ProviderInstanceID string            `json:"providerInstanceId"`
	Model              string            `json:"model"`
	Options            map[string]string `json:"options,omitempty"`
	QuotaPoolID        string            `json:"quotaPoolId,omitempty"`
}

// Task is an immutable node in a workflow definition.
type Task struct {
	// DirectoryBindings are resolved operator identities, never raw capsule paths.
	DirectoryBindings  []directoryresource.Binding `json:"directoryBindings,omitempty"`
	RunID              string                      `json:"runId,omitempty"`
	DefinitionRevision int64                       `json:"definitionRevision,omitempty"`
	Timeout            time.Duration               `json:"timeout,omitempty"`
	ExternalNeeds      []NodeRef                   `json:"externalNeeds,omitempty"`
	ID                 string                      `json:"id"`
	WorkflowID         string                      `json:"workflowId"`
	Name               string                      `json:"name"`
	Class              TaskClass                   `json:"class"`
	Needs              []string                    `json:"needs,omitempty"`
	PromptArtifactID   string                      `json:"promptArtifactId"`
	InputArtifactIDs   []string                    `json:"inputArtifactIds,omitempty"`
	DependencyInputs   map[string][]string         `json:"dependencyInputs,omitempty"`
	// CarriedInputs are dependency artifacts a rerun carried over from a
	// source run by reference. Their producer is not a node of this graph,
	// which is why they cannot be expressed as DependencyInputs.
	CarriedInputs []CarriedInput        `json:"carriedInputs,omitempty"`
	Outputs       []ArtifactDeclaration `json:"outputs,omitempty"`
	Verification  []string              `json:"verification,omitempty"`
	Placement     Placement             `json:"placement"`
	// ResourceDemand sizes the task independently of the eligibility rules in
	// Placement, which is why it is a sibling field rather than a member.
	ResourceDemand ResourceDemand `json:"resourceDemand,omitempty"`
	// Preflight is the ordered evidence the worker establishes after the
	// workspace is prepared and before a provider session is created. It is
	// durable task state rather than a dispatch-time lookup, so a retry
	// re-establishes the same declared baseline.
	Preflight     []PreflightStep `json:"preflight,omitempty"`
	Routes        []ProviderRoute `json:"routes,omitempty"`
	ResourceLocks []string        `json:"resourceLocks,omitempty"`
	Importance    int             `json:"importance"`
	Difficulty    int             `json:"difficulty"`
	EstimatedCost *float64        `json:"estimatedCost,omitempty"`
	MaxTurns      int             `json:"maxTurns"`
	NotBefore     *time.Time      `json:"notBefore,omitempty"`
	Deadline      *time.Time      `json:"deadline,omitempty"`
	ExpiresAt     *time.Time      `json:"expiresAt,omitempty"`
}

// Attempt is one try to complete a task in a workflow run.
type Attempt struct {
	ID                     string            `json:"id"`
	WorkflowRunID          string            `json:"workflowRunId"`
	TaskID                 string            `json:"taskId"`
	Number                 int               `json:"number"`
	Progress               ProgressState     `json:"progress"`
	Control                ControlState      `json:"control"`
	Revision               int64             `json:"revision"`
	LastTurnOutcomeID      string            `json:"lastTurnOutcomeId,omitempty"`
	LastTurnOutcomeMarker  TurnOutcomeMarker `json:"lastTurnOutcomeMarker,omitempty"`
	AssignmentID           string            `json:"assignmentId,omitempty"`
	ThreadID               string            `json:"threadId,omitempty"`
	CheckpointArtifactID   string            `json:"checkpointArtifactId,omitempty"`
	FinalSummaryArtifactID string            `json:"finalSummaryArtifactId,omitempty"`
	AdminNotBefore         *time.Time        `json:"adminNotBefore,omitempty"`
	AdminForceStart        bool              `json:"adminForceStart,omitempty"`
	Failure                string            `json:"failure,omitempty"`
	StartedAt              *time.Time        `json:"startedAt,omitempty"`
	UpdatedAt              time.Time         `json:"updatedAt"`
	CompletedAt            *time.Time        `json:"completedAt,omitempty"`
}

// TurnLive reports whether this attempt currently has a turn that a thread is
// executing for it, and that the thread may therefore still act on.
//
// A parked attempt counts. Its turn has not ended: the same thread resumes it
// when the wait settles, which is why it may register a second wait. An attempt
// that is being verified, released, paused or drained does not count, because
// whatever its thread believes, the turn it belonged to is over or is being
// taken away from it.
func (a Attempt) TurnLive() bool {
	switch a.Progress {
	case ProgressActive, ProgressWaitingExternal:
	default:
		return false
	}
	switch a.Control {
	case ControlPreparing, ControlRunning, ControlResuming, ControlWaitingExternal:
		return true
	default:
		return false
	}
}

// AssignmentState is the coordinator's knowledge of an assignment lease.
type AssignmentState string

const (
	AssignmentOffered   AssignmentState = "offered"
	AssignmentClaimed   AssignmentState = "claimed"
	AssignmentUnknown   AssignmentState = "unknown"
	AssignmentReleased  AssignmentState = "released"
	AssignmentCompleted AssignmentState = "completed"
)

// TaskAdmissionEstimate is the durable remaining quota and runtime estimate
// fixed when an assignment route is committed.
type TaskAdmissionEstimate struct {
	RemainingCost    float64       `json:"remainingCost"`
	ExpectedRuntime  time.Duration `json:"expectedRuntime"`
	CheckpointMargin time.Duration `json:"checkpointMargin"`
}

// Assignment commits one attempt to a worker and provider route. It is also
// the durable capacity reservation: its worker and its task's resource demand
// are what an executor registry is rebuilt from, and reaching a terminal state
// is what releases the capacity it held.
type Assignment struct {
	// Placement explains why this worker was chosen: the candidates, the
	// constraints that rejected the others, the preference scores and the
	// snapshots read. It is written once, with the assignment.
	Placement           *PlacementDecision     `json:"placement,omitempty"`
	GraphRevision       int64                  `json:"graphRevision,omitempty"`
	TaskRevision        int64                  `json:"taskRevision,omitempty"`
	TaskDigest          string                 `json:"taskDigest,omitempty"`
	ID                  string                 `json:"id"`
	AttemptID           string                 `json:"attemptId"`
	WorkerID            string                 `json:"workerId"`
	WorkerEpoch         string                 `json:"workerEpoch,omitempty"`
	Route               ProviderRoute          `json:"route"`
	Estimate            *TaskAdmissionEstimate `json:"estimate,omitempty"`
	State               AssignmentState        `json:"state"`
	Epoch               int64                  `json:"epoch"`
	LeaseToken          string                 `json:"leaseToken"`
	LeaseExpiresAt      time.Time              `json:"leaseExpiresAt"`
	DispatchToken       string                 `json:"dispatchToken"`
	ThreadID            string                 `json:"threadId,omitempty"`
	DispatchState       DispatchState          `json:"dispatchState,omitempty"`
	DispatchRevision    int64                  `json:"dispatchRevision,omitempty"`
	DispatchConfirmedAt *time.Time             `json:"dispatchConfirmedAt,omitempty"`
	DispatchError       string                 `json:"dispatchError,omitempty"`
	CreatedAt           time.Time              `json:"createdAt"`
	UpdatedAt           time.Time              `json:"updatedAt"`
}

// Schedule overlap, misfire, and failure policies.
type ScheduleOverlapPolicy string
type ScheduleMisfirePolicy string
type ScheduleFailurePolicy string

const (
	ScheduleOverlapForbid ScheduleOverlapPolicy = "forbid"

	ScheduleMisfireSkip ScheduleMisfirePolicy = "skip"

	ScheduleFailureNextCycle ScheduleFailurePolicy = "next-cycle"
	ScheduleFailureHold      ScheduleFailurePolicy = "hold"
)

// ScheduleTemplate is one immutable version of a recurring workflow definition.
// Schedule.Version selects the template used for future triggers; existing triggers
// retain the version they observed.
type ScheduleTemplate struct {
	ScheduleID   string                `json:"scheduleId"`
	Version      int                   `json:"version"`
	WorkflowID   string                `json:"workflowId"`
	Expression   string                `json:"expression"`
	Timezone     string                `json:"timezone"`
	Overlap      ScheduleOverlapPolicy `json:"overlap"`
	Misfire      ScheduleMisfirePolicy `json:"misfire"`
	AfterFailure ScheduleFailurePolicy `json:"afterFailure"`
	CreatedAt    time.Time             `json:"createdAt"`
}

// Schedule is the mutable projection of a recurring definition's current template and execution state.
type Schedule struct {
	ID            string                `json:"id"`
	Name          string                `json:"name"`
	Version       int                   `json:"version"`
	WorkflowID    string                `json:"workflowId"`
	Expression    string                `json:"expression"`
	Timezone      string                `json:"timezone"`
	Overlap       ScheduleOverlapPolicy `json:"overlap"`
	Misfire       ScheduleMisfirePolicy `json:"misfire"`
	AfterFailure  ScheduleFailurePolicy `json:"afterFailure"`
	Enabled       bool                  `json:"enabled"`
	ActiveRunID   string                `json:"activeRunId,omitempty"`
	NextNotBefore *time.Time            `json:"nextNotBefore,omitempty"`
	Revision      int64                 `json:"revision"`
	CreatedAt     time.Time             `json:"createdAt"`
	UpdatedAt     time.Time             `json:"updatedAt"`
}

// TriggerState records whether one nominal schedule firing created a run.
type TriggerState string

const (
	TriggerAccepted   TriggerState = "accepted"
	TriggerSuppressed TriggerState = "suppressed"
)

// Trigger is one observed firing of a schedule.
type Trigger struct {
	ID              string       `json:"id"`
	ScheduleID      string       `json:"scheduleId"`
	ScheduleVersion int          `json:"scheduleVersion"`
	NominalAt       time.Time    `json:"nominalAt"`
	OccurrenceKey   string       `json:"occurrenceKey"`
	State           TriggerState `json:"state"`
	WorkflowRunID   string       `json:"workflowRunId,omitempty"`
	Reason          string       `json:"reason,omitempty"`
	ObservedAt      time.Time    `json:"observedAt"`
}

// QuotaPool groups provider instances that consume the same provider limit.
type QuotaPool struct {
	ChecksDisabled      bool           `json:"checksDisabled,omitempty"`
	ID                  string         `json:"id"`
	Provider            string         `json:"provider"`
	AccountID           string         `json:"accountId,omitempty"`
	ProviderInstanceIDs []string       `json:"providerInstanceIds"`
	Buckets             []BucketKey    `json:"buckets,omitempty"`
	Admission           AdmissionState `json:"admission"`
	MaxConcurrent       int            `json:"maxConcurrent"`
	ActiveAssignments   int            `json:"activeAssignments"`
	UpdatedAt           time.Time      `json:"updatedAt"`
}

// ArtifactKind identifies how an immutable artifact participates in a run.
type ArtifactKind string

const (
	ArtifactInput        ArtifactKind = "input"
	ArtifactOutput       ArtifactKind = "output"
	ArtifactCheckpoint   ArtifactKind = "checkpoint"
	ArtifactLog          ArtifactKind = "log"
	ArtifactSummary      ArtifactKind = "summary"
	ArtifactGitState     ArtifactKind = "git-state"
	ArtifactVerification ArtifactKind = "verification"
)

// Artifact is immutable metadata for retained content.
type Artifact struct {
	ID            string       `json:"id"`
	WorkflowRunID string       `json:"workflowRunId"`
	TaskID        string       `json:"taskId,omitempty"`
	AttemptID     string       `json:"attemptId,omitempty"`
	Kind          ArtifactKind `json:"kind"`
	Name          string       `json:"name"`
	MediaType     string       `json:"mediaType"`
	Size          int64        `json:"size"`
	SHA256        string       `json:"sha256"`
	StoragePath   string       `json:"storagePath"`
	Producer      string       `json:"producer"`
	CreatedAt     time.Time    `json:"createdAt"`
}

// ArtifactRetentionSkip reports one run whose artifacts a retention pass left
// alone, and why.
//
// A pinned run cannot be pruned: the pin is what a rerun, a node wait or a
// cross-run edge holds so that the evidence it points at stays readable. A pass
// that met one used to abort, which pruned nothing anywhere, so a single rerun
// held the whole fleet's retention. Skipping and saying so is the difference
// between a policy that is not applied here and a policy that is not applied at
// all.
type ArtifactRetentionSkip struct {
	WorkflowRunID string `json:"workflowRunId"`
	// Artifacts is how many of the run's artifacts were old enough to prune.
	Artifacts int `json:"artifacts"`
	// Reason names the owners of the pins that held them, so an operator can
	// find what is still referring to the run rather than guessing.
	Reason string `json:"reason"`
}

// AdminCommandState is the lifecycle of an audited mutation request.
type AdminCommandState string

const (
	AdminCommandPending  AdminCommandState = "pending"
	AdminCommandApplied  AdminCommandState = "applied"
	AdminCommandRejected AdminCommandState = "rejected"
	AdminCommandFailed   AdminCommandState = "failed"
)

// AdminCommand is a version-checked request to change scheduler intent.
type AdminCommand struct {
	ID               string            `json:"id"`
	Kind             AdminCommandKind  `json:"kind"`
	TargetType       AdminTargetType   `json:"targetType"`
	TargetID         string            `json:"targetId"`
	ExpectedRevision int64             `json:"expectedRevision"`
	Reason           string            `json:"reason"`
	RequestedBy      string            `json:"requestedBy"`
	Payload          json.RawMessage   `json:"payload,omitempty"`
	State            AdminCommandState `json:"state"`
	Failure          string            `json:"failure,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	AppliedAt        *time.Time        `json:"appliedAt,omitempty"`
}
