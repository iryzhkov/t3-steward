package domain

import (
	"encoding/json"
	"time"
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
	ProgressVerifying  ProgressState = "verifying"
	ProgressSucceeded  ProgressState = "succeeded"
	ProgressFailed     ProgressState = "failed"
	ProgressCancelled  ProgressState = "cancelled"
	ProgressSkipped    ProgressState = "skipped"
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
	ID               string        `json:"id"`
	WorkflowID       string        `json:"workflowId"`
	ScheduleID       string        `json:"scheduleId,omitempty"`
	TriggerID        string        `json:"triggerId,omitempty"`
	Progress         ProgressState `json:"progress"`
	InputArtifactIDs []string      `json:"inputArtifactIds,omitempty"`
	Revision         int64         `json:"revision"`
	CreatedAt        time.Time     `json:"createdAt"`
	UpdatedAt        time.Time     `json:"updatedAt"`
	CompletedAt      *time.Time    `json:"completedAt,omitempty"`
}

// ArtifactDeclaration names an output a task promises to retain.
type ArtifactDeclaration struct {
	Name      string `json:"name"`
	MediaType string `json:"mediaType,omitempty"`
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
	ID               string                `json:"id"`
	WorkflowID       string                `json:"workflowId"`
	Name             string                `json:"name"`
	Class            TaskClass             `json:"class"`
	Needs            []string              `json:"needs,omitempty"`
	PromptArtifactID string                `json:"promptArtifactId"`
	InputArtifactIDs []string              `json:"inputArtifactIds,omitempty"`
	DependencyInputs map[string][]string   `json:"dependencyInputs,omitempty"`
	Outputs          []ArtifactDeclaration `json:"outputs,omitempty"`
	Verification     []string              `json:"verification,omitempty"`
	Placement        Placement             `json:"placement"`
	Routes           []ProviderRoute       `json:"routes,omitempty"`
	ResourceLocks    []string              `json:"resourceLocks,omitempty"`
	Importance       int                   `json:"importance"`
	Difficulty       int                   `json:"difficulty"`
	EstimatedCost    *float64              `json:"estimatedCost,omitempty"`
	MaxTurns         int                   `json:"maxTurns"`
	NotBefore        *time.Time            `json:"notBefore,omitempty"`
	Deadline         *time.Time            `json:"deadline,omitempty"`
	ExpiresAt        *time.Time            `json:"expiresAt,omitempty"`
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
	Failure                string            `json:"failure,omitempty"`
	StartedAt              *time.Time        `json:"startedAt,omitempty"`
	UpdatedAt              time.Time         `json:"updatedAt"`
	CompletedAt            *time.Time        `json:"completedAt,omitempty"`
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

// Assignment commits one attempt to a worker and provider route.
type Assignment struct {
	ID                  string          `json:"id"`
	AttemptID           string          `json:"attemptId"`
	WorkerID            string          `json:"workerId"`
	WorkerEpoch         string          `json:"workerEpoch,omitempty"`
	Route               ProviderRoute   `json:"route"`
	State               AssignmentState `json:"state"`
	Epoch               int64           `json:"epoch"`
	LeaseToken          string          `json:"leaseToken"`
	LeaseExpiresAt      time.Time       `json:"leaseExpiresAt"`
	DispatchToken       string          `json:"dispatchToken"`
	ThreadID            string          `json:"threadId,omitempty"`
	DispatchState       DispatchState   `json:"dispatchState,omitempty"`
	DispatchRevision    int64           `json:"dispatchRevision,omitempty"`
	DispatchConfirmedAt *time.Time      `json:"dispatchConfirmedAt,omitempty"`
	DispatchError       string          `json:"dispatchError,omitempty"`
	CreatedAt           time.Time       `json:"createdAt"`
	UpdatedAt           time.Time       `json:"updatedAt"`
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
	ID           string                `json:"id"`
	Name         string                `json:"name"`
	Version      int                   `json:"version"`
	WorkflowID   string                `json:"workflowId"`
	Expression   string                `json:"expression"`
	Timezone     string                `json:"timezone"`
	Overlap      ScheduleOverlapPolicy `json:"overlap"`
	Misfire      ScheduleMisfirePolicy `json:"misfire"`
	AfterFailure ScheduleFailurePolicy `json:"afterFailure"`
	Enabled      bool                  `json:"enabled"`
	ActiveRunID  string                `json:"activeRunId,omitempty"`
	Revision     int64                 `json:"revision"`
	CreatedAt    time.Time             `json:"createdAt"`
	UpdatedAt    time.Time             `json:"updatedAt"`
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
