package domain

import "time"

// ThrottleSeverity is the outward safety action requested for a quota pool.
type ThrottleSeverity string

const (
	ThrottleWarn  ThrottleSeverity = "warn"
	ThrottleDrain ThrottleSeverity = "drain"
	ThrottleStop  ThrottleSeverity = "stop"
)

// QuotaBucketEpoch binds an admission record and directive to one observed
// provider-window epoch.
type QuotaBucketEpoch struct {
	Bucket BucketKey `json:"bucket"`
	Epoch  string    `json:"epoch"`
}

// QuotaAdmissionRecord is the durable scheduler admission projection for one
// quota pool.
type QuotaAdmissionRecord struct {
	QuotaPoolID  string             `json:"quotaPoolId"`
	Revision     int64              `json:"revision"`
	Admission    AdmissionState     `json:"admission"`
	ObservedAt   time.Time          `json:"observedAt"`
	BucketEpochs []QuotaBucketEpoch `json:"bucketEpochs"`
	Reason       string             `json:"reason"`
	AppliedAt    time.Time          `json:"appliedAt"`
}

// ThrottleDirective becomes eligible for delivery only after the admission
// transition carrying it has committed.
type ThrottleDirective struct {
	ID                string             `json:"id"`
	QuotaPoolID       string             `json:"quotaPoolId"`
	AdmissionRevision int64              `json:"admissionRevision"`
	Severity          ThrottleSeverity   `json:"severity"`
	BucketEpochs      []QuotaBucketEpoch `json:"bucketEpochs"`
	Reason            string             `json:"reason"`
	Deadline          *time.Time         `json:"deadline,omitempty"`
	CreatedAt         time.Time          `json:"createdAt"`
}

// QuotaAdmissionTransition is one optimistic admission update and its optional
// outward directive. A store commits a batch atomically.
type QuotaAdmissionTransition struct {
	ExpectedRevision int64                `json:"expectedRevision"`
	Record           QuotaAdmissionRecord `json:"admission"`
	Directive        *ThrottleDirective   `json:"directive,omitempty"`
}

// ThrottleCommandKind is the worker action requested by a durable command.
type ThrottleCommandKind string

const (
	ThrottleCommandWarn     ThrottleCommandKind = "warn"
	ThrottleCommandDrain    ThrottleCommandKind = "drain"
	ThrottleCommandHardStop ThrottleCommandKind = "hard-stop"
	ThrottleCommandResume   ThrottleCommandKind = "resume"
)

// ThrottleDeliveryState is the durable delivery state of the current command.
type ThrottleDeliveryState string

const (
	ThrottleDeliveryPending      ThrottleDeliveryState = "pending"
	ThrottleDeliveryAcknowledged ThrottleDeliveryState = "acknowledged"
	ThrottleDeliveryRejected     ThrottleDeliveryState = "rejected"
	ThrottleDeliveryCancelled    ThrottleDeliveryState = "cancelled"
)

// ThrottleAcknowledgementResult is the structured outcome reported by a worker.
type ThrottleAcknowledgementResult string

const (
	ThrottleResultWarned           ThrottleAcknowledgementResult = "warned"
	ThrottleResultCheckpointed     ThrottleAcknowledgementResult = "checkpointed"
	ThrottleResultCheckpointFailed ThrottleAcknowledgementResult = "checkpoint-failed"
	ThrottleResultStopped          ThrottleAcknowledgementResult = "stopped"
	ThrottleResultResumed          ThrottleAcknowledgementResult = "resumed"
)

// CheckpointMetadata identifies a checkpoint retained by the worker.
type CheckpointMetadata struct {
	ArtifactID string    `json:"artifactId"`
	Path       string    `json:"path"`
	SHA256     string    `json:"sha256"`
	Size       int64     `json:"size"`
	CapturedAt time.Time `json:"capturedAt"`
}

// ThrottleCommand is an idempotent worker request. Its execution identity and
// placement are fixed for the lifetime of the affected attempt.
type ThrottleCommand struct {
	ID              string              `json:"id"`
	DirectiveID     string              `json:"directiveId"`
	AttemptID       string              `json:"attemptId"`
	AssignmentID    string              `json:"assignmentId"`
	AssignmentEpoch int64               `json:"assignmentEpoch"`
	WorkerID        string              `json:"workerId"`
	ThreadID        string              `json:"threadId"`
	WorkspacePath   string              `json:"workspacePath"`
	Route           ProviderRoute       `json:"route"`
	Kind            ThrottleCommandKind `json:"kind"`
	QuotaPoolID     string              `json:"quotaPoolId"`
	BucketEpochs    []QuotaBucketEpoch  `json:"bucketEpochs"`
	Reason          string              `json:"reason"`
	Deadline        *time.Time          `json:"deadline,omitempty"`
	Checkpoint      *CheckpointMetadata `json:"checkpoint,omitempty"`
	CreatedAt       time.Time           `json:"createdAt"`
}

// ThrottleAcknowledgement is a worker's replay-safe response to one command.
type ThrottleAcknowledgement struct {
	CommandID      string                        `json:"commandId"`
	AttemptID      string                        `json:"attemptId"`
	Accepted       bool                          `json:"accepted"`
	Result         ThrottleAcknowledgementResult `json:"result,omitempty"`
	Checkpoint     *CheckpointMetadata           `json:"checkpoint,omitempty"`
	Error          string                        `json:"error,omitempty"`
	AcknowledgedAt time.Time                     `json:"acknowledgedAt"`
}

// ThrottleAttemptRecord is the coordinator's durable projection of the latest
// control command affecting one attempt for one throttle directive.
type ThrottleAttemptRecord struct {
	DirectiveID     string                        `json:"directiveId"`
	AttemptID       string                        `json:"attemptId"`
	Revision        int64                         `json:"revision"`
	Command         ThrottleCommand               `json:"command"`
	PriorCommandIDs []string                      `json:"priorCommandIds,omitempty"`
	Delivery        ThrottleDeliveryState         `json:"delivery"`
	Result          ThrottleAcknowledgementResult `json:"result,omitempty"`
	Control         ControlState                  `json:"control"`
	Checkpoint      *CheckpointMetadata           `json:"checkpoint,omitempty"`
	AcknowledgedAt  *time.Time                    `json:"acknowledgedAt,omitempty"`
	Failure         string                        `json:"failure,omitempty"`
	UpdatedAt       time.Time                     `json:"updatedAt"`
}

// ThrottleAttemptTransition is one optimistic affected-attempt update.
type ThrottleAttemptTransition struct {
	ExpectedRevision int64                 `json:"expectedRevision"`
	Record           ThrottleAttemptRecord `json:"record"`
}
