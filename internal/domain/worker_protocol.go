package domain

import "time"

// WorkerSnapshot is a worker's epoch-bound report to the authoritative
// coordinator. Sequence increases within one worker process epoch.
type WorkerSnapshot struct {
	WorkerID         string                        `json:"workerId"`
	WorkerEpoch      string                        `json:"workerEpoch"`
	CoordinatorEpoch int64                         `json:"coordinatorEpoch"`
	Sequence         int64                         `json:"sequence"`
	Connected        bool                          `json:"connected"`
	Inventory        WorkerInventory               `json:"inventory"`
	Assignments      []WorkerAssignmentObservation `json:"assignments,omitempty"`
	ObservedAt       time.Time                     `json:"observedAt"`
	ValidUntil       time.Time                     `json:"validUntil"`
}

// WorkerAssignmentObservation is the worker's view of one assigned attempt.
type WorkerAssignmentObservation struct {
	AssignmentID    string          `json:"assignmentId"`
	AssignmentEpoch int64           `json:"assignmentEpoch"`
	State           AssignmentState `json:"state"`
	ThreadID        string          `json:"threadId,omitempty"`
	ObservedAt      time.Time       `json:"observedAt"`
}

// WorkerCommandKind identifies transport-neutral coordinator commands.
type WorkerCommandKind string

const (
	WorkerCommandPrepare  WorkerCommandKind = "prepare"
	WorkerCommandDispatch WorkerCommandKind = "dispatch"
	WorkerCommandStop     WorkerCommandKind = "stop"
	WorkerCommandCollect  WorkerCommandKind = "collect"
)

// WorkerCommand binds every mutation to coordinator, worker, and assignment
// epochs so a delayed command cannot affect a newer owner.
type WorkerCommand struct {
	ID                     string            `json:"id"`
	Kind                   WorkerCommandKind `json:"kind"`
	WorkerID               string            `json:"workerId"`
	WorkerEpoch            string            `json:"workerEpoch"`
	CoordinatorEpoch       int64             `json:"coordinatorEpoch"`
	AssignmentID           string            `json:"assignmentId"`
	AssignmentEpoch        int64             `json:"assignmentEpoch"`
	ExpectedWorkerSequence int64             `json:"expectedWorkerSequence"`
	CreatedAt              time.Time         `json:"createdAt"`
}

// WorkerAcknowledgement is an idempotent response to one worker command.
type WorkerAcknowledgement struct {
	CommandID        string    `json:"commandId"`
	WorkerID         string    `json:"workerId"`
	WorkerEpoch      string    `json:"workerEpoch"`
	CoordinatorEpoch int64     `json:"coordinatorEpoch"`
	AssignmentID     string    `json:"assignmentId"`
	AssignmentEpoch  int64     `json:"assignmentEpoch"`
	WorkerSequence   int64     `json:"workerSequence"`
	Accepted         bool      `json:"accepted"`
	Detail           string    `json:"detail,omitempty"`
	AcknowledgedAt   time.Time `json:"acknowledgedAt"`
}

// WorkerCommandRecord is the coordinator's durable view of a command and its
// optional immutable acknowledgement.
type WorkerCommandRecord struct {
	Command         WorkerCommand          `json:"command"`
	Acknowledgement *WorkerAcknowledgement `json:"acknowledgement,omitempty"`
}

// AssignmentPlanItem is one planner proposal prepared for atomic commit.
type AssignmentPlanItem struct {
	Assignment              Assignment `json:"assignment"`
	ExpectedAttemptRevision int64      `json:"expectedAttemptRevision"`
	WorkerEpoch             string     `json:"workerEpoch"`
	WorkerSnapshotSequence  int64      `json:"workerSnapshotSequence"`
}

// AssignmentPlanCommit atomically turns planner proposals into offered
// assignments and attaches them to their attempts.
type AssignmentPlanCommit struct {
	CoordinatorEpoch int64                `json:"coordinatorEpoch"`
	CommittedAt      time.Time            `json:"committedAt"`
	Items            []AssignmentPlanItem `json:"items"`
}

// AssignmentClaimRequest is an epoch-bound worker claim. LeaseToken is the
// capability committed with the offered assignment.
type AssignmentClaimRequest struct {
	CoordinatorEpoch int64     `json:"coordinatorEpoch"`
	WorkerID         string    `json:"workerId"`
	WorkerEpoch      string    `json:"workerEpoch"`
	AssignmentID     string    `json:"assignmentId"`
	AssignmentEpoch  int64     `json:"assignmentEpoch"`
	LeaseToken       string    `json:"leaseToken"`
	ClaimedAt        time.Time `json:"claimedAt"`
	LeaseExpiresAt   time.Time `json:"leaseExpiresAt"`
}

// AssignmentLeaseRenewal extends a claimed assignment lease against an exact
// worker snapshot. The lease token proves possession of the assignment.
type AssignmentLeaseRenewal struct {
	CoordinatorEpoch int64     `json:"coordinatorEpoch"`
	WorkerID         string    `json:"workerId"`
	WorkerEpoch      string    `json:"workerEpoch"`
	WorkerSequence   int64     `json:"workerSequence"`
	AssignmentID     string    `json:"assignmentId"`
	AssignmentEpoch  int64     `json:"assignmentEpoch"`
	LeaseToken       string    `json:"leaseToken"`
	RenewedAt        time.Time `json:"renewedAt"`
	LeaseExpiresAt   time.Time `json:"leaseExpiresAt"`
}
