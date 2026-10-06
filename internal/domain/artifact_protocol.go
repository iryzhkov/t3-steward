package domain

// ArtifactPublication is the transport-neutral request used by a worker to
// publish one immutable artifact into coordinator-owned storage.
type ArtifactPublication struct {
	CoordinatorEpoch int64    `json:"coordinatorEpoch"`
	WorkerID         string   `json:"workerId"`
	WorkerEpoch      string   `json:"workerEpoch"`
	AssignmentID     string   `json:"assignmentId"`
	AssignmentEpoch  int64    `json:"assignmentEpoch"`
	AttemptRevision  int64    `json:"attemptRevision"`
	Artifact         Artifact `json:"artifact"`
	// LiveContinuation declares a continuation.md snapshot an attempt handed
	// on while it ran. It is fenced to the dispatch that took it rather than to
	// the assignment's current state; see sqlite.Store.CommitArtifactPublication.
	LiveContinuation bool `json:"liveContinuation,omitempty"`
}

// ArtifactFetchRequest describes the coordinator metadata a worker needs in
// order to materialize declared predecessor outputs.
type ArtifactFetchRequest struct {
	WorkflowRunID string   `json:"workflowRunId"`
	ArtifactIDs   []string `json:"artifactIds"`
}

// ArtifactFetchResult contains immutable metadata; artifact bytes are streamed
// separately by the transport so large files do not enter control messages.
type ArtifactFetchResult struct {
	Artifacts []Artifact `json:"artifacts"`
}

// ArtifactRetentionRequest applies the coordinator's retention cutoff while
// preserving explicitly protected workflow runs.
type ArtifactRetentionRequest struct {
	Before                  int64    `json:"beforeUnixNano"`
	ProtectedWorkflowRunIDs []string `json:"protectedWorkflowRunIds,omitempty"`
}
