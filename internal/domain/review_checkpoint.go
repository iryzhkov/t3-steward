package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ReviewCheckpointRequest asks the coordinator to open a review round for the
// current work of the task attempt that sends it.
//
// The identity fields are the task's own execution identity, the record the
// worker wrote into .t3-steward/task.env, and they are fenced exactly as a
// task-bound wait registration is: the attempt must be the current, live turn
// of its task, running on the named thread under the named claimed assignment.
//
// HeadCommit is optional and is never trusted. The coordinator resolves the
// checkpoint branch on the project remote itself, through the worker assigned
// to the attempt; a stated hash is only compared with that answer, and a
// difference is refused.
type ReviewCheckpointRequest struct {
	WorkflowRunID  string `json:"workflowRunId"`
	TaskID         string `json:"taskId"`
	AttemptID      string `json:"attemptId"`
	IssuedRevision int64  `json:"issuedRevision"`
	AssignmentID   string `json:"assignmentId"`
	ThreadID       string `json:"threadId"`
	CheckpointID   string `json:"checkpointId"`
	HeadCommit     string `json:"headCommit,omitempty"`
}

// Validate checks the shape of the request. Whether the identity is current is
// the coordinator's question, answered from its own records.
func (r ReviewCheckpointRequest) Validate() error {
	for name, value := range map[string]string{
		"workflow run": r.WorkflowRunID, "task": r.TaskID, "attempt": r.AttemptID,
		"assignment": r.AssignmentID, "thread": r.ThreadID, "checkpoint": r.CheckpointID,
	} {
		if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
			return fmt.Errorf("review checkpoint request: %s identity is required", name)
		}
	}
	if r.IssuedRevision < 1 {
		return errors.New("review checkpoint request: issued attempt revision must be positive")
	}
	if r.HeadCommit != "" && !fullHexObjectID(r.HeadCommit) {
		return errors.New("review checkpoint request: a stated head must be a full lowercase commit ID")
	}
	return nil
}

func fullHexObjectID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	return strings.Trim(value, "0123456789abcdef") == ""
}

// ReviewCheckpointBranch is the branch a task pushes its checkpoint to. The
// coordinator resolves exactly this ref and nothing else, so a checkpoint
// cannot name a commit that exists only in the worker's local repository.
func ReviewCheckpointBranch(runID, taskID, checkpointID string) string {
	return "refs/heads/steward/" + runID + "/" + taskID + "/" + checkpointID
}

// ReviewCheckpointMember is one reviewer task of a materialized round.
type ReviewCheckpointMember struct {
	ID       string `json:"id"`
	TaskID   string `json:"taskId"`
	Route    string `json:"route"`
	Required bool   `json:"required,omitempty"`
}

// ReviewCheckpointRound is the round a checkpoint opened and the identity of
// the review child run that carries it. Replayed reports that the checkpoint
// had already been materialized by an earlier identical call.
type ReviewCheckpointRound struct {
	RoundID         string                   `json:"roundId"`
	CheckpointID    string                   `json:"checkpointId"`
	Number          int                      `json:"number"`
	Branch          string                   `json:"branch"`
	BaseCommit      string                   `json:"baseCommit"`
	HeadCommit      string                   `json:"headCommit"`
	ChildWorkflowID string                   `json:"childWorkflowId"`
	ChildRunID      string                   `json:"childRunId"`
	Members         []ReviewCheckpointMember `json:"members"`
	Deadline        time.Time                `json:"deadline"`
	Replayed        bool                     `json:"replayed,omitempty"`
}

// ReviewCheckpointCode names why a checkpoint was refused.
type ReviewCheckpointCode string

const (
	ReviewCheckpointInvalidRequest   ReviewCheckpointCode = "invalid-request"
	ReviewCheckpointUnavailable      ReviewCheckpointCode = "unavailable"
	ReviewCheckpointStaleCoordinator ReviewCheckpointCode = "stale-coordinator-epoch"
	ReviewCheckpointAttemptStale     ReviewCheckpointCode = "attempt-not-current"
	ReviewCheckpointNotDeclared      ReviewCheckpointCode = "review-not-declared"
	ReviewCheckpointAdmission        ReviewCheckpointCode = "admission-refused"
	ReviewCheckpointWorkerSupport    ReviewCheckpointCode = "worker-unsupported"
	ReviewCheckpointBranchNotFound   ReviewCheckpointCode = "branch-not-found"
	ReviewCheckpointRemote           ReviewCheckpointCode = "remote-unreachable"
	ReviewCheckpointHeadMismatch     ReviewCheckpointCode = "head-mismatch"
	ReviewCheckpointHeadConflict     ReviewCheckpointCode = "checkpoint-head-conflict"
	ReviewCheckpointRoundLimit       ReviewCheckpointCode = "round-limit-exhausted"
	ReviewCheckpointDeadlineExpired  ReviewCheckpointCode = "deadline-expired"
	ReviewCheckpointInternal         ReviewCheckpointCode = "internal"
	ReviewCheckpointTransport        ReviewCheckpointCode = "transport"
	// ReviewCheckpointNotOpen refuses to park on a checkpoint whose round was
	// never opened.
	ReviewCheckpointNotOpen ReviewCheckpointCode = "checkpoint-not-open"
	// ReviewCheckpointPushRefused and ReviewCheckpointRemoteMissing are the
	// client's own refusals: the checkpoint branch could not be published, so
	// the coordinator was never asked.
	ReviewCheckpointPushRefused   ReviewCheckpointCode = "push-refused"
	ReviewCheckpointRemoteMissing ReviewCheckpointCode = "remote-missing"
)

// ReviewCheckpointRefusal is a structured refusal: a stable code, a reason for
// a person, and whether repeating the identical call can succeed.
type ReviewCheckpointRefusal struct {
	Code      ReviewCheckpointCode `json:"code"`
	Reason    string               `json:"reason"`
	Retryable bool                 `json:"retryable"`
}

func (r *ReviewCheckpointRefusal) Error() string {
	retry := "not retryable"
	if r.Retryable {
		retry = "retryable"
	}
	return fmt.Sprintf("review checkpoint refused (%s, %s): %s", r.Code, retry, r.Reason)
}

// ReviewCheckpointResult carries exactly one of a round or a refusal, so that a
// refusal reaches the task with its code intact across either admin carrier.
type ReviewCheckpointResult struct {
	Round   *ReviewCheckpointRound   `json:"round,omitempty"`
	Refusal *ReviewCheckpointRefusal `json:"refusal,omitempty"`
}

// ReviewParentWaitPrefix begins the request ID of the task-bound wait that
// parks a task on the review child of one of its checkpoints. The rest of the
// ID is the checkpoint's durable key, so each checkpoint parks at most once.
const ReviewParentWaitPrefix = "review-parent:"

// ReviewRoundID names the review round a task-bound wait parks on, or "" when
// the wait is not a review parent wait. The round shares its ID with the review
// child run, whose sink the wait observes.
func (w TaskWait) ReviewRoundID() string {
	if !strings.HasPrefix(w.RequestID, ReviewParentWaitPrefix) || w.Kind != WaitKindNode || w.Node == nil {
		return ""
	}
	return w.Node.Target.RunID
}

// ReviewCheckpointPark is the coordinator's answer to parking a task on the
// review round of one checkpoint.
//
// Status is parked when the task-bound wait holds the attempt now, settled
// when the wait for this checkpoint already settled (a replay after the task
// resumed), and finished when the review child had already ended before
// anything could park on it. Only parked means the turn has to end.
type ReviewCheckpointPark struct {
	Status            string    `json:"status"`
	CheckpointID      string    `json:"checkpointId"`
	RoundID           string    `json:"roundId"`
	RoundState        string    `json:"roundState,omitempty"`
	CollectionPending bool      `json:"collectionPending,omitempty"`
	Wait              *TaskWait `json:"wait,omitempty"`
}

// Parked reports whether the attempt is held by the wait, so the turn must end.
func (p ReviewCheckpointPark) Parked() bool { return p.Status == "parked" }

// ReviewCheckpointWaitResult carries exactly one of a park or a refusal.
type ReviewCheckpointWaitResult struct {
	Park    *ReviewCheckpointPark    `json:"park,omitempty"`
	Refusal *ReviewCheckpointRefusal `json:"refusal,omitempty"`
}

// Outcome returns the park, or the refusal as an error.
func (r ReviewCheckpointWaitResult) Outcome() (ReviewCheckpointPark, error) {
	switch {
	case r.Refusal != nil && r.Park == nil:
		return ReviewCheckpointPark{}, r.Refusal
	case r.Park != nil && r.Refusal == nil:
		return *r.Park, nil
	default:
		return ReviewCheckpointPark{}, &ReviewCheckpointRefusal{Code: ReviewCheckpointTransport, Reason: "the coordinator answer carried neither exactly one park nor one refusal"}
	}
}

// Outcome returns the round, or the refusal as an error.
func (r ReviewCheckpointResult) Outcome() (ReviewCheckpointRound, error) {
	switch {
	case r.Refusal != nil && r.Round == nil:
		return ReviewCheckpointRound{}, r.Refusal
	case r.Round != nil && r.Refusal == nil:
		return *r.Round, nil
	default:
		return ReviewCheckpointRound{}, &ReviewCheckpointRefusal{Code: ReviewCheckpointTransport, Reason: "the coordinator answer carried neither exactly one round nor one refusal"}
	}
}
