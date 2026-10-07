package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// reviewCheckpointDeadline is how long the reviewers of one round have. It is
// counted from the durable creation of the round, never from the time of the
// call, because the child stage retains its deadline on first use and every
// later identical call has to present the same one.
const reviewCheckpointDeadline = 24 * time.Hour

// reviewCheckpointRefs is the one worker exchange a checkpoint needs: which
// object an exact ref names on the project remote, as seen by a named worker.
type reviewCheckpointRefs interface {
	ResolveRef(context.Context, repositoryRefQuery) (repositoryRefAnswer, error)
}

// coordinatorReviewCheckpoint opens the review round of one checkpoint of a
// running task. It is the production caller of the declared review admission,
// child staging, checkpoint allocation and child materialization, in that
// order, and it is idempotent per (attempt, checkpoint ID): the same ID with
// the same remote head returns the same round, and the same ID with a moved
// head is refused.
//
// Nothing the task says about its work is trusted. Its identity is fenced
// against the coordinator's records before anything is written, the base is
// the stored immutable workflow base, and the head is what the worker assigned
// to the attempt reads from the checkpoint branch on the project remote.
type coordinatorReviewCheckpoint struct {
	store     *sqlite.Store
	admission backlog.ReviewAdmissionService
	staging   *backlog.DeclaredReviewStaging
	refs      reviewCheckpointRefs
	epoch     int64
	now       func() time.Time
	// mu serializes the writing half of concurrent calls in this process. Each
	// durable step is idempotent on its own; this only keeps two identical
	// calls from racing through staging side by side.
	mu sync.Mutex
	// fault is a test seam at the durable boundaries between steps: frozen,
	// allocated, staged and confirmed.
	fault func(string) error
}

func newCoordinatorReviewCheckpoint(settings config.BacklogV2, store *sqlite.Store, epoch int64, refs reviewCheckpointRefs) (*coordinatorReviewCheckpoint, error) {
	if store == nil || refs == nil {
		return nil, errors.New("review checkpoint: store and worker ref resolution are required")
	}
	admission, err := newCoordinatorDeclaredReviewAdmission(settings, store)
	if err != nil {
		return nil, err
	}
	staging, err := newCoordinatorDeclaredReviewStaging(settings, store)
	if err != nil {
		return nil, err
	}
	return &coordinatorReviewCheckpoint{store: store, admission: admission.service, staging: staging, refs: refs, epoch: epoch, now: time.Now}, nil
}

func (c *coordinatorReviewCheckpoint) boundary(phase string) error {
	if c.fault != nil {
		return c.fault(phase)
	}
	return nil
}

func checkpointRefusal(code domain.ReviewCheckpointCode, retryable bool, format string, args ...any) domain.ReviewCheckpointResult {
	return domain.ReviewCheckpointResult{Refusal: &domain.ReviewCheckpointRefusal{Code: code, Reason: fmt.Sprintf(format, args...), Retryable: retryable}}
}

// checkpointStoreRefusal classifies an error from a durable step. Only the
// refusals the store names are final; anything else, including an injected or
// real crash between steps, is retryable because every step replays.
func checkpointStoreRefusal(step string, err error) domain.ReviewCheckpointResult {
	switch {
	case errors.Is(err, sqlite.ErrStaleCoordinatorEpoch):
		return checkpointRefusal(domain.ReviewCheckpointStaleCoordinator, true, "%s: this coordinator no longer holds the durable epoch; repeat the call against the current coordinator: %v", step, err)
	case errors.Is(err, sqlite.ErrReviewAuthorityConflict):
		return checkpointRefusal(domain.ReviewCheckpointHeadConflict, false, "%s: this checkpoint ID is already bound to a different head or input; push to a new checkpoint ID: %v", step, err)
	case errors.Is(err, sqlite.ErrReviewAuthorityLimit):
		return checkpointRefusal(domain.ReviewCheckpointRoundLimit, false, "%s: %v", step, err)
	case errors.Is(err, sqlite.ErrReviewAuthorityIdentity):
		return checkpointRefusal(domain.ReviewCheckpointAttemptStale, false, "%s: %v", step, err)
	default:
		return checkpointRefusal(domain.ReviewCheckpointInternal, true, "%s failed; repeating the identical call resumes it: %v", step, err)
	}
}

// OpenReviewCheckpoint answers one checkpoint request. Every refusal it can
// explain is returned as a structured refusal, never as a transport error.
func (c *coordinatorReviewCheckpoint) OpenReviewCheckpoint(ctx context.Context, request domain.ReviewCheckpointRequest) domain.ReviewCheckpointResult {
	if err := request.Validate(); err != nil {
		return checkpointRefusal(domain.ReviewCheckpointInvalidRequest, false, "%v", err)
	}
	if !review.IDPattern.MatchString(request.CheckpointID) {
		return checkpointRefusal(domain.ReviewCheckpointInvalidRequest, false, "checkpoint ID %q must match %s", request.CheckpointID, review.IDPattern)
	}
	branch := domain.ReviewCheckpointBranch(request.WorkflowRunID, request.TaskID, request.CheckpointID)
	if err := backlog.ValidateExactRef(branch); err != nil {
		return checkpointRefusal(domain.ReviewCheckpointInvalidRequest, false, "checkpoint branch %q: %v", branch, err)
	}
	// Every fence is read-only and precedes the first write. The epoch is also
	// bound to every durable step below, whose own transaction compares it
	// again: the remote probe and staging IO leave time for a replacement
	// coordinator to advance it.
	ctx = sqlite.WithCoordinatorEpochFence(ctx, c.epoch)
	assignment, refusal := c.fence(ctx, request)
	if refusal != nil {
		return *refusal
	}
	parent := backlog.DeclaredAdmissionRequest{RunID: request.WorkflowRunID, TaskID: request.TaskID, AttemptID: request.AttemptID}
	resolved, err := c.admission.ResolveDeclared(ctx, parent)
	if err != nil {
		return checkpointRefusal(domain.ReviewCheckpointAdmission, false, "declared review admission: %v", err)
	}
	head, refused := c.resolveHead(ctx, assignment.WorkerID, branch, resolved.Provenance, request.HeadCommit)
	if refused != nil {
		return *refused
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// A repeated call is answered from what it already created, before any
	// write, and a reused ID whose head moved is refused the same way.
	if cp, receipt, found, err := c.store.ReviewCheckpointReplay(ctx, resolved.Authority, request.CheckpointID); err != nil {
		return checkpointStoreRefusal("checkpoint replay", err)
	} else if found && cp.Checkpoint.HeadCommit != head {
		return checkpointRefusal(domain.ReviewCheckpointHeadConflict, false, "checkpoint %q opened round %d for head %s; the branch now names %s; push the new work to a new checkpoint ID", request.CheckpointID, cp.Number, cp.Checkpoint.HeadCommit, head)
	} else if receipt != nil {
		return domain.ReviewCheckpointResult{Round: checkpointRound(resolved.Authority, cp, *receipt, branch, true)}
	}

	snapshot, err := c.admission.FreezeDeclared(ctx, parent)
	if err != nil {
		return checkpointStoreRefusal("freeze declared review", err)
	}
	if err := c.boundary("frozen"); err != nil {
		return checkpointStoreRefusal("freeze declared review", err)
	}
	candidate := review.Checkpoint{ID: request.CheckpointID, HeadCommit: head, InputDigest: snapshot.Provenance.InputManifest.Digest}
	// The round is allocated before staging so that its durable creation time
	// fixes the deadline every replay of this checkpoint presents. Allocation is
	// idempotent and staging repeats it under its own owner lock.
	allocated, err := c.store.AllocateReviewCheckpoint(ctx, snapshot.Authority, candidate)
	if err != nil {
		return c.checkpointAllocationRefusal(ctx, "allocate review checkpoint", err, request.WorkflowRunID, request.TaskID)
	}
	round, err := c.store.GetReviewRound(ctx, allocated.RoundID)
	if err != nil {
		return checkpointStoreRefusal("load review round", err)
	}
	deadline := round.CreatedAt.UTC().Add(reviewCheckpointDeadline)
	if !deadline.After(c.now()) {
		return checkpointRefusal(domain.ReviewCheckpointDeadlineExpired, false, "round %d of checkpoint %q was allocated at %s and its review deadline %s has passed without a child", allocated.Number, request.CheckpointID, round.CreatedAt.UTC().Format(time.RFC3339), deadline.Format(time.RFC3339))
	}
	if err := c.boundary("allocated"); err != nil {
		return checkpointStoreRefusal("allocate review checkpoint", err)
	}
	staged, err := c.staging.StageDeclared(ctx, backlog.DeclaredChildStageRequest{Parent: parent, CheckpointID: request.CheckpointID, HeadCommit: head, Deadline: deadline})
	if err != nil {
		return checkpointStoreRefusal("stage review child", err)
	}
	if err := c.boundary("staged"); err != nil {
		return checkpointStoreRefusal("stage review child", err)
	}
	// The allocation is confirmed current once more, after all staging IO and
	// immediately before the child is created from it.
	current, err := c.store.AllocateReviewCheckpoint(ctx, staged.Admission.Authority, candidate)
	if err != nil {
		return c.checkpointAllocationRefusal(ctx, "confirm review checkpoint", err, request.WorkflowRunID, request.TaskID)
	}
	if current != staged.Checkpoint || current != allocated {
		return checkpointRefusal(domain.ReviewCheckpointHeadConflict, false, "checkpoint %q changed while it was staged", request.CheckpointID)
	}
	if err := c.boundary("confirmed"); err != nil {
		return checkpointStoreRefusal("confirm review checkpoint", err)
	}
	receipt, err := c.store.MaterializeReviewChild(ctx, staged.Admission.Authority, current, staged.Preparation)
	if err != nil {
		return checkpointStoreRefusal("materialize review child", err)
	}
	return domain.ReviewCheckpointResult{Round: checkpointRound(staged.Admission.Authority, current, receipt, branch, false)}
}

// checkpointAllocationRefusal adds the task's actual allocation count and
// frozen budget to a limit refusal, including rounds that never materialized.
// The count comes from the durable authority, not the current attempt or child
// runs. A failed read stays retryable rather than inventing a count.
func (c *coordinatorReviewCheckpoint) checkpointAllocationRefusal(ctx context.Context, step string, err error, runID, taskID string) domain.ReviewCheckpointResult {
	if errors.Is(err, sqlite.ErrReviewAuthorityLimit) {
		latest, found, readErr := c.store.LatestReviewRoundHead(ctx, runID, taskID)
		if readErr != nil {
			return checkpointStoreRefusal("read exhausted review budget", readErr)
		}
		if !found {
			return checkpointStoreRefusal("read exhausted review budget", errors.New("round limit was exhausted but no allocated round was found"))
		}
		err = fmt.Errorf("%w: %d of %d review rounds allocated; no further round can be opened; the lead can start a new run with a fresh budget using campaign rerun %s --from %s", err, latest.RoundsUsed, latest.RoundLimit, runID, taskID)
	}
	return checkpointStoreRefusal(step, err)
}

// fence authenticates the caller as the current live turn of its task, using
// the same identity a task-bound wait registration carries, and requires the
// coordinator that answers to hold the current epoch and the task to declare a
// review. It reads only.
func (c *coordinatorReviewCheckpoint) fence(ctx context.Context, request domain.ReviewCheckpointRequest) (domain.Assignment, *domain.ReviewCheckpointResult) {
	refuse := func(code domain.ReviewCheckpointCode, retryable bool, format string, args ...any) (domain.Assignment, *domain.ReviewCheckpointResult) {
		result := checkpointRefusal(code, retryable, format, args...)
		return domain.Assignment{}, &result
	}
	epoch, err := c.store.CoordinatorEpoch(ctx)
	if err != nil {
		return refuse(domain.ReviewCheckpointInternal, true, "%v", err)
	}
	if epoch != c.epoch {
		return refuse(domain.ReviewCheckpointStaleCoordinator, true, "this coordinator holds epoch %d but the durable epoch is %d; repeat the call against the current coordinator", c.epoch, epoch)
	}
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return refuse(domain.ReviewCheckpointInternal, true, "%v", err)
	}
	stale := func(format string, args ...any) (domain.Assignment, *domain.ReviewCheckpointResult) {
		return refuse(domain.ReviewCheckpointAttemptStale, false, "attempt %q: "+format, append([]any{request.AttemptID}, args...)...)
	}
	var attempt *domain.Attempt
	for i := range records.Attempts {
		if records.Attempts[i].ID == request.AttemptID {
			attempt = &records.Attempts[i]
		}
	}
	switch {
	case attempt == nil:
		return stale("no such attempt")
	case attempt.WorkflowRunID != request.WorkflowRunID || attempt.TaskID != request.TaskID:
		return stale("belongs to %s/%s, not %s/%s", attempt.WorkflowRunID, attempt.TaskID, request.WorkflowRunID, request.TaskID)
	case attempt.Progress.Terminal():
		return stale("is finished (%s)", attempt.Progress)
	case !attempt.TurnLive() || attempt.SupervisionActivationID != "":
		return stale("has no live turn (%s/%s)", attempt.Progress, attempt.Control)
	case attempt.ThreadID == "" || attempt.ThreadID != request.ThreadID:
		return stale("runs on thread %q, not %q", attempt.ThreadID, request.ThreadID)
	case attempt.AssignmentID != request.AssignmentID:
		return stale("is held by assignment %q, not %q", attempt.AssignmentID, request.AssignmentID)
	case request.IssuedRevision > attempt.Revision:
		return stale("revision %d was never issued; the attempt is at revision %d", request.IssuedRevision, attempt.Revision)
	}
	for _, other := range records.Attempts {
		if other.WorkflowRunID == attempt.WorkflowRunID && other.TaskID == attempt.TaskID && other.SupervisionActivationID == "" && other.ID != attempt.ID && other.Number >= attempt.Number {
			return stale("is superseded by attempt %q", other.ID)
		}
	}
	var assignment *domain.Assignment
	for i := range records.Assignments {
		if records.Assignments[i].ID == attempt.AssignmentID {
			assignment = &records.Assignments[i]
		}
	}
	if assignment == nil || assignment.AttemptID != attempt.ID || assignment.State != domain.AssignmentClaimed ||
		assignment.ThreadID != attempt.ThreadID || assignment.ActivationID != "" || assignment.WorkerID == "" {
		return stale("assignment %q is not the current claimed assignment of this attempt", attempt.AssignmentID)
	}
	var run *domain.WorkflowRun
	for i := range records.WorkflowRuns {
		if records.WorkflowRuns[i].ID == request.WorkflowRunID {
			run = &records.WorkflowRuns[i]
		}
	}
	if run == nil || run.Progress.Terminal() {
		return stale("run %q is missing or finished", request.WorkflowRunID)
	}
	var task *domain.Task
	for _, candidate := range domain.TasksForRun(*run, records.Tasks) {
		if candidate.ID == request.TaskID {
			candidate := candidate
			task = &candidate
		}
	}
	if task == nil {
		return stale("task %q is not in run %q", request.TaskID, request.WorkflowRunID)
	}
	if task.ReviewRequirements == nil {
		return refuse(domain.ReviewCheckpointNotDeclared, false, "task %q declares no review: requirements in its manifest", request.TaskID)
	}
	return *assignment, nil
}

// resolveHead asks the worker assigned to the attempt which commit the
// checkpoint branch names on the project remote. A stated head is only
// compared with that answer.
func (c *coordinatorReviewCheckpoint) resolveHead(ctx context.Context, workerID, branch string, provenance backlog.AdmissionProvenance, stated string) (string, *domain.ReviewCheckpointResult) {
	refuse := func(code domain.ReviewCheckpointCode, retryable bool, format string, args ...any) (string, *domain.ReviewCheckpointResult) {
		result := checkpointRefusal(code, retryable, format, args...)
		return "", &result
	}
	answer, err := c.refs.ResolveRef(ctx, repositoryRefQuery{
		WorkerID: workerID, Repository: provenance.RepositoryURL, Ref: branch,
		CredentialRefs: append([]string(nil), provenance.Environment.RequiredCredentials...),
	})
	if errors.Is(err, errRepositoryRefUnsupportedByWorker) {
		return refuse(domain.ReviewCheckpointWorkerSupport, false, "%v", err)
	}
	if err != nil {
		return refuse(domain.ReviewCheckpointRemote, true, "worker %q could not resolve %s: %v", workerID, branch, err)
	}
	switch answer.Status {
	case workerproto.RefResolutionResolved:
	case workerproto.RefResolutionNotFound:
		return refuse(domain.ReviewCheckpointBranchNotFound, true, "%s does not exist on the project remote; push the checkpoint branch first (a worker-local commit is never reviewed)", branch)
	case workerproto.RefResolutionUnreachable:
		return refuse(domain.ReviewCheckpointRemote, true, "worker %q could not reach the project remote: %s", workerID, answer.Detail)
	case workerproto.RefResolutionInvalidRef:
		return refuse(domain.ReviewCheckpointInvalidRequest, false, "%s is not a valid exact ref: %s", branch, answer.Detail)
	default:
		return refuse(domain.ReviewCheckpointInternal, true, "worker %q answered %q for %s", workerID, answer.Status, branch)
	}
	if answer.Ref != branch || !review.FullCommitID(answer.ObjectID) {
		return refuse(domain.ReviewCheckpointInternal, true, "worker %q answered an unusable resolution for %s", workerID, branch)
	}
	if stated != "" && stated != answer.ObjectID {
		return refuse(domain.ReviewCheckpointHeadMismatch, false, "the task stated head %s but %s names %s on the project remote", stated, branch, answer.ObjectID)
	}
	return answer.ObjectID, nil
}

func checkpointRound(frozen review.FrozenAuthority, cp review.CheckpointAuthority, receipt sqlite.ReviewMaterialization, branch string, replayed bool) *domain.ReviewCheckpointRound {
	round := &domain.ReviewCheckpointRound{
		RoundID: cp.RoundID, CheckpointID: cp.Checkpoint.ID, Number: cp.Number, Branch: branch,
		BaseCommit: frozen.Parent.BaseCommit, HeadCommit: cp.Checkpoint.HeadCommit,
		ChildWorkflowID: receipt.Graph.Workflow.ID, ChildRunID: receipt.Graph.Run.ID, Replayed: replayed,
	}
	if len(receipt.Graph.Tasks) != 0 && receipt.Graph.Tasks[0].Deadline != nil {
		round.Deadline = receipt.Graph.Tasks[0].Deadline.UTC()
	}
	for _, member := range frozen.Requirements.Members {
		round.Members = append(round.Members, domain.ReviewCheckpointMember{ID: member.ID, TaskID: cp.MemberTaskID(member.ID), Route: member.Route, Required: member.Required})
	}
	return round
}
