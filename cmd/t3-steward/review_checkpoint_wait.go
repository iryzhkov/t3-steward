package main

import (
	"context"
	"errors"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// WaitReviewCheckpoint parks the calling task on the review round its
// checkpoint opened, with the task-bound node wait the store keeps for exactly
// that: one wait per checkpoint, on the review child's sink, due at the round
// deadline. The caller is fenced exactly as for opening the round, the round
// must already exist, and nothing the task states chooses the run it waits on.
//
// A replay answers from the wait that exists: parked while it holds the
// attempt, settled once it has settled, so a repeated command after the task
// resumed is told the round is over rather than parked a second time.
func (c *coordinatorReviewCheckpoint) WaitReviewCheckpoint(ctx context.Context, request domain.ReviewCheckpointRequest) domain.ReviewCheckpointWaitResult {
	refused := func(result domain.ReviewCheckpointResult) domain.ReviewCheckpointWaitResult {
		return domain.ReviewCheckpointWaitResult{Refusal: result.Refusal}
	}
	if err := request.Validate(); err != nil {
		return refused(checkpointRefusal(domain.ReviewCheckpointInvalidRequest, false, "%v", err))
	}
	if !review.IDPattern.MatchString(request.CheckpointID) {
		return refused(checkpointRefusal(domain.ReviewCheckpointInvalidRequest, false, "checkpoint ID %q must match %s", request.CheckpointID, review.IDPattern))
	}
	ctx = sqlite.WithCoordinatorEpochFence(ctx, c.epoch)
	if _, refusal := c.fence(ctx, request); refusal != nil {
		return refused(*refusal)
	}
	parent := backlog.DeclaredAdmissionRequest{RunID: request.WorkflowRunID, TaskID: request.TaskID, AttemptID: request.AttemptID}
	resolved, err := c.admission.ResolveDeclared(ctx, parent)
	if err != nil {
		return refused(checkpointRefusal(domain.ReviewCheckpointAdmission, false, "declared review admission: %v", err))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	cp, receipt, found, err := c.store.ReviewCheckpointReplay(ctx, resolved.Authority, request.CheckpointID)
	if err != nil {
		return refused(checkpointStoreRefusal("checkpoint replay", err))
	}
	if !found || receipt == nil {
		return refused(checkpointRefusal(domain.ReviewCheckpointNotOpen, false, "checkpoint %q has no materialized review round; open it first with t3-steward review --task current --checkpoint %s", request.CheckpointID, request.CheckpointID))
	}
	if request.HeadCommit != "" && request.HeadCommit != cp.Checkpoint.HeadCommit {
		return refused(checkpointRefusal(domain.ReviewCheckpointHeadConflict, false, "checkpoint %q reviews head %s, not %s; push the new work to a new checkpoint ID", request.CheckpointID, cp.Checkpoint.HeadCommit, request.HeadCommit))
	}
	parked, err := c.store.WaitReviewParent(ctx, resolved.Authority, cp)
	switch {
	case err == nil:
	case errors.Is(err, domain.ErrTaskWaitReplayChanged), errors.Is(err, domain.ErrTaskWaitReplayNotParked):
		return refused(checkpointRefusal(domain.ReviewCheckpointInternal, false, "park on review round %s: %v", cp.RoundID, err))
	default:
		return refused(checkpointStoreRefusal("park on review round", err))
	}
	return domain.ReviewCheckpointWaitResult{Park: &domain.ReviewCheckpointPark{
		Status: parked.Status, CheckpointID: request.CheckpointID, RoundID: parked.RoundID,
		RoundState: parked.RoundState, CollectionPending: parked.CollectionPending, Wait: parked.Wait,
	}}
}
