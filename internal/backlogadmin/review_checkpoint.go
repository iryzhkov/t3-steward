package backlogadmin

import (
	"context"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ReviewCheckpointAction is the node-wait action a running task uses to ask
// the coordinator to open a review round for its current work.
const ReviewCheckpointAction = "review-checkpoint"

// ReviewCheckpointWaitAction parks the calling task on the review round its
// checkpoint opened, through the same fenced identity as the round itself.
const ReviewCheckpointWaitAction = "review-checkpoint-wait"

// ReviewCheckpointOpener opens, or replays, the review round of one checkpoint.
// Every refusal it can explain is returned in the result rather than as an
// error, so that its code and retry flag reach the task unchanged.
type ReviewCheckpointOpener interface {
	OpenReviewCheckpoint(context.Context, domain.ReviewCheckpointRequest) domain.ReviewCheckpointResult
}

// SetReviewCheckpoint installs the coordinator's checkpoint operation. A
// coordinator that never sets it refuses every checkpoint as unavailable.
func (s *Service) SetReviewCheckpoint(opener ReviewCheckpointOpener) { s.reviewCheckpoint = opener }

// ReviewCheckpointParker parks a task on the review round of a checkpoint it
// opened. Every refusal it can explain is returned in the result.
type ReviewCheckpointParker interface {
	WaitReviewCheckpoint(context.Context, domain.ReviewCheckpointRequest) domain.ReviewCheckpointWaitResult
}

func (s *Service) waitReviewCheckpoint(ctx context.Context, op NodeWaitOperation) (NodeWaitResponse, error) {
	refuse := func(code domain.ReviewCheckpointCode, reason string) (NodeWaitResponse, error) {
		return NodeWaitResponse{CheckpointWait: &domain.ReviewCheckpointWaitResult{Refusal: &domain.ReviewCheckpointRefusal{Code: code, Reason: reason}}}, nil
	}
	if op.Checkpoint == nil || op.Task != nil || op.Decision != nil || op.Answer != nil || op.Relay != nil || op.Result != nil || len(op.Events) != 0 {
		return refuse(domain.ReviewCheckpointInvalidRequest, "a review checkpoint wait carries exactly one checkpoint request")
	}
	parker, ok := s.reviewCheckpoint.(ReviewCheckpointParker)
	if !ok {
		return refuse(domain.ReviewCheckpointUnavailable, "this coordinator cannot park a task on a review checkpoint")
	}
	result := parker.WaitReviewCheckpoint(ctx, *op.Checkpoint)
	return NodeWaitResponse{CheckpointWait: &result}, nil
}

func (s *Service) openReviewCheckpoint(ctx context.Context, op NodeWaitOperation) (NodeWaitResponse, error) {
	refuse := func(code domain.ReviewCheckpointCode, reason string) (NodeWaitResponse, error) {
		return NodeWaitResponse{Checkpoint: &domain.ReviewCheckpointResult{Refusal: &domain.ReviewCheckpointRefusal{Code: code, Reason: reason}}}, nil
	}
	if op.Checkpoint == nil || op.Task != nil || op.Decision != nil || op.Answer != nil || op.Relay != nil || op.Result != nil || len(op.Events) != 0 {
		return refuse(domain.ReviewCheckpointInvalidRequest, "a review checkpoint operation carries exactly one checkpoint request")
	}
	if s.reviewCheckpoint == nil {
		return refuse(domain.ReviewCheckpointUnavailable, "this coordinator has no review checkpoint operation configured")
	}
	result := s.reviewCheckpoint.OpenReviewCheckpoint(ctx, *op.Checkpoint)
	return NodeWaitResponse{Checkpoint: &result}, nil
}
