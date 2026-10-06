package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// ReviewParentWaitResult describes execution/collection, never review acceptance.
// Settled is a consumed registration; replay cannot create another park.
type ReviewParentWaitResult struct {
	Status            string // parked, settled, or finished
	RoundID           string
	RoundState        string
	CollectionPending bool
	Wait              *domain.TaskWait
}

// WaitReviewParent is a trusted internal lifecycle boundary. The issued authority
// and allocated checkpoint must be the originals; no policy is resolved here.
// No public submission, CLI, completion gate or production caller uses it.
func (s *Store) WaitReviewParent(ctx context.Context, expected review.FrozenAuthority, allocated review.CheckpointAuthority) (ReviewParentWaitResult, error) {
	var zero ReviewParentWaitResult
	expected, err := expected.Canonical()
	if err != nil {
		return zero, err
	}
	tx, err := s.reviewAuthorityWriteTx(ctx, expected.Parent)
	if err != nil {
		return zero, err
	}
	defer tx.Rollback()
	frozen, cp, round, err := reviewChildAuthorityTx(ctx, tx, expected, allocated)
	if err != nil {
		return zero, err
	}
	receipt, err := loadReviewJSONTx[ReviewMaterialization](ctx, tx, "SELECT record FROM coordinator_review_materializations WHERE checkpoint_id=? AND workflow_id=? AND run_id=?", cp.Key(), review.ChildWorkflowID(cp), cp.RoundID)
	if err != nil {
		return zero, err
	}
	if !reflect.DeepEqual(receipt.Authority, frozen) || receipt.Checkpoint != cp ||
		receipt.Graph.Run.ID != cp.RoundID || receipt.Graph.Workflow.ID != review.ChildWorkflowID(cp) || receipt.Graph.Run.Sink == nil {
		return zero, ErrReviewMaterialization
	}
	if err := validateReviewChildTx(ctx, tx, receipt); err != nil {
		return zero, err
	}
	// The immutable graph owns the original finite deadline, not a mutable round.
	if len(receipt.Graph.Tasks) == 0 {
		return zero, ErrReviewMaterialization
	}
	if receipt.Graph.Tasks[0].Deadline == nil {
		return zero, ErrReviewMaterialization
	}
	deadline := *receipt.Graph.Tasks[0].Deadline
	if deadline.IsZero() || !round.Deadline.Equal(deadline) {
		return zero, ErrReviewMaterialization
	}
	for _, task := range receipt.Graph.Tasks {
		if task.Deadline == nil || !task.Deadline.Equal(deadline) {
			return zero, ErrReviewMaterialization
		}
	}
	if err := reviewParentCurrentTx(ctx, tx, frozen.Parent, false); err != nil {
		return zero, err
	}
	attempt, err := loadAttemptTx(ctx, tx, frozen.Parent.AttemptID)
	if err != nil {
		return zero, err
	}
	now := s.now().UTC()
	p := frozen.Parent
	node := domain.NodeWaitCondition{Target: domain.NodeRef{RunID: cp.RoundID, TaskID: receipt.Graph.Run.Sink.ID}, State: domain.NodeStateTerminal}
	requestID := "review-parent:" + cp.Key()
	result := ReviewParentWaitResult{RoundID: round.ID, RoundState: round.Combined, CollectionPending: !round.Terminal()}
	// Look up both identities: a foreign row cannot masquerade as a registration.
	rows, err := tx.QueryContext(ctx, "SELECT id,request_id,attempt_id,thread_id,record FROM coordinator_task_waits WHERE request_id=? OR id=?", requestID, taskWaitID(requestID))
	if err != nil {
		return zero, err
	}
	var waits []domain.TaskWait
	for rows.Next() {
		var id, rid, owner, thread string
		var raw []byte
		if err = rows.Scan(&id, &rid, &owner, &thread, &raw); err != nil {
			rows.Close()
			return zero, err
		}
		var w domain.TaskWait
		if err = json.Unmarshal(raw, &w); err != nil {
			rows.Close()
			return zero, err
		}
		if id != w.ID || rid != w.RequestID || owner != w.AttemptID || thread != w.ThreadID {
			rows.Close()
			return zero, domain.ErrTaskWaitReplayChanged
		}
		waits = append(waits, w)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return zero, err
	}
	if len(waits) > 1 {
		return zero, domain.ErrTaskWaitReplayChanged
	}
	if len(waits) == 1 {
		w := waits[0]
		if w.ID != taskWaitID(requestID) || w.RequestID != requestID ||
			w.WorkflowRunID != p.RunID || w.TaskID != p.TaskID || w.AttemptID != p.AttemptID ||
			w.ThreadID != p.ThreadID || w.IssuedRevision != p.IssuedRevision ||
			w.Kind != domain.WaitKindNode || w.Wake != domain.WakeEach || w.OrTimeout ||
			!reflect.DeepEqual(w.Node, &node) || w.Quota != nil || w.Attention != nil || w.Ask != nil ||
			w.Name != node.String() || w.Condition != node.String() ||
			!w.Deadline.Equal(deadline) || w.MaxDuration != deadline.Sub(w.RegisteredAt) ||
			w.RegisteredAt.IsZero() || w.MaxDuration <= 0 || w.RegisteredRevision <= p.IssuedRevision || w.RegisteredRevision > attempt.Revision {
			return zero, domain.ErrTaskWaitReplayChanged
		}
		result.Wait = &w
		if !w.Live() {
			result.Status = "settled"
		} else {
			if !deadline.After(now) {
				return zero, errors.New("review parent wait deadline expired")
			}
			if attempt.Progress != domain.ProgressWaitingExternal || attempt.Control != domain.ControlWaitingExternal ||
				attempt.LastTurnOutcomeID != "turn-outcome:"+w.ID || attempt.LastTurnOutcomeMarker != domain.TurnOutcomeWaiting {
				return zero, domain.ErrTaskWaitReplayNotParked
			}
			result.Status = "parked"
		}
		return result, tx.Commit()
	}
	if !deadline.After(now) {
		return zero, errors.New("review parent wait deadline expired")
	}
	records, err := nodeStateRecordsTx(ctx, tx, s.quotaStaleAfter)
	if err != nil {
		return zero, err
	}
	observation, err := resolveNodeState(node, records)
	if err != nil {
		return zero, err
	}
	if round.Terminal() || observation.Outcome != "" {
		result.Status = "finished"
		return result, tx.Commit()
	}
	request := domain.TaskWaitRegistration{RequestID: requestID, WorkflowRunID: p.RunID, TaskID: p.TaskID,
		AttemptID: p.AttemptID, IssuedRevision: p.IssuedRevision, ThreadID: p.ThreadID,
		Wake: domain.WakeEach, Kind: domain.WaitKindNode, Node: &node, MaxDuration: deadline.Sub(now)}
	if err := request.Validate(); err != nil {
		return zero, err
	}
	if err := validateStructuredRegistrationTx(ctx, tx, &request, now, s.quotaStaleAfter); err != nil {
		return zero, err
	}
	w, err := parkTaskWaitTx(ctx, tx, request, attempt, now)
	if err != nil {
		return zero, err
	}
	result.Status, result.Wait = "parked", &w
	return result, tx.Commit()
}
