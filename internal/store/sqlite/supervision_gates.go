package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Gate evidence and the pending-evidence to ready-for-review transition.
//
// The evidence a gate is reviewed against is not a stored document. It is
// recomputed from the rows that are the evidence — the observed tasks' succeeded
// attempts, their revisions and their artifact digests — and identified by a
// digest of exactly those facts. That is what makes a retried or replaced
// producer detectable: the recomputed identity changes, so a decision naming the
// old one loses the race by name rather than by a timestamp comparison.

// GateAdvance is one gate whose state the coordinator advanced, with the
// evidence the advance bound.
type GateAdvance struct {
	Gate     domain.Gate             `json:"gate"`
	Evidence domain.EvidenceSnapshot `json:"evidence"`
}

// AdvanceSupervisionGates moves every gate of one run whose observed producers
// have all succeeded from pending-evidence to ready-for-review, binding the
// evidence identity it was made ready against.
//
// It is idempotent: a gate already ready against the same evidence is left
// alone, and a gate whose evidence changed is made ready against the new
// identity. It advances no other state, raises no incident and sends nothing;
// the caller decides what to do with what changed.
func (s *Store) AdvanceSupervisionGates(ctx context.Context, runID string, now time.Time) ([]GateAdvance, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin supervision gate advance: %w", err)
	}
	defer tx.Rollback()
	state, supervised, err := supervisionContextTx(ctx, tx, runID, true)
	if err != nil || !supervised {
		return nil, err
	}
	if state.Snapshot.RunTerminal || state.Snapshot.RunCancelled {
		return nil, nil
	}
	at := now.UTC()
	if at.IsZero() {
		at = s.now().UTC()
	}
	var advanced []GateAdvance
	for _, gate := range state.Snapshot.Gates {
		if gate.State != domain.GatePendingEvidence && gate.State != "" {
			continue
		}
		evidence, verified, err := supervisionGateEvidenceTx(ctx, tx, runID, gate)
		if err != nil {
			return nil, err
		}
		if !verified {
			continue
		}
		next, err := domain.GateTransition(domain.GateTransitionInput{
			Gate:              gate,
			Event:             domain.GateEventProducersSucceeded,
			ProducersVerified: true,
			SinkSettled:       state.Snapshot.RunTerminal,
		})
		if err != nil {
			return nil, err
		}
		gate.State = next
		gate.EvidenceSnapshotID = evidence.ID
		gate.Revision++
		gate.UpdatedAt = at
		if err := saveSupervisionGateTx(ctx, tx, gate); err != nil {
			return nil, err
		}
		advanced = append(advanced, GateAdvance{Gate: gate, Evidence: evidence})
	}
	if len(advanced) == 0 {
		return nil, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit supervision gate advance: %w", err)
	}
	return advanced, nil
}

// GateReconsiderRequest reopens a held or escalated gate for review.
type GateReconsiderRequest struct {
	RunID     string
	GateID    string
	RequestID string
	Actor     domain.Actor
	Reason    string
	At        time.Time
}

// ReconsiderGate returns a held or escalated gate to ready-for-review against a
// freshly taken evidence snapshot.
//
// It is operator-authorized and the domain refuses anything else, which is what
// stops an overseer waking itself on the same rejected evidence. The snapshot is
// recomputed here rather than accepted from the caller: "a fresh snapshot" has
// to mean one the coordinator took, or the guard would be a promise.
func (s *Store) ReconsiderGate(ctx context.Context, request GateReconsiderRequest) (SupervisionDecision, error) {
	if strings.TrimSpace(request.Reason) == "" {
		return SupervisionDecision{}, errors.New("a gate reconsideration needs a reason")
	}
	return s.supervisionDecisionTx(ctx, "gate-reconsider", request.RunID, request.RequestID, request,
		func(ctx context.Context, tx *sql.Tx, state supervisionContext) (SupervisionDecision, error) {
			gate, err := loadSupervisionGateTx(ctx, tx, request.RunID, request.GateID)
			if err != nil {
				return SupervisionDecision{}, err
			}
			evidence, verified, err := supervisionGateEvidenceTx(ctx, tx, request.RunID, gate)
			if err != nil {
				return SupervisionDecision{}, err
			}
			next, err := domain.GateTransition(domain.GateTransitionInput{
				Gate:              gate,
				Event:             domain.GateEventReconsider,
				Actor:             request.Actor,
				ProducersVerified: verified,
				FreshSnapshot:     verified,
				SinkSettled:       state.Snapshot.RunTerminal,
			})
			if err != nil {
				return SupervisionDecision{}, err
			}
			at := request.At.UTC()
			if at.IsZero() {
				at = s.now().UTC()
			}
			gate.State = next
			gate.EvidenceSnapshotID = evidence.ID
			gate.Revision++
			gate.UpdatedAt = at
			if err := saveSupervisionGateTx(ctx, tx, gate); err != nil {
				return SupervisionDecision{}, err
			}
			return SupervisionDecision{Gate: &gate}, nil
		})
}

// supervisionGateEvidenceTx recomputes one gate's evidence from the rows that
// are the evidence, and reports whether every observed task has a succeeded
// attempt in coordinator custody.
func supervisionGateEvidenceTx(ctx context.Context, tx *sql.Tx, runID string, gate domain.Gate) (domain.EvidenceSnapshot, bool, error) {
	snapshot := domain.EvidenceSnapshot{GraphRevision: gate.GraphRevision}
	verified := len(gate.Definition.ObservedTaskIDs) > 0
	for _, taskID := range gate.Definition.ObservedTaskIDs {
		attempt, found, err := succeededAttemptTx(ctx, tx, runID, taskID)
		if err != nil {
			return domain.EvidenceSnapshot{}, false, err
		}
		if !found {
			verified = false
			continue
		}
		digests, err := attemptArtifactDigestsTx(ctx, tx, attempt.ID)
		if err != nil {
			return domain.EvidenceSnapshot{}, false, err
		}
		snapshot.Producers = append(snapshot.Producers, domain.ProducerEvidence{
			TaskID:          taskID,
			AttemptID:       attempt.ID,
			ResultRevision:  attempt.Revision,
			ArtifactDigests: digests,
		})
	}
	if !verified {
		return snapshot, false, nil
	}
	digest, err := supervisionPayloadDigest(struct {
		GateID    string                    `json:"gateId"`
		Revision  int64                     `json:"graphRevision"`
		Producers []domain.ProducerEvidence `json:"producers"`
	}{gate.Definition.ID, gate.GraphRevision, snapshot.Producers})
	if err != nil {
		return domain.EvidenceSnapshot{}, false, err
	}
	snapshot.ID = "evidence:" + gate.Definition.ID + ":" + digest[:16]
	return snapshot, true, nil
}

func succeededAttemptTx(ctx context.Context, tx *sql.Tx, runID, taskID string) (domain.Attempt, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `
		SELECT record FROM coordinator_attempts
		WHERE workflow_run_id = ? AND task_id = ? ORDER BY number DESC LIMIT 1`, runID, taskID).Scan(&raw)
	if err == sql.ErrNoRows {
		return domain.Attempt{}, false, nil
	}
	if err != nil {
		return domain.Attempt{}, false, fmt.Errorf("load attempt of task %q: %w", taskID, err)
	}
	var attempt domain.Attempt
	if err := json.Unmarshal(raw, &attempt); err != nil {
		return domain.Attempt{}, false, fmt.Errorf("decode attempt of task %q: %w", taskID, err)
	}
	if attempt.Progress != domain.ProgressSucceeded {
		return attempt, false, nil
	}
	return attempt, true, nil
}

func attemptArtifactDigestsTx(ctx context.Context, tx *sql.Tx, attemptID string) ([]domain.ArtifactDigest, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT id, sha256 FROM coordinator_artifacts WHERE attempt_id = ? ORDER BY id", attemptID)
	if err != nil {
		return nil, fmt.Errorf("load artifacts of attempt %q: %w", attemptID, err)
	}
	defer rows.Close()
	var digests []domain.ArtifactDigest
	for rows.Next() {
		var digest domain.ArtifactDigest
		if err := rows.Scan(&digest.ArtifactID, &digest.Digest); err != nil {
			return nil, err
		}
		digests = append(digests, digest)
	}
	return digests, rows.Err()
}

// supervisionSuccessorLiveTx reports whether any task a gate protects already
// has an attempt past the offer boundary. An invalidation of an acceptance whose
// protected task is already offered or started is refused in version 1, because
// a started effect cannot be pretended away.
func supervisionSuccessorLiveTx(ctx context.Context, tx *sql.Tx, runID string, gate domain.Gate) (bool, error) {
	for _, taskID := range gate.Definition.ProtectedTaskIDs {
		var live int
		err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM coordinator_attempts AS attempt
			JOIN coordinator_assignments AS assignment ON assignment.attempt_id = attempt.id
			WHERE attempt.workflow_run_id = ? AND attempt.task_id = ?
			  AND assignment.assignment_state IN (?, ?)`,
			runID, taskID, string(domain.AssignmentOffered), string(domain.AssignmentClaimed)).Scan(&live)
		if err != nil {
			return false, fmt.Errorf("count live assignments of task %q: %w", taskID, err)
		}
		if live > 0 {
			return true, nil
		}
	}
	return false, nil
}
