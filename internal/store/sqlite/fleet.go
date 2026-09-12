package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV8 = `
CREATE TABLE coordinator_runtime (
	id INTEGER PRIMARY KEY CHECK(id = 1),
	epoch INTEGER NOT NULL CHECK(epoch > 0)
);
INSERT INTO coordinator_runtime(id, epoch) VALUES (1, 1);
CREATE TABLE coordinator_worker_snapshots (
	worker_id TEXT PRIMARY KEY,
	worker_epoch TEXT NOT NULL,
	coordinator_epoch INTEGER NOT NULL,
	sequence INTEGER NOT NULL,
	connected INTEGER NOT NULL,
	valid_until TEXT NOT NULL,
	record TEXT NOT NULL
);
CREATE INDEX coordinator_worker_snapshots_freshness
	ON coordinator_worker_snapshots(connected, valid_until);
ALTER TABLE coordinator_assignments ADD COLUMN worker_id TEXT NOT NULL DEFAULT '';
ALTER TABLE coordinator_assignments ADD COLUMN worker_epoch TEXT NOT NULL DEFAULT '';
ALTER TABLE coordinator_assignments ADD COLUMN assignment_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE coordinator_assignments ADD COLUMN assignment_state TEXT NOT NULL DEFAULT '';
UPDATE coordinator_assignments SET
	worker_id = COALESCE(json_extract(record, '$.workerId'), ''),
	worker_epoch = COALESCE(json_extract(record, '$.workerEpoch'), ''),
	assignment_epoch = COALESCE(CAST(json_extract(record, '$.epoch') AS INTEGER), 0),
	assignment_state = COALESCE(json_extract(record, '$.state'), '');
CREATE INDEX coordinator_assignments_worker_state
	ON coordinator_assignments(worker_id, assignment_state);
`

var (
	ErrStaleCoordinatorEpoch = errors.New("stale coordinator epoch")
	ErrStaleWorkerSnapshot   = errors.New("stale worker snapshot")
	ErrWorkerUnavailable     = errors.New("worker unavailable")
	ErrStaleAttemptRevision  = errors.New("stale attempt revision")
	ErrAssignmentClaim       = errors.New("assignment claim rejected")
)

// CoordinatorEpoch returns the durable authoritative coordinator epoch.
func (s *Store) CoordinatorEpoch(ctx context.Context) (int64, error) {
	var epoch int64
	if err := s.db.QueryRowContext(ctx, `SELECT epoch FROM coordinator_runtime WHERE id = 1`).Scan(&epoch); err != nil {
		return 0, fmt.Errorf("load coordinator epoch: %w", err)
	}
	return epoch, nil
}

// AdvanceCoordinatorEpoch starts a new coordinator session. Workers must publish
// a snapshot bound to the returned epoch before receiving new assignments.
func (s *Store) AdvanceCoordinatorEpoch(ctx context.Context, expected int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin coordinator epoch advance: %w", err)
	}
	defer tx.Rollback()
	var epoch int64
	err = tx.QueryRowContext(ctx,
		`UPDATE coordinator_runtime SET epoch = epoch + 1 WHERE id = 1 AND epoch = ? RETURNING epoch`,
		expected,
	).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		var current int64
		loadErr := tx.QueryRowContext(ctx, `SELECT epoch FROM coordinator_runtime WHERE id = 1`).Scan(&current)
		if loadErr != nil {
			return 0, loadErr
		}
		return 0, fmt.Errorf("%w: expected %d, current %d", ErrStaleCoordinatorEpoch, expected, current)
	}
	if err != nil {
		return 0, fmt.Errorf("advance coordinator epoch: %w", err)
	}
	now := s.now().UTC()
	identity := fmt.Sprintf("coordinator-epoch:%d", epoch)
	if _, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: identity, Kind: "coordinator-epoch-advanced",
		TargetType: domain.AuditTargetCoordinator, TargetID: "coordinator",
		Actor: "coordinator", Reason: "coordinator epoch advanced", CreatedAt: now,
		Detail: nativeAuditDetail{CoordinatorEpoch: epoch, ExpectedRevision: expected, Revision: epoch,
			IdempotencyIdentity: identity, Outcome: "advanced"},
	}); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit coordinator epoch advance: %w", err)
	}
	return epoch, nil
}

// SaveWorkerSnapshot stores a monotonic report for the current coordinator
// epoch. A restarted worker begins a new worker epoch at sequence one.
func (s *Store) SaveWorkerSnapshot(ctx context.Context, snapshot domain.WorkerSnapshot) error {
	if err := validateWorkerSnapshot(snapshot); err != nil {
		return err
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode worker snapshot %q: %w", snapshot.WorkerID, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin worker snapshot: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, snapshot.CoordinatorEpoch); err != nil {
		return err
	}
	current, exists, err := loadWorkerSnapshotTx(ctx, tx, snapshot.WorkerID)
	if err != nil {
		return err
	}
	if exists {
		if reflect.DeepEqual(current, snapshot) {
			if _, err := insertWorkerSnapshotAuditEvent(ctx, tx, snapshot); err != nil {
				return err
			}
			return tx.Commit()
		}
		sameEpoch := current.WorkerEpoch == snapshot.WorkerEpoch
		if (sameEpoch && snapshot.Sequence <= current.Sequence) ||
			(!sameEpoch && snapshot.Sequence != 1) ||
			!snapshot.ObservedAt.After(current.ObservedAt) {
			return fmt.Errorf("%w: worker %q epoch %q sequence %d", ErrStaleWorkerSnapshot, snapshot.WorkerID, snapshot.WorkerEpoch, snapshot.Sequence)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO coordinator_worker_snapshots(
			worker_id, worker_epoch, coordinator_epoch, sequence, connected, valid_until, record
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET
			worker_epoch = excluded.worker_epoch,
			coordinator_epoch = excluded.coordinator_epoch,
			sequence = excluded.sequence,
			connected = excluded.connected,
			valid_until = excluded.valid_until,
			record = excluded.record
	`, snapshot.WorkerID, snapshot.WorkerEpoch, snapshot.CoordinatorEpoch,
		snapshot.Sequence, snapshot.Connected, snapshot.ValidUntil.UTC().Format(time.RFC3339Nano), raw); err != nil {
		return fmt.Errorf("save worker snapshot %q: %w", snapshot.WorkerID, err)
	}
	if _, err := insertWorkerSnapshotAuditEvent(ctx, tx, snapshot); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit worker snapshot %q: %w", snapshot.WorkerID, err)
	}
	return nil
}

func insertWorkerSnapshotAuditEvent(ctx context.Context, tx *sql.Tx, snapshot domain.WorkerSnapshot) (domain.AuditEvent, error) {
	identity := fmt.Sprintf("worker-snapshot:%s:%s:%d", snapshot.WorkerID, snapshot.WorkerEpoch, snapshot.Sequence)
	outcome := "observed"
	if !snapshot.Connected {
		outcome = "disconnected"
	}
	return insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: identity, Kind: "worker-snapshot-" + outcome,
		TargetType: domain.AuditTargetWorker, TargetID: snapshot.WorkerID,
		Actor: "worker:" + snapshot.WorkerID, Reason: "worker snapshot observed", CreatedAt: snapshot.ObservedAt,
		Detail: nativeAuditDetail{CoordinatorEpoch: snapshot.CoordinatorEpoch, WorkerEpoch: snapshot.WorkerEpoch,
			WorkerSequence: snapshot.Sequence, Revision: snapshot.Sequence,
			IdempotencyIdentity: identity, Outcome: outcome},
	})
}

// LoadWorkerSnapshots returns the coordinator's durable worker projections.
func (s *Store) LoadWorkerSnapshots(ctx context.Context) ([]domain.WorkerSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT worker_id, record FROM coordinator_worker_snapshots ORDER BY worker_id`)
	if err != nil {
		return nil, fmt.Errorf("load worker snapshots: %w", err)
	}
	defer rows.Close()
	var snapshots []domain.WorkerSnapshot
	for rows.Next() {
		var workerID, raw string
		if err := rows.Scan(&workerID, &raw); err != nil {
			return nil, fmt.Errorf("scan worker snapshot: %w", err)
		}
		var snapshot domain.WorkerSnapshot
		if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
			return nil, fmt.Errorf("decode worker snapshot %q: %w", workerID, err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worker snapshots: %w", err)
	}
	return snapshots, nil
}

// CommitAssignmentPlan atomically persists every offered assignment and attaches
// it to the expected attempt. Nothing is published if any input is stale.
func (s *Store) CommitAssignmentPlan(ctx context.Context, commit domain.AssignmentPlanCommit) ([]domain.Assignment, error) {
	if commit.CoordinatorEpoch < 1 || commit.CommittedAt.IsZero() || len(commit.Items) == 0 {
		return nil, errors.New("assignment plan requires coordinator epoch, commit time, and items")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin assignment plan: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, commit.CoordinatorEpoch); err != nil {
		return nil, err
	}
	assignments := make([]domain.Assignment, 0, len(commit.Items))
	assignmentIDs := make(map[string]struct{}, len(commit.Items))
	attemptIDs := make(map[string]struct{}, len(commit.Items))
	for _, item := range commit.Items {
		assignment := item.Assignment
		assignment.WorkerEpoch = item.WorkerEpoch
		if err := validateOfferedAssignment(assignment, item); err != nil {
			return nil, err
		}
		if _, exists := assignmentIDs[assignment.ID]; exists {
			return nil, fmt.Errorf("assignment plan repeats assignment %q", assignment.ID)
		}
		if _, exists := attemptIDs[assignment.AttemptID]; exists {
			return nil, fmt.Errorf("assignment plan repeats attempt %q", assignment.AttemptID)
		}
		assignmentIDs[assignment.ID] = struct{}{}
		attemptIDs[assignment.AttemptID] = struct{}{}

		attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
		if err != nil {
			return nil, err
		}
		// Items that fail their own fences are skipped so the rest of the
		// plan still commits; the planner re-evaluates them next cycle.
		skip := func(reason string) {
			slog.Warn("assignment plan item skipped", "attempt", assignment.AttemptID, "reason", reason)
		}
		if attempt.AssignmentID != "" {
			if attempt.AssignmentID != assignment.ID {
				skip(fmt.Sprintf("attempt is already attached to assignment %q", attempt.AssignmentID))
				continue
			}
			current, err := loadAssignmentTx(ctx, tx, assignment.ID)
			if err != nil {
				return nil, err
			}
			if !sameAssignmentPlanIdentity(current, assignment) {
				skip(fmt.Sprintf("assignment plan replay changes identity for %q", assignment.ID))
				continue
			}
			if _, err := insertAssignmentPlanAuditEvent(ctx, tx, commit, item, current, attempt); err != nil {
				return nil, err
			}
			assignments = append(assignments, current)
			continue
		}
		if attempt.Revision != item.ExpectedAttemptRevision {
			skip(fmt.Sprintf("%v: expected revision %d, current %d", ErrStaleAttemptRevision, item.ExpectedAttemptRevision, attempt.Revision))
			continue
		}
		if attempt.Progress.Terminal() || attempt.Control != domain.ControlUnassigned {
			skip("attempt is not assignable")
			continue
		}
		var released domain.Assignment
		var releasedRaw []byte
		releasedExists := false
		err = tx.QueryRowContext(ctx, `SELECT record FROM coordinator_assignments WHERE attempt_id = ?`, attempt.ID).Scan(&releasedRaw)
		if err == nil {
			if err := json.Unmarshal(releasedRaw, &released); err != nil {
				return nil, fmt.Errorf("decode released assignment for attempt %q: %w", attempt.ID, err)
			}
			if released.State != domain.AssignmentReleased {
				skip(fmt.Sprintf("attempt retains non-released assignment %q in state %q", released.ID, released.State))
				continue
			}
			releasedExists = true
			assignment.ID = released.ID
			assignment.Epoch = released.Epoch + 1
			assignment.LeaseToken = fmt.Sprintf("%s-e%d", released.LeaseToken, assignment.Epoch)
			assignment.DispatchToken = fmt.Sprintf("%s-e%d", released.DispatchToken, assignment.Epoch)
			assignment.ThreadID = fmt.Sprintf("thread-%s-e%d", assignment.ID, assignment.Epoch)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("load released assignment for attempt %q: %w", attempt.ID, err)
		}

		snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, assignment.WorkerID)
		if err != nil {
			return nil, err
		}
		if !exists || snapshot.WorkerEpoch != item.WorkerEpoch ||
			snapshot.Sequence != item.WorkerSnapshotSequence ||
			snapshot.CoordinatorEpoch != commit.CoordinatorEpoch {
			return nil, fmt.Errorf("%w: worker %q planning snapshot changed", ErrStaleWorkerSnapshot, assignment.WorkerID)
		}
		if !workerAccepts(snapshot, commit.CommittedAt) {
			return nil, fmt.Errorf("%w: worker %q is disconnected, stale, or not ready", ErrWorkerUnavailable, assignment.WorkerID)
		}

		assignment.CreatedAt = commit.CommittedAt
		assignment.UpdatedAt = commit.CommittedAt
		raw, err := json.Marshal(assignment)
		if err != nil {
			return nil, fmt.Errorf("encode assignment %q: %w", assignment.ID, err)
		}
		statement := `
			INSERT INTO coordinator_assignments(
				id, attempt_id, dispatch_token, dispatch_revision, dispatch_state,
				worker_id, worker_epoch, assignment_epoch, assignment_state, lease_expires_at, record
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)
		`
		if releasedExists {
			statement = `UPDATE coordinator_assignments SET dispatch_token = ?, dispatch_revision = ?, dispatch_state = ?,
				worker_id = ?, worker_epoch = ?, assignment_epoch = ?, assignment_state = ?, lease_expires_at = '', record = ?
				WHERE id = ? AND attempt_id = ?`
			if _, err := tx.ExecContext(ctx, statement, assignment.DispatchToken, 0, "", assignment.WorkerID,
				assignment.WorkerEpoch, assignment.Epoch, assignment.State, raw, assignment.ID, assignment.AttemptID); err != nil {
				return nil, fmt.Errorf("re-arm assignment %q: %w", assignment.ID, err)
			}
		} else if _, err := tx.ExecContext(ctx, statement, assignment.ID, assignment.AttemptID, assignment.DispatchToken, 0, "",
			assignment.WorkerID, assignment.WorkerEpoch, assignment.Epoch, assignment.State, raw); err != nil {
			return nil, fmt.Errorf("commit assignment %q: %w", assignment.ID, err)
		}
		attempt.AssignmentID = assignment.ID
		attempt.Revision++
		attempt.UpdatedAt = commit.CommittedAt
		if err := updateAttemptTx(ctx, tx, attempt, item.ExpectedAttemptRevision); err != nil {
			return nil, err
		}
		if _, err := insertAssignmentPlanAuditEvent(ctx, tx, commit, item, assignment, attempt); err != nil {
			return nil, err
		}
		assignments = append(assignments, assignment)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit assignment plan: %w", err)
	}
	return assignments, nil
}

func insertAssignmentPlanAuditEvent(
	ctx context.Context,
	tx *sql.Tx,
	commit domain.AssignmentPlanCommit,
	item domain.AssignmentPlanItem,
	assignment domain.Assignment,
	attempt domain.Attempt,
) (domain.AuditEvent, error) {
	identity := "assignment-offer:" + assignment.ID
	if assignment.Epoch > 1 {
		identity = fmt.Sprintf("%s:epoch:%d", identity, assignment.Epoch)
	}
	return insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: identity, Kind: "assignment-offered",
		WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID, AttemptID: attempt.ID,
		TargetType: domain.AdminTargetAssignment, TargetID: assignment.ID,
		Actor: "coordinator", Reason: "assignment offered by planner", CreatedAt: assignment.CreatedAt,
		Detail: nativeAuditDetail{CoordinatorEpoch: commit.CoordinatorEpoch, WorkerEpoch: item.WorkerEpoch,
			AssignmentEpoch: assignment.Epoch, WorkerSequence: item.WorkerSnapshotSequence,
			ExpectedRevision: item.ExpectedAttemptRevision, Revision: item.ExpectedAttemptRevision + 1,
			IdempotencyIdentity: identity, Outcome: string(domain.AssignmentOffered)},
	})
}

// ClaimAssignment atomically activates one offered assignment and its attempt.
// Exact replay by the same epoch-bound worker returns the existing lease.
func (s *Store) ClaimAssignment(ctx context.Context, request domain.AssignmentClaimRequest) (domain.Assignment, error) {
	if err := validateAssignmentClaim(request); err != nil {
		return domain.Assignment{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("begin assignment claim: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, request.CoordinatorEpoch); err != nil {
		return domain.Assignment{}, err
	}
	snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, request.WorkerID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if !exists || snapshot.WorkerEpoch != request.WorkerEpoch ||
		snapshot.CoordinatorEpoch != request.CoordinatorEpoch {
		return domain.Assignment{}, fmt.Errorf("%w: worker %q epoch is not current", ErrStaleWorkerSnapshot, request.WorkerID)
	}
	if !workerAccepts(snapshot, request.ClaimedAt) {
		return domain.Assignment{}, fmt.Errorf("%w: worker %q is disconnected, stale, or not ready", ErrWorkerUnavailable, request.WorkerID)
	}
	assignment, err := loadAssignmentTx(ctx, tx, request.AssignmentID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if assignment.WorkerID != request.WorkerID || assignment.WorkerEpoch != request.WorkerEpoch ||
		assignment.Epoch != request.AssignmentEpoch || assignment.LeaseToken != request.LeaseToken {
		return domain.Assignment{}, fmt.Errorf("%w: identity mismatch for %q", ErrAssignmentClaim, request.AssignmentID)
	}
	if assignment.State == domain.AssignmentClaimed {
		if assignment.LeaseExpiresAt.Equal(request.LeaseExpiresAt) {
			attempt, loadErr := loadAttemptTx(ctx, tx, assignment.AttemptID)
			if loadErr != nil {
				return domain.Assignment{}, loadErr
			}
			if _, auditErr := insertAssignmentClaimAuditEvent(ctx, tx, request, assignment, attempt); auditErr != nil {
				return domain.Assignment{}, auditErr
			}
			if err := tx.Commit(); err != nil {
				return domain.Assignment{}, err
			}
			return assignment, nil
		}
		return domain.Assignment{}, fmt.Errorf("%w: assignment %q is already claimed", ErrAssignmentClaim, assignment.ID)
	}
	if assignment.State != domain.AssignmentOffered {
		return domain.Assignment{}, fmt.Errorf("%w: assignment %q is %s", ErrAssignmentClaim, assignment.ID, assignment.State)
	}

	attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if attempt.AssignmentID != assignment.ID || attempt.Progress.Terminal() ||
		attempt.Control != domain.ControlUnassigned {
		return domain.Assignment{}, fmt.Errorf("%w: attempt %q is not claimable", ErrAssignmentClaim, attempt.ID)
	}
	assignment.State = domain.AssignmentClaimed
	assignment.LeaseExpiresAt = request.LeaseExpiresAt
	assignment.UpdatedAt = request.ClaimedAt
	raw, err := json.Marshal(assignment)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("encode claimed assignment %q: %w", assignment.ID, err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE coordinator_assignments SET assignment_state = ?, lease_expires_at = ?, record = ?
		WHERE id = ? AND assignment_state = ?
	`, assignment.State, assignment.LeaseExpiresAt.UTC().Format(time.RFC3339Nano), raw,
		assignment.ID, domain.AssignmentOffered)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("claim assignment %q: %w", assignment.ID, err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return domain.Assignment{}, fmt.Errorf("%w: assignment %q changed concurrently", ErrAssignmentClaim, assignment.ID)
	}
	expectedAttemptRevision := attempt.Revision
	attempt.Progress = domain.ProgressActive
	attempt.Control = domain.ControlPreparing
	attempt.Revision++
	attempt.UpdatedAt = request.ClaimedAt
	if err := updateAttemptTx(ctx, tx, attempt, expectedAttemptRevision); err != nil {
		return domain.Assignment{}, err
	}
	if _, err := insertAssignmentClaimAuditEvent(ctx, tx, request, assignment, attempt); err != nil {
		return domain.Assignment{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Assignment{}, fmt.Errorf("commit assignment claim %q: %w", assignment.ID, err)
	}
	return assignment, nil
}

func insertAssignmentClaimAuditEvent(
	ctx context.Context,
	tx *sql.Tx,
	request domain.AssignmentClaimRequest,
	assignment domain.Assignment,
	attempt domain.Attempt,
) (domain.AuditEvent, error) {
	identity := fmt.Sprintf("assignment-claim:%s:%d", assignment.ID, assignment.Epoch)
	return insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: identity, Kind: "assignment-claimed",
		WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID, AttemptID: attempt.ID,
		TargetType: domain.AdminTargetAssignment, TargetID: assignment.ID,
		Actor: "worker:" + request.WorkerID, Reason: "worker accepted assignment offer", CreatedAt: assignment.UpdatedAt,
		Detail: nativeAuditDetail{CoordinatorEpoch: request.CoordinatorEpoch, WorkerEpoch: request.WorkerEpoch,
			AssignmentEpoch: request.AssignmentEpoch, Revision: request.AssignmentEpoch,
			IdempotencyIdentity: identity, Outcome: string(domain.AssignmentClaimed)},
	})
}

func validateWorkerSnapshot(snapshot domain.WorkerSnapshot) error {
	if strings.TrimSpace(snapshot.WorkerID) != snapshot.WorkerID || snapshot.WorkerID == "" ||
		strings.TrimSpace(snapshot.WorkerEpoch) != snapshot.WorkerEpoch || snapshot.WorkerEpoch == "" ||
		snapshot.CoordinatorEpoch < 1 || snapshot.Sequence < 1 ||
		snapshot.ObservedAt.IsZero() || !snapshot.ValidUntil.After(snapshot.ObservedAt) {
		return errors.New("invalid worker snapshot identity, epoch, sequence, or validity")
	}
	if snapshot.Inventory.ID != snapshot.WorkerID {
		return fmt.Errorf("worker snapshot %q inventory identity mismatch", snapshot.WorkerID)
	}
	return nil
}

func validateOfferedAssignment(assignment domain.Assignment, item domain.AssignmentPlanItem) error {
	if assignment.ID == "" || assignment.AttemptID == "" || assignment.WorkerID == "" ||
		assignment.WorkerEpoch == "" || assignment.DispatchToken == "" || assignment.LeaseToken == "" ||
		assignment.ThreadID == "" || assignment.Epoch < 1 || assignment.State != domain.AssignmentOffered ||
		!assignment.LeaseExpiresAt.IsZero() || assignment.DispatchState != "" ||
		assignment.DispatchRevision != 0 || item.ExpectedAttemptRevision < 0 ||
		item.WorkerSnapshotSequence < 1 {
		return fmt.Errorf("invalid offered assignment %q", assignment.ID)
	}
	if assignment.Route.ProviderInstanceID == "" || assignment.Route.Model == "" {
		return fmt.Errorf("assignment %q requires a provider route", assignment.ID)
	}
	return nil
}

func sameAssignmentPlanIdentity(current, offered domain.Assignment) bool {
	return current.ID == offered.ID &&
		current.AttemptID == offered.AttemptID &&
		current.WorkerID == offered.WorkerID &&
		current.WorkerEpoch == offered.WorkerEpoch &&
		reflect.DeepEqual(current.Route, offered.Route) &&
		reflect.DeepEqual(current.Estimate, offered.Estimate) &&
		current.Epoch == offered.Epoch &&
		current.LeaseToken == offered.LeaseToken &&
		current.DispatchToken == offered.DispatchToken &&
		current.ThreadID == offered.ThreadID
}

func validateAssignmentClaim(request domain.AssignmentClaimRequest) error {
	if request.CoordinatorEpoch < 1 || request.WorkerID == "" || request.WorkerEpoch == "" ||
		request.AssignmentID == "" || request.AssignmentEpoch < 1 || request.LeaseToken == "" ||
		request.ClaimedAt.IsZero() || !request.LeaseExpiresAt.After(request.ClaimedAt) {
		return fmt.Errorf("%w: invalid claim identity or lease", ErrAssignmentClaim)
	}
	return nil
}

func workerAccepts(snapshot domain.WorkerSnapshot, now time.Time) bool {
	return snapshot.Connected && snapshot.ValidUntil.After(now) &&
		snapshot.Inventory.AcceptBacklog && snapshot.Inventory.Health == domain.WorkerHealthReady
}

func requireCoordinatorEpoch(ctx context.Context, tx *sql.Tx, expected int64) error {
	var actual int64
	if err := tx.QueryRowContext(ctx, `SELECT epoch FROM coordinator_runtime WHERE id = 1`).Scan(&actual); err != nil {
		return fmt.Errorf("load coordinator epoch: %w", err)
	}
	if actual != expected {
		return fmt.Errorf("%w: expected %d, current %d", ErrStaleCoordinatorEpoch, expected, actual)
	}
	return nil
}

func loadWorkerSnapshotTx(ctx context.Context, tx *sql.Tx, workerID string) (domain.WorkerSnapshot, bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_worker_snapshots WHERE worker_id = ?`, workerID,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkerSnapshot{}, false, nil
	}
	if err != nil {
		return domain.WorkerSnapshot{}, false, fmt.Errorf("load worker snapshot %q: %w", workerID, err)
	}
	var snapshot domain.WorkerSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return domain.WorkerSnapshot{}, false, fmt.Errorf("decode worker snapshot %q: %w", workerID, err)
	}
	return snapshot, true, nil
}

func loadAttemptTx(ctx context.Context, tx *sql.Tx, attemptID string) (domain.Attempt, error) {
	var raw string
	if err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_attempts WHERE id = ?`, attemptID,
	).Scan(&raw); err != nil {
		return domain.Attempt{}, fmt.Errorf("load attempt %q: %w", attemptID, err)
	}
	var attempt domain.Attempt
	if err := json.Unmarshal([]byte(raw), &attempt); err != nil {
		return domain.Attempt{}, fmt.Errorf("decode attempt %q: %w", attemptID, err)
	}
	return attempt, nil
}

func updateAttemptTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt, expectedRevision int64) error {
	raw, err := json.Marshal(attempt)
	if err != nil {
		return fmt.Errorf("encode attempt %q: %w", attempt.ID, err)
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE coordinator_attempts SET revision = ?, record = ? WHERE id = ? AND revision = ?`,
		attempt.Revision, raw, attempt.ID, expectedRevision,
	)
	if err != nil {
		return fmt.Errorf("update attempt %q: %w", attempt.ID, err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return fmt.Errorf("%w: attempt %q expected %d", ErrStaleAttemptRevision, attempt.ID, expectedRevision)
	}
	return nil
}

func loadAssignmentTx(ctx context.Context, tx *sql.Tx, assignmentID string) (domain.Assignment, error) {
	var raw string
	if err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_assignments WHERE id = ?`, assignmentID,
	).Scan(&raw); err != nil {
		return domain.Assignment{}, fmt.Errorf("load assignment %q: %w", assignmentID, err)
	}
	var assignment domain.Assignment
	if err := json.Unmarshal([]byte(raw), &assignment); err != nil {
		return domain.Assignment{}, fmt.Errorf("decode assignment %q: %w", assignmentID, err)
	}
	return assignment, nil
}
