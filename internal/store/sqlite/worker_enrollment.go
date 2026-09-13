package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"reflect"
	"strings"
	"time"
)

const coordinatorMigrationV15 = `
CREATE TABLE IF NOT EXISTS coordinator_worker_requirements(worker_id TEXT PRIMARY KEY,record TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS coordinator_worker_enrollments(worker_id TEXT PRIMARY KEY,record TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS coordinator_worker_enrollment_requests(id TEXT PRIMARY KEY,record TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS immutable_worker_enrollment_request BEFORE UPDATE ON coordinator_worker_enrollment_requests BEGIN SELECT RAISE(ABORT,'worker enrollment request is immutable'); END;
`

func (s *Store) SaveWorkerRequirements(ctx context.Context, requirements []domain.WorkerRequirement) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Omission drains previously managed workers; it never reopens admission.
	if _, err = tx.ExecContext(ctx, "UPDATE coordinator_worker_requirements SET record=json_set(record,'$.connection','removed')"); err != nil {
		return err
	}
	for _, r := range requirements {
		if r.WorkerID == "" || r.WorkerEpoch == "" || len(r.CatalogRevision) != 64 {
			return errors.New("invalid worker requirement")
		}
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_worker_requirements(worker_id,record) VALUES(?,?) ON CONFLICT(worker_id) DO UPDATE SET record=excluded.record", r.WorkerID, string(raw)); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Store) WorkerEnrollmentReplay(ctx context.Context, r domain.WorkerEnrollmentRequest, actor string) (domain.WorkerEnrollment, bool, error) {
	var value domain.WorkerEnrollment
	var raw string
	err := s.db.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_enrollment_requests WHERE id=?", r.ID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return value, false, nil
	}
	if err != nil {
		return value, false, err
	}
	if err = json.Unmarshal([]byte(raw), &value); err != nil {
		return value, false, err
	}
	if !reflect.DeepEqual(value.Request, r) || value.Actor != actor {
		return value, false, errors.New("enrollment request replay changed")
	}
	return value, true, nil
}
func (s *Store) CommitWorkerEnrollment(ctx context.Context, value domain.WorkerEnrollment, observed domain.WorkerSnapshot) (domain.WorkerEnrollment, error) {
	r := value.Request
	if r.ID == "" || len(r.ID) > 128 || r.WorkerID == "" || r.ExpectedRevision < 0 || len(r.CatalogRevision) != 64 || strings.TrimSpace(r.Reason) == "" || len(r.Reason) > 4096 || value.Actor == "" || value.EnrolledAt.IsZero() {
		return value, errors.New("invalid enrollment intent")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return value, err
	}
	defer tx.Rollback()
	var raw string
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_enrollment_requests WHERE id=?", r.ID).Scan(&raw)
	if err == nil {
		var prior domain.WorkerEnrollment
		if err = json.Unmarshal([]byte(raw), &prior); err != nil {
			return value, err
		}
		if !reflect.DeepEqual(prior.Request, r) || prior.Actor != value.Actor {
			return value, errors.New("enrollment request replay changed")
		}
		return prior, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return value, err
	}
	var requirement domain.WorkerRequirement
	if err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_requirements WHERE worker_id=?", r.WorkerID).Scan(&raw); err != nil {
		return value, err
	}
	if err = json.Unmarshal([]byte(raw), &requirement); err != nil {
		return value, err
	}
	if requirement.Draining || requirement.CatalogRevision != r.CatalogRevision || requirement.WorkerEpoch != value.WorkerEpoch || requirement.CredentialRef != value.CredentialRef || requirement.Connection != value.Connection {
		return value, errors.New("worker requirement changed")
	}
	var current domain.WorkerEnrollment
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_enrollments WHERE worker_id=?", r.WorkerID).Scan(&raw)
	if err == nil {
		if err = json.Unmarshal([]byte(raw), &current); err != nil {
			return value, err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return value, err
	}
	if current.Revision != r.ExpectedRevision {
		return value, errors.New("stale worker enrollment revision")
	}
	snapshot, found, err := loadWorkerSnapshotTx(ctx, tx, r.WorkerID)
	if err != nil {
		return value, err
	}
	now := value.EnrolledAt
	if !found || !reflect.DeepEqual(snapshot, observed) || !snapshot.Connected || !snapshot.ValidUntil.After(now) || snapshot.ObservedAt.After(now) || now.Sub(snapshot.ObservedAt) > 5*time.Minute || snapshot.WorkerEpoch != value.WorkerEpoch || snapshot.Inventory.CatalogRevision != r.CatalogRevision || snapshot.Inventory.Health != domain.WorkerHealthReady || !snapshot.Inventory.AcceptBacklog {
		return value, errors.New("worker enrollment needs matching fresh observed readiness")
	}
	if err := requireCoordinatorEpoch(ctx, tx, snapshot.CoordinatorEpoch); err != nil {
		return value, err
	}
	if value.CoordinatorID == "" || value.Principal != "ssh:"+r.WorkerID {
		return value, errors.New("invalid enrollment identity")
	}
	value.Revision = current.Revision + 1
	value.SnapshotSequence = snapshot.Sequence
	rawBytes, err := json.Marshal(value)
	if err != nil {
		return value, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_worker_enrollments(worker_id,record) VALUES(?,?) ON CONFLICT(worker_id) DO UPDATE SET record=excluded.record", r.WorkerID, string(rawBytes)); err != nil {
		return value, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO coordinator_worker_enrollment_requests(id,record) VALUES(?,?)", r.ID, string(rawBytes)); err != nil {
		return value, err
	}
	if _, err = insertNativeAuditEventTx(ctx, tx, nativeAuditInput{ID: "worker-enrolled:" + r.ID, Kind: "worker-enrolled", TargetType: "worker", TargetID: r.WorkerID, Actor: value.Actor, Reason: r.Reason, CreatedAt: now, Detail: nativeAuditDetail{ExpectedRevision: r.ExpectedRevision, Revision: value.Revision, IdempotencyIdentity: r.ID, Outcome: r.CatalogRevision}}); err != nil {
		return value, err
	}
	return value, tx.Commit()
}
func (s *Store) LoadWorkerEnrollments(ctx context.Context) ([]domain.WorkerEnrollment, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	values, err := loadWorkerRows[domain.WorkerEnrollment](ctx, tx, "SELECT record FROM coordinator_worker_enrollments ORDER BY worker_id")
	if err != nil {
		return nil, err
	}
	return values, tx.Commit()
}
func (s *Store) LoadWorkerRequirements(ctx context.Context) ([]domain.WorkerRequirement, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	values, err := loadWorkerRows[domain.WorkerRequirement](ctx, tx, "SELECT record FROM coordinator_worker_requirements ORDER BY worker_id")
	if err != nil {
		return nil, err
	}
	return values, tx.Commit()
}
func loadWorkerRows[T any](ctx context.Context, tx *sql.Tx, query string) ([]T, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []T
	for rows.Next() {
		var raw string
		var value T
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Store) WorkerEnrolled(ctx context.Context, workerID string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	ok, err := workerEnrolledTx(ctx, tx, workerID)
	if err != nil {
		return false, err
	}
	return ok, tx.Commit()
}
func workerEnrolledTx(ctx context.Context, tx *sql.Tx, workerID string) (bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_requirements WHERE worker_id=?", workerID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var requirement domain.WorkerRequirement
	if err = json.Unmarshal([]byte(raw), &requirement); err != nil {
		return false, err
	}
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_enrollments WHERE worker_id=?", workerID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var enrollment domain.WorkerEnrollment
	if err = json.Unmarshal([]byte(raw), &enrollment); err != nil {
		return false, err
	}
	snapshot, found, err := loadWorkerSnapshotTx(ctx, tx, workerID)
	if err != nil {
		return false, err
	}
	if !found || snapshot.WorkerEpoch != requirement.WorkerEpoch || snapshot.Inventory.CatalogRevision != requirement.CatalogRevision {
		return false, nil
	}
	return !requirement.Draining && enrollment.Request.CatalogRevision == requirement.CatalogRevision && enrollment.WorkerEpoch == requirement.WorkerEpoch && enrollment.CredentialRef == requirement.CredentialRef && enrollment.Connection == requirement.Connection, nil
}
