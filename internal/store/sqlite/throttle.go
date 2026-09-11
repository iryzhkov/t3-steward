package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV3 = `
CREATE TABLE coordinator_quota_admissions (
	id TEXT PRIMARY KEY,
	revision INTEGER NOT NULL,
	record TEXT NOT NULL
);
CREATE TABLE coordinator_throttle_directives (
	id TEXT PRIMARY KEY,
	quota_pool_id TEXT NOT NULL,
	admission_revision INTEGER NOT NULL,
	record TEXT NOT NULL,
	UNIQUE(quota_pool_id, admission_revision)
);
CREATE INDEX coordinator_throttle_directives_pool
	ON coordinator_throttle_directives(quota_pool_id, admission_revision);
`

// ErrStaleQuotaAdmissionRevision means another coordinator changed at least one
// pool after the caller loaded its admission snapshot.
var ErrStaleQuotaAdmissionRevision = errors.New("stale quota admission revision")

// LoadQuotaAdmissions returns the durable admission projection in pool order.
func (s *Store) LoadQuotaAdmissions(ctx context.Context) ([]domain.QuotaAdmissionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT record FROM coordinator_quota_admissions ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query quota admissions: %w", err)
	}
	defer rows.Close()

	var records []domain.QuotaAdmissionRecord
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan quota admission: %w", err)
		}
		var record domain.QuotaAdmissionRecord
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			return nil, fmt.Errorf("decode quota admission: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate quota admissions: %w", err)
	}
	return records, nil
}

// LoadThrottleDirectives returns all committed directives in deterministic
// pool/revision order. Only committed rows are eligible for outward delivery.
func (s *Store) LoadThrottleDirectives(ctx context.Context) ([]domain.ThrottleDirective, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT record FROM coordinator_throttle_directives ORDER BY quota_pool_id, admission_revision`)
	if err != nil {
		return nil, fmt.Errorf("query throttle directives: %w", err)
	}
	defer rows.Close()

	var directives []domain.ThrottleDirective
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan throttle directive: %w", err)
		}
		var directive domain.ThrottleDirective
		if err := json.Unmarshal([]byte(raw), &directive); err != nil {
			return nil, fmt.Errorf("decode throttle directive: %w", err)
		}
		directives = append(directives, directive)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate throttle directives: %w", err)
	}
	return directives, nil
}

// CommitQuotaAdmissionTransitions checks every expected revision and commits
// the full fleet batch atomically. Exact replay is a successful no-op.
func (s *Store) CommitQuotaAdmissionTransitions(ctx context.Context, input []domain.QuotaAdmissionTransition) error {
	transitions := append([]domain.QuotaAdmissionTransition(nil), input...)
	sort.Slice(transitions, func(i, j int) bool {
		return transitions[i].Record.QuotaPoolID < transitions[j].Record.QuotaPoolID
	})
	for index, transition := range transitions {
		if transition.Record.QuotaPoolID == "" {
			return fmt.Errorf("quota admission transition pool ID is required")
		}
		if index > 0 && transitions[index-1].Record.QuotaPoolID == transition.Record.QuotaPoolID {
			return fmt.Errorf("quota admission transition repeats pool %q", transition.Record.QuotaPoolID)
		}
		if transition.ExpectedRevision < 0 || transition.Record.Revision != transition.ExpectedRevision+1 {
			return fmt.Errorf("quota admission transition %q has invalid revision %d after %d",
				transition.Record.QuotaPoolID, transition.Record.Revision, transition.ExpectedRevision)
		}
		if transition.Directive != nil {
			if transition.Directive.QuotaPoolID != transition.Record.QuotaPoolID ||
				transition.Directive.AdmissionRevision != transition.Record.Revision ||
				transition.Directive.ID == "" ||
				!reflect.DeepEqual(transition.Directive.BucketEpochs, transition.Record.BucketEpochs) ||
				!directiveMatchesAdmission(transition.Directive.Severity, transition.Record.Admission) {
				return fmt.Errorf("quota admission transition %q has an invalid directive binding", transition.Record.QuotaPoolID)
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin quota admission transition: %w", err)
	}
	defer tx.Rollback()

	for _, transition := range transitions {
		replayed, err := compareQuotaAdmissionTransition(ctx, tx, transition)
		if err != nil {
			return err
		}
		if !replayed {
			recordJSON, err := json.Marshal(transition.Record)
			if err != nil {
				return fmt.Errorf("encode quota admission %q: %w", transition.Record.QuotaPoolID, err)
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO coordinator_quota_admissions(id, revision, record) VALUES (?, ?, ?)
				 ON CONFLICT(id) DO UPDATE SET revision = excluded.revision, record = excluded.record`,
				transition.Record.QuotaPoolID, transition.Record.Revision, recordJSON,
			); err != nil {
				return fmt.Errorf("save quota admission %q: %w", transition.Record.QuotaPoolID, err)
			}
			if transition.Directive != nil {
				directiveJSON, err := json.Marshal(transition.Directive)
				if err != nil {
					return fmt.Errorf("encode throttle directive %q: %w", transition.Directive.ID, err)
				}
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO coordinator_throttle_directives(id, quota_pool_id, admission_revision, record)
					 VALUES (?, ?, ?, ?)`,
					transition.Directive.ID, transition.Directive.QuotaPoolID,
					transition.Directive.AdmissionRevision, directiveJSON,
				); err != nil {
					return fmt.Errorf("save throttle directive %q: %w", transition.Directive.ID, err)
				}
			}
		}
		if _, err := insertQuotaAdmissionAuditEvent(ctx, tx, transition); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit quota admission transitions: %w", err)
	}
	return nil
}

func insertQuotaAdmissionAuditEvent(
	ctx context.Context,
	tx *sql.Tx,
	transition domain.QuotaAdmissionTransition,
) (domain.AuditEvent, error) {
	detail, err := json.Marshal(transition)
	if err != nil {
		return domain.AuditEvent{}, fmt.Errorf(
			"encode quota admission %q audit detail: %w",
			transition.Record.QuotaPoolID,
			err,
		)
	}
	record := transition.Record
	event := domain.AuditEvent{
		ID:         fmt.Sprintf("quota-admission:%s:%d", record.QuotaPoolID, record.Revision),
		Kind:       "quota-admission-" + string(record.Admission),
		TargetType: domain.AuditTargetQuotaPool, TargetID: record.QuotaPoolID,
		Actor: "coordinator", Reason: record.Reason, Detail: detail, CreatedAt: record.AppliedAt,
	}
	return insertAuditEventTx(ctx, tx, event)
}

func compareQuotaAdmissionTransition(
	ctx context.Context,
	tx *sql.Tx,
	transition domain.QuotaAdmissionTransition,
) (bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_quota_admissions WHERE id = ?`,
		transition.Record.QuotaPoolID,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		if transition.ExpectedRevision != 0 {
			return false, staleQuotaAdmissionError(transition, 0)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load quota admission %q: %w", transition.Record.QuotaPoolID, err)
	}

	var existing domain.QuotaAdmissionRecord
	if err := json.Unmarshal([]byte(raw), &existing); err != nil {
		return false, fmt.Errorf("decode quota admission %q: %w", transition.Record.QuotaPoolID, err)
	}
	if reflect.DeepEqual(existing, transition.Record) {
		if transition.Directive == nil {
			return true, nil
		}
		var directiveRaw string
		err := tx.QueryRowContext(ctx,
			`SELECT record FROM coordinator_throttle_directives WHERE id = ?`,
			transition.Directive.ID,
		).Scan(&directiveRaw)
		if err != nil {
			return false, fmt.Errorf("load replayed throttle directive %q: %w", transition.Directive.ID, err)
		}
		var existingDirective domain.ThrottleDirective
		if err := json.Unmarshal([]byte(directiveRaw), &existingDirective); err != nil {
			return false, fmt.Errorf("decode replayed throttle directive %q: %w", transition.Directive.ID, err)
		}
		if !reflect.DeepEqual(existingDirective, *transition.Directive) {
			return false, fmt.Errorf("throttle directive %q replay differs from committed record", transition.Directive.ID)
		}
		return true, nil
	}
	if existing.Revision != transition.ExpectedRevision {
		return false, staleQuotaAdmissionError(transition, existing.Revision)
	}
	return false, nil
}

func directiveMatchesAdmission(severity domain.ThrottleSeverity, admission domain.AdmissionState) bool {
	switch severity {
	case domain.ThrottleWarn:
		return admission == domain.AdmissionConstrained
	case domain.ThrottleDrain:
		return admission == domain.AdmissionDraining
	case domain.ThrottleStop:
		return admission == domain.AdmissionClosed
	default:
		return false
	}
}

func staleQuotaAdmissionError(transition domain.QuotaAdmissionTransition, actual int64) error {
	return fmt.Errorf("%w for pool %q: expected %d, current %d",
		ErrStaleQuotaAdmissionRevision, transition.Record.QuotaPoolID,
		transition.ExpectedRevision, actual)
}
