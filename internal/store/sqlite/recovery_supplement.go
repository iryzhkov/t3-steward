package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// LoadRecoverySupplement returns the immutable repair inputs attached to an
// ordinary retry attempt. Attempts without a recovery supplement are unchanged.
func (s *Store) LoadRecoverySupplement(ctx context.Context, attemptID string) (domain.RepairAttemptSupplement, bool, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT record FROM coordinator_recovery_supplements WHERE attempt_id = ?`, attemptID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RepairAttemptSupplement{}, false, nil
	}
	if err != nil {
		return domain.RepairAttemptSupplement{}, false, err
	}
	var supplement domain.RepairAttemptSupplement
	if err := json.Unmarshal(raw, &supplement); err != nil {
		return domain.RepairAttemptSupplement{}, false, fmt.Errorf("decode recovery supplement for attempt %q: %w", attemptID, err)
	}
	return supplement, true, nil
}
