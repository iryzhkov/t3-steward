package sqlite

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// SaveAutomaticWorkerEnrollment changes observation only, leaving admission settings intact.
func (s *Store) SaveAutomaticWorkerEnrollment(ctx context.Context, workerID string, attempt domain.AutomaticWorkerEnrollment) error {
	raw, err := json.Marshal(attempt)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, "UPDATE coordinator_worker_requirements SET record=json_set(record,'$.automaticEnrollment',json(?)) WHERE worker_id=?", string(raw), workerID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("automatic enrollment requires a configured worker requirement")
	}
	return nil
}
