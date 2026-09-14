package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// quarantinedIntake projects the durable quarantine markers into the read view.
//
// A quarantined submission is reported once and then deliberately silent, and
// its audit event names no workflow run, so before this view the only record an
// operator could reach was a log line that had already scrolled away and a row
// in coordinator_submissions. The view is read-only: it releases nothing and
// resubmits nothing, because the only honest way to retry impossible content is
// to change it.
func (s *Service) quarantinedIntake(ctx context.Context) ([]QuarantinedIntake, error) {
	if s.quarantine == nil {
		return nil, errors.New("this coordinator does not record quarantined intake")
	}
	records, err := s.quarantine.ListQuarantinedSubmissions(ctx)
	if err != nil {
		return nil, fmt.Errorf("load quarantined intake: %w", err)
	}
	quarantined := make([]QuarantinedIntake, 0, len(records))
	for _, record := range records {
		if record.State != domain.SubmissionQuarantined {
			continue
		}
		quarantined = append(quarantined, QuarantinedIntake{
			Key:           strings.TrimPrefix(record.Key, sqlite.QuarantineKeyPrefix),
			RecordKey:     record.Key,
			Digest:        record.Digest,
			QuarantinedAt: record.CreatedAt,
			Reason:        record.Reason,
			Retry:         QuarantineRetryAdvice,
		})
	}
	if len(quarantined) == 0 {
		return nil, nil
	}
	return quarantined, nil
}
