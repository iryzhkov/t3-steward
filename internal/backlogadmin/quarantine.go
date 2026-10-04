package backlogadmin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// QuarantineReleaseKind is the operation name a quarantine release is
// authorized under. It is deliberately not one of the read kinds: reading what
// intake refused and clearing a retained marker are different authorities.
const QuarantineReleaseKind QueryKind = "quarantine-release"

// ReleaseQuarantine clears one intake quarantine on an operator's instruction.
//
// Historical files are no longer scanned, so neither file nor configuration
// changes automatically release a marker. This authenticated operation clears
// only the retained marker and audits the operator's reason. It never retries
// a file or reenables intake.
func (s *Service) ReleaseQuarantine(
	ctx context.Context,
	principal Principal,
	request QuarantineReleaseRequest,
) (domain.QuarantineRelease, error) {
	if s.quarantineOps == nil {
		return domain.QuarantineRelease{}, errors.New("this coordinator does not record quarantined intake")
	}
	key := strings.TrimSpace(request.Key)
	reason := strings.TrimSpace(request.Reason)
	if key == "" || key != request.Key {
		return domain.QuarantineRelease{}, fmt.Errorf("%w: a quarantine release needs the intake key", ErrInvalidQuery)
	}
	if reason == "" || reason != request.Reason {
		return domain.QuarantineRelease{}, fmt.Errorf("%w: a quarantine release needs a reason", ErrInvalidQuery)
	}
	action := Action{Kind: QuarantineReleaseKind, CommandKind: domain.AdminCommandKind(QuarantineReleaseKind)}
	if err := s.authorizer.Authorize(ctx, principal, action); err != nil {
		return domain.QuarantineRelease{}, fmt.Errorf("authorize %s: %w", QuarantineReleaseKind, err)
	}
	return s.quarantineOps.ReleaseQuarantinedSubmission(ctx, key, principal.ID, reason, s.now().UTC())
}

// quarantinedIntake projects the durable quarantine markers into the read view.
//
// A quarantined submission is reported once and then deliberately silent, and
// its audit event names no workflow run, so before this view the only record an
// operator could reach was a log line that had already scrolled away and a row
// in coordinator_submissions. The view is read-only: it releases nothing and
// resubmits nothing. Only an authenticated operator release clears a retained
// marker; new work must use native task or campaign submission.
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
