package backlogadmin

import (
	"context"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// FailureClassificationStore records the coordinator's classification of
// terminal attempts. The coordinator's SQLite store implements it.
type FailureClassificationStore interface {
	RecordAttemptFailureClassifications(context.Context, []sqlite.AttemptFailureStamp) (int, error)
}

// AutomaticRetryReport is what one automatic retry pass did.
type AutomaticRetryReport struct {
	// Classified counts the terminal attempts whose classification was
	// recorded in this pass.
	Classified int
	// Submitted are the automatic retry commands submitted in this pass. They
	// are applied by ExecutePendingCommands like any other admin command.
	Submitted []MutationResponse
}

// SubmitAutomaticRetries classifies the failed and cancelled attempts that
// have no recorded classification yet, and submits an automatic retry command
// for every infrastructure failure with budget left (see
// backlog.PlanAutomaticRetries). coordinatorMax is the coordinator's ceiling
// on any task's budget; zero submits no retries and only classifies.
//
// It is coordinator-internal: there is no principal to authorize, because no
// client asked for it, and the commands it submits are recorded with the
// coordinator's own automatic-retry principal and audited on submission.
func (s *Service) SubmitAutomaticRetries(ctx context.Context, coordinatorMax int) (AutomaticRetryReport, error) {
	var report AutomaticRetryReport
	mutator, ok := s.reader.(Mutator)
	if !ok {
		return report, ErrReadOnly
	}
	records, err := s.reader.LoadCoordinatorRecords(ctx)
	if err != nil {
		return report, fmt.Errorf("load automatic retry snapshot: %w", err)
	}
	plan := backlog.PlanAutomaticRetries(records, coordinatorMax, s.now().UTC())
	if len(plan.Stamps) != 0 {
		if classifier, ok := s.reader.(FailureClassificationStore); ok {
			stamps := make([]sqlite.AttemptFailureStamp, 0, len(plan.Stamps))
			for _, stamp := range plan.Stamps {
				stamps = append(stamps, sqlite.AttemptFailureStamp{
					AttemptID: stamp.AttemptID, Revision: stamp.Revision, Classification: stamp.Classification,
				})
			}
			if report.Classified, err = classifier.RecordAttemptFailureClassifications(ctx, stamps); err != nil {
				return report, fmt.Errorf("record failure classifications: %w", err)
			}
		}
	}
	for _, command := range plan.Commands {
		decision, err := mutator.SubmitAdminCommand(ctx, command)
		if err != nil {
			return report, fmt.Errorf("submit automatic retry %q: %w", command.ID, err)
		}
		report.Submitted = append(report.Submitted, mutationResponse(decision))
	}
	return report, nil
}
