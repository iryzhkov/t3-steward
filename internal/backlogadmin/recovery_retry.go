package backlogadmin

import (
	"context"
	"errors"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// RecoveryRetryStore is the sole mutation capability granted to a repair
// executor. It intentionally excludes every reviewer, gate, hold, graph and
// operator action.
type RecoveryRetryStore interface {
	CommitRecoveryRetry(context.Context, domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error)
}

type RecoveryExecutor struct {
	Store RecoveryRetryStore
}

func (e RecoveryExecutor) Retry(ctx context.Context, request domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error) {
	if e.Store == nil {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery retry is unavailable")
	}
	return e.Store.CommitRecoveryRetry(ctx, request)
}
