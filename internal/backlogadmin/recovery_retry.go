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

// RetryRecovery binds the scoped mutation to the transport-authenticated
// principal. The request's serialized Principal field is never authoritative.
func (s *Service) RetryRecovery(ctx context.Context, principal Principal, request domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error) {
	store, ok := s.reader.(RecoveryRetryStore)
	if !ok {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery retry is unavailable")
	}
	return (RecoveryExecutor{Store: store}).Retry(ctx, principal.ID, request)
}

type RecoveryRetryTransport interface {
	RetryRecovery(context.Context, domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error)
}

func (e RecoveryExecutor) Retry(ctx context.Context, authenticatedPrincipal string, request domain.RecoveryRetryRequest) (domain.RecoveryRetryReceipt, error) {
	if e.Store == nil {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery retry is unavailable")
	}
	if authenticatedPrincipal == "" {
		return domain.RecoveryRetryReceipt{}, errors.New("recovery retry requires an authenticated principal")
	}
	request.Principal = authenticatedPrincipal
	return e.Store.CommitRecoveryRetry(ctx, request)
}
