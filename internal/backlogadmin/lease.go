package backlogadmin

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type LeaseStore interface {
	ExecuteLease(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error)
}
type LeaseTransport interface {
	Lease(context.Context, domain.LeaseRequest) (domain.LeaseResponse, error)
}

func (s *Service) Lease(ctx context.Context, p Principal, r domain.LeaseRequest) (domain.LeaseResponse, error) {
	store, ok := s.reader.(LeaseStore)
	if !ok {
		return domain.LeaseResponse{}, errors.New("lease service is unavailable")
	}
	return executeLease(ctx, store, p, r)
}
func executeLease(ctx context.Context, store LeaseStore, p Principal, r domain.LeaseRequest) (domain.LeaseResponse, error) {
	if p.ID == "" {
		return domain.LeaseResponse{}, errors.New("lease requires an authenticated principal")
	}
	admin := false
	for _, role := range p.Roles {
		if role == SupervisorRole && r.Mutating() {
			return domain.LeaseResponse{Code: 10, Message: "supervisor principals cannot mutate leases"}, nil
		}
		if role == LocalAdminRole || role == RemoteAdminRole || (role == SupervisorRole && !r.Mutating()) {
			admin = true
		}
	}
	if !admin {
		return domain.LeaseResponse{Code: 10, Message: "lease requires coordinator admin authority"}, nil
	}
	if store == nil {
		return domain.LeaseResponse{}, errors.New("lease service is unavailable")
	}
	r.Principal = p.ID
	if err := r.Validate(); err != nil {
		return domain.LeaseResponse{}, err
	}
	return store.ExecuteLease(ctx, r)
}
func (d adminDispatch) lease(ctx context.Context, p Principal, r localRequest, response *localResponse) {
	if r.Lease == nil || r.Query != nil || r.Mutation != nil || r.ArtifactID != "" || r.Submission != nil || r.SubmissionSize != 0 ||
		r.ScheduleDefinition != nil || r.UnknownRecovery != nil || r.QuarantineRelease != nil || r.Supervision != nil ||
		r.RecoveryRetry != nil || r.NodeWait != nil || r.GraphAmendment != nil || r.WorkerEnrollment != nil || r.ApprovalFrame != nil {
		response.Error = "malformed lease request"
		return
	}
	handler, ok := d.service.(interface {
		Lease(context.Context, Principal, domain.LeaseRequest) (domain.LeaseResponse, error)
	})
	if !ok {
		response.Error = "lease service is unavailable"
		return
	}
	value, err := handler.Lease(ctx, p, *r.Lease)
	if err != nil {
		response.Error = err.Error()
		return
	}
	response.LeaseResponse = &value
}

func (c LocalClient) Lease(ctx context.Context, r domain.LeaseRequest) (domain.LeaseResponse, error) {
	var response localResponse
	if err := c.call(ctx, localRequest{Version: LocalTransportVersion, Operation: localOperationLease, Lease: &r}, &response); err != nil {
		return domain.LeaseResponse{}, leaseCompatibilityError(err)
	}
	if response.LeaseResponse == nil {
		return domain.LeaseResponse{}, c.fail(ClassProtocol, localOperationLease, errors.New("coordinator lease returned no response"))
	}
	return *response.LeaseResponse, nil
}
func (c *SSHClient) Lease(ctx context.Context, r domain.LeaseRequest) (domain.LeaseResponse, error) {
	response, _, err := c.roundTrip(ctx, localRequest{Version: LocalTransportVersion, Operation: localOperationLease, Lease: &r}, nil, false)
	if err != nil {
		return domain.LeaseResponse{}, leaseCompatibilityError(err)
	}
	if response.LeaseResponse == nil {
		return domain.LeaseResponse{}, c.fail(ClassProtocol, localOperationLease, errors.New("coordinator lease returned no response"))
	}
	return *response.LeaseResponse, nil
}
func leaseCompatibilityError(err error) error {
	if err == nil {
		return nil
	}
	// Match the coordinator's own answer exactly rather than anywhere in the
	// text: a refusal that echoes client input (an unknown lease action) must
	// not be rewritten into upgrade guidance.
	var transport *TransportError
	answer := err
	if errors.As(err, &transport) && transport.Err != nil {
		answer = transport.Err
	}
	// An older SSH coordinator refuses the unknown operation unsigned and exits
	// nonzero, so the client appends its stderr to the refusal; the
	// coordinator's own answer is then the one wrapped cause.
	if !olderCoordinatorAnswer(answer.Error()) {
		cause := errors.Unwrap(answer)
		if cause == nil || !olderCoordinatorAnswer(cause.Error()) {
			return err
		}
	}
	upgrade := errors.New("the coordinator does not support leases; upgrade it")
	if transport != nil {
		copy := *transport
		copy.Err = upgrade
		return &copy
	}
	return upgrade
}

// olderCoordinatorAnswer reports whether message is exactly what a coordinator
// without leases answers to a lease request.
func olderCoordinatorAnswer(message string) bool {
	switch message {
	// Older local coordinators strictly decode the envelope before dispatch:
	// the missing Lease field therefore fails before the unknown-operation path.
	case `decode local admin frame: json: unknown field "lease"`,
		"unknown local admin operation", "unknown admin frame operation",
		`coordinator-exchange: unknown operation "lease"`:
		return true
	}
	return false
}
