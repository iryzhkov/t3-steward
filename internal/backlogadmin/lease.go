package backlogadmin

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
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
	message := err.Error()
	if !strings.Contains(message, "unknown local admin operation") && !strings.Contains(message, "unknown admin frame operation") && !strings.Contains(message, "unknown operation \"lease\"") {
		return err
	}
	upgrade := errors.New("the coordinator does not support leases; upgrade it")
	var transport *TransportError
	if errors.As(err, &transport) {
		copy := *transport
		copy.Err = upgrade
		return &copy
	}
	return upgrade
}
