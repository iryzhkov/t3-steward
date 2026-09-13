package backlogadmin

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type WorkerEnrollmentHandler func(context.Context, Principal, domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error)

func (s *Service) SetWorkerEnrollmentHandler(handler WorkerEnrollmentHandler) {
	s.enrollWorker = handler
}
func (s *Service) EnrollWorker(ctx context.Context, p Principal, r domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error) {
	if err := s.authorizer.Authorize(ctx, p, Action{Kind: "worker-enrollment"}); err != nil {
		return domain.WorkerEnrollment{}, err
	}
	if s.enrollWorker == nil {
		return domain.WorkerEnrollment{}, errors.New("worker enrollment unavailable")
	}
	return s.enrollWorker(ctx, p, r)
}
func (c LocalClient) EnrollWorker(ctx context.Context, r domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error) {
	var response localResponse
	err := c.call(ctx, localRequest{Version: LocalTransportVersion, Operation: "worker-enrollment", WorkerEnrollment: &r}, &response)
	if err != nil {
		return domain.WorkerEnrollment{}, err
	}
	if response.WorkerEnrollment == nil {
		return domain.WorkerEnrollment{}, errors.New("missing worker enrollment response")
	}
	return *response.WorkerEnrollment, nil
}
