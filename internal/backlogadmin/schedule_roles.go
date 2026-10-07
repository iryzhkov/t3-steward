package backlogadmin

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// SetScheduleTriggerResolver installs the coordinator's shared timer/manual seam.
func (s *Service) SetScheduleTriggerResolver(resolve func(context.Context, domain.ScheduleTriggerRequest) (domain.ScheduleTriggerRequest, error)) {
	s.scheduleTriggerResolver = resolve
}
