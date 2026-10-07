package workerruntime

import (
	"context"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// withAttemptProcessLimits carries an attempt's reservation to the setup and
// verification processes, and the first-turn instructions, of the driver call
// made under the returned context. The reservation is the demand the
// coordinator committed with the assignment; an older coordinator, or an
// unsized task, commits none and the processes run unlimited as before.
func withAttemptProcessLimits(ctx context.Context, record AttemptRecord) context.Context {
	var demand domain.ResourceDemand
	switch {
	case record.Assignment.ExecutorDemand != nil:
		demand = *record.Assignment.ExecutorDemand
	case record.Assignment.Placement != nil:
		demand = record.Assignment.Placement.Demand
	}
	return backlog.WithProcessLimits(ctx, backlog.ProcessLimitsFor(demand))
}
