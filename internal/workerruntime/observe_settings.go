package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func observerForSettings(options WorkerServiceOptions) func(context.Context, domain.WorkerInventory) (domain.WorkerInventory, error) {
	if options.ObserveInventory == nil {
		return nil
	}
	return func(ctx context.Context, w domain.WorkerInventory) (domain.WorkerInventory, error) {
		return options.ObserveInventory(ctx, options.Settings, w)
	}
}
