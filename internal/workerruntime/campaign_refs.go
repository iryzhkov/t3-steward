package workerruntime

import (
	"context"
	"io"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// CampaignRefCustodian is the worker-local store of campaign-scoped commits.
// backlog.CampaignRefStore satisfies it; a worker configured without one keeps
// no commits and has nothing to release.
type CampaignRefCustodian interface {
	// Runs reports the workflow runs the store currently holds commits for.
	Runs() ([]string, error)
	ReleaseRun(ctx context.Context, workflowRunID string, log io.Writer) error
}

// ReleaseUnretainedCampaignRuns applies the coordinator's statement of which
// campaign runs this worker must keep commits for.
//
// The list is complete, so a run the worker holds and the coordinator did not
// name is a run nothing can ask about any more, and its refs go. That is the
// only way a worker on another host can ever release them: it has no read of
// coordinator state and must be told, in the exchange it already makes.
//
// The statement is refused whole by validation, because releasing on half of a
// complete list would delete commits that the missing half was keeping. Once
// accepted, applying it is best effort: a ref that could not be deleted is
// reported and tried again on the next exchange, and never fails the exchange
// or any task, because a leftover ref costs disk and a failed exchange costs
// work.
func (r *Runtime) ReleaseUnretainedCampaignRuns(ctx context.Context, request workerproto.SnapshotRequest) error {
	if err := workerproto.ValidateSnapshotRequest(request); err != nil {
		return err
	}
	if !request.CampaignRefsReported || r.config.CampaignRefs == nil {
		// An older coordinator says nothing about campaign refs, and silence is
		// not a statement: the worker keeps what it holds, exactly as it did
		// before this message existed.
		return nil
	}
	held, err := r.config.CampaignRefs.Runs()
	if err != nil {
		r.log.Error("campaign ref store could not be listed; nothing released", "error", err)
		return nil
	}
	retained := make(map[string]struct{}, len(request.RetainedCampaignRuns))
	for _, runID := range request.RetainedCampaignRuns {
		retained[runID] = struct{}{}
	}
	released := 0
	for _, runID := range held {
		if _, keep := retained[runID]; keep {
			continue
		}
		if err := r.config.CampaignRefs.ReleaseRun(ctx, runID, nil); err != nil {
			r.log.Error("campaign commit release failed; it will be retried", "run", runID, "error", err)
			continue
		}
		released++
	}
	if released != 0 {
		r.log.Info("campaign commits released", "runs", released, "retained", len(retained))
	}
	return nil
}
