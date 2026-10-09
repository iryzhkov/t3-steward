package workerruntime

import (
	"context"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ProviderTurnError reads the attempt's thread from T3 and reports the
// provider-side error its latest turn ended with; see
// backlog.ClassifyProviderTurnEnd. No provider runs in no-effects mode, so
// there is never one there.
func (d *LocalDriver) ProviderTurnError(ctx context.Context, pkg workerproto.ExecutionPackage) (backlog.ProviderTurnError, bool, error) {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return backlog.ProviderTurnError{}, false, err
	} else if scoped != nil {
		return scoped.ProviderTurnError(ctx, pkg)
	}
	if d.Config.DryRun {
		return backlog.ProviderTurnError{}, false, nil
	}
	archive, err := d.T3.ExportThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return backlog.ProviderTurnError{}, false, err
	}
	return backlog.ClassifyProviderTurnEnd(archive, pkg.Identity.ThreadID)
}

// ResumeAfterProviderError starts the resume turn in the attempt's own
// session, through the same turn start the turn-end nudge uses, under
// identities derived from token, so a retry is recognised by T3.
func (d *LocalDriver) ResumeAfterProviderError(ctx context.Context, pkg workerproto.ExecutionPackage, token, text string) error {
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return err
	} else if scoped != nil {
		return scoped.ResumeAfterProviderError(ctx, pkg, token, text)
	}
	if d.Config.DryRun {
		return nil
	}
	thread, err := d.requiredThread(ctx, pkg.Identity.ThreadID)
	if err != nil {
		return err
	}
	if sender, ok := d.T3.(interface {
		SendOnce(context.Context, domain.Thread, string, string, string) error
	}); ok {
		return sender.SendOnce(ctx, thread, token, providerResumePurpose, text)
	}
	return d.T3.ResumeThread(ctx, thread, text)
}
