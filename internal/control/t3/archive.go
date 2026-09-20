package t3

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// UnarchiveThread makes an archived thread readable again.
//
// T3 omits an archived thread from both the shell snapshot and thread detail,
// so its transcript cannot be exported while it is archived: the export answers
// 404. Cold storage therefore unarchives a thread to read it and puts the
// archived state back if it does not go on to delete it. The window is one
// export long and the thread is visible in the T3 UI for it.
func (c *Control) UnarchiveThread(ctx context.Context, threadID string) error {
	if c.DryRun {
		c.log.Info("dry-run: would unarchive thread", "thread", threadID)
		return nil
	}
	return c.dispatchThreadArchiveState(ctx, "thread.unarchive", threadID)
}

// RearchiveThread puts back the archived state of a thread this steward
// unarchived in order to read it.
//
// It re-checks none of the settlement ArchiveSettledThread does. That check
// decides whether a live thread may be hidden; this one restores a state the
// thread had a moment ago and would still have had if nothing had read it.
func (c *Control) RearchiveThread(ctx context.Context, threadID string) error {
	if c.DryRun {
		c.log.Info("dry-run: would restore the archived state of thread", "thread", threadID)
		return nil
	}
	return c.dispatchThreadArchiveState(ctx, "thread.archive", threadID)
}

// dispatchThreadArchiveState sends one archive-state command and requires the
// committed sequence T3 answers with. An archived thread is invisible to every
// read this adapter has, so absence proves nothing and only the acknowledgement
// does.
func (c *Control) dispatchThreadArchiveState(ctx context.Context, command, threadID string) error {
	result, err := c.client.Dispatch(ctx, map[string]any{
		"type": command, "commandId": newID(), "threadId": threadID,
	})
	if err == nil && result != nil && result.Sequence > 0 {
		return nil
	}
	return fmt.Errorf("%s on thread %s unproven: %w", command, threadID,
		errors.Join(err, errors.New("missing committed sequence")))
}

// ArchiveSettledThread is a reversible UI action, never thread deletion.
// Recheck the settlement just before dispatch because archiving stops sessions.
func (c *Control) ArchiveSettledThread(ctx context.Context, expected domain.Thread) error {
	current, err := c.GetThread(ctx, expected.ID)
	if err != nil {
		return err
	}
	if current == nil {
		return errors.New("archive thread disappeared")
	}
	if current.ArchivedAt != nil {
		return nil
	}
	if current.SettledAt == nil || expected.SettledAt == nil || !current.SettledAt.Equal(*expected.SettledAt) ||
		!current.Settled() || IsRunning(*current) || current.SessionStatus == "running" || current.SessionStatus == "starting" ||
		current.BackgroundWork != "" || current.HasPendingApprovals || current.HasPendingUserInput || current.HasActionableProposedPlan ||
		(current.LatestUserMessageAt != nil && current.LatestUserMessageAt.After(*current.SettledAt)) {
		return errors.New("archive eligibility changed")
	}
	if c.DryRun {
		return errors.New("UI archive control is dry-run")
	}
	token := expected.ID + ":" + expected.SettledAt.UTC().Format(time.RFC3339Nano)
	result, dispatchErr := c.client.Dispatch(ctx, map[string]any{"type": "thread.archive", "commandId": deterministicID(token, "thread.archive"), "threadId": expected.ID})
	// T3 0.0.38 acknowledges only after its event, projection and command
	// receipt transaction commits. Archived threads are omitted from both
	// shell and thread-detail reads, so their absence is not a confirmation.
	if dispatchErr == nil && result != nil && result.Sequence > 0 {
		return nil
	}
	return fmt.Errorf("archive commit unproven: %w", errors.Join(dispatchErr, errors.New("missing committed sequence")))
}
