package daemon

import (
	"context"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The interactive override (S-18): a thread the user resumed or started by
// hand after a watchdog stop is the user choosing to spend the quota. It is
// recorded in the bucket's thread notices as user-resumed, with the user
// message time as the evidence, and for the rest of that window epoch no path
// stops or drains it: not the stopped-phase poll, not execute on a reading,
// not the grace timer. It is warned at most once. The record is evidence, not
// a change of phase; a rearm clears the notices with the epoch, and the next
// stop holds the thread again until the user acts. Threads a live steward
// attempt owns never reach this rule: the worker owns them.

// userMessageTolerance is how much newer than the stop a user message must be
// to count as the user's: the watchdog's own warn and drain messages are user
// messages too, sent around the stop time.
const userMessageTolerance = 10 * time.Second

// userResumedAfter reports whether the thread's latest user message is newer
// than the bucket's stop, beyond the tolerance.
func userResumedAfter(stoppedAt *time.Time, t domain.Thread) bool {
	if stoppedAt == nil || t.LatestUserMessageAt == nil {
		return false
	}
	return t.LatestUserMessageAt.After(stoppedAt.Add(userMessageTolerance))
}

// splitUserResumed separates the threads the watchdog may act on from those
// the user resumed after the bucket's stop. A thread already recorded as
// user-resumed in this epoch is exempt whatever its current message time; a
// thread whose latest user message is newer than the stop is recorded now.
// When the notices cannot be read nothing is exempt beyond the live rule, so
// a store failure degrades to today's behaviour and is logged.
func (d *Daemon) splitUserResumed(ctx context.Context, threads []domain.Thread, st domain.BucketState) (watchdog, userResumed []domain.Thread) {
	if len(threads) == 0 {
		return nil, nil
	}
	recorded, err := d.store.ThreadNotices(ctx, st.Key, st.Epoch, domain.NoticeUserResumed)
	if err != nil {
		d.log.Error("read user-resumed notices", "bucket", st.Key.String(), "err", err)
		recorded = nil
	}
	for _, t := range threads {
		if at, ok := recorded[t.ID]; ok {
			d.log.Debug("thread is user-resumed this epoch; left alone", "thread", t.ID, "bucket", st.Key.String(), "user_message_at", at)
			userResumed = append(userResumed, t)
			continue
		}
		if !userResumedAfter(st.StoppedAt, t) {
			watchdog = append(watchdog, t)
			continue
		}
		if _, err := d.store.MarkThreadNotice(ctx, t.ID, st.Key, st.Epoch, domain.NoticeUserResumed, *t.LatestUserMessageAt); err != nil {
			d.log.Error("record user-resumed notice", "thread", t.ID, "bucket", st.Key.String(), "err", err)
		}
		d.log.Info("thread resumed by the user after the stop; not stopped or drained again this window",
			"thread", t.ID, "title", t.Title, "bucket", st.Key.String(), "stopped_at", st.StoppedAt, "user_message_at", *t.LatestUserMessageAt)
		userResumed = append(userResumed, t)
	}
	return watchdog, userResumed
}
