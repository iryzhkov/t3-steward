package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestQuotaResetNoticeIsClaimedOnce(t *testing.T) {
	s, err := OpenMigrated(filepath.Join(t.TempDir(), "state", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	key := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "primary"}
	warned := time.Now().UTC().Truncate(time.Second)
	resets := warned.Add(2 * time.Hour)
	epoch := domain.EpochFor(&resets)
	notice := domain.QuotaResetNotice{
		ThreadID: "t", Key: key, Epoch: epoch, Kind: domain.ActionWarn, LimitName: "Codex 5-hour",
		Threshold: 85, UsedPercent: 86, WarnedAt: warned, ResetsAt: resets,
	}
	if err := s.RecordQuotaNoticeDelivery(ctx, notice); err != nil {
		t.Fatal(err)
	}
	// The same thread, bucket and window escalating to a stop keeps one row
	// and the time it was first told.
	escalated := notice
	escalated.Kind, escalated.Threshold, escalated.UsedPercent = domain.ActionStop, 95, 96
	escalated.WarnedAt, escalated.StoppedByWatchdog = warned.Add(time.Minute), true
	if err := s.RecordQuotaNoticeDelivery(ctx, escalated); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.QuotaResetNotice(ctx, "t", key, epoch)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got.Kind != domain.ActionStop || got.Threshold != 95 || !got.StoppedByWatchdog || !got.WarnedAt.Equal(warned) {
		t.Fatalf("escalated notice = %+v", got)
	}

	// Nothing is owed before the window ends.
	if pending, err := s.PendingQuotaResetNotices(ctx, resets.Add(-time.Second)); err != nil || len(pending) != 0 {
		t.Fatalf("pending before the reset = %+v err=%v", pending, err)
	}
	pending, err := s.PendingQuotaResetNotices(ctx, resets)
	if err != nil || len(pending) != 1 || pending[0].ThreadID != "t" || !pending[0].ResetsAt.Equal(resets) {
		t.Fatalf("pending = %+v err=%v", pending, err)
	}

	// Exactly one caller may claim the advisory.
	claimed, err := s.SettleQuotaResetNotice(ctx, "t", key, epoch, resets.Add(time.Minute), "sent")
	if err != nil || !claimed {
		t.Fatalf("first claim = %v err=%v", claimed, err)
	}
	again, err := s.SettleQuotaResetNotice(ctx, "t", key, epoch, resets.Add(2*time.Minute), "sent")
	if err != nil || again {
		t.Fatalf("second claim = %v err=%v", again, err)
	}
	// A later notice for the same thread and bucket must not reopen it.
	if err := s.RecordQuotaNoticeDelivery(ctx, escalated); err != nil {
		t.Fatal(err)
	}
	if pending, err := s.PendingQuotaResetNotices(ctx, resets.Add(time.Hour)); err != nil || len(pending) != 0 {
		t.Fatalf("settled notice reopened: %+v err=%v", pending, err)
	}

	// A new window is a new advisory.
	next := resets.Add(5 * time.Hour)
	second := notice
	second.ResetsAt, second.Epoch, second.WarnedAt = next, domain.EpochFor(&next), resets.Add(time.Hour)
	if err := s.RecordQuotaNoticeDelivery(ctx, second); err != nil {
		t.Fatal(err)
	}
	if pending, err := s.PendingQuotaResetNotices(ctx, next); err != nil || len(pending) != 1 {
		t.Fatalf("new window pending = %+v err=%v", pending, err)
	}

	// A notice without a reported reset time is not recorded: there is no
	// window end for the clock to recognise.
	noReset := notice
	noReset.ThreadID, noReset.Epoch, noReset.ResetsAt = "u", "", time.Time{}
	if err := s.RecordQuotaNoticeDelivery(ctx, noReset); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.QuotaResetNotice(ctx, "u", key, ""); ok {
		t.Fatal("a notice without a reset time was recorded")
	}

	if err := s.PruneQuotaResetNotices(ctx, resets.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.QuotaResetNotice(ctx, "t", key, epoch); ok {
		t.Fatal("prune kept an old notice")
	}
}
