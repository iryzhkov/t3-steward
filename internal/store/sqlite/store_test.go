package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestRoundTrips(t *testing.T) {
	s, err := OpenMigrated(filepath.Join(t.TempDir(), "state", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	key := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "primary"}
	now := time.Now()
	if err := s.SaveBucket(ctx, domain.BucketState{Key: key, Phase: domain.PhaseWarned, UsedPercent: 86, ObservedAt: now}); err != nil {
		t.Fatal(err)
	}
	st, err := s.LoadBucket(ctx, key)
	if err != nil || st.Phase != domain.PhaseWarned || st.UsedPercent != 86 {
		t.Fatalf("bucket = %+v err=%v", st, err)
	}
	if fresh, _ := s.MarkEventSeen(ctx, "e1", now); !fresh {
		t.Fatal("first event not fresh")
	}
	if fresh, _ := s.MarkEventSeen(ctx, "e1", now); fresh {
		t.Fatal("duplicate event fresh")
	}
	if fresh, _ := s.MarkThreadNotice(ctx, "t", key, "1", domain.ActionWarn, now); !fresh {
		t.Fatal("notice not fresh")
	}
	if fresh, _ := s.MarkThreadNotice(ctx, "t", key, "1", domain.ActionWarn, now); fresh {
		t.Fatal("duplicate notice fresh")
	}
	if err := s.ClearThreadNotices(ctx, key); err != nil {
		t.Fatal(err)
	}
	if fresh, _ := s.MarkThreadNotice(ctx, "t", key, "1", domain.ActionWarn, now); !fresh {
		t.Fatal("notice not cleared")
	}
	intent := domain.ResumeIntent{ThreadID: "t", Status: domain.ResumePending, StoppedAt: now, Buckets: []domain.BucketKey{key}}
	if err := s.SaveResumeIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListResumeIntents(ctx, domain.ResumePending)
	if err != nil || len(list) != 1 || len(list[0].Buckets) != 1 {
		t.Fatalf("intents = %+v err=%v", list, err)
	}
	if err := s.SaveLogPosition(ctx, domain.LogPosition{Path: "/x", Inode: 7, Offset: 9}); err != nil {
		t.Fatal(err)
	}
	pos, ok, _ := s.LoadLogPosition(ctx, "/x")
	if !ok || pos.Inode != 7 || pos.Offset != 9 {
		t.Fatalf("pos = %+v", pos)
	}
	if err := s.RecordAction(ctx, domain.ActionRecord{Kind: domain.ActionStop, Bucket: key.String(), ThreadID: "t", Detail: "d"}); err != nil {
		t.Fatal(err)
	}
	acts, _ := s.RecentActions(ctx, 5)
	if len(acts) != 1 || acts[0].Kind != domain.ActionStop {
		t.Fatalf("actions = %+v", acts)
	}
}

func TestSQLiteFilePathsWithURLMetacharacters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state ?# percent%.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetKV(context.Background(), "key", "value"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	readOnly, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	value, found, err := readOnly.GetKV(context.Background(), "key")
	if err != nil || !found || value != "value" {
		t.Fatalf("read-only value = %q, found %t, err %v", value, found, err)
	}
}
