package sessionarchive

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestFailuresCountAgainstPerPassLimit(t *testing.T) {
	now := time.Now()
	settled := now.Add(-25 * time.Hour)
	thread := domain.Thread{ID: "a", SettledAt: &settled}
	store := &memoryStore{kv: map[string]string{}}
	control := &fakeControl{thread: thread, err: errors.New("unproven")}
	a := &Archiver{Options: Options{BackgroundAfter: 2 * time.Hour, UserAfter: 24 * time.Hour, MaxPerPass: 2}, Store: store, Control: control, Now: func() time.Time { return now },
		States: func(context.Context) (map[string]State, error) { return nil, nil }}
	threads := []domain.Thread{thread, thread, thread}
	threads[1].ID = "b"
	threads[2].ID = "c"
	n, err := a.Run(context.Background(), threads)
	if n != 0 || err == nil || control.archives != 2 || len(store.actions) != 2 {
		t.Fatalf("count=%d err=%v attempts=%d audit=%d", n, err, control.archives, len(store.actions))
	}
}
