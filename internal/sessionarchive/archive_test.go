package sessionarchive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestEligibilityUsesSettlementAndProtectsActiveWork(t *testing.T) {
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	settled := now.Add(-2 * time.Hour)
	base := domain.Thread{ID: "background", SettledAt: &settled, TurnState: "completed", UpdatedAt: now}
	opts := Options{BackgroundAfter: 2 * time.Hour, UserAfter: 24 * time.Hour}
	if !Eligible(base, State{Background: true}, now, opts) {
		t.Fatal("exact background boundary refused")
	}
	if Eligible(base, State{Background: true}, now.Add(-time.Nanosecond), opts) {
		t.Fatal("early background archive")
	}
	if Eligible(base, State{}, now, opts) {
		t.Fatal("user received background delay")
	}
	userSettled := now.Add(-24 * time.Hour)
	user := base
	user.SettledAt = &userSettled
	if !Eligible(user, State{}, now, opts) {
		t.Fatal("exact user boundary refused")
	}
	if Eligible(user, State{}, now.Add(-time.Nanosecond), opts) {
		t.Fatal("early user archive")
	}
	for name, change := range map[string]func(*domain.Thread){
		"unknown settlement": func(t *domain.Thread) { t.SettledAt = nil },
		"active pinned":      func(t *domain.Thread) { t.SettledOverride = "active" },
		"running":            func(t *domain.Thread) { t.Running = true },
		"session starting":   func(t *domain.Thread) { t.SessionStatus = "starting" },
		"approval":           func(t *domain.Thread) { t.HasPendingApprovals = true },
		"input":              func(t *domain.Thread) { t.HasPendingUserInput = true },
		"background":         func(t *domain.Thread) { t.BackgroundWork = "monitoring" },
		"new activity":       func(t *domain.Thread) { t.LatestUserMessageAt = &now },
		"archived":           func(t *domain.Thread) { t.ArchivedAt = &now },
	} {
		t.Run(name, func(t *testing.T) {
			thread := base
			change(&thread)
			if Eligible(thread, State{Background: true}, now, opts) {
				t.Fatal("unsafe candidate")
			}
		})
	}
	if Eligible(base, State{Background: true, Busy: "pending wait"}, now, opts) {
		t.Fatal("busy thread accepted")
	}
}

type memoryStore struct {
	kv      map[string]string
	actions []domain.ActionRecord
}

func (s *memoryStore) GetKV(_ context.Context, k string) (string, bool, error) {
	v, ok := s.kv[k]
	return v, ok, nil
}
func (s *memoryStore) SetKV(_ context.Context, k, v string) error { s.kv[k] = v; return nil }
func (s *memoryStore) RecordAction(_ context.Context, a domain.ActionRecord) error {
	s.actions = append(s.actions, a)
	return nil
}

type fakeControl struct {
	thread   domain.Thread
	archives int
	err      error
}

func (c *fakeControl) GetThread(context.Context, string) (*domain.Thread, error) {
	t := c.thread
	return &t, nil
}
func (c *fakeControl) ArchiveSettledThread(context.Context, domain.Thread) error {
	c.archives++
	return c.err
}

func TestArchiveRechecksBusyStateAndRespectsManualUnarchive(t *testing.T) {
	now := time.Now()
	settled := now.Add(-3 * time.Hour)
	thread := domain.Thread{ID: "thread", SettledAt: &settled}
	store := &memoryStore{kv: map[string]string{}}
	control := &fakeControl{thread: thread}
	reads := 0
	a := &Archiver{Options: Options{BackgroundAfter: 2 * time.Hour, UserAfter: 24 * time.Hour}, Store: store, Control: control, Now: func() time.Time { return now },
		States: func(context.Context) (map[string]State, error) {
			reads++
			s := State{Background: true}
			if reads == 2 {
				s.Busy = "new pending wait"
			}
			return map[string]State{"thread": s}, nil
		}}
	if n, err := a.Run(context.Background(), []domain.Thread{thread}); err != nil || n != 0 || control.archives != 0 {
		t.Fatalf("busy recheck: %d %v", n, err)
	}
	a.States = func(context.Context) (map[string]State, error) {
		return map[string]State{"thread": {Background: true}}, nil
	}
	if n, err := a.Run(context.Background(), []domain.Thread{thread}); err != nil || n != 1 {
		t.Fatalf("archive: %d %v", n, err)
	}
	// T3 now reports the manually unarchived thread with its old settlement.
	if n, err := a.Run(context.Background(), []domain.Thread{thread}); err != nil || n != 0 || control.archives != 1 {
		t.Fatalf("unarchive respected: %d %v", n, err)
	}
	next := settled.Add(time.Minute)
	thread.SettledAt = &next
	control.thread = thread
	if n, err := a.Run(context.Background(), []domain.Thread{thread}); err != nil || n != 1 {
		t.Fatalf("new settlement: %d %v", n, err)
	}
}
func TestArchiveDryRunAndUncertainOutcome(t *testing.T) {
	now := time.Now()
	settled := now.Add(-25 * time.Hour)
	thread := domain.Thread{ID: "user", SettledAt: &settled}
	store := &memoryStore{kv: map[string]string{}}
	control := &fakeControl{thread: thread}
	a := &Archiver{Options: Options{BackgroundAfter: 2 * time.Hour, UserAfter: 24 * time.Hour, DryRun: true}, Store: store, Control: control, Now: func() time.Time { return now },
		States: func(context.Context) (map[string]State, error) { return map[string]State{}, nil }}
	if n, err := a.Run(context.Background(), []domain.Thread{thread}); err != nil || n != 1 || control.archives != 0 || len(store.kv) != 0 {
		t.Fatalf("dry-run: %d %v", n, err)
	}
	a.Options.DryRun = false
	control.err = errors.New("lost response")
	if n, err := a.Run(context.Background(), []domain.Thread{thread}); err == nil || n != 0 || len(store.kv) != 0 {
		t.Fatal("uncertainty became success")
	}
}
