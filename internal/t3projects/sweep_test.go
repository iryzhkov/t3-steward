package t3projects

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fakeControl struct {
	projects []t3control.Project
	// threads is what T3 still holds, archived ones included, which is what
	// ListAllThreads answers.
	threads      []domain.Thread
	threadsError error
	deleted      []string
	refuse       map[string]error
}

func (c *fakeControl) ListProjects(context.Context) ([]t3control.Project, error) {
	return c.projects, nil
}

func (c *fakeControl) ListAllThreads(context.Context) ([]domain.Thread, error) {
	return c.threads, c.threadsError
}

func (c *fakeControl) DeleteProject(_ context.Context, id string) error {
	if err, refused := c.refuse[id]; refused {
		return err
	}
	c.deleted = append(c.deleted, id)
	return nil
}

type fakeStore struct {
	kv      map[string]string
	actions []domain.ActionRecord
}

func newFakeStore() *fakeStore { return &fakeStore{kv: map[string]string{}} }

func (s *fakeStore) GetKV(_ context.Context, key string) (string, bool, error) {
	value, ok := s.kv[key]
	return value, ok, nil
}

func (s *fakeStore) SetKV(_ context.Context, key, value string) error {
	s.kv[key] = value
	return nil
}

func (s *fakeStore) RecordAction(_ context.Context, a domain.ActionRecord) error {
	s.actions = append(s.actions, a)
	return nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testOptions(root string) Options {
	return Options{OwnedRoots: []string{root}, After: 24 * time.Hour, Every: time.Hour, MaxPerPass: 20}
}

// The sweep removes an empty managed project and leaves everything else: a
// project a person opened, one that still holds a thread (archived threads
// included), one that is younger than the retention and one whose record
// carries no time at all.
func TestSweepDeletesOnlyEmptyManagedProjects(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	owned := filepath.Join("/state", "t3-steward", "worker", "workspaces")
	old, recent := now.Add(-72*time.Hour), now.Add(-time.Minute)
	control := &fakeControl{projects: []t3control.Project{
		{ID: "empty-managed", Title: "Steward: fleet-ops", WorkspaceRoot: filepath.Join(owned, "workers", "abc", ".projects", "hash"), UpdatedAt: old},
		{ID: "supervision", Title: "Steward supervision: run-1", WorkspaceRoot: filepath.Join(owned, "workers", "abc", "runs", "run-1"), UpdatedAt: old},
		{ID: "human", Title: "huyang development", WorkspaceRoot: "/home/igor/Work/huyang", UpdatedAt: old},
		{ID: "home", Title: "gaming-pc home", WorkspaceRoot: "/home/igor", UpdatedAt: old},
		{ID: "at-the-root", Title: "the workspaces root itself", WorkspaceRoot: owned, UpdatedAt: old},
		{ID: "occupied", Title: "Steward: busy", WorkspaceRoot: filepath.Join(owned, "workers", "abc", ".projects", "busy"), UpdatedAt: old},
		{ID: "young", Title: "Steward: young", WorkspaceRoot: filepath.Join(owned, "workers", "abc", ".projects", "young"), UpdatedAt: recent},
		{ID: "undated", Title: "Steward: undated", WorkspaceRoot: filepath.Join(owned, "workers", "abc", ".projects", "undated")},
	}}
	store := newFakeStore()
	sweeper := &Sweeper{Options: testOptions(owned), Control: control, Store: store,
		Now: func() time.Time { return now }, Logger: discardLogger()}
	// An archived thread is still a thread: T3 refuses to delete its project
	// without force, and this pass never sends force. The shell snapshot the
	// caller passes cannot see it, so it arrives from ListAllThreads -- which is
	// the whole reason the pass reads them itself.
	archived := now.Add(-48 * time.Hour)
	control.threads = []domain.Thread{{ID: "thread-1", ProjectID: "occupied", ArchivedAt: &archived}}
	deleted, err := sweeper.Run(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 || len(control.deleted) != 2 {
		t.Fatalf("deleted %d projects (%v), want the two empty managed ones", deleted, control.deleted)
	}
	for _, id := range control.deleted {
		if id != "empty-managed" && id != "supervision" {
			t.Fatalf("deleted %q, which this host does not own or is not empty", id)
		}
	}
	if len(store.actions) != 2 || store.actions[0].Kind != ActionSweep {
		t.Fatalf("audit = %+v", store.actions)
	}
	if !strings.Contains(store.actions[0].Detail, "root=") || store.actions[0].DryRun {
		t.Fatalf("an audit record does not say what was deleted: %+v", store.actions[0])
	}
}

// A thread list that cannot be read fences the pass. Deciding emptiness from
// half the threads is how a project with work in it gets deleted.
func TestSweepFencesOnAnUnreadableThreadList(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	owned := "/state/workspaces"
	control := &fakeControl{
		projects:     []t3control.Project{{ID: "empty", Title: "Steward: a", WorkspaceRoot: owned + "/a", UpdatedAt: now.Add(-72 * time.Hour)}},
		threadsError: errors.New("T3 API 503"),
	}
	sweeper := &Sweeper{Options: testOptions(owned), Control: control, Store: newFakeStore(),
		Now: func() time.Time { return now }, Logger: discardLogger()}
	deleted, err := sweeper.Run(context.Background(), nil)
	if err == nil || deleted != 0 || len(control.deleted) != 0 {
		t.Fatalf("deleted %d (%v) err=%v", deleted, control.deleted, err)
	}
}

// A dry run reports what it would delete and deletes nothing.
func TestSweepDryRunDeletesNothing(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	owned := "/state/workspaces"
	control := &fakeControl{projects: []t3control.Project{
		{ID: "empty", Title: "Steward: fleet-ops", WorkspaceRoot: owned + "/a", UpdatedAt: now.Add(-72 * time.Hour)},
	}}
	store := newFakeStore()
	options := testOptions(owned)
	options.DryRun = true
	sweeper := &Sweeper{Options: options, Control: control, Store: store,
		Now: func() time.Time { return now }, Logger: discardLogger()}
	deleted, err := sweeper.Run(context.Background(), nil)
	if err != nil || deleted != 1 {
		t.Fatalf("deleted = %d, %v", deleted, err)
	}
	if len(control.deleted) != 0 {
		t.Fatalf("a dry run deleted %v", control.deleted)
	}
	if len(store.actions) != 1 || !store.actions[0].DryRun {
		t.Fatalf("audit = %+v", store.actions)
	}
}

// A refusal belongs to the project it came from: the rest of the pass runs and
// the reason is both returned and recorded.
func TestSweepReportsOneRefusalAndContinues(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	owned := "/state/workspaces"
	idle := now.Add(-72 * time.Hour)
	control := &fakeControl{
		projects: []t3control.Project{
			{ID: "first", Title: "Steward: a", WorkspaceRoot: owned + "/a", UpdatedAt: idle},
			{ID: "second", Title: "Steward: b", WorkspaceRoot: owned + "/b", UpdatedAt: idle.Add(time.Minute)},
		},
		refuse: map[string]error{"first": errors.New("Project 'first' is not empty and cannot be deleted without force=true.")},
	}
	store := newFakeStore()
	sweeper := &Sweeper{Options: testOptions(owned), Control: control, Store: store,
		Now: func() time.Time { return now }, Logger: discardLogger()}
	deleted, err := sweeper.Run(context.Background(), nil)
	if deleted != 1 || len(control.deleted) != 1 || control.deleted[0] != "second" {
		t.Fatalf("deleted %d (%v), want only the project that could be deleted", deleted, control.deleted)
	}
	if err == nil || !strings.Contains(err.Error(), "first") {
		t.Fatalf("the refusal was not reported: %v", err)
	}
	if len(store.actions) != 2 || store.actions[0].Err == "" {
		t.Fatalf("audit = %+v", store.actions)
	}
}

// The cadence holds between passes and survives a restart, because it is read
// from the store rather than from process memory.
func TestSweepRunsOnItsCadence(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	owned := "/state/workspaces"
	control := &fakeControl{projects: []t3control.Project{
		{ID: "a", Title: "Steward: a", WorkspaceRoot: owned + "/a", UpdatedAt: now.Add(-72 * time.Hour)},
		{ID: "b", Title: "Steward: b", WorkspaceRoot: owned + "/b", UpdatedAt: now.Add(-72 * time.Hour)},
	}}
	store := newFakeStore()
	clock := now
	sweeper := &Sweeper{Options: testOptions(owned), Control: control, Store: store,
		Now: func() time.Time { return clock }, Logger: discardLogger()}
	ctx := context.Background()
	sweeper.Tick(ctx, nil, nil)
	if len(control.deleted) != 2 {
		t.Fatalf("the first pass deleted %v", control.deleted)
	}
	control.projects = []t3control.Project{{ID: "c", Title: "Steward: c", WorkspaceRoot: owned + "/c", UpdatedAt: now.Add(-72 * time.Hour)}}
	clock = now.Add(30 * time.Minute)
	sweeper.Tick(ctx, nil, nil)
	if len(control.deleted) != 2 {
		t.Fatalf("a pass ran before its cadence: %v", control.deleted)
	}
	clock = now.Add(2 * time.Hour)
	sweeper.Tick(ctx, nil, nil)
	if len(control.deleted) != 3 || control.deleted[2] != "c" {
		t.Fatalf("the pass after the cadence deleted %v", control.deleted)
	}
}

// A pass is bounded, and it takes the oldest records first so a long backlog
// drains in a stable order rather than by whatever order T3 listed.
func TestSweepIsBoundedAndTakesTheOldestFirst(t *testing.T) {
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	owned := "/state/workspaces"
	control := &fakeControl{projects: []t3control.Project{
		{ID: "newer", Title: "Steward: newer", WorkspaceRoot: owned + "/newer", UpdatedAt: now.Add(-25 * time.Hour)},
		{ID: "oldest", Title: "Steward: oldest", WorkspaceRoot: owned + "/oldest", UpdatedAt: now.Add(-300 * time.Hour)},
		{ID: "middle", Title: "Steward: middle", WorkspaceRoot: owned + "/middle", UpdatedAt: now.Add(-100 * time.Hour)},
	}}
	options := testOptions(owned)
	options.MaxPerPass = 2
	sweeper := &Sweeper{Options: options, Control: control, Store: newFakeStore(),
		Now: func() time.Time { return now }, Logger: discardLogger()}
	if _, err := sweeper.Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(control.deleted) != 2 || control.deleted[0] != "oldest" || control.deleted[1] != "middle" {
		t.Fatalf("deleted %v, want the two oldest records in order", control.deleted)
	}
}

// An ownership rule that would select projects this host did not create is
// refused, and a pass with one deletes nothing.
func TestSweepRefusesAnOwnershipRuleThatSelectsEverything(t *testing.T) {
	for _, roots := range [][]string{nil, {""}, {"/"}, {"relative/path"}} {
		options := Options{OwnedRoots: OwnedRoots(roots...), After: 24 * time.Hour, Every: time.Hour, MaxPerPass: 5}
		if err := options.Validate(); err == nil {
			t.Fatalf("owned roots %v were accepted", roots)
		}
		control := &fakeControl{projects: []t3control.Project{
			{ID: "human", Title: "huyang development", WorkspaceRoot: "/home/igor/Work/huyang", UpdatedAt: time.Now().Add(-300 * time.Hour)},
		}}
		sweeper := &Sweeper{Options: options, Control: control, Store: newFakeStore(), Logger: discardLogger()}
		if _, err := sweeper.Run(context.Background(), nil); err == nil {
			t.Fatalf("a pass ran with owned roots %v", roots)
		}
		if len(control.deleted) != 0 {
			t.Fatalf("owned roots %v deleted %v", roots, control.deleted)
		}
	}
}

// The owned roots of a host are its absolute paths below the filesystem root,
// once each: the watchdog and a bootstrap worker name their storage
// differently, and a duplicate must not make one project two candidates.
func TestOwnedRootsKeepsTheUsableRootsOnce(t *testing.T) {
	roots := OwnedRoots("/state/workspaces", "/state/workspaces/", "", "/", "relative", "/home/igor/.local/state/t3-steward/worker/workspaces")
	want := []string{"/state/workspaces", "/home/igor/.local/state/t3-steward/worker/workspaces"}
	if len(roots) != len(want) {
		t.Fatalf("roots = %v, want %v", roots, want)
	}
	for i := range want {
		if roots[i] != want[i] {
			t.Fatalf("roots = %v, want %v", roots, want)
		}
	}
	if Owned("/state/workspaces", roots) || !Owned("/state/workspaces/workers/a/.projects/b", roots) {
		t.Fatal("containment is not strict, or does not hold below an owned root")
	}
	if Owned("/state/workspaces-other/project", roots) {
		t.Fatal("a sibling directory whose name starts with an owned root was treated as owned")
	}
}
