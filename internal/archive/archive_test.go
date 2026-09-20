package archive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type memStore struct {
	recs map[string]Record
	busy map[string]string
	kv   map[string]string
}

func (m *memStore) SaveArchive(_ context.Context, r Record) error { m.recs[r.ThreadID] = r; return nil }
func (m *memStore) ListArchives(context.Context) ([]Record, error) {
	var out []Record
	for _, r := range m.recs {
		out = append(out, r)
	}
	return out, nil
}
func (m *memStore) BusyThreads(context.Context) (map[string]string, error)  { return m.busy, nil }
func (m *memStore) RecordAction(context.Context, domain.ActionRecord) error { return nil }
func (m *memStore) GetKV(_ context.Context, k string) (string, bool, error) {
	v, ok := m.kv[k]
	return v, ok, nil
}
func (m *memStore) SetKV(_ context.Context, k, v string) error { m.kv[k] = v; return nil }

type memControl struct {
	threads []domain.Thread
	deleted []string
	// archived is the state T3 holds: an archived thread cannot be exported.
	archived   map[string]bool
	unarchived []string
	rearchived []string
}

// ListAllThreads is what the archiver reads: every thread T3 holds, archived
// ones included.
func (c *memControl) ListAllThreads(context.Context) ([]domain.Thread, error) { return c.threads, nil }

// The fake refuses an export while the thread is archived, exactly as T3 does.
func (c *memControl) ExportThread(_ context.Context, id string) ([]byte, error) {
	if c.archived[id] {
		return nil, errors.New("T3 API 404 EnvironmentResourceNotFoundError (thread_not_found)")
	}
	return []byte(`{"thread":{"id":"` + id + `"}}`), nil
}

func (c *memControl) UnarchiveThread(_ context.Context, id string) error {
	c.unarchived = append(c.unarchived, id)
	delete(c.archived, id)
	return nil
}

func (c *memControl) RearchiveThread(_ context.Context, id string) error {
	c.rearchived = append(c.rearchived, id)
	c.archived[id] = true
	return nil
}
func (c *memControl) DeleteThread(_ context.Context, id string) error {
	c.deleted = append(c.deleted, id)
	return nil
}
func (c *memControl) ProjectTitle(context.Context, string) string { return "proj" }

// A thread archived in T3 is what cold storage is for, and it reaches this pass
// only because the pass reads the full thread index.
//
// The shell snapshot leaves an archived thread out entirely, so reading it meant
// a thread hidden in the T3 UI was never bundled and never deleted: the
// database kept every one of them, and the project holding them could not be
// removed either.
func TestArchivedThreadsAreCandidates(t *testing.T) {
	now := time.Date(2030, 1, 10, 4, 0, 0, 0, time.UTC)
	archived := now.Add(-70 * time.Hour)
	store := &memStore{recs: map[string]Record{}, busy: map[string]string{}, kv: map[string]string{}}
	control := &memControl{archived: map[string]bool{"hidden": true, "recent-archive": true}, threads: []domain.Thread{
		{ID: "hidden", Title: "archived in the UI", UpdatedAt: now.Add(-72 * time.Hour), ArchivedAt: &archived},
		{ID: "recent-archive", Title: "archived an hour ago", UpdatedAt: now.Add(-time.Hour), ArchivedAt: ptr(now.Add(-time.Hour))},
	}}
	dest := t.TempDir()
	a := New(Options{After: 48 * time.Hour, Destination: dest, HostName: "h", DataDir: t.TempDir(), DeleteFromT3: true}, store, control)
	a.SetClock(func() time.Time { return now })
	cands, skipped, err := a.Candidates(context.Background())
	if err != nil || len(cands) != 1 || cands[0].ID != "hidden" {
		t.Fatalf("candidates = %+v err=%v", cands, err)
	}
	if skipped["recent-archive"] == "" {
		t.Fatalf("a thread archived inside the retention was not kept: %v", skipped)
	}
	// T3 refuses to export an archived thread, so the bundle only exists if the
	// pass unarchived it first; the deletion then leaves no state to restore.
	n, err := a.Run(context.Background(), false)
	if err != nil || n != 1 {
		t.Fatalf("run n=%d err=%v", n, err)
	}
	if len(control.unarchived) != 1 || control.unarchived[0] != "hidden" {
		t.Fatalf("unarchived = %v", control.unarchived)
	}
	if len(control.rearchived) != 0 {
		t.Fatalf("a deleted thread was archived again: %v", control.rearchived)
	}
	if rec := store.recs["hidden"]; rec.Error != "" || !rec.DeletedFromT3 || rec.SHA256 == "" {
		t.Fatalf("record = %+v", rec)
	}
	if _, err := os.Stat(filepath.Join(dest, "h", "2030-01", "hidden.tar.gz")); err != nil {
		t.Fatalf("bundle missing: %v", err)
	}
}

// Hiding a session in the T3 UI does not restart its retention.
//
// Archiving a thread updates it, so a pass that measured idleness from
// UpdatedAt gave every hidden session the whole retention again, counted from
// the act of hiding it. A thread that settled three days ago is idle whether or
// not the UI archive touched it a minute ago.
func TestRetentionIsMeasuredFromSettlementNotFromHiding(t *testing.T) {
	now := time.Date(2030, 1, 10, 4, 0, 0, 0, time.UTC)
	settled := now.Add(-72 * time.Hour)
	hidden := now.Add(-time.Minute)
	store := &memStore{recs: map[string]Record{}, busy: map[string]string{}, kv: map[string]string{}}
	control := &memControl{archived: map[string]bool{"hidden": true}, threads: []domain.Thread{
		// Settled three days ago, hidden a minute ago, which is what set
		// UpdatedAt.
		{ID: "hidden", Title: "hidden just now", UpdatedAt: hidden, SettledAt: &settled, ArchivedAt: &hidden},
		// Settled three days ago and then used again an hour ago: still busy
		// work, not bookkeeping.
		{ID: "resumed", Title: "used after settling", UpdatedAt: now.Add(-time.Hour), SettledAt: &settled,
			LatestUserMessageAt: ptr(now.Add(-time.Hour))},
	}}
	a := New(Options{After: 48 * time.Hour, Destination: t.TempDir(), HostName: "h", DataDir: t.TempDir()}, store, control)
	a.SetClock(func() time.Time { return now })
	cands, skipped, err := a.Candidates(context.Background())
	if err != nil || len(cands) != 1 || cands[0].ID != "hidden" {
		t.Fatalf("candidates = %+v err=%v", cands, err)
	}
	if skipped["resumed"] == "" {
		t.Fatalf("a thread used an hour ago was archived: %v", skipped)
	}
}

// A bundle that fails leaves the T3 UI as it found it: the thread this pass
// unarchived in order to read it is archived again, and nothing is deleted.
func TestAFailedBundleRestoresTheArchivedState(t *testing.T) {
	now := time.Date(2030, 1, 10, 4, 0, 0, 0, time.UTC)
	store := &memStore{recs: map[string]Record{}, busy: map[string]string{}, kv: map[string]string{}}
	control := &memControl{archived: map[string]bool{"hidden": true}, threads: []domain.Thread{
		{ID: "hidden", Title: "archived in the UI", UpdatedAt: now.Add(-72 * time.Hour), ArchivedAt: ptr(now.Add(-70 * time.Hour))},
	}}
	// A destination inside a file cannot be written to, so shipping fails after
	// the export has already unarchived the thread.
	broken := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(broken, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := New(Options{After: 48 * time.Hour, Destination: broken, HostName: "h", DataDir: t.TempDir(), DeleteFromT3: true}, store, control)
	a.SetClock(func() time.Time { return now })
	if n, _ := a.Run(context.Background(), false); n != 0 {
		t.Fatalf("a failed bundle counted as archived: %d", n)
	}
	if len(control.deleted) != 0 {
		t.Fatalf("a thread with no bundle was deleted: %v", control.deleted)
	}
	if len(control.rearchived) != 1 || !control.archived["hidden"] {
		t.Fatalf("the archived state was not restored: rearchived=%v archived=%v", control.rearchived, control.archived)
	}
}

func TestCandidatesAndBundle(t *testing.T) {
	now := time.Date(2030, 1, 10, 4, 0, 0, 0, time.UTC)
	dataDir := t.TempDir()
	logDir := filepath.Join(dataDir, "userdata", "logs", "provider")
	_ = os.MkdirAll(logDir, 0o700)
	logFile := filepath.Join(logDir, "events.old.log")
	_ = os.WriteFile(logFile, []byte(`[x] CANON: {"providerThreadId":"sess-1","type":"x"}`+"\n"), 0o600)
	tdir := t.TempDir()
	transcript := filepath.Join(tdir, "sess-1.jsonl")
	_ = os.WriteFile(transcript, []byte("{}\n"), 0o600)
	_ = os.Chtimes(transcript, now, now) // written "today" by the fake clock
	dest := t.TempDir()
	store := &memStore{recs: map[string]Record{}, busy: map[string]string{"parked": "wait w-1 is waiting"}, kv: map[string]string{}}
	control := &memControl{archived: map[string]bool{}, threads: []domain.Thread{
		{ID: "old", Title: "old thread", UpdatedAt: now.Add(-72 * time.Hour), SettledAt: ptr(now.Add(-60 * time.Hour))},
		{ID: "fresh", Title: "fresh", UpdatedAt: now.Add(-time.Hour), SettledAt: ptr(now.Add(-time.Hour))},
		{ID: "running", Title: "running", Running: true, UpdatedAt: now.Add(-72 * time.Hour)},
		{ID: "parked", Title: "parked", UpdatedAt: now.Add(-72 * time.Hour), SettledAt: ptr(now.Add(-72 * time.Hour))},
		{ID: "active", Title: "idle but active", UpdatedAt: now.Add(-72 * time.Hour)},
		{ID: "pinned", Title: "pinned active", UpdatedAt: now.Add(-72 * time.Hour), SettledAt: ptr(now.Add(-72 * time.Hour)), SettledOverride: "active"},
	}}
	a := New(Options{After: 48 * time.Hour, Destination: dest, HostName: "h", DataDir: dataDir, TranscriptDirs: []string{tdir},
		DeleteFromT3: true, RemoveLocal: true, KeepTranscripts: 24 * time.Hour}, store, control)
	a.SetClock(func() time.Time { return now })
	cands, skipped, err := a.Candidates(context.Background())
	if err != nil || len(cands) != 1 || cands[0].ID != "old" {
		t.Fatalf("candidates = %+v err=%v", cands, err)
	}
	if skipped["fresh"] == "" || skipped["running"] != "running" || skipped["parked"] == "" || skipped["active"] == "" || skipped["pinned"] == "" {
		t.Fatalf("skipped = %v", skipped)
	}
	n, err := a.Run(context.Background(), false)
	if err != nil || n != 1 {
		t.Fatalf("run n=%d err=%v", n, err)
	}
	rec := store.recs["old"]
	if rec.Error != "" || !rec.DeletedFromT3 || !rec.LocalRemoved || rec.SHA256 == "" {
		t.Fatalf("record = %+v", rec)
	}
	if _, err := os.Stat(filepath.Join(dest, "h", "2030-01", "old.tar.gz")); err != nil {
		t.Fatalf("bundle missing: %v", err)
	}
	if _, err := os.Stat(logFile); !os.IsNotExist(err) {
		t.Fatal("provider log not removed")
	}
	if _, err := os.Stat(transcript); err != nil {
		t.Fatal("young transcript must be kept locally")
	}
	if len(control.deleted) != 1 || control.deleted[0] != "old" {
		t.Fatalf("deleted = %v", control.deleted)
	}
	if _, err := os.Stat(filepath.Join(dest, "h", "index.jsonl")); err != nil {
		t.Fatal("index not written")
	}
	// A second run archives nothing more.
	if n, _ := a.Run(context.Background(), false); n != 0 {
		t.Fatalf("second run archived %d", n)
	}
	// Daily schedule: due once after 03:30, then not again that day.
	due, _ := a.dueToday(context.Background(), now)
	if !due {
		t.Fatal("should be due")
	}
	_ = store.SetKV(context.Background(), "archive.last_run", now.Format(time.RFC3339))
	due, _ = a.dueToday(context.Background(), now.Add(time.Hour))
	if due {
		t.Fatal("should not be due twice in a day")
	}
}

func ptr(t time.Time) *time.Time { return &t }
