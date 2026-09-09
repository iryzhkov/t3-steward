package archive

import (
	"context"
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
}

func (c *memControl) ListThreads(context.Context) ([]domain.Thread, error) { return c.threads, nil }
func (c *memControl) ExportThread(_ context.Context, id string) ([]byte, error) {
	return []byte(`{"thread":{"id":"` + id + `"}}`), nil
}
func (c *memControl) DeleteThread(_ context.Context, id string) error {
	c.deleted = append(c.deleted, id)
	return nil
}
func (c *memControl) ProjectTitle(context.Context, string) string { return "proj" }

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
	control := &memControl{threads: []domain.Thread{
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
