package providerlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

type memPositions struct {
	mu sync.Mutex
	m  map[string]domain.LogPosition
}

func newMemPositions() *memPositions { return &memPositions{m: map[string]domain.LogPosition{}} }

func (p *memPositions) LoadLogPosition(_ context.Context, path string) (domain.LogPosition, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pos, ok := p.m[path]
	return pos, ok, nil
}

func (p *memPositions) SaveLogPosition(_ context.Context, pos domain.LogPosition) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.m[pos.Path] = pos
	return nil
}

func (p *memPositions) DeleteLogPosition(_ context.Context, path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.m, path)
	return nil
}

func codexLine(ts string, used int, id string) string {
	return fmt.Sprintf(`[%s] CANON: {"type":"account.rate-limits.updated","eventId":"%s","provider":"codex","createdAt":"%s","payload":{"rateLimits":{"rateLimits":{"limitId":"codex","primary":{"resetsAt":1893474000,"usedPercent":%d,"windowDurationMins":300}}}}}`+"\n", ts, id, ts, used)
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
}

func collect(t *testing.T, out <-chan domain.QuotaSnapshot, n int, timeout time.Duration) []domain.QuotaSnapshot {
	t.Helper()
	var got []domain.QuotaSnapshot
	deadline := time.After(timeout)
	for len(got) < n {
		select {
		case s := <-out:
			got = append(got, s)
		case <-deadline:
			t.Fatalf("timed out with %d/%d snapshots", len(got), n)
		}
	}
	return got
}

func TestTailerBootstrapTailAndFollow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.thread-a.log")
	appendFile(t, path, codexLine("2030-01-01T00:00:00Z", 10, "old-1")+codexLine("2030-01-01T00:01:00Z", 20, "old-2"))
	store := newMemPositions()
	tailer := NewTailer(Options{Dir: dir, ScanInterval: 50 * time.Millisecond}, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan domain.QuotaSnapshot, 64)
	go func() { _ = tailer.Run(ctx, out) }()

	// Bootstrap emits only the newest state per bucket, not history.
	got := collect(t, out, 1, 2*time.Second)
	if got[0].SourceEventID != "old-2" {
		t.Fatalf("bootstrap emitted %s", got[0].SourceEventID)
	}
	select {
	case s := <-out:
		t.Fatalf("unexpected extra bootstrap snapshot %s", s.SourceEventID)
	case <-time.After(200 * time.Millisecond):
	}

	// New complete lines are followed; a partial line waits.
	appendFile(t, path, codexLine("2030-01-01T00:02:00Z", 30, "new-1"))
	partial := codexLine("2030-01-01T00:03:00Z", 40, "new-2")
	appendFile(t, path, partial[:len(partial)/2])
	got = collect(t, out, 1, 2*time.Second)
	if got[0].SourceEventID != "new-1" {
		t.Fatalf("got %s", got[0].SourceEventID)
	}
	select {
	case s := <-out:
		t.Fatalf("partial line emitted: %s", s.SourceEventID)
	case <-time.After(200 * time.Millisecond):
	}
	appendFile(t, path, partial[len(partial)/2:])
	got = collect(t, out, 1, 2*time.Second)
	if got[0].SourceEventID != "new-2" {
		t.Fatalf("got %s", got[0].SourceEventID)
	}
}

func TestTailerRotationByRename(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("rotation by rename is not detected without inodes; documented Windows limitation")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "events.thread-b.log")
	appendFile(t, path, codexLine("2030-01-01T00:00:00Z", 10, "r-1"))
	store := newMemPositions()
	tailer := NewTailer(Options{Dir: dir, ScanInterval: 50 * time.Millisecond}, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan domain.QuotaSnapshot, 64)
	go func() { _ = tailer.Run(ctx, out) }()
	collect(t, out, 1, 2*time.Second)

	// Write a line that the tailer may or may not have seen before the
	// rename, then rotate the way T3 does: rename to .1 and create anew.
	tailer.mu.Lock() // freeze scanning while we rotate
	appendFile(t, path, codexLine("2030-01-01T00:01:00Z", 20, "r-2"))
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, codexLine("2030-01-01T00:02:00Z", 30, "r-3"))
	tailer.mu.Unlock()

	got := collect(t, out, 2, 3*time.Second)
	ids := map[string]bool{}
	for _, s := range got {
		ids[s.SourceEventID] = true
	}
	if !ids["r-2"] || !ids["r-3"] {
		t.Fatalf("after rotation got %v", ids)
	}
}

func TestTailerTruncation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.thread-c.log")
	appendFile(t, path, codexLine("2030-01-01T00:00:00Z", 10, "t-1")+codexLine("2030-01-01T00:01:00Z", 20, "t-2"))
	store := newMemPositions()
	tailer := NewTailer(Options{Dir: dir, ScanInterval: 50 * time.Millisecond}, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan domain.QuotaSnapshot, 64)
	go func() { _ = tailer.Run(ctx, out) }()
	collect(t, out, 1, 2*time.Second)

	if err := os.WriteFile(path, []byte(codexLine("2030-01-01T00:05:00Z", 5, "t-3")), 0o644); err != nil {
		t.Fatal(err)
	}
	got := collect(t, out, 1, 3*time.Second)
	if got[0].SourceEventID != "t-3" {
		t.Fatalf("after truncation got %s", got[0].SourceEventID)
	}
}

func TestTailerResumesFromStoredPosition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.thread-d.log")
	first := codexLine("2030-01-01T00:00:00Z", 10, "p-1")
	appendFile(t, path, first)
	store := newMemPositions()
	inode, _, err := statFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = store.SaveLogPosition(context.Background(), domain.LogPosition{Path: path, Inode: inode, Offset: int64(len(first))})
	appendFile(t, path, codexLine("2030-01-01T00:01:00Z", 20, "p-2"))

	tailer := NewTailer(Options{Dir: dir, ScanInterval: 50 * time.Millisecond}, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan domain.QuotaSnapshot, 64)
	go func() { _ = tailer.Run(ctx, out) }()
	got := collect(t, out, 1, 2*time.Second)
	if got[0].SourceEventID != "p-2" {
		t.Fatalf("resumed at wrong place: %s", got[0].SourceEventID)
	}
	select {
	case s := <-out:
		t.Fatalf("replayed old record %s", s.SourceEventID)
	case <-time.After(200 * time.Millisecond):
	}
}
