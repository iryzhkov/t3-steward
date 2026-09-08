package providerlog

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// PositionStore persists per-file read positions across restarts.
type PositionStore interface {
	LoadLogPosition(ctx context.Context, path string) (domain.LogPosition, bool, error)
	SaveLogPosition(ctx context.Context, p domain.LogPosition) error
	DeleteLogPosition(ctx context.Context, path string) error
}

// Options configures the tailer.
type Options struct {
	// Dir is the provider log directory.
	Dir string
	// ScanInterval is the fallback poll when file notifications are missed.
	ScanInterval time.Duration
	// BootstrapTailBytes bounds how much of each existing file is read on the
	// very first start to learn the latest quota state. Zero disables.
	BootstrapTailBytes int64
	// MaxLineBytes bounds a single record.
	MaxLineBytes int
	Logger       *slog.Logger
}

// Tailer follows every events.*.log file in a directory.
type Tailer struct {
	opts  Options
	store PositionStore
	log   *slog.Logger

	mu      sync.Mutex
	files   map[string]*fileState
	scanReq chan struct{}
}

type fileState struct {
	path   string
	inode  uint64
	offset int64
	// partial holds an incomplete trailing line between scans.
	partial []byte
}

// NewTailer builds a tailer; it does not start reading.
func NewTailer(opts Options, store PositionStore) *Tailer {
	if opts.ScanInterval <= 0 {
		opts.ScanInterval = 10 * time.Second
	}
	if opts.MaxLineBytes <= 0 {
		opts.MaxLineBytes = 8 << 20
	}
	if opts.BootstrapTailBytes == 0 {
		opts.BootstrapTailBytes = 512 << 10
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Tailer{
		opts:    opts,
		store:   store,
		log:     opts.Logger.With("component", "providerlog"),
		files:   map[string]*fileState{},
		scanReq: make(chan struct{}, 1),
	}
}

// Run tails the directory until ctx ends, sending snapshots to output. It
// returns when ctx is done. A missing directory is not fatal: the tailer
// waits for it to appear.
func (t *Tailer) Run(ctx context.Context, output chan<- domain.QuotaSnapshot) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		t.log.Warn("file notifications unavailable; polling only", "err", err)
		watcher = nil
	} else {
		defer watcher.Close()
	}
	watching := false
	addWatch := func() {
		if watcher == nil || watching {
			return
		}
		if err := watcher.Add(t.opts.Dir); err != nil {
			t.log.Debug("cannot watch provider log directory yet", "dir", t.opts.Dir, "err", err)
			return
		}
		watching = true
	}
	addWatch()

	if err := t.bootstrap(ctx, output); err != nil {
		t.log.Warn("bootstrap scan failed", "err", err)
	}

	ticker := time.NewTicker(t.opts.ScanInterval)
	defer ticker.Stop()
	debounce := time.NewTimer(time.Hour)
	debounce.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			addWatch()
			t.scan(ctx, output)
		case <-debounce.C:
			t.scan(ctx, output)
		case <-t.scanReq:
			t.scan(ctx, output)
		case ev, ok := <-watcherEvents(watcher):
			if !ok {
				continue
			}
			if ev.Has(fsnotify.Write) || ev.Has(fsnotify.Create) || ev.Has(fsnotify.Rename) || ev.Has(fsnotify.Remove) {
				if !debounce.Stop() {
					select {
					case <-debounce.C:
					default:
					}
				}
				debounce.Reset(250 * time.Millisecond)
			}
		case err, ok := <-watcherErrors(watcher):
			if ok && err != nil {
				t.log.Debug("watcher error", "err", err)
			}
		}
	}
}

func watcherEvents(w *fsnotify.Watcher) <-chan fsnotify.Event {
	if w == nil {
		return nil
	}
	return w.Events
}

func watcherErrors(w *fsnotify.Watcher) <-chan error {
	if w == nil {
		return nil
	}
	return w.Errors
}

// RequestScan asks the run loop to scan now.
func (t *Tailer) RequestScan() {
	select {
	case t.scanReq <- struct{}{}:
	default:
	}
}

func (t *Tailer) listFiles() ([]string, error) {
	entries, err := os.ReadDir(t.opts.Dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "events.") || !strings.HasSuffix(name, ".log") {
			continue
		}
		files = append(files, filepath.Join(t.opts.Dir, name))
	}
	sort.Strings(files)
	return files, nil
}

// bootstrap runs once at start. Files with a stored position resume from
// it. Files without one are read from their tail so the daemon learns the
// latest quota state without replaying history, then positioned at EOF.
func (t *Tailer) bootstrap(ctx context.Context, output chan<- domain.QuotaSnapshot) error {
	files, err := t.listFiles()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.log.Info("provider log directory does not exist yet; waiting", "dir", t.opts.Dir)
			return nil
		}
		return err
	}
	var latest []domain.QuotaSnapshot
	for _, path := range files {
		pos, found, err := t.store.LoadLogPosition(ctx, path)
		if err != nil {
			return err
		}
		inode, size, err := statFile(path)
		if err != nil {
			continue
		}
		if found && pos.Inode == inode && pos.Offset <= size {
			t.files[path] = &fileState{path: path, inode: inode, offset: pos.Offset}
			continue
		}
		if found && pos.Inode != inode {
			// Rotated while we were down: drain the renamed file first.
			if rotated := t.findRotated(path, pos.Inode); rotated != "" {
				t.log.Debug("draining rotated file", "file", rotated, "from", pos.Offset)
				fs := &fileState{path: rotated, inode: pos.Inode, offset: pos.Offset}
				t.readNew(ctx, fs, output)
			}
		}
		if found && pos.Inode == inode && pos.Offset > size {
			t.log.Info("provider log truncated; restarting from the beginning", "file", path)
			t.files[path] = &fileState{path: path, inode: inode, offset: 0}
			continue
		}
		// New file, or rotated: read its tail for the latest state.
		snaps := t.tailSnapshots(path, size)
		latest = append(latest, snaps...)
		t.files[path] = &fileState{path: path, inode: inode, offset: size}
		if err := t.store.SaveLogPosition(ctx, domain.LogPosition{Path: path, Inode: inode, Offset: size}); err != nil {
			return err
		}
	}
	sort.SliceStable(latest, func(i, j int) bool { return latest[i].ObservedAt.Before(latest[j].ObservedAt) })
	for _, s := range latest {
		select {
		case output <- s:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t.log.Info("bootstrap complete", "files", len(files), "snapshots", len(latest))
	return nil
}

// tailSnapshots reads the last BootstrapTailBytes of a file and returns the
// newest snapshot per bucket found there.
func (t *Tailer) tailSnapshots(path string, size int64) []domain.QuotaSnapshot {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	start := int64(0)
	if size > t.opts.BootstrapTailBytes {
		start = size - t.opts.BootstrapTailBytes
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(f, size-start))
	if err != nil {
		return nil
	}
	if start > 0 {
		// Drop the partial first line.
		if nl := bytes.IndexByte(data, '\n'); nl >= 0 {
			data = data[nl+1:]
		} else {
			return nil
		}
	}
	newest := map[domain.BucketKey]domain.QuotaSnapshot{}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		snaps, err := ParseLine(string(line))
		if err != nil {
			continue
		}
		for _, s := range snaps {
			if prev, ok := newest[s.Key]; !ok || s.ObservedAt.After(prev.ObservedAt) {
				newest[s.Key] = s
			}
		}
	}
	out := make([]domain.QuotaSnapshot, 0, len(newest))
	for _, s := range newest {
		out = append(out, s)
	}
	return out
}

func (t *Tailer) findRotated(path string, inode uint64) string {
	for i := 1; i <= 10; i++ {
		candidate := fmt.Sprintf("%s.%d", path, i)
		if ino, _, err := statFile(candidate); err == nil && ino == inode {
			return candidate
		}
	}
	return ""
}

// scan reads new data from every file.
func (t *Tailer) scan(ctx context.Context, output chan<- domain.QuotaSnapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	files, err := t.listFiles()
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			t.log.Debug("list provider logs", "err", err)
		}
		return
	}
	seen := map[string]bool{}
	for _, path := range files {
		seen[path] = true
		inode, size, err := statFile(path)
		if err != nil {
			continue
		}
		fs, ok := t.files[path]
		switch {
		case !ok:
			// A file created after start: read it fully; it is new work.
			fs = &fileState{path: path, inode: inode, offset: 0}
			t.files[path] = fs
		case fs.inode != inode:
			// Rotated: drain the renamed file, then start over.
			if rotated := t.findRotated(path, fs.inode); rotated != "" {
				old := &fileState{path: rotated, inode: fs.inode, offset: fs.offset, partial: fs.partial}
				t.readNew(ctx, old, output)
			}
			fs.inode = inode
			fs.offset = 0
			fs.partial = nil
		case size < fs.offset:
			t.log.Info("provider log truncated; restarting from the beginning", "file", path)
			fs.offset = 0
			fs.partial = nil
		}
		if size > fs.offset {
			t.readNew(ctx, fs, output)
		}
	}
	for path := range t.files {
		if !seen[path] {
			delete(t.files, path)
			_ = t.store.DeleteLogPosition(ctx, path)
		}
	}
}

// readNew reads complete lines from fs.offset to EOF and emits snapshots.
func (t *Tailer) readNew(ctx context.Context, fs *fileState, output chan<- domain.QuotaSnapshot) {
	f, err := os.Open(fs.path)
	if err != nil {
		return
	}
	defer f.Close()
	if _, err := f.Seek(fs.offset, io.SeekStart); err != nil {
		return
	}
	reader := bufio.NewReaderSize(f, 256<<10)
	consumed := fs.offset
	for {
		chunk, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			// Long line: accumulate.
			fs.partial = append(fs.partial, chunk...)
			consumed += int64(len(chunk))
			if len(fs.partial) > t.opts.MaxLineBytes {
				t.log.Warn("dropping oversized provider log record", "file", fs.path, "offset", consumed)
				fs.partial = fs.partial[:0]
			}
			continue
		}
		if err != nil {
			// EOF or read error: keep whatever is incomplete for next time.
			if len(chunk) > 0 {
				fs.partial = append(fs.partial, chunk...)
			}
			break
		}
		consumed += int64(len(chunk))
		var line []byte
		if len(fs.partial) > 0 {
			line = append(fs.partial, chunk...)
			fs.partial = nil
		} else {
			line = chunk
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			continue
		}
		snaps, perr := ParseLine(string(line))
		if perr != nil {
			if !errors.Is(perr, ErrNotRateLimit) {
				t.log.Warn("malformed provider log record", "file", fs.path, "offset", consumed-int64(len(chunk)), "err", perr)
			}
			continue
		}
		for _, s := range snaps {
			select {
			case output <- s:
			case <-ctx.Done():
				return
			}
		}
		// Persist after every complete record so a crash never replays it.
		fs.offset = consumed - int64(len(fs.partial))
	}
	fs.offset = consumed - int64(len(fs.partial))
	if err := t.store.SaveLogPosition(ctx, domain.LogPosition{Path: fs.path, Inode: fs.inode, Offset: fs.offset}); err != nil {
		t.log.Warn("persist log position", "file", fs.path, "err", err)
	}
}

// ReadFile parses every rate-limit record of one file, in order. It is used
// by the replay command and by tests.
func ReadFile(path string) ([]domain.QuotaSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []domain.QuotaSnapshot
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var snaps []domain.QuotaSnapshot
		var perr error
		if strings.HasPrefix(line, "{") {
			snaps, perr = ParseJSON([]byte(line), time.Time{})
		} else {
			snaps, perr = ParseLine(line)
		}
		if perr != nil {
			if errors.Is(perr, ErrNotRateLimit) {
				continue
			}
			return nil, fmt.Errorf("%s:%d: %w", path, lineNo, perr)
		}
		out = append(out, snaps...)
	}
	return out, scanner.Err()
}
