// Package archive moves finished T3 threads to cold storage: one bundle
// per thread with the thread's full export from T3, its provider log
// files and the provider's own transcript, shipped to a destination
// (a local directory or host:path over SSH), verified by checksum, and
// only then removed locally and deleted from T3.
package archive

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Record is what the store keeps about one archived thread.
type Record struct {
	ThreadID      string    `json:"threadId"`
	Title         string    `json:"title"`
	Project       string    `json:"project"`
	Host          string    `json:"host"`
	ArchivedAt    time.Time `json:"archivedAt"`
	Destination   string    `json:"destination"`
	Bytes         int64     `json:"bytes"`
	SHA256        string    `json:"sha256"`
	Files         []string  `json:"files"`
	DeletedFromT3 bool      `json:"deletedFromT3"`
	LocalRemoved  bool      `json:"localRemoved"`
	Error         string    `json:"error,omitempty"`
}

// Store persists archive records and answers whether a thread is busy.
type Store interface {
	SaveArchive(ctx context.Context, r Record) error
	ListArchives(ctx context.Context) ([]Record, error)
	// BusyThreads returns thread ids that must not be archived: pending
	// resume intents, running or parked backlog tasks, waiting waits.
	BusyThreads(ctx context.Context) (map[string]string, error)
	RecordAction(ctx context.Context, a domain.ActionRecord) error
	GetKV(ctx context.Context, key string) (string, bool, error)
	SetKV(ctx context.Context, key, value string) error
}

// Control is what the archiver needs from T3.
type Control interface {
	ListThreads(ctx context.Context) ([]domain.Thread, error)
	ExportThread(ctx context.Context, threadID string) ([]byte, error)
	DeleteThread(ctx context.Context, threadID string) error
	ProjectTitle(ctx context.Context, projectID string) string
}

// Options configure the archiver.
type Options struct {
	// After is how long a thread must have been idle.
	After time.Duration
	// Destination is a directory, or host:path for SSH.
	Destination string
	// HostName labels this machine's bundles.
	HostName string
	// DataDir is T3's base directory.
	DataDir string
	// TranscriptDirs are searched for the provider transcript by session
	// id: ~/.claude/projects and ~/.codex/sessions by default.
	TranscriptDirs []string
	// DeleteFromT3 deletes the thread from T3 after the bundle is verified.
	DeleteFromT3 bool
	// RemoveLocal removes provider logs and transcripts after verification.
	RemoveLocal bool
	// KeepTranscripts keeps a bundled transcript locally until its last
	// modification is this old.
	KeepTranscripts time.Duration
	// At is the local time of day ("03:30") the daily run starts.
	At string
	// MaxPerRun bounds the threads archived in one run.
	MaxPerRun int
	DryRun    bool
	Logger    *slog.Logger
}

// Archiver runs the daily archive.
type Archiver struct {
	opts    Options
	store   Store
	control Control
	log     *slog.Logger
	now     func() time.Time
}

// New builds an archiver.
func New(opts Options, store Store, control Control) *Archiver {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.After <= 0 {
		opts.After = 48 * time.Hour
	}
	if opts.At == "" {
		opts.At = "03:30"
	}
	if opts.MaxPerRun <= 0 {
		opts.MaxPerRun = 50
	}
	if len(opts.TranscriptDirs) == 0 {
		home, _ := os.UserHomeDir()
		opts.TranscriptDirs = []string{filepath.Join(home, ".claude", "projects"), filepath.Join(home, ".codex", "sessions")}
	}
	return &Archiver{opts: opts, store: store, control: control, log: opts.Logger.With("component", "archive"), now: time.Now}
}

// SetClock replaces the clock, for tests.
func (a *Archiver) SetClock(now func() time.Time) { a.now = now }

// Tick runs the daily archive when its time has come.
func (a *Archiver) Tick(ctx context.Context, _ []domain.Thread, _ []domain.BucketState) {
	now := a.now()
	due, err := a.dueToday(ctx, now)
	if err != nil || !due {
		return
	}
	a.log.Info("daily archive starting")
	n, err := a.Run(ctx, false)
	if err != nil {
		a.log.Error("archive run failed", "err", err)
		return
	}
	_ = a.store.SetKV(ctx, "archive.last_run", now.UTC().Format(time.RFC3339))
	a.log.Info("daily archive finished", "archived", n)
}

func (a *Archiver) dueToday(ctx context.Context, now time.Time) (bool, error) {
	var hh, mm int
	if _, err := fmt.Sscanf(a.opts.At, "%d:%d", &hh, &mm); err != nil {
		return false, err
	}
	local := now.Local()
	at := time.Date(local.Year(), local.Month(), local.Day(), hh, mm, 0, 0, local.Location())
	if local.Before(at) {
		return false, nil
	}
	last, ok, err := a.store.GetKV(ctx, "archive.last_run")
	if err != nil {
		return false, err
	}
	if ok {
		if t, err := time.Parse(time.RFC3339, last); err == nil && !t.Before(at) {
			return false, nil
		}
	}
	return true, nil
}

// Candidates lists threads eligible for archiving now.
func (a *Archiver) Candidates(ctx context.Context) ([]domain.Thread, map[string]string, error) {
	threads, err := a.control.ListThreads(ctx)
	if err != nil {
		return nil, nil, err
	}
	busy, err := a.store.BusyThreads(ctx)
	if err != nil {
		return nil, nil, err
	}
	done, err := a.store.ListArchives(ctx)
	if err != nil {
		return nil, nil, err
	}
	archived := map[string]bool{}
	for _, r := range done {
		if r.Error == "" {
			archived[r.ThreadID] = true
		}
	}
	now := a.now()
	skipped := map[string]string{}
	var out []domain.Thread
	for _, t := range threads {
		switch {
		case archived[t.ID]:
			continue
		case t.Running:
			skipped[t.ID] = "running"
		case now.Sub(t.UpdatedAt) < a.opts.After:
			skipped[t.ID] = fmt.Sprintf("updated %s ago", now.Sub(t.UpdatedAt).Round(time.Minute))
		case t.HasPendingApprovals || t.HasPendingUserInput:
			skipped[t.ID] = "waiting for user input"
		case !t.Settled():
			skipped[t.ID] = "still active in T3 (not settled or archived)"
		default:
			if why, ok := busy[t.ID]; ok {
				skipped[t.ID] = why
				continue
			}
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	return out, skipped, nil
}

// Run archives every candidate (up to MaxPerRun) and returns how many.
func (a *Archiver) Run(ctx context.Context, dryRun bool) (int, error) {
	candidates, _, err := a.Candidates(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, t := range candidates {
		if n >= a.opts.MaxPerRun {
			break
		}
		if dryRun || a.opts.DryRun {
			a.log.Info("dry-run: would archive", "thread", t.ID, "title", t.Title, "idle", a.now().Sub(t.UpdatedAt).Round(time.Hour))
			n++
			continue
		}
		rec, err := a.ArchiveThread(ctx, t)
		if err != nil {
			a.log.Error("archive thread", "thread", t.ID, "title", t.Title, "err", err)
			rec.Error = err.Error()
			_ = a.store.SaveArchive(ctx, rec)
			continue
		}
		n++
		if ctx.Err() != nil {
			break
		}
	}
	return n, nil
}

// ArchiveThread bundles, ships, verifies, then removes and deletes.
func (a *Archiver) ArchiveThread(ctx context.Context, t domain.Thread) (Record, error) {
	now := a.now()
	rec := Record{ThreadID: t.ID, Title: t.Title, Project: a.control.ProjectTitle(ctx, t.ProjectID), Host: a.opts.HostName, ArchivedAt: now}
	export, err := a.control.ExportThread(ctx, t.ID)
	if err != nil {
		return rec, fmt.Errorf("export: %w", err)
	}
	logFiles := a.providerLogs(t.ID)
	session := providerSessionID(logFiles)
	transcripts := a.transcripts(session)
	meta := map[string]any{
		"threadId": t.ID, "title": t.Title, "project": rec.Project, "projectId": t.ProjectID, "host": a.opts.HostName,
		"providerInstance": t.ProviderInstanceID, "model": t.Model, "providerSessionId": session,
		"updatedAt": t.UpdatedAt, "archivedAt": now, "providerLogs": logFiles, "transcripts": transcripts,
		"steward": "t3-steward",
	}
	metaJSON, _ := json.MarshalIndent(meta, "", "  ")

	tmp, err := os.CreateTemp("", "t3-archive-*.tar.gz")
	if err != nil {
		return rec, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	hasher := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(tmp, hasher))
	tw := tar.NewWriter(gz)
	add := func(name string, data []byte) error {
		return addBytes(tw, name, data, now)
	}
	if err := add("meta.json", metaJSON); err != nil {
		return rec, err
	}
	if err := add("thread.json", export); err != nil {
		return rec, err
	}
	rec.Files = []string{"meta.json", "thread.json"}
	for _, f := range logFiles {
		name := "provider-logs/" + filepath.Base(f)
		if err := addFile(tw, name, f); err != nil {
			return rec, err
		}
		rec.Files = append(rec.Files, name)
	}
	for _, f := range transcripts {
		name := "transcripts/" + filepath.Base(f)
		if err := addFile(tw, name, f); err != nil {
			return rec, err
		}
		rec.Files = append(rec.Files, name)
	}
	if err := tw.Close(); err != nil {
		return rec, err
	}
	if err := gz.Close(); err != nil {
		return rec, err
	}
	if err := tmp.Close(); err != nil {
		return rec, err
	}
	rec.SHA256 = hex.EncodeToString(hasher.Sum(nil))
	if info, err := os.Stat(tmpPath); err == nil {
		rec.Bytes = info.Size()
	}

	// Ship and verify.
	relDir := filepath.Join(a.opts.HostName, now.Format("2006-01"))
	name := t.ID + ".tar.gz"
	dest, err := ship(ctx, a.opts.Destination, relDir, name, tmpPath, rec.SHA256)
	if err != nil {
		return rec, fmt.Errorf("ship: %w", err)
	}
	rec.Destination = dest
	indexLine, _ := json.Marshal(map[string]any{
		"threadId": t.ID, "title": t.Title, "project": rec.Project, "host": a.opts.HostName,
		"archivedAt": now, "updatedAt": t.UpdatedAt, "bytes": rec.Bytes, "sha256": rec.SHA256, "file": filepath.Join(relDir, name),
	})
	if err := appendIndex(ctx, a.opts.Destination, a.opts.HostName, string(indexLine)); err != nil {
		a.log.Warn("append archive index", "err", err)
	}
	a.log.Info("thread archived", "thread", t.ID, "title", t.Title, "bytes", rec.Bytes, "dest", dest)

	// Only after the far side verified the checksum.
	if a.opts.RemoveLocal {
		for _, f := range logFiles {
			if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
				a.log.Warn("remove local file", "file", f, "err", err)
			}
		}
		// Transcripts stay until they are old enough that nothing else
		// still reads them; the bundle already holds a copy.
		for _, f := range transcripts {
			info, err := os.Stat(f)
			if err != nil {
				continue
			}
			if a.opts.KeepTranscripts > 0 && now.Sub(info.ModTime()) < a.opts.KeepTranscripts {
				continue
			}
			if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
				a.log.Warn("remove local file", "file", f, "err", err)
			}
		}
		rec.LocalRemoved = true
	}
	if a.opts.DeleteFromT3 {
		if err := a.control.DeleteThread(ctx, t.ID); err != nil {
			a.log.Warn("delete thread from T3", "thread", t.ID, "err", err)
		} else {
			rec.DeletedFromT3 = true
		}
	}
	if err := a.store.SaveArchive(ctx, rec); err != nil {
		return rec, err
	}
	_ = a.store.RecordAction(ctx, domain.ActionRecord{At: now, Kind: "archive", ThreadID: t.ID,
		Detail: fmt.Sprintf("%s -> %s (%d bytes, deleted_from_t3=%v)", t.Title, dest, rec.Bytes, rec.DeletedFromT3)})
	return rec, nil
}

func (a *Archiver) providerLogs(threadID string) []string {
	dir := filepath.Join(a.opts.DataDir, "userdata", "logs", "provider")
	matches, _ := filepath.Glob(filepath.Join(dir, "events."+strings.ToLower(threadID)+".log*"))
	sort.Strings(matches)
	return matches
}

// providerSessionID reads the provider's own session id from the thread's
// log (Claude session id, Codex thread id).
func providerSessionID(logFiles []string) string {
	for _, f := range logFiles {
		fh, err := os.Open(f)
		if err != nil {
			continue
		}
		r := bufio.NewReaderSize(fh, 1<<20)
		for i := 0; i < 2000; i++ {
			line, err := r.ReadBytes('\n')
			if idx := bytes.Index(line, []byte(`"providerThreadId":"`)); idx >= 0 {
				rest := line[idx+len(`"providerThreadId":"`):]
				if end := bytes.IndexByte(rest, '"'); end > 0 {
					fh.Close()
					return string(rest[:end])
				}
			}
			if err != nil {
				break
			}
		}
		fh.Close()
	}
	return ""
}

// transcripts finds files named after the session id under the
// transcript directories (Claude: <session>.jsonl; Codex: rollout-...-<id>.jsonl).
func (a *Archiver) transcripts(session string) []string {
	if session == "" {
		return nil
	}
	var out []string
	for _, root := range a.opts.TranscriptDirs {
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if strings.Contains(d.Name(), session) {
				out = append(out, path)
			}
			return nil
		})
	}
	sort.Strings(out)
	return out
}

func addBytes(tw *tar.Writer, name string, data []byte, mod time.Time) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(data)), ModTime: mod}); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

func addFile(tw *tar.Writer, name, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: info.Size(), ModTime: info.ModTime()}); err != nil {
		return err
	}
	_, err = io.Copy(tw, f)
	return err
}

// ship copies the bundle to destination/relDir/name and verifies the
// checksum on the far side. It returns the final path.
func ship(ctx context.Context, destination, relDir, name, src, sum string) (string, error) {
	host, base, remote := splitDestination(destination)
	dir := filepath.Join(base, relDir)
	final := filepath.Join(dir, name)
	if !remote {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return "", err
		}
		tmp := final + ".part"
		if err := copyFile(src, tmp); err != nil {
			return "", err
		}
		got, err := fileSHA256(tmp)
		if err != nil {
			return "", err
		}
		if got != sum {
			os.Remove(tmp)
			return "", fmt.Errorf("checksum mismatch after copy")
		}
		return final, os.Rename(tmp, final)
	}
	run := func(args ...string) ([]byte, error) {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(cctx, args[0], args[1:]...)
		return cmd.CombinedOutput()
	}
	if out, err := run("ssh", "-o", "BatchMode=yes", host, "mkdir", "-p", shellQuote(dir)); err != nil {
		return "", fmt.Errorf("mkdir on %s: %v: %s", host, err, strings.TrimSpace(string(out)))
	}
	tmp := final + ".part"
	if out, err := run("scp", "-q", "-o", "BatchMode=yes", src, host+":"+tmp); err != nil {
		return "", fmt.Errorf("scp to %s: %v: %s", host, err, strings.TrimSpace(string(out)))
	}
	out, err := run("ssh", "-o", "BatchMode=yes", host, "sha256sum", shellQuote(tmp))
	if err != nil {
		return "", fmt.Errorf("sha256sum on %s: %v: %s", host, err, strings.TrimSpace(string(out)))
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 || fields[0] != sum {
		_, _ = run("ssh", "-o", "BatchMode=yes", host, "rm", "-f", shellQuote(tmp))
		return "", fmt.Errorf("checksum mismatch on %s", host)
	}
	if out, err := run("ssh", "-o", "BatchMode=yes", host, "mv", shellQuote(tmp), shellQuote(final)); err != nil {
		return "", fmt.Errorf("mv on %s: %v: %s", host, err, strings.TrimSpace(string(out)))
	}
	return host + ":" + final, nil
}

func appendIndex(ctx context.Context, destination, hostName, line string) error {
	host, base, remote := splitDestination(destination)
	index := filepath.Join(base, hostName, "index.jsonl")
	if !remote {
		f, err := os.OpenFile(index, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.WriteString(line + "\n")
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, "ssh", "-o", "BatchMode=yes", host, "sh", "-c", shellQuote("cat >> "+shellQuote(index)))
	cmd.Stdin = strings.NewReader(line + "\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// splitDestination parses "host:/path" or "/path".
func splitDestination(dest string) (host, path string, remote bool) {
	if i := strings.Index(dest, ":"); i > 0 && !strings.Contains(dest[:i], "/") {
		return dest[:i], dest[i+1:], true
	}
	return "", dest, false
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
