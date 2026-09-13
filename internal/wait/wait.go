// Package wait lets an agent park its thread until an external condition
// holds. The agent registers a command, ends its turn, and the steward runs
// the command periodically; when it succeeds (exit 0), gives up (exit 2) or
// the wait times out, the steward wakes the thread with a message carrying
// the outcome and the command's output, which starts the next turn.
package wait

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Status of a wait.
type Status string

const (
	StatusWaiting   Status = "waiting"
	StatusMet       Status = "met"
	StatusFailed    Status = "failed"
	StatusTimedOut  Status = "timed-out"
	StatusWoken     Status = "woken"
	StatusCancelled Status = "cancelled"
)

// WakeMode decides when a thread is woken.
type WakeMode string

const (
	// WakeEach wakes the thread as soon as this wait settles.
	WakeEach WakeMode = "each"
	// WakeAll wakes the thread once every wait in the group has settled.
	WakeAll WakeMode = "all"
)

// Wait is one registered poll.
type Wait struct {
	ID       string   `json:"id"`
	ThreadID string   `json:"threadId"`
	Name     string   `json:"name"`
	Command  []string `json:"command"`
	Dir      string   `json:"dir"`
	// Every is the interval after the first run; it doubles after every
	// "not yet" result up to MaxEvery, so a long wait polls lightly.
	Every    time.Duration `json:"every"`
	MaxEvery time.Duration `json:"maxEvery"`
	// Interval is the current interval after backoff.
	Interval time.Duration `json:"interval"`
	Timeout  time.Duration `json:"timeout"`
	// RunTimeout bounds one execution of the command.
	RunTimeout time.Duration `json:"runTimeout"`
	Group      string        `json:"group,omitempty"`
	Wake       WakeMode      `json:"wake"`
	Status     Status        `json:"status"`
	CreatedAt  time.Time     `json:"createdAt"`
	LastRunAt  *time.Time    `json:"lastRunAt,omitempty"`
	SettledAt  *time.Time    `json:"settledAt,omitempty"`
	WokenAt    *time.Time    `json:"wokenAt,omitempty"`
	Runs       int           `json:"runs"`
	LastExit   int           `json:"lastExit"`
	LastOutput string        `json:"lastOutput"`
	Reason     string        `json:"reason,omitempty"`
}

// Settled reports whether the wait has an outcome.
func (w Wait) Settled() bool {
	return w.Status == StatusMet || w.Status == StatusFailed || w.Status == StatusTimedOut
}

// currentInterval is the interval after backoff, never below Every.
func (w Wait) currentInterval() time.Duration {
	if w.Interval > w.Every {
		return w.Interval
	}
	return w.Every
}

// maxInterval is the backoff ceiling, ten minutes unless set.
func (w Wait) maxInterval() time.Duration {
	if w.MaxEvery > 0 {
		return w.MaxEvery
	}
	if w.Every > 10*time.Minute {
		return w.Every
	}
	return 10 * time.Minute
}

// Store persists waits.
type Store interface {
	SaveWait(ctx context.Context, w Wait) error
	ListWaits(ctx context.Context, threadID string) ([]Wait, error)
	RecordAction(ctx context.Context, a domain.ActionRecord) error
}

// Control wakes threads.
type Control interface {
	GetThread(ctx context.Context, threadID string) (*domain.Thread, error)
	ResumeThread(ctx context.Context, thread domain.Thread, prompt string) error
}

// Runner polls the waits and wakes threads.
type Runner struct {
	store   Store
	control Control
	log     *slog.Logger
	now     func() time.Time
	// Exec runs a command and returns its combined output and exit code;
	// replaceable in tests.
	Exec       func(ctx context.Context, w Wait) (string, int, error)
	DryRun     bool
	NodeDryRun bool
	NodeHost   string
	// DisableQuotaChecks bypasses quota-based wake holds, independently of DryRun.
	DisableQuotaChecks bool

	buckets []domain.BucketState
}

// New builds a runner.
func New(store Store, control Control, logger *slog.Logger) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{store: store, control: control, log: logger.With("component", "wait"), now: time.Now, Exec: execCommand}
}

// SetClock replaces the clock, for tests.
func (r *Runner) SetClock(now func() time.Time) { r.now = now }

// execCommand runs the wait's command with its run timeout.
func execCommand(ctx context.Context, w Wait) (string, int, error) {
	timeout := w.RunTimeout
	if timeout <= 0 {
		timeout = time.Minute
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, w.Command[0], w.Command[1:]...)
	cmd.Dir = w.Dir
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if cctx.Err() != nil {
		return out.String(), -1, fmt.Errorf("command exceeded its run timeout of %s", timeout)
	}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return out.String(), exitErr.ExitCode(), nil
		}
		return out.String(), -1, err
	}
	return out.String(), 0, nil
}

// Tick runs due waits and wakes threads whose waits settled.
func (r *Runner) Tick(ctx context.Context, _ []domain.Thread, buckets []domain.BucketState) {
	r.buckets = buckets
	r.tickNodes(ctx)
	waits, err := r.store.ListWaits(ctx, "")
	if err != nil {
		r.log.Error("list waits", "err", err)
		return
	}
	now := r.now()
	for i := range waits {
		w := &waits[i]
		if w.Status != StatusWaiting {
			continue
		}
		if w.Timeout > 0 && now.Sub(w.CreatedAt) >= w.Timeout {
			w.Status, w.Reason = StatusTimedOut, fmt.Sprintf("no result within %s", w.Timeout)
			t := now
			w.SettledAt = &t
			r.log.Info("wait timed out", "wait", w.ID, "name", w.Name, "thread", w.ThreadID)
			r.save(ctx, *w)
			continue
		}
		if w.LastRunAt != nil && now.Sub(*w.LastRunAt) < w.currentInterval() {
			continue
		}
		r.runOnce(ctx, w, now)
	}
	r.wake(ctx, waits, now)
}

// runOnce executes a wait's command and records the outcome.
func (r *Runner) runOnce(ctx context.Context, w *Wait, now time.Time) {
	out, code, err := r.Exec(ctx, *w)
	t := now
	w.LastRunAt = &t
	w.Runs++
	w.LastExit = code
	w.LastOutput = tail(out, 4000)
	switch {
	case err != nil:
		// A command that cannot run at all is a failed wait, not a poll
		// that keeps returning nothing.
		w.Status, w.Reason = StatusFailed, err.Error()
		w.SettledAt = &t
	case code == 0:
		w.Status, w.Reason = StatusMet, "condition met"
		w.SettledAt = &t
	case code == 2:
		w.Status, w.Reason = StatusFailed, "the check gave up (exit 2)"
		w.SettledAt = &t
	}
	if w.Settled() {
		r.log.Info("wait settled", "wait", w.ID, "name", w.Name, "thread", w.ThreadID, "status", string(w.Status), "runs", w.Runs)
	} else {
		// Not yet: back off so a long wait polls lightly.
		next := w.currentInterval() * 2
		if max := w.maxInterval(); next > max {
			next = max
		}
		w.Interval = next
	}
	r.save(ctx, *w)
}

// wake delivers outcomes: each settled wait on its own, or a whole group
// once every member settled.
func (r *Runner) wake(ctx context.Context, waits []Wait, now time.Time) {
	byThread := map[string][]Wait{}
	for _, w := range waits {
		byThread[w.ThreadID] = append(byThread[w.ThreadID], w)
	}
	for threadID, ws := range byThread {
		var due []Wait
		groups := map[string][]Wait{}
		for _, w := range ws {
			switch {
			case w.Status == StatusCancelled || w.Status == StatusWoken:
			case w.Wake == WakeAll && w.Group != "":
				groups[w.Group] = append(groups[w.Group], w)
			case w.Settled():
				due = append(due, w)
			}
		}
		for _, members := range groups {
			all := true
			for _, m := range members {
				if !m.Settled() {
					all = false
					break
				}
			}
			if all {
				due = append(due, members...)
			}
		}
		if len(due) == 0 {
			continue
		}
		sort.Slice(due, func(i, j int) bool { return due[i].CreatedAt.Before(due[j].CreatedAt) })
		r.wakeThread(ctx, threadID, due, now)
	}
}

func (r *Runner) wakeThread(ctx context.Context, threadID string, due []Wait, now time.Time) {
	log := r.log.With("thread", threadID)
	thread, err := r.control.GetThread(ctx, threadID)
	if err != nil {
		log.Warn("cannot read thread for wake", "err", err)
		return
	}
	if thread == nil || thread.ArchivedAt != nil {
		for _, w := range due {
			w.Status, w.Reason = StatusCancelled, "thread gone or archived before it could be woken"
			r.save(ctx, w)
		}
		return
	}
	if ok, why := r.healthy(*thread); !ok {
		log.Debug("wake held", "reason", why)
		return
	}
	text := WakeMessage(due)
	if r.DryRun {
		log.Info("dry-run: would wake thread", "waits", len(due))
		return
	}
	if err := r.control.ResumeThread(ctx, *thread, text); err != nil {
		log.Error("wake thread", "err", err)
		return
	}
	log.Info("thread woken", "waits", len(due))
	names := make([]string, 0, len(due))
	for _, w := range due {
		w.Status = StatusWoken
		t := now
		w.WokenAt = &t
		r.save(ctx, w)
		names = append(names, w.Name)
	}
	_ = r.store.RecordAction(ctx, domain.ActionRecord{At: now, Kind: "wake", ThreadID: threadID, Detail: "woken by: " + strings.Join(names, ", ")})
}

// WakeMessage renders the message that starts the thread's next turn.
func WakeMessage(due []Wait) string {
	var b strings.Builder
	if len(due) == 1 {
		fmt.Fprintf(&b, "Wait finished (T3 steward): %q %s.\n", due[0].Name, outcome(due[0]))
	} else {
		fmt.Fprintf(&b, "Waits finished (T3 steward): %d conditions settled.\n", len(due))
	}
	for _, w := range due {
		fmt.Fprintf(&b, "\n## %s: %s\nCommand: %s\nRuns: %d, last exit %d, registered %s.\n",
			w.Name, outcome(w), strings.Join(w.Command, " "), w.Runs, w.LastExit, w.CreatedAt.Local().Format("2006-01-02 15:04"))
		if out := strings.TrimSpace(w.LastOutput); out != "" {
			fmt.Fprintf(&b, "Last output:\n```\n%s\n```\n", out)
		}
	}
	b.WriteString("\nContinue the work that was waiting on this. Inspect the current state first; do not assume anything else changed while the thread was parked.")
	return b.String()
}

func outcome(w Wait) string {
	switch w.Status {
	case StatusMet:
		return "condition met"
	case StatusFailed:
		return "failed (" + w.Reason + ")"
	case StatusTimedOut:
		return "timed out (" + w.Reason + ")"
	default:
		return string(w.Status)
	}
}

func (r *Runner) save(ctx context.Context, w Wait) {
	if err := r.store.SaveWait(ctx, w); err != nil {
		r.log.Error("save wait", "wait", w.ID, "err", err)
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "...\n" + s[len(s)-n:]
}

// MarshalCommand encodes argv for storage.
func MarshalCommand(argv []string) string {
	raw, _ := json.Marshal(argv)
	return string(raw)
}

// healthy reports whether every bucket that applies to the thread is in the
// normal phase (or belongs to a window that already passed).
func (r *Runner) healthy(thread domain.Thread) (bool, string) {
	if r.DisableQuotaChecks {
		return true, ""
	}
	now := r.now()
	for _, b := range r.buckets {
		if !thread.MatchesBucket(b.Key, b.ModelSelector) {
			continue
		}
		if b.ResetsAt != nil && !b.ResetsAt.After(now) {
			continue
		}
		// A warning is advice the woken thread will receive itself; only a
		// bucket that is draining or stopped keeps the thread parked.
		if b.Phase == domain.PhaseDraining || b.Phase == domain.PhaseStopped {
			return false, fmt.Sprintf("%s is %s at %.0f%%", b.Key, b.Phase, b.UsedPercent)
		}
	}
	return true, ""
}

// ResolveThread finds the T3 thread whose provider session id matches, by
// looking for the id in the provider log file names' contents. T3 writes
// one events.<thread-id>.log per thread and records the provider's own
// session id inside it as providerThreadId.
func ResolveThread(logDir, providerSessionID string) (string, error) {
	if providerSessionID == "" {
		return "", fmt.Errorf("no provider session id")
	}
	entries, err := readDir(logDir)
	if err != nil {
		return "", err
	}
	needle := []byte(`"providerThreadId":"` + providerSessionID + `"`)
	needle2 := []byte(`"session_id":"` + providerSessionID + `"`)
	// Newest files first: the session is almost always the most recent.
	sort.Slice(entries, func(i, j int) bool { return entries[i].modTime.After(entries[j].modTime) })
	for _, e := range entries {
		if e.age > 14*24*time.Hour {
			break
		}
		if fileContains(e.path, needle, needle2) {
			name := strings.TrimSuffix(strings.TrimPrefix(e.name, "events."), ".log")
			if i := strings.Index(name, ".log."); i >= 0 {
				name = name[:i]
			}
			return name, nil
		}
	}
	return "", fmt.Errorf("no T3 thread found for provider session %s (pass --thread)", providerSessionID)
}
