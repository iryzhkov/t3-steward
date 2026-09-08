package backlog

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fakeStore struct {
	states     map[string]json.RawMessage
	dispatched map[string]string
	obs        []domain.Observation
	actions    []domain.ActionRecord
}

func newFakeStore() *fakeStore {
	return &fakeStore{states: map[string]json.RawMessage{}, dispatched: map[string]string{}}
}

func (f *fakeStore) SaveTaskState(_ context.Context, id, _ string, state any) error {
	raw, err := json.Marshal(state)
	f.states[id] = raw
	return err
}
func (f *fakeStore) LoadTaskStates(context.Context) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	for k, v := range f.states {
		out[k] = v
	}
	return out, nil
}
func (f *fakeStore) RegisterDispatchedThread(_ context.Context, threadID, taskID, _ string, _ time.Time) error {
	f.dispatched[threadID] = taskID
	return nil
}
func (f *fakeStore) DispatchedThreads(context.Context) (map[string]string, error) {
	return f.dispatched, nil
}
func (f *fakeStore) Observations(_ context.Context, from, to time.Time) ([]domain.Observation, error) {
	var out []domain.Observation
	for _, o := range f.obs {
		if !o.ObservedAt.Before(from) && o.ObservedAt.Before(to) {
			out = append(out, o)
		}
	}
	return out, nil
}
func (f *fakeStore) RecordAction(_ context.Context, a domain.ActionRecord) error {
	f.actions = append(f.actions, a)
	return nil
}

type fakeControl struct {
	projects  []t3control.Project
	started   []t3control.NewThreadInput
	resumed   []string
	lastText  map[string]string
	nextID    int
	failStart bool
}

func (c *fakeControl) ListProjects(context.Context) ([]t3control.Project, error) {
	return c.projects, nil
}
func (c *fakeControl) CreateAndStartThread(_ context.Context, in t3control.NewThreadInput) (string, error) {
	if c.failStart {
		return "", context.DeadlineExceeded
	}
	c.nextID++
	c.started = append(c.started, in)
	return "thread-" + string(rune('0'+c.nextID)), nil
}
func (c *fakeControl) ResumeThread(_ context.Context, t domain.Thread, _ string) error {
	c.resumed = append(c.resumed, t.ID)
	return nil
}
func (c *fakeControl) LastAssistantMessage(_ context.Context, id string) (string, error) {
	return c.lastText[id], nil
}

func writeTask(t *testing.T, dir, id, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, id+".md"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func healthy(used float64, resets time.Time) domain.BucketState {
	r := resets
	return domain.BucketState{
		Key:   domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"},
		Phase: domain.PhaseNormal, Healthy: true, UsedPercent: used, ResetsAt: &r,
	}
}

func setup(t *testing.T, quiet time.Duration) (*Runner, *fakeStore, *fakeControl, string, *time.Time) {
	t.Helper()
	dir := t.TempDir()
	store := newFakeStore()
	control := &fakeControl{
		projects: []t3control.Project{{ID: "p1", Title: "laptop home", DefaultModelSelection: map[string]any{"instanceId": "claudeAgent", "model": "claude-opus-5"}}},
		lastText: map[string]string{},
	}
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, "caches"), 0o700); err != nil {
		t.Fatal(err)
	}
	cache := `{"instanceId":"claudeAgent","driver":"claudeAgent","enabled":true,"installed":true,"status":"ready","auth":{"status":"authenticated"},"models":[{"slug":"claude-opus-5","capabilities":{"optionDescriptors":[{"id":"effort","type":"select","options":[{"id":"high"},{"id":"medium"},{"id":"low"}]}]}},{"slug":"claude-sonnet-5"}]}`
	if err := os.WriteFile(filepath.Join(dataDir, "caches", "claudeAgent.json"), []byte(cache), 0o600); err != nil {
		t.Fatal(err)
	}
	r := New(Options{Dir: dir, QuietFor: quiet, SafetyMargin: 10, FallbackPerHour: 5, MinSamples: 3, LongWindowCap: 80, DataDir: dataDir}, store, control)
	now := time.Date(2030, 1, 2, 1, 0, 0, 0, time.UTC) // Wednesday 01:00
	r.SetClock(func() time.Time { return now })
	return r, store, control, dir, &now
}

func TestDispatchOrderAndStatusProtocol(t *testing.T) {
	r, store, control, dir, now := setup(t, 30*time.Minute)
	writeTask(t, dir, "low", "---\nproject: laptop home\nimportance: 2\n---\nlow task")
	writeTask(t, dir, "high", "---\nproject: laptop home\nimportance: 5\ndifficulty: 2\n---\nhigh task")
	ctx := context.Background()
	buckets := []domain.BucketState{healthy(10, now.Add(4*time.Hour))}

	// Interactive thread running: nothing starts.
	r.Tick(ctx, []domain.Thread{{ID: "me", Running: true, ProviderInstanceID: "claudeAgent"}}, buckets)
	if len(control.started) != 0 {
		t.Fatalf("dispatched during interactive use: %+v", control.started)
	}
	// Quiet for 31 minutes: the important task starts, the other waits.
	*now = now.Add(31 * time.Minute)
	r.Tick(ctx, nil, buckets)
	if len(control.started) != 1 || control.started[0].Title != "high task" {
		t.Fatalf("started = %+v", control.started)
	}
	if control.started[0].ModelSelection["model"] != "claude-opus-5" || control.started[0].RuntimeMode != "full-access" {
		t.Fatalf("selection = %+v", control.started[0])
	}
	if store.dispatched["thread-1"] != "high" {
		t.Fatalf("dispatched registry = %v", store.dispatched)
	}
	// While it runs (same provider), nothing else starts.
	running := []domain.Thread{{ID: "thread-1", Running: true, TurnState: "running", TurnID: "t1", ProviderInstanceID: "claudeAgent"}}
	*now = now.Add(10 * time.Minute)
	r.Tick(ctx, running, buckets)
	if len(control.started) != 1 {
		t.Fatalf("second task started while the first runs")
	}
	// The turn completes with "continue": a second turn is dispatched on
	// the same thread after the quiet period (the backlog thread itself is
	// not interactive).
	store.obs = []domain.Observation{
		{Key: buckets[0].Key, ObservedAt: now.Add(-9 * time.Minute), UsedPercent: 10, ResetsAt: buckets[0].ResetsAt, EventID: "o1", ThreadID: "thread-1"},
		{Key: buckets[0].Key, ObservedAt: now.Add(-1 * time.Minute), UsedPercent: 18, ResetsAt: buckets[0].ResetsAt, EventID: "o2", ThreadID: "thread-1"},
	}
	control.lastText["thread-1"] = "Did half.\n\nBACKLOG STATUS: continue"
	done := []domain.Thread{{ID: "thread-1", Running: false, TurnState: "completed", TurnID: "t1", ProviderInstanceID: "claudeAgent"}}
	r.Tick(ctx, done, buckets)
	// The continuation goes out in the same tick: the backlog thread is
	// not interactive, so the gate is still open.
	st := r.States()["high"]
	if st.Status != StatusRunning || st.Turns != 1 || st.MeasuredCost != 8 || st.EstimatedCost != 8 {
		t.Fatalf("state after continue = %+v", st)
	}
	if len(control.resumed) != 1 || control.resumed[0] != "thread-1" {
		t.Fatalf("resumed = %v", control.resumed)
	}
	if r.States()["high"].Status != StatusRunning {
		t.Fatalf("status = %s", r.States()["high"].Status)
	}
	// Second turn ends with done.
	control.lastText["thread-1"] = "All done.\nBACKLOG STATUS: done"
	done[0].TurnID = "t2"
	r.Tick(ctx, done, buckets)
	if r.States()["high"].Status != StatusDone {
		t.Fatalf("status = %s (%s)", r.States()["high"].Status, r.States()["high"].Reason)
	}
	// Now the low task goes.
	*now = now.Add(time.Minute)
	r.Tick(ctx, nil, buckets)
	if len(control.started) != 2 || control.started[1].Title != "low task" {
		t.Fatalf("started = %+v", control.started)
	}
}

func TestGateRespectsQuotaAndForecast(t *testing.T) {
	r, _, control, dir, now := setup(t, 0)
	writeTask(t, dir, "big", "---\nproject: laptop home\ndifficulty: 5\n---\nbig task") // 50%
	ctx := context.Background()
	// 60% used, reset in 4h, fallback demand 5%/h = 20%: 60+50+20 > 90.
	r.Tick(ctx, nil, []domain.BucketState{healthy(60, now.Add(4*time.Hour))})
	if len(control.started) != 0 {
		t.Fatalf("started despite no headroom: %+v", r.States()["big"])
	}
	// Reset in 30 minutes: only a slice of the task lands before it.
	r.Tick(ctx, nil, []domain.BucketState{healthy(60, now.Add(30*time.Minute))})
	if len(control.started) != 1 {
		t.Fatalf("not started although the window resets soon: %s", r.States()["big"].Reason)
	}
}

func TestUnhealthyBucketBlocksEvenUngatedTasks(t *testing.T) {
	r, _, control, dir, now := setup(t, 0)
	writeTask(t, dir, "urgent", "---\nproject: laptop home\ngate: false\n---\nurgent")
	b := healthy(96, now.Add(time.Hour))
	b.Healthy, b.Phase = false, domain.PhaseStopped
	r.Tick(context.Background(), nil, []domain.BucketState{b})
	if len(control.started) != 0 {
		t.Fatal("started while the bucket is stopped")
	}
}

func TestNeedsInputAndFailure(t *testing.T) {
	r, _, control, dir, now := setup(t, 0)
	writeTask(t, dir, "ask", "---\nproject: laptop home\n---\nask")
	ctx := context.Background()
	buckets := []domain.BucketState{healthy(5, now.Add(4*time.Hour))}
	r.Tick(ctx, nil, buckets)
	if len(control.started) != 1 {
		t.Fatal("not started")
	}
	r.Tick(ctx, []domain.Thread{{ID: "thread-1", Running: false, TurnState: "running", HasPendingUserInput: true, ProviderInstanceID: "claudeAgent"}}, buckets)
	if r.States()["ask"].Status != StatusNeedsInput {
		t.Fatalf("status = %s", r.States()["ask"].Status)
	}
	// Retry, then the thread disappears.
	st := r.States()["ask"]
	st.Status, st.ThreadID = StatusPending, ""
	_ = r.store.SaveTaskState(ctx, st.ID, string(st.Status), st)
	r.Tick(ctx, nil, buckets)
	r.Tick(ctx, nil, buckets) // thread-2 missing from the list
	if r.States()["ask"].Status != StatusFailed {
		t.Fatalf("status = %s", r.States()["ask"].Status)
	}
}

func TestParseTask(t *testing.T) {
	task, err := Parse([]byte("---\nproject: x\nimportance: 4\nnot_before: 2030-01-01T00:00:00Z\n---\n# Do the thing\n\nbody"))
	if err != nil {
		t.Fatal(err)
	}
	if task.Importance != 4 || task.Difficulty != 3 || task.MaxTurns != 3 || task.NotBefore == nil || task.Prompt == "" {
		t.Fatalf("task = %+v", task)
	}
	if _, err := Parse([]byte("no frontmatter")); err == nil {
		t.Fatal("missing project accepted")
	}
	if _, err := Parse([]byte("---\nproject: x\nimportance: 9\n---\nbody")); err == nil {
		t.Fatal("importance 9 accepted")
	}
}

func TestForwardToOtherHost(t *testing.T) {
	r, _, control, dir, now := setup(t, 0)
	var forwarded []string
	r.opts.LocalHost = "laptop"
	r.opts.Forward = func(_ context.Context, host string, task Task) error {
		forwarded = append(forwarded, host+":"+task.ID)
		return nil
	}
	writeTask(t, dir, "remote", "---\nproject: laptop home\nhost: normandy\n---\nremote task")
	writeTask(t, dir, "here", "---\nproject: laptop home\nhost: laptop\n---\nlocal task")
	buckets := []domain.BucketState{healthy(5, now.Add(4*time.Hour))}
	r.Tick(context.Background(), nil, buckets)
	if len(forwarded) != 1 || forwarded[0] != "normandy:remote" {
		t.Fatalf("forwarded = %v", forwarded)
	}
	if r.States()["remote"].Status != StatusForwarded {
		t.Fatalf("status = %s", r.States()["remote"].Status)
	}
	if len(control.started) != 1 || control.started[0].Title != "local task" {
		t.Fatalf("started = %+v", control.started)
	}
	// A default host that is another machine forwards unnamed tasks.
	r.opts.DefaultHost = "homelab"
	writeTask(t, dir, "unnamed", "---\nproject: laptop home\n---\nunnamed task")
	r.Tick(context.Background(), nil, buckets)
	if len(forwarded) != 2 || forwarded[1] != "homelab:unnamed" {
		t.Fatalf("forwarded = %v", forwarded)
	}
	if !IsLocalHost("omarchy-normandy", "omarchy-normandy") || IsLocalHost("normandy", "omarchy-normandy") || !IsLocalHost("local", "x") {
		t.Fatal("IsLocalHost rules")
	}
}

func TestValidationParksBadTasks(t *testing.T) {
	r, _, control, dir, now := setup(t, 0)
	writeTask(t, dir, "badmodel", "---\nproject: laptop home\nmodel: claude-opus-9\ninstance: claudeAgent\n---\nuses a model that does not exist on this host at all")
	writeTask(t, dir, "badoption", "---\nproject: laptop home\nmodel: claude-opus-5\ninstance: claudeAgent\noptions: {effort: extreme}\n---\nuses an option value the model does not offer, long enough prompt")
	writeTask(t, dir, "badproject", "---\nproject: nowhere\n---\nnames a project that does not exist on this host, long enough prompt")
	writeTask(t, dir, "good", "---\nproject: laptop home\nmodel: claude-sonnet-5\ninstance: claudeAgent\n---\nvalid task with a prompt that is long enough to pass the length warning")
	buckets := []domain.BucketState{healthy(5, now.Add(4*time.Hour))}
	r.Tick(context.Background(), nil, buckets)
	for _, id := range []string{"badmodel", "badoption", "badproject"} {
		st := r.States()[id]
		if st.Status != StatusFailed || !strings.HasPrefix(st.Reason, "invalid: ") {
			t.Fatalf("%s: %+v", id, st)
		}
	}
	if len(control.started) != 1 || control.started[0].ModelSelection["model"] != "claude-sonnet-5" {
		t.Fatalf("started = %+v", control.started)
	}
	if !strings.Contains(r.States()["badmodel"].Reason, "available: claude-opus-5, claude-sonnet-5") {
		t.Fatalf("reason = %s", r.States()["badmodel"].Reason)
	}
}
