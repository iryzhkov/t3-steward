package quotatelemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// fakeSource is an in-memory coordinator database: the audit stream, the
// current records and the retained check reports.
type fakeSource struct {
	mu          sync.Mutex
	audit       []domain.AuditEvent
	assignments map[string]domain.Assignment
	frozen      map[string]domain.Assignment
	attempts    map[string]domain.Attempt
	tasks       map[string]domain.Task
	snapshots   []domain.WorkerSnapshot
	buckets     []domain.BucketState
	artifacts   []domain.Artifact
	content     map[string][]byte
	opened      []string
	fail        error
	panics      bool
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		assignments: map[string]domain.Assignment{},
		frozen:      map[string]domain.Assignment{},
		attempts:    map[string]domain.Attempt{},
		tasks:       map[string]domain.Task{},
		content:     map[string][]byte{},
	}
}

func (f *fakeSource) check() error {
	if f.panics {
		panic("source exploded")
	}
	return f.fail
}

func (f *fakeSource) MaxAuditSequence(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	return int64(len(f.audit)), nil
}

func (f *fakeSource) AuditEventsAfter(_ context.Context, after int64, limit int) ([]domain.AuditEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return nil, err
	}
	var out []domain.AuditEvent
	for _, event := range f.audit {
		if event.Sequence > after && len(out) < limit {
			out = append(out, event)
		}
	}
	return out, nil
}

func (f *fakeSource) LoadAssignment(_ context.Context, id string) (domain.Assignment, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return domain.Assignment{}, false, err
	}
	assignment, found := f.assignments[id]
	return assignment, found, nil
}

func (f *fakeSource) LoadAssignmentEpoch(_ context.Context, id string, epoch int64) (domain.Assignment, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return domain.Assignment{}, false, err
	}
	assignment, found := f.frozen[workKey(id, epoch)]
	return assignment, found, nil
}

func (f *fakeSource) LoadAttempt(_ context.Context, id string) (domain.Attempt, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return domain.Attempt{}, false, err
	}
	attempt, found := f.attempts[id]
	return attempt, found, nil
}

func (f *fakeSource) LoadTask(_ context.Context, id string) (domain.Task, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return domain.Task{}, false, err
	}
	task, found := f.tasks[id]
	return task, found, nil
}

func (f *fakeSource) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return nil, err
	}
	return append([]domain.WorkerSnapshot(nil), f.snapshots...), nil
}

func (f *fakeSource) ListBuckets(context.Context) ([]domain.BucketState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return nil, err
	}
	return append([]domain.BucketState(nil), f.buckets...), nil
}

func (f *fakeSource) ListCheckArtifacts(_ context.Context, taskID, attemptID string) ([]domain.Artifact, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return nil, err
	}
	var out []domain.Artifact
	for _, artifact := range f.artifacts {
		if artifact.TaskID == taskID && artifact.AttemptID == attemptID &&
			(artifact.Kind == domain.ArtifactVerification || artifact.Kind == domain.ArtifactGate) {
			out = append(out, artifact)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *fakeSource) Close() error { return nil }

func (f *fakeSource) open(_ context.Context, id string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = append(f.opened, id)
	content, found := f.content[id]
	if !found {
		return nil, fmt.Errorf("artifact %q not found", id)
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

// appendAudit adds one audit row with the next sequence.
func (f *fakeSource) appendAudit(kind, assignmentID, attemptID, taskID string, epoch int64, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sequence := int64(len(f.audit) + 1)
	f.audit = append(f.audit, domain.AuditEvent{
		ID: fmt.Sprintf("%s:%s:%d:%d", kind, assignmentID, epoch, sequence), Sequence: sequence, Kind: kind,
		WorkflowRunID: "run-1", TaskID: taskID, AttemptID: attemptID,
		TargetType: domain.AdminTargetAssignment, TargetID: assignmentID,
		Detail:    json.RawMessage(fmt.Sprintf(`{"assignmentEpoch":%d,"idempotencyIdentity":"x","outcome":"y"}`, epoch)),
		CreatedAt: at,
	})
}

func (f *fakeSource) setAttempt(attempt domain.Attempt) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts[attempt.ID] = attempt
}

func (f *fakeSource) setAssignment(assignment domain.Assignment) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assignments[assignment.ID] = assignment
}

// addWork registers a task, an attempt and an assignment for it.
func (f *fakeSource) addWork(task domain.Task, attempt domain.Attempt, assignment domain.Assignment) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[task.ID] = task
	f.attempts[attempt.ID] = attempt
	f.assignments[assignment.ID] = assignment
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var testBase = time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)

func newTestRecorder(t *testing.T, storePath string, source *fakeSource, clock *fakeClock) *Recorder {
	t.Helper()
	recorder := &Recorder{
		StorePath:    storePath,
		OpenSource:   func(context.Context) (Source, error) { return source, nil },
		OpenArtifact: source.open,
		Now:          clock.Now,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	t.Cleanup(recorder.Close)
	return recorder
}

func testStorePath(t *testing.T) string {
	return filepath.Join(t.TempDir(), "quota-telemetry", "recorder.sqlite")
}

// allEvents reads every retained event through the read-only path.
func allEvents(t *testing.T, path string, now time.Time) []Event {
	t.Helper()
	reader, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	result, err := reader.Query(context.Background(), Filter{Limit: MaxQueryLimit}, now)
	if err != nil {
		t.Fatal(err)
	}
	return result.Events
}

func eventsOfKind(events []Event, kind string) []Event {
	var out []Event
	for _, event := range events {
		if event.Kind == kind {
			out = append(out, event)
		}
	}
	return out
}

func readMeta(t *testing.T, path string) Meta {
	t.Helper()
	reader, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	meta, err := reader.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func mustTick(t *testing.T, recorder *Recorder) {
	t.Helper()
	if err := recorder.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}
}

var errInjected = errors.New("injected failure")

func testWork(id string, number int, name string, route domain.ProviderRoute, role domain.ExecutionRole) (domain.Task, domain.Attempt, domain.Assignment) {
	task := domain.Task{ID: "task-" + name, WorkflowID: "workflow-1", Name: name, Class: domain.TaskClassRequired, PromptArtifactID: "prompt"}
	attempt := domain.Attempt{ID: "attempt-" + id, WorkflowRunID: "run-1", TaskID: task.ID, Number: number,
		Progress: domain.ProgressActive, Control: domain.ControlRunning, AssignmentID: "assignment-" + id, UpdatedAt: testBase}
	assignment := domain.Assignment{ID: "assignment-" + id, AttemptID: attempt.ID, Project: "t3-steward", WorkerID: "worker-1",
		Route: route, State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-secret", DispatchToken: "dispatch-secret",
		ExecutionRole: role, CreatedAt: testBase, UpdatedAt: testBase}
	return task, attempt, assignment
}

var opusRoute = domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "claude-opus-5-5",
	Options: map[string]string{"effort": "medium"}, QuotaPoolID: "claude-main"}
