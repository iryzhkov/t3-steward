package backlog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/jocasta"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// fakeLedgerJocasta is the in-memory Jocasta the ledger tests write to. It
// enforces the same guard the real service does: a create needs a free path and
// an update must name the current revision.
type fakeLedgerJocasta struct {
	mu          sync.Mutex
	docs        map[string]*jocasta.Document
	unavailable bool
	// interleave is how many updates race another writer: before the guard is
	// checked, an operator appends a note and the revision moves on.
	interleave int
	calls      []string
}

func newFakeLedgerJocasta() *fakeLedgerJocasta {
	return &fakeLedgerJocasta{docs: map[string]*jocasta.Document{}}
}

func (f *fakeLedgerJocasta) Get(_ context.Context, path string) (jocasta.Document, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "get "+path)
	if f.unavailable {
		return jocasta.Document{}, &jocasta.Error{Op: "get", Code: jocasta.CodeUnavailable, Message: "service unreachable"}
	}
	doc, ok := f.docs[path]
	if !ok {
		return jocasta.Document{}, &jocasta.Error{Op: "get", Code: jocasta.CodeNotFound, Message: "document or revision does not exist"}
	}
	return jocasta.Document{Path: path, Revision: doc.Revision, Content: append([]byte(nil), doc.Content...)}, nil
}

func (f *fakeLedgerJocasta) Create(_ context.Context, path string, content []byte, requestID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "create "+path)
	if f.unavailable {
		return 0, &jocasta.Error{Op: "put", Code: jocasta.CodeUnavailable, Message: "service unreachable"}
	}
	if requestID == "" {
		return 0, errors.New("create without a request id")
	}
	if _, taken := f.docs[path]; taken {
		return 0, &jocasta.Error{Op: "put", Code: jocasta.CodeConflict, Message: "path taken"}
	}
	f.docs[path] = &jocasta.Document{Path: path, Revision: 1, Content: append([]byte(nil), content...)}
	return 1, nil
}

func (f *fakeLedgerJocasta) Update(_ context.Context, path string, content []byte, ifRevision int64, requestID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "update "+path)
	if f.unavailable {
		return 0, &jocasta.Error{Op: "put", Code: jocasta.CodeUnavailable, Message: "service unreachable"}
	}
	if requestID == "" {
		return 0, errors.New("update without a request id")
	}
	doc, ok := f.docs[path]
	if !ok {
		return 0, &jocasta.Error{Op: "put", Code: jocasta.CodeNotFound, Message: "no document"}
	}
	if f.interleave > 0 {
		f.interleave--
		doc.Content = append(doc.Content, []byte("\nOperator note.\n")...)
		doc.Revision++
	}
	if ifRevision != doc.Revision {
		return 0, &jocasta.Error{Op: "put", Code: jocasta.CodeConflict, Message: "guard did not hold"}
	}
	doc.Content = append([]byte(nil), content...)
	doc.Revision++
	return doc.Revision, nil
}

func (f *fakeLedgerJocasta) content(path string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if doc, ok := f.docs[path]; ok {
		return string(doc.Content)
	}
	return ""
}

func (f *fakeLedgerJocasta) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// memoryLedgerStates is the coordinator's durable ledger state, kept in memory.
// failNextSave simulates a coordinator that dies after Jocasta accepted a write
// and before it recorded that it had.
type memoryLedgerStates struct {
	mu           sync.Mutex
	states       map[string]domain.LedgerState
	failNextSave bool
	saves        int
}

func newMemoryLedgerStates() *memoryLedgerStates {
	return &memoryLedgerStates{states: map[string]domain.LedgerState{}}
}

func (m *memoryLedgerStates) LoadLedgerStates(context.Context) ([]domain.LedgerState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]domain.LedgerState, 0, len(m.states))
	for _, state := range m.states {
		state.Applied = append([]string(nil), state.Applied...)
		result = append(result, state)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RunID < result[j].RunID })
	return result, nil
}

func (m *memoryLedgerStates) SaveLedgerState(_ context.Context, state domain.LedgerState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNextSave {
		m.failNextSave = false
		return errors.New("coordinator stopped")
	}
	m.saves++
	state.Applied = append([]string(nil), state.Applied...)
	m.states[state.RunID] = state
	return nil
}

func (m *memoryLedgerStates) get(runID string) domain.LedgerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.states[runID]
}

// ledgerWorld is a two-task campaign, build then review, whose records the
// tests advance by hand.
type ledgerWorld struct {
	mu        sync.Mutex
	now       time.Time
	records   sqlite.CoordinatorRecords
	contents  map[string]string
	jocasta   *fakeLedgerJocasta
	states    *memoryLedgerStates
	maxQuoted int
}

const ledgerTestPath = "steward/handoffs/run-1.md"

func newLedgerWorld(optIn bool) *ledgerWorld {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	workflow := domain.Workflow{
		ID: "workflow-1", Version: 2, Name: "m16-ledger", Project: "t3-steward",
		TaskIDs: []string{"task-build", "task-review"}, CreatedAt: now,
	}
	if optIn {
		workflow.Ledger = &domain.WorkflowLedger{
			JocastaProject: "steward", Plan: "steward/plans/m16-plan.md", Risk: "medium",
			Acceptance: []string{"Submission creates the ledger once"},
		}
	}
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{workflow},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1,
			CreatedAt: now, UpdatedAt: now,
			Sink: &domain.SinkTask{ID: "sink-1", Name: "sink", Progress: domain.ProgressBlocked},
		}},
		Tasks: []domain.Task{
			{ID: "task-build", RunID: "run-1", WorkflowID: "workflow-1", Name: "build",
				Outputs: []domain.ArtifactDeclaration{{Name: "handoff.md"}, {Name: "notes.md"}}},
			{ID: "task-review", RunID: "run-1", WorkflowID: "workflow-1", Name: "review",
				Needs: []string{"build"}, Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}}},
		},
		Attempts: []domain.Attempt{
			{ID: "attempt-build", WorkflowRunID: "run-1", TaskID: "task-build", Number: 1,
				Progress: domain.ProgressActive, Control: domain.ControlRunning, AssignmentID: "assignment-build", UpdatedAt: now},
			{ID: "attempt-review", WorkflowRunID: "run-1", TaskID: "task-review", Number: 1,
				Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned, UpdatedAt: now},
		},
		Assignments: []domain.Assignment{{
			ID: "assignment-build", AttemptID: "attempt-build", WorkerID: "worker-a",
			Route: domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "claude-opus-5-5", Options: map[string]string{"effort": "medium"}},
			State: domain.AssignmentClaimed,
		}},
	}
	return &ledgerWorld{
		now: now, records: records, contents: map[string]string{},
		jocasta: newFakeLedgerJocasta(), states: newMemoryLedgerStates(),
	}
}

func (w *ledgerWorld) reconciler() *LedgerReconciler {
	return &LedgerReconciler{
		Records: func(context.Context) (sqlite.CoordinatorRecords, error) {
			w.mu.Lock()
			defer w.mu.Unlock()
			return w.records, nil
		},
		States: w.states,
		Client: w.jocasta,
		Open: func(_ context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
			w.mu.Lock()
			defer w.mu.Unlock()
			for _, artifact := range w.records.Artifacts {
				if artifact.ID == id {
					return artifact, io.NopCloser(strings.NewReader(w.contents[id])), nil
				}
			}
			return domain.Artifact{}, nil, errors.New("no such artifact")
		},
		Usage: func(_ context.Context, runID string) (domain.UsageReport, error) {
			return domain.UsageReport{WorkflowRunID: runID, ByAttempt: []domain.UsageAggregate{{
				AttemptID: "attempt-build",
				Totals:    domain.UsageTotals{Calls: 3, Turns: 2, UncachedInputTokens: 100, OutputTokens: 50},
			}}}, nil
		},
		Waits: func(context.Context) ([]domain.TaskWait, error) {
			return []domain.TaskWait{{
				ID: "wait-1", WorkflowRunID: "run-1", TaskID: "task-build", AttemptID: "attempt-build",
				AskAnswer: &domain.AskAnswer{AskID: "ask-1", Question: "Proceed with V37?", Options: []string{"yes"},
					Source: domain.AskSourceCLI, AnsweredBy: "operator", AnsweredAt: w.now},
			}}, nil
		},
		Now:             func() time.Time { w.mu.Lock(); defer w.mu.Unlock(); return w.now },
		MaxHandoffBytes: w.maxQuoted,
	}
}

func (w *ledgerWorld) advance(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(d)
}

// finishBuild records the build attempt's success and its retained outputs.
func (w *ledgerWorld) finishBuild(handoff string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	completed := w.now
	w.records.Attempts[0].Progress = domain.ProgressSucceeded
	w.records.Attempts[0].Control = domain.ControlStopped
	w.records.Attempts[0].CompletedAt = &completed
	w.records.Attempts[1].Progress = domain.ProgressReady
	w.records.Artifacts = append(w.records.Artifacts,
		domain.Artifact{ID: "artifact-handoff", WorkflowRunID: "run-1", TaskID: "task-build", AttemptID: "attempt-build",
			Kind: domain.ArtifactOutput, Name: "handoff.md", Size: int64(len(handoff)), SHA256: "aa11", Producer: "worker"},
		domain.Artifact{ID: "artifact-notes", WorkflowRunID: "run-1", TaskID: "task-build", AttemptID: "attempt-build",
			Kind: domain.ArtifactOutput, Name: "notes.md", Size: 5, SHA256: "bb22", Producer: "worker"},
	)
	w.contents["artifact-handoff"] = handoff
}

// finishReview records the review's success, its recorded verdict and the
// run's terminal sink.
func (w *ledgerWorld) finishReview() {
	w.mu.Lock()
	defer w.mu.Unlock()
	completed := w.now
	w.records.Attempts[1].Progress = domain.ProgressSucceeded
	w.records.Attempts[1].Control = domain.ControlStopped
	w.records.Attempts[1].CompletedAt = &completed
	w.records.ReviewRounds = []review.Round{{
		ID: "round-1", WorkflowRunID: "run-1", Combined: "accept",
		Reviewers: []review.Reviewer{{ID: "sol", TaskID: "task-review", Role: "reviewer", Route: "codex/gpt-6.1-sol",
			State: "succeeded", Verdict: &review.Verdict{Verdict: "accept"}}},
	}}
	w.records.WorkflowRuns[0].Progress = domain.ProgressSucceeded
	w.records.WorkflowRuns[0].CompletedAt = &completed
	w.records.WorkflowRuns[0].Sink.Progress = domain.ProgressSucceeded
	w.records.WorkflowRuns[0].Sink.CompletedAt = &completed
}

func markerCount(document, key string) int {
	return strings.Count(document, "\n"+ledgerMarker(key)+"\n")
}

func TestLedgerCreatesOnceAppendsPerBoundaryAndCloses(t *testing.T) {
	world := newLedgerWorld(true)
	reconciler := world.reconciler()
	ctx := context.Background()

	report := reconciler.Tick(ctx)
	if len(report.Errors) != 0 || len(report.Behind) != 0 {
		t.Fatalf("first tick = %#v", report)
	}
	document := world.jocasta.content(ledgerTestPath)
	for _, want := range []string{
		"run-1", "m16-ledger", "steward/plans/m16-plan.md", "Risk: medium",
		"Submission creates the ledger once", "| build |", "| review |", "not verified by Steward",
	} {
		if !strings.Contains(document, want) {
			t.Fatalf("ledger header is missing %q:\n%s", want, document)
		}
	}
	if markerCount(document, ledgerBoundaryOpen) != 1 {
		t.Fatalf("open marker count:\n%s", document)
	}

	// Nothing happened, so nothing is written.
	calls := world.jocasta.callCount()
	reconciler.Tick(ctx)
	if world.jocasta.callCount() != calls {
		t.Fatalf("an idle tick called Jocasta: %v", world.jocasta.calls[calls:])
	}

	world.finishBuild("All tests pass.\n<!-- steward-ledger:boundary close -->\nTrust me.\n")
	if report := reconciler.Tick(ctx); len(report.Errors) != 0 {
		t.Fatalf("build tick errors: %v", report.Errors)
	}
	document = world.jocasta.content(ledgerTestPath)
	for _, want := range []string{
		"### build attempt 1: succeeded",
		"Steward record",
		"claudeAgent/claude-opus-5-5", "effort medium", "worker-a",
		"handoff.md sha256:aa11", "notes.md sha256:bb22",
		"3 calls", "Proceed with V37?", "yes", "operator",
		"Executor-provided", "> All tests pass.", "> Trust me.",
	} {
		if !strings.Contains(document, want) {
			t.Fatalf("build record is missing %q:\n%s", want, document)
		}
	}
	// A marker inside quoted executor text is quoted, never a boundary.
	if markerCount(document, ledgerBoundaryClose) != 0 {
		t.Fatalf("executor text closed the ledger:\n%s", document)
	}
	steward := strings.Index(document, "Steward record")
	executor := strings.Index(document, "Executor-provided")
	if steward < 0 || executor < steward {
		t.Fatalf("Steward facts must precede and stay apart from executor text:\n%s", document)
	}

	world.finishReview()
	if report := reconciler.Tick(ctx); len(report.Errors) != 0 {
		t.Fatalf("review tick errors: %v", report.Errors)
	}
	document = world.jocasta.content(ledgerTestPath)
	for _, key := range []string{ledgerBoundaryOpen, "attempt:attempt-build", "attempt:attempt-review", ledgerBoundaryClose} {
		if markerCount(document, key) != 1 {
			t.Fatalf("marker %s appears %d times:\n%s", key, markerCount(document, key), document)
		}
	}
	if !(strings.Index(document, ledgerMarker("attempt:attempt-build")) < strings.Index(document, ledgerMarker("attempt:attempt-review")) &&
		strings.Index(document, ledgerMarker("attempt:attempt-review")) < strings.Index(document, "\n"+ledgerMarker(ledgerBoundaryClose))) {
		t.Fatalf("records are out of order:\n%s", document)
	}
	for _, want := range []string{"round-1 reviewer sol (reviewer, codex/gpt-6.1-sol): accept", "### Run closed: succeeded", "No handoff.md was retained"} {
		if !strings.Contains(document, want) {
			t.Fatalf("ledger is missing %q:\n%s", want, document)
		}
	}
	state := world.states.get("run-1")
	if !state.Closed || state.Behind || state.Path != ledgerTestPath || state.Revision != world.jocasta.docs[ledgerTestPath].Revision ||
		state.LastBoundary != ledgerBoundaryClose {
		t.Fatalf("state = %#v", state)
	}

	// A closed ledger is never touched again.
	calls = world.jocasta.callCount()
	reconciler.Tick(ctx)
	if world.jocasta.callCount() != calls {
		t.Fatal("a closed ledger was written again")
	}
}

func TestLedgerConflictRereadsAndKeepsTheNewerContent(t *testing.T) {
	world := newLedgerWorld(true)
	reconciler := world.reconciler()
	ctx := context.Background()
	reconciler.Tick(ctx)
	world.finishBuild("done\n")
	world.jocasta.interleave = 2
	if report := reconciler.Tick(ctx); len(report.Errors) != 0 || len(report.Behind) != 0 {
		t.Fatalf("conflicts within the bound must reconcile: %#v", report)
	}
	document := world.jocasta.content(ledgerTestPath)
	if strings.Count(document, "Operator note.") != 2 {
		t.Fatalf("a newer revision was overwritten:\n%s", document)
	}
	if markerCount(document, "attempt:attempt-build") != 1 {
		t.Fatalf("record not appended exactly once:\n%s", document)
	}
	if state := world.states.get("run-1"); state.Revision != world.jocasta.docs[ledgerTestPath].Revision {
		t.Fatalf("state revision %d, document revision %d", state.Revision, world.jocasta.docs[ledgerTestPath].Revision)
	}
}

func TestLedgerConflictBoundMarksBehindThenCatchesUp(t *testing.T) {
	world := newLedgerWorld(true)
	reconciler := world.reconciler()
	ctx := context.Background()
	reconciler.Tick(ctx)
	world.finishBuild("done\n")
	world.jocasta.interleave = 100
	report := reconciler.Tick(ctx)
	if len(report.Behind) != 1 || report.Behind[0] != "run-1" {
		t.Fatalf("exhausted conflicts must mark the ledger behind: %#v", report)
	}
	state := world.states.get("run-1")
	if !state.Behind || !strings.Contains(state.LastError, "conflict") || state.NextAttemptAt == nil {
		t.Fatalf("state = %#v", state)
	}
	if markerCount(world.jocasta.content(ledgerTestPath), "attempt:attempt-build") != 0 {
		t.Fatal("a record was written without its guard holding")
	}
	world.jocasta.interleave = 0
	world.advance(time.Hour)
	if report := reconciler.Tick(ctx); len(report.Behind) != 0 || len(report.Errors) != 0 {
		t.Fatalf("catch-up = %#v", report)
	}
	if markerCount(world.jocasta.content(ledgerTestPath), "attempt:attempt-build") != 1 {
		t.Fatal("catch-up did not append the record")
	}
	if state := world.states.get("run-1"); state.Behind || state.NextAttemptAt != nil || state.LastError != "" || state.Failures != 0 {
		t.Fatalf("caught-up state = %#v", state)
	}
}

func TestLedgerUnavailableMarksBehindBacksOffAndCatchesUp(t *testing.T) {
	world := newLedgerWorld(true)
	reconciler := world.reconciler()
	ctx := context.Background()
	world.jocasta.unavailable = true

	report := reconciler.Tick(ctx)
	if len(report.Behind) != 1 {
		t.Fatalf("unavailable Jocasta must mark the ledger behind: %#v", report)
	}
	state := world.states.get("run-1")
	if !state.Behind || state.Failures != 1 || state.NextAttemptAt == nil || !state.NextAttemptAt.After(world.now) ||
		!strings.Contains(state.LastError, "unavailable") {
		t.Fatalf("state = %#v", state)
	}

	// Within the backoff nothing is attempted; the run carries on regardless.
	calls := world.jocasta.callCount()
	world.finishBuild("done\n")
	reconciler.Tick(ctx)
	if world.jocasta.callCount() != calls {
		t.Fatal("a ledger inside its backoff was retried")
	}

	// A second failure backs off further.
	firstDelay := state.NextAttemptAt.Sub(world.now)
	world.advance(firstDelay)
	reconciler.Tick(ctx)
	state = world.states.get("run-1")
	if state.Failures != 2 || state.NextAttemptAt == nil || state.NextAttemptAt.Sub(world.now) <= firstDelay {
		t.Fatalf("second failure state = %#v (first delay %s)", state, firstDelay)
	}

	world.jocasta.unavailable = false
	world.advance(2 * time.Hour)
	if report := reconciler.Tick(ctx); len(report.Behind) != 0 || len(report.Errors) != 0 {
		t.Fatalf("catch-up = %#v", report)
	}
	document := world.jocasta.content(ledgerTestPath)
	if markerCount(document, ledgerBoundaryOpen) != 1 || markerCount(document, "attempt:attempt-build") != 1 {
		t.Fatalf("catch-up did not write the missed boundaries once each:\n%s", document)
	}
	if state := world.states.get("run-1"); state.Behind || state.Failures != 0 {
		t.Fatalf("caught-up state = %#v", state)
	}
}

func TestLedgerBackoffGrows(t *testing.T) {
	first := ledgerBackoff(1)
	second := ledgerBackoff(2)
	if first <= 0 || second <= first || ledgerBackoff(50) != ledgerMaxBackoff {
		t.Fatalf("backoff 1=%s 2=%s 50=%s", first, second, ledgerBackoff(50))
	}
}

func TestLedgerRestartAndReplayDoNotDuplicate(t *testing.T) {
	world := newLedgerWorld(true)
	ctx := context.Background()
	world.reconciler().Tick(ctx)
	world.finishBuild("done\n")

	// The coordinator dies after Jocasta accepted the append and before the
	// applied boundary was recorded.
	world.states.failNextSave = true
	if report := world.reconciler().Tick(ctx); len(report.Errors) == 0 {
		t.Fatal("a lost state write must be reported")
	}
	if markerCount(world.jocasta.content(ledgerTestPath), "attempt:attempt-build") != 1 {
		t.Fatal("the append did not reach Jocasta")
	}

	// A restarted coordinator replays the boundary and finds it already there.
	world.advance(time.Hour)
	if report := world.reconciler().Tick(ctx); len(report.Errors) != 0 || len(report.Behind) != 0 {
		t.Fatalf("replay = %#v", report)
	}
	document := world.jocasta.content(ledgerTestPath)
	if markerCount(document, "attempt:attempt-build") != 1 || markerCount(document, ledgerBoundaryOpen) != 1 {
		t.Fatalf("replay duplicated a record:\n%s", document)
	}
	state := world.states.get("run-1")
	if len(state.Applied) != 2 || state.LastBoundary != "attempt:attempt-build" || state.Revision != world.jocasta.docs[ledgerTestPath].Revision {
		t.Fatalf("replayed state = %#v", state)
	}

	// Losing the state of the open boundary is replayed the same way.
	fresh := newLedgerWorld(true)
	fresh.states.failNextSave = true
	fresh.reconciler().Tick(ctx)
	fresh.advance(time.Hour)
	if report := fresh.reconciler().Tick(ctx); len(report.Errors) != 0 || len(report.Behind) != 0 {
		t.Fatalf("open replay = %#v", report)
	}
	if markerCount(fresh.jocasta.content(ledgerTestPath), ledgerBoundaryOpen) != 1 {
		t.Fatal("open replay duplicated the header")
	}
}

func TestLedgerRefusesToAdoptAForeignDocument(t *testing.T) {
	world := newLedgerWorld(true)
	world.jocasta.docs[ledgerTestPath] = &jocasta.Document{Path: ledgerTestPath, Revision: 4, Content: []byte("# someone else's notes\n")}
	report := world.reconciler().Tick(context.Background())
	if len(report.Behind) != 1 {
		t.Fatalf("a foreign document at the ledger path must leave the ledger behind: %#v", report)
	}
	if got := world.jocasta.content(ledgerTestPath); got != "# someone else's notes\n" {
		t.Fatalf("foreign document was changed:\n%s", got)
	}
}

func TestLedgerOptOutChangesNothing(t *testing.T) {
	world := newLedgerWorld(false)
	reconciler := world.reconciler()
	ctx := context.Background()
	reconciler.Tick(ctx)
	world.finishBuild("done\n")
	world.finishReview()
	report := reconciler.Tick(ctx)
	if len(report.Applied) != 0 || len(report.Behind) != 0 || len(report.Errors) != 0 {
		t.Fatalf("opt-out report = %#v", report)
	}
	if world.jocasta.callCount() != 0 || world.states.saves != 0 {
		t.Fatalf("opt-out touched Jocasta (%d calls) or ledger state (%d saves)", world.jocasta.callCount(), world.states.saves)
	}

	// An unconfigured reconciler is inert too.
	var nothing *LedgerReconciler
	if report := nothing.Tick(ctx); len(report.Applied) != 0 || len(report.Errors) != 0 {
		t.Fatalf("nil reconciler = %#v", report)
	}
}

func TestLedgerTruncatesOversizedHandoffWithAPointer(t *testing.T) {
	world := newLedgerWorld(true)
	world.maxQuoted = 64
	reconciler := world.reconciler()
	ctx := context.Background()
	reconciler.Tick(ctx)
	handoff := strings.Repeat("ü", 40) + strings.Repeat("long line of executor text\n", 50)
	world.finishBuild(handoff)
	if report := reconciler.Tick(ctx); len(report.Errors) != 0 {
		t.Fatal(report.Errors)
	}
	document := world.jocasta.content(ledgerTestPath)
	for _, want := range []string{"Truncated", "artifact-handoff", "sha256:aa11"} {
		if !strings.Contains(document, want) {
			t.Fatalf("truncation pointer is missing %q:\n%s", want, document)
		}
	}
	quoted := 0
	for _, line := range strings.Split(document, "\n") {
		if strings.HasPrefix(line, "> ") || line == ">" {
			quoted += len(strings.TrimPrefix(strings.TrimPrefix(line, ">"), " "))
		}
	}
	if quoted > 64 {
		t.Fatalf("quoted %d bytes, limit 64:\n%s", quoted, document)
	}
	if !bytes.Equal([]byte(strings.ToValidUTF8(document, "?")), []byte(document)) {
		t.Fatal("truncation split a UTF-8 sequence")
	}
}
