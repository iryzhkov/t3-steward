package sqlite

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/ownernotify"
)

// recordingSink accepts every notification and remembers it.
type recordingSink struct {
	mu     sync.Mutex
	events []ownernotify.Event
	got    []ownernotify.Notification
}

func (s *recordingSink) Name() string                { return "recording" }
func (s *recordingSink) Events() []ownernotify.Event { return s.events }
func (s *recordingSink) Deliver(_ context.Context, n ownernotify.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, n)
	return nil
}

func (s *recordingSink) delivered() []ownernotify.Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ownernotify.Notification(nil), s.got...)
}

func ownerNotificationState(t *testing.T, store *Store, id string) (string, int) {
	t.Helper()
	var state string
	var attempts int
	if err := store.db.QueryRow(`SELECT state, attempts FROM coordinator_owner_notifications WHERE id = ?`, id).
		Scan(&state, &attempts); err != nil {
		t.Fatalf("read owner notification %s: %v", id, err)
	}
	return state, attempts
}

// terminalRun saves a run of workflow w that has already settled.
func terminalRun(t *testing.T, store *Store, id string, progress domain.ProgressState, at time.Time) {
	t.Helper()
	run := domain.WorkflowRun{ID: id, WorkflowID: "w", Revision: 2, Progress: progress, CreatedAt: at, UpdatedAt: at, CompletedAt: &at}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
}

// The coordinator's history is baselined, a run that settles afterwards is
// delivered exactly once, and neither a restart before the send nor one after
// it changes that. The thread wake registered on the same run settles exactly
// as it does without an owner channel.
func TestOwnerNotificationsBaselineHistoryAndDeliverOnceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Workflows: []domain.Workflow{{ID: "w", Name: "nightly", CreatedAt: now}}}); err != nil {
		t.Fatal(err)
	}
	terminalRun(t, store, "history", domain.ProgressSucceeded, now.Add(-time.Hour))

	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	sent := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), bodies...)
	}
	notifierFor := func(store *Store) *ownernotify.Notifier {
		return &ownernotify.Notifier{
			Store:  store,
			Sinks:  []ownernotify.Sink{ownernotify.NewDiscord(server.URL+"/api/webhooks/1/token", ownernotify.DefaultEvents(), server.Client())},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			Now:    func() time.Time { return now },
		}
	}

	notifierFor(store).Tick(ctx)
	if len(sent()) != 0 {
		t.Fatalf("history was sent: %v", sent())
	}
	if state, _ := ownerNotificationState(t, store, "discord|run-succeeded|history"); state != string(ownernotify.StateBaseline) {
		t.Fatalf("history row state = %s", state)
	}

	// A live run with a failed task and a thread waiting on its sink.
	before := WorkflowProjectionSnapshot{
		Run:      domain.WorkflowRun{ID: "r", WorkflowID: "w", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now},
		Tasks:    []domain.Task{{ID: "t", Name: "implement", WorkflowID: "w"}},
		Attempts: []domain.Attempt{{ID: "a", TaskID: "t", WorkflowRunID: "r", Number: 1, Revision: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, UpdatedAt: now}},
	}
	if before.Run, err = domain.BindRunSink(before.Run, before.Tasks); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{before.Run}, Tasks: before.Tasks, Attempts: before.Attempts}); err != nil {
		t.Fatal(err)
	}
	sinkRef := domain.NodeRef{RunID: "r", TaskID: domain.SinkTaskName}
	if _, err := store.RegisterNodeWait(ctx, domain.NodeWaitRequest{
		ID: "nw-campaign-key", ThreadID: "thread-one", Name: sinkRef.String(), Target: sinkRef, Timeout: time.Hour,
	}, "operator", "host", now); err != nil {
		t.Fatal(err)
	}
	notifierFor(store).Tick(ctx)
	if len(sent()) != 0 {
		t.Fatalf("a live run was reported: %v", sent())
	}

	// The run settles as a failure. The coordinator records the event and
	// then stops before sending anything.
	run := sinkProjected(t, before, now)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	detection, err := store.DetectOwnerNotifications(ctx, ownernotify.DiscordSinkName, ownernotify.DefaultEvents(), now)
	if err != nil || detection.Enqueued != 1 {
		t.Fatalf("detection = %+v, %v", detection, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// After the restart the recorded event is sent once, and detection does
	// not record it a second time.
	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	notifierFor(store).Tick(ctx)
	notifierFor(store).Tick(ctx)
	if got := sent(); len(got) != 1 {
		t.Fatalf("sent %d messages after restart, want 1: %v", len(got), got)
	}
	var message struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(sent()[0]), &message); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"**nightly**", "`r`", "**failed**", "`implement`", "t3-steward task result r"} {
		if !strings.Contains(message.Content, want) {
			t.Fatalf("message %q lacks %q", message.Content, want)
		}
	}
	if state, attempts := ownerNotificationState(t, store, "discord|run-failed|r"); state != string(ownernotify.StateDelivered) || attempts != 1 {
		t.Fatalf("delivered row = %s after %d attempts", state, attempts)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// And a restart after delivery sends nothing again.
	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	notifierFor(store).Tick(ctx)
	if got := sent(); len(got) != 1 {
		t.Fatalf("sent %d messages after a second restart, want 1", len(got))
	}

	// The thread wake is untouched: the wait on the sink settles with the
	// failure, bound to its own thread, and its delivery is still the wait
	// runner's to make.
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, err := store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(waits) != 1 || waits[0].Observation == nil || waits[0].Observation.ExitCode != 2 ||
		waits[0].Request.ThreadID != "thread-one" || waits[0].Delivery == "delivered" {
		t.Fatalf("thread wake after owner delivery: %+v", waits)
	}
}

// needs-input, supervision-escalated and gate-review are detected from the
// coordinator's own attention and supervision records, each once.
func TestOwnerNotificationsDetectOperatorAttention(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "w", Name: "release", CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}},
		Tasks:        []domain.Task{{ID: "t", Name: "deploy", WorkflowID: "w"}},
	}); err != nil {
		t.Fatal(err)
	}
	sink := &recordingSink{events: []ownernotify.Event{
		ownernotify.EventNeedsInput, ownernotify.EventSupervisionEscalated, ownernotify.EventGateReview,
	}}
	notifier := &ownernotify.Notifier{Store: store, Sinks: []ownernotify.Sink{sink},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }}
	notifier.Tick(ctx) // Nothing exists yet, so the baseline is empty.

	insertJSON := func(query string, args ...any) {
		t.Helper()
		if _, err := store.db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	marshal := func(v any) string {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	wait := domain.TaskWait{ID: "tw-q", WorkflowRunID: "r", TaskID: "t", AttemptID: "a", RequestID: "q",
		Kind: domain.WaitKindAttention, RegisteredAt: now,
		Attention: &domain.AttentionRequest{Kind: domain.AttentionApproval, Prompt: "Deploy to production?", AssignmentID: "as"}}
	insertJSON(`INSERT INTO coordinator_task_waits(id, request_id, attempt_id, thread_id, record) VALUES (?, ?, ?, ?, ?)`,
		wait.ID, wait.RequestID, wait.AttemptID, "thread", marshal(wait))
	answered := wait
	answered.ID, answered.RequestID, answered.SettledAt = "tw-answered", "answered", &now
	insertJSON(`INSERT INTO coordinator_task_waits(id, request_id, attempt_id, thread_id, record) VALUES (?, ?, ?, ?, ?)`,
		answered.ID, answered.RequestID, answered.AttemptID, "thread", marshal(answered))
	incident := domain.ReviewIncident{ID: "inc-1", RunID: "r", State: domain.IncidentEscalated, Reason: "producer failed twice", OpenedAt: now}
	insertJSON(`INSERT INTO coordinator_supervision_incidents(id, run_id, state, revision, gate_id, record) VALUES (?, ?, ?, 1, '', ?)`,
		incident.ID, incident.RunID, string(incident.State), marshal(incident))
	activation := domain.Activation{ID: "act-1", RunID: "r", Epoch: 1, State: domain.ActivationEscalated}
	insertJSON(`INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, record) VALUES (?, ?, 1, ?, ?)`,
		activation.ID, activation.RunID, string(activation.State), marshal(activation))
	gate := domain.Gate{Definition: domain.GateDefinition{ID: "g-1", Name: "design-review"}, RunID: "r", State: domain.GateReadyForReview, EvidenceSnapshotID: "ev-1"}
	insertJSON(`INSERT INTO coordinator_supervision_gates(id, run_id, state, graph_revision, revision, evidence_snapshot_id, record) VALUES (?, ?, ?, 1, 1, ?, ?)`,
		gate.Definition.ID, gate.RunID, string(gate.State), gate.EvidenceSnapshotID, marshal(gate))

	notifier.Tick(ctx)
	notifier.Tick(ctx)
	byEvent := map[ownernotify.Event][]ownernotify.Notification{}
	for _, n := range sink.delivered() {
		byEvent[n.Event] = append(byEvent[n.Event], n)
	}
	if got := byEvent[ownernotify.EventNeedsInput]; len(got) != 1 || got[0].WaitID != "tw-q" || got[0].Task != "deploy" ||
		got[0].Prompt != "Deploy to production?" || got[0].Campaign != "release" {
		t.Fatalf("needs-input = %+v", got)
	}
	if got := byEvent[ownernotify.EventSupervisionEscalated]; len(got) != 2 {
		t.Fatalf("supervision-escalated = %+v", got)
	}
	if got := byEvent[ownernotify.EventGateReview]; len(got) != 1 || got[0].Gate != "design-review" {
		t.Fatalf("gate-review = %+v", got)
	}

	// New evidence puts the same gate back into review, which is a new event;
	// the unchanged records are not reported again.
	insertJSON(`UPDATE coordinator_supervision_gates SET evidence_snapshot_id = 'ev-2' WHERE id = 'g-1'`)
	notifier.Tick(ctx)
	if got := len(sink.delivered()); got != 5 {
		t.Fatalf("delivered %d notifications after new gate evidence, want 5", got)
	}
}
