package sqlite

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/ownernotify"
)

// recordingSink accepts every notification and remembers it.
type recordingSink struct {
	mu        sync.Mutex
	name      string
	selection ownernotify.Selection
	got       []ownernotify.Notification
}

func (s *recordingSink) Name() string {
	if s.name == "" {
		return "recording"
	}
	return s.name
}
func (s *recordingSink) Selection() ownernotify.Selection { return s.selection }
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
			Sinks:  []ownernotify.Sink{ownernotify.NewDiscord(server.URL+"/api/webhooks/1/token", ownernotify.Selection{Events: ownernotify.DefaultEvents()}, server.Client())},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			Now:    func() time.Time { return now },
		}
	}

	notifierFor(store).Tick(ctx)
	if len(sent()) != 0 {
		t.Fatalf("history was sent: %v", sent())
	}
	// History is older than the watermark, so it is not even recorded.
	if err := store.db.QueryRow(`SELECT 1 FROM coordinator_owner_notifications WHERE run_id = 'history'`).Scan(new(int)); err == nil {
		t.Fatal("a run older than the watermark was recorded")
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
	detection, err := store.DetectOwnerNotifications(ctx, ownernotify.DiscordSinkName, ownernotify.Selection{Events: ownernotify.DefaultEvents()}, now)
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
	sink := &recordingSink{selection: ownernotify.Selection{Events: []ownernotify.Event{
		ownernotify.EventNeedsInput, ownernotify.EventSupervisionEscalated, ownernotify.EventGateReview,
	}}}
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
	// The overseer woken for that incident spent its budget: the same episode.
	activation := domain.Activation{ID: "act-1", RunID: "r", Epoch: 1, State: domain.ActivationEscalated, IncidentID: "inc-1"}
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
	if got := byEvent[ownernotify.EventSupervisionEscalated]; len(got) != 1 || got[0].ID != "recording|supervision-escalated|incident:inc-1:1" {
		t.Fatalf("one episode was not collapsed into one message: %+v", got)
	}
	if got := byEvent[ownernotify.EventGateReview]; len(got) != 1 || got[0].Gate != "design-review" {
		t.Fatalf("gate-review = %+v", got)
	}

	// New evidence puts the same gate back into review, which is a new event;
	// the unchanged records are not reported again.
	insertJSON(`UPDATE coordinator_supervision_gates SET evidence_snapshot_id = 'ev-2' WHERE id = 'g-1'`)
	notifier.Tick(ctx)
	if got := len(sink.delivered()); got != 4 {
		t.Fatalf("delivered %d notifications after new gate evidence, want 4", got)
	}

	// An escalated activation with no incident behind it is its own episode.
	lone := domain.Activation{ID: "act-2", RunID: "r", Epoch: 2, State: domain.ActivationRecoveryRequired}
	insertJSON(`INSERT INTO coordinator_supervision_activations(id, run_id, epoch, state, record) VALUES (?, ?, 2, ?, ?)`,
		lone.ID, lone.RunID, string(lone.State), marshal(lone))
	notifier.Tick(ctx)
	if got := sink.delivered(); len(got) != 5 || got[4].ActivationID != "act-2" {
		t.Fatalf("a lone activation was not reported: %+v", got)
	}
}

// clockedNotifier is a notifier over the store with a clock the test moves.
func clockedNotifier(store *Store, now *time.Time, sinks ...ownernotify.Sink) *ownernotify.Notifier {
	return &ownernotify.Notifier{Store: store, Sinks: sinks,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return *now }}
}

func openOwnerNotificationStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func deliveredRuns(sink *recordingSink) string {
	var ids []string
	for _, n := range sink.delivered() {
		ids = append(ids, n.RunID)
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// A channel removed and added back later, or a scheduled_success turned off
// and on again, does not replay what finished while it was gone.
func TestOwnerNotificationsDoNotReplayAGapWhenAScopeComesBack(t *testing.T) {
	ctx := context.Background()
	store := openOwnerNotificationStore(t)
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	discord := &recordingSink{name: "discord", selection: ownernotify.Selection{Events: ownernotify.DefaultEvents()}}
	scheduled := &recordingSink{name: "command", selection: ownernotify.Selection{Events: ownernotify.DefaultEvents(), ScheduledSuccess: true}}
	notifier := clockedNotifier(store, &now, discord, scheduled)
	notifier.Tick(ctx)

	// Discord is removed from the configuration, and the command sink stops
	// asking for scheduled successes.
	now = now.Add(time.Minute)
	scheduled.selection.ScheduledSuccess = false
	notifier.Sinks = []ownernotify.Sink{scheduled}
	notifier.Tick(ctx)
	terminalRun(t, store, "gap-failed", domain.ProgressFailed, now.Add(time.Minute))
	gapScheduled := domain.WorkflowRun{ID: "gap-scheduled-ok", WorkflowID: "w", ScheduleID: "hourly", Revision: 2,
		Progress: domain.ProgressSucceeded, CreatedAt: now, UpdatedAt: now, CompletedAt: &now}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{gapScheduled}}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	notifier.Tick(ctx)

	// Both come back.
	now = now.Add(time.Minute)
	scheduled.selection.ScheduledSuccess = true
	notifier.Sinks = []ownernotify.Sink{discord, scheduled}
	notifier.Tick(ctx)
	if got := deliveredRuns(discord); got != "" {
		t.Fatalf("re-added discord replayed %s", got)
	}
	if got := deliveredRuns(scheduled); got != "gap-failed" {
		t.Fatalf("the command sink delivered %q; the failure was in scope throughout, the scheduled success was not", got)
	}

	// What finishes after the return is delivered.
	terminalRun(t, store, "after", domain.ProgressFailed, now.Add(time.Minute))
	now = now.Add(2 * time.Minute)
	notifier.Tick(ctx)
	if got := deliveredRuns(discord); got != "after" {
		t.Fatalf("after the return discord delivered %q", got)
	}
}

// Settled rows are pruned after the retention bound, and a pruned run is not
// detected again, because the watermark moved past it.
func TestOwnerNotificationsPruneSettledRowsWithoutRedelivering(t *testing.T) {
	ctx := context.Background()
	store := openOwnerNotificationStore(t)
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	sink := &recordingSink{name: "discord", selection: ownernotify.Selection{Events: ownernotify.DefaultEvents()}}
	notifier := clockedNotifier(store, &now, sink)
	notifier.Tick(ctx)
	terminalRun(t, store, "old", domain.ProgressFailed, now.Add(time.Minute))
	now = now.Add(2 * time.Minute)
	notifier.Tick(ctx)
	if got := deliveredRuns(sink); got != "old" {
		t.Fatalf("delivered %q", got)
	}

	now = now.Add(40 * 24 * time.Hour)
	notifier.Tick(ctx)
	var rows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM coordinator_owner_notifications`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rows after prune = %d, %v", rows, err)
	}
	now = now.Add(time.Minute)
	notifier.Tick(ctx)
	if got := deliveredRuns(sink); got != "old" {
		t.Fatalf("a pruned run was delivered again: %q", got)
	}
}

// flakySink fails its first send and accepts the rest.
type flakySink struct {
	recordingSink
	failed bool
}

func (s *flakySink) Deliver(ctx context.Context, n ownernotify.Notification) error {
	if !s.failed {
		s.failed = true
		return &ownernotify.DeliveryError{Reason: "webhook down"}
	}
	return s.recordingSink.Deliver(ctx, n)
}

// A question answered while its notification waited for a retry is dropped as
// resolved rather than sent.
func TestOwnerNotificationRetryIsDroppedWhenTheQuestionWasAnswered(t *testing.T) {
	ctx := context.Background()
	store := openOwnerNotificationStore(t)
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "r", WorkflowID: "w", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}},
	}); err != nil {
		t.Fatal(err)
	}
	sink := &flakySink{recordingSink: recordingSink{name: "discord", selection: ownernotify.Selection{Events: []ownernotify.Event{ownernotify.EventNeedsInput}}}}
	notifier := clockedNotifier(store, &now, sink)
	notifier.Tick(ctx)

	wait := domain.TaskWait{ID: "tw-q", WorkflowRunID: "r", TaskID: "t", AttemptID: "a", RequestID: "q",
		Kind: domain.WaitKindAttention, RegisteredAt: now.Add(time.Minute),
		Attention: &domain.AttentionRequest{Kind: domain.AttentionApproval, Prompt: "Ship?", AssignmentID: "as"}}
	raw, _ := json.Marshal(wait)
	if _, err := store.db.Exec(`INSERT INTO coordinator_task_waits(id, request_id, attempt_id, thread_id, record) VALUES (?, ?, ?, ?, ?)`,
		wait.ID, wait.RequestID, wait.AttemptID, "thread", string(raw)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	notifier.Tick(ctx) // The first send fails.
	if state, attempts := ownerNotificationState(t, store, "discord|needs-input|tw-q"); state != "pending" || attempts != 1 {
		t.Fatalf("after a failed send: %s, %d", state, attempts)
	}

	wait.SettledAt = &now
	raw, _ = json.Marshal(wait)
	if _, err := store.db.Exec(`UPDATE coordinator_task_waits SET record = ? WHERE id = ?`, string(raw), wait.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	notifier.Tick(ctx)
	if got := sink.delivered(); len(got) != 0 {
		t.Fatalf("an answered question was sent: %+v", got)
	}
	if state, _ := ownerNotificationState(t, store, "discord|needs-input|tw-q"); state != string(ownernotify.StateResolved) {
		t.Fatalf("row state = %s, want resolved", state)
	}
}

// A run a schedule created reports its failures by default and its successes
// only to a sink that opted in; campaign runs report both. Opting in later
// does not replay the scheduled successes that already happened.
func TestOwnerNotificationsReportScheduledSuccessesOnlyWhenOptedIn(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	save := func(id, schedule string, progress domain.ProgressState) {
		t.Helper()
		run := domain.WorkflowRun{ID: id, WorkflowID: "w", ScheduleID: schedule, Revision: 2, Progress: progress, CreatedAt: now, UpdatedAt: now, CompletedAt: &now}
		if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
			t.Fatal(err)
		}
	}
	save("old-scheduled-ok", "hourly", domain.ProgressSucceeded)
	defaults := &recordingSink{name: "defaults", selection: ownernotify.Selection{Events: ownernotify.DefaultEvents()}}
	optedIn := &recordingSink{name: "opted-in", selection: ownernotify.Selection{Events: ownernotify.DefaultEvents(), ScheduledSuccess: true}}
	notifier := &ownernotify.Notifier{Store: store, Sinks: []ownernotify.Sink{defaults, optedIn},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }}
	notifier.Tick(ctx) // Baseline: the old success is history for both.

	save("campaign-ok", "", domain.ProgressSucceeded)
	save("scheduled-ok", "hourly", domain.ProgressSucceeded)
	save("scheduled-skipped", "hourly", domain.ProgressSkipped)
	save("scheduled-failed", "hourly", domain.ProgressFailed)
	notifier.Tick(ctx)
	runs := func(sink *recordingSink) []string {
		var ids []string
		for _, n := range sink.delivered() {
			ids = append(ids, n.RunID)
		}
		sort.Strings(ids)
		return ids
	}
	if got := strings.Join(runs(defaults), ","); got != "campaign-ok,scheduled-failed" {
		t.Fatalf("default sink delivered %s", got)
	}
	if got := strings.Join(runs(optedIn), ","); got != "campaign-ok,scheduled-failed,scheduled-ok,scheduled-skipped" {
		t.Fatalf("opted-in sink delivered %s", got)
	}

	// The default sink opts in: what already happened is baselined, and only
	// a scheduled success after that is sent.
	defaults.selection.ScheduledSuccess = true
	notifier.Tick(ctx)
	if got := strings.Join(runs(defaults), ","); got != "campaign-ok,scheduled-failed" {
		t.Fatalf("opting in replayed history: %s", got)
	}
	save("scheduled-ok-2", "hourly", domain.ProgressSucceeded)
	notifier.Tick(ctx)
	if got := strings.Join(runs(defaults), ","); got != "campaign-ok,scheduled-failed,scheduled-ok-2" {
		t.Fatalf("after opting in: %s", got)
	}
}
