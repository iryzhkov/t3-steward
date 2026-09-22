package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestM5RetainedAccumulatedHistorySurvivesRestartAndActivation(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	statePath := filepath.Join(root, "state.db")
	store, err := sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	store.SetClock(func() time.Time { return now })
	service := &SubmissionService{
		StorageRoot: filepath.Join(root, "bundles"), Store: store,
		MaxBytes: 8 << 20, MaxFiles: 1000, Now: func() time.Time { return now },
		NewKey: func() string { return "generated-history-key" },
	}
	t.Cleanup(func() { _ = removeIngestedTree(service.StorageRoot) })
	result, err := service.SubmitDirectory(ctx, DirectorySubmission{
		IdempotencyKey: "m5-retained-history", BundleDir: m5HistoryBundle(t),
		Principal: "local:1000",
	})
	if err != nil {
		t.Fatalf("supported manifest/run ingestion: %v", err)
	}
	runID := result.Record.RunID
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Tasks) != 43 || len(records.Attempts) != 43 {
		t.Fatalf("ingested tasks=%d attempts=%d, want 43 each", len(records.Tasks), len(records.Attempts))
	}
	supervision, err := store.LoadSupervisionSnapshot(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(supervision.Gates) != 43 {
		t.Fatalf("ingested gates=%d, want 43", len(supervision.Gates))
	}

	activationStore := CoordinatorSupervisionStore{Store: store}
	activationService := SupervisionActivationService{Store: activationStore, Now: func() time.Time { return now }}
	reasons := []string{
		"accepted gate and evidence custody retained without rerun",
		"parallel recovery exhausted at its bounded incident budget",
		"worker loss after effect reconciled from unknown without duplicate effect",
		"dispatch replay retained its idempotency identity",
		"context replacement started from durable evidence rather than transcript",
		"quota close then reopen preserved fair current trigger",
		"notification delivery failed and remained retryable",
		"notification retry was acknowledged exactly once",
		"authorized operator approval advanced the fenced revision",
		"stop containment was observed before custody release",
	}
	kinds := []SupervisionTriggerKind{
		TriggerGateReviewReady, TriggerTaskJudgmentRequired, TriggerRouteBlockPersistent,
		TriggerOperatorReassessment, TriggerReviewTimeout,
	}
	for batchStart := 0; batchStart < 1000; batchStart += 50 {
		events := make([]SupervisionEvent, 0, 50)
		for index := batchStart; index < batchStart+50; index++ {
			events = append(events, SupervisionEvent{
				ID: fmt.Sprintf("history-event-%04d", index+1), RunID: runID,
				Kind: kinds[index%len(kinds)], Reason: reasons[index%len(reasons)],
				GateID:        fmt.Sprintf("review-%02d", index%43),
				TaskID:        fmt.Sprintf("task-%02d", index%43),
				AttemptID:     fmt.Sprintf("attempt-history-%04d", index+1),
				IncidentID:    fmt.Sprintf("incident-history-%02d", index%17),
				GraphRevision: 1, OccurredAt: now.Add(time.Duration(index) * time.Millisecond),
				Artifacts: []domain.ArtifactDigest{{
					ArtifactID: fmt.Sprintf("retained-evidence-%04d", index+1),
					Digest:     "sha256:" + strings.Repeat("a", 64),
				}},
			})
		}
		written, err := activationService.Observe(ctx, runID, events...)
		if err != nil || written != len(events) {
			t.Fatalf("append supported event batch at %d: written=%d err=%v", batchStart, written, err)
		}
	}
	// Exact replay is a supported append and must not move the cursor.
	replay := SupervisionEvent{
		ID: "history-event-1000", RunID: runID, Kind: TriggerReviewTimeout,
		Reason: reasons[9], OccurredAt: now.Add(999 * time.Millisecond),
	}
	if written, err := activationService.Observe(ctx, runID, replay); err != nil || written != 0 {
		t.Fatalf("event replay appended again: written=%d err=%v", written, err)
	}
	beforeRestart, err := activationStore.LoadSupervisionActivationState(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeRestart.Pending) != 1000 || beforeRestart.Pending[0].Sequence != 1 ||
		beforeRestart.Pending[999].Sequence != 1000 {
		t.Fatalf("pre-restart cursor continuity: first=%d last=%d count=%d",
			beforeRestart.Pending[0].Sequence, beforeRestart.Pending[len(beforeRestart.Pending)-1].Sequence, len(beforeRestart.Pending))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = sqlite.OpenMigrated(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.SetClock(func() time.Time { return now.Add(time.Minute) })
	activationStore = CoordinatorSupervisionStore{Store: store}
	activationService = SupervisionActivationService{Store: activationStore, Now: func() time.Time { return now.Add(time.Minute) }}
	reopened, err := activationStore.LoadSupervisionActivationState(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.Pending) != 1000 || reopened.Pending[999].Sequence != 1000 {
		t.Fatalf("restart lost retained history: count=%d", len(reopened.Pending))
	}
	for index, want := range reasons {
		event := reopened.Pending[990+index]
		if event.Reason != want || event.Sequence != int64(991+index) {
			t.Fatalf("causal tail[%d]=%+v, want reason %q and sequence %d", index, event, want, 991+index)
		}
	}

	notificationID := "history-final-notification"
	if written, err := store.AppendSupervisionOutboxRows(ctx, runID, []sqlite.SupervisionOutboxRow{{
		ID: notificationID, RunID: runID, Delivery: "pending",
		Record: []byte(`{"id":"history-final-notification","runId":"` + runID + `","delivery":"pending","attempts":0}`),
	}}); err != nil || written != 1 {
		t.Fatalf("append final notification: written=%d err=%v", written, err)
	}
	for _, transition := range [][2]string{{"pending", "sending"}, {"sending", "offline"}, {"offline", "sending"}, {"sending", "delivered"}} {
		if changed, err := store.TransitionSupervisionOutboxRow(ctx, notificationID, transition[0], transition[1], now.Add(time.Minute)); err != nil || !changed {
			t.Fatalf("notification transition %s->%s changed=%v err=%v", transition[0], transition[1], changed, err)
		}
	}
	deliveries, err := store.ListSupervisionOutboxRows(ctx, runID)
	if err != nil || len(deliveries) != 1 || deliveries[0].Delivery != "delivered" {
		t.Fatalf("final notification not delivered after fail/retry/ack: rows=%+v err=%v", deliveries, err)
	}

	plan, err := activationService.Advance(ctx, runID, ActivationSignal{
		Event: domain.ActivationEventTriggerFired, IncidentID: "incident-history-current",
		Principal: "supervisor:history",
	})
	if err != nil {
		t.Fatalf("real activation cycle after restart: %v", err)
	}
	if plan.Activation.State != domain.ActivationPendingDispatch ||
		plan.Activation.ConsumedEventCursor != 1000 || plan.Inbox.HighWaterMark != 1000 ||
		plan.Activation.ReadyTieID != "history-event-0001" {
		t.Fatalf("activation/cursor/current-trigger result=%+v activation=%+v", plan.Result, plan.Activation)
	}
	afterActivation, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterActivation.Attempts) != 43 {
		t.Fatalf("accepted task branches reran during recovery: attempts=%d, want retained 43", len(afterActivation.Attempts))
	}
	for index := range records.Attempts {
		if afterActivation.Attempts[index].ID != records.Attempts[index].ID ||
			afterActivation.Attempts[index].Revision != records.Attempts[index].Revision {
			t.Fatalf("accepted branch changed at %d: before=%+v after=%+v", index, records.Attempts[index], afterActivation.Attempts[index])
		}
	}
	if _, err := activationService.Advance(ctx, runID, ActivationSignal{
		Event: domain.ActivationEventTriggerFired,
	}); !errors.Is(err, domain.ErrSupervisionIllegalTransition) {
		t.Fatalf("accepted branch reran after activation: %v", err)
	}

	inbox := CoalesceSupervisionEvents(runID, 0, reopened.Pending)
	if inbox.HighWaterMark != 1000 || len(inbox.Triggers) == 0 {
		t.Fatalf("coalesced current trigger=%+v", inbox)
	}
	snapshot := ActivationSnapshot{
		ActivationID: plan.Activation.ID, RunID: runID, Epoch: plan.Activation.Epoch,
		GraphRevision: 1, RecordRevision: plan.Record.Revision,
		TurnsRemaining: plan.Record.Config.MaxTurnsPerActivation,
		Triggers:       inbox.Triggers, ConsumedThrough: inbox.HighWaterMark,
		Actions:     []ActivationAction{{Name: "decide retained review"}},
		Constraints: []string{"bounded escalation", "no accepted branch rerun"},
	}
	for _, task := range records.Tasks {
		snapshot.Tasks = append(snapshot.Tasks, ActivationTaskView{TaskID: task.ID, State: "retained"})
	}
	for _, gate := range supervision.Gates {
		snapshot.Gates = append(snapshot.Gates, ActivationGateView{
			GateID: gate.Definition.ID, State: gate.State, GraphRevision: gate.GraphRevision,
			EvidenceSnapshotID: gate.EvidenceSnapshotID,
			ObservedTaskIDs:    gate.Definition.ObservedTaskIDs,
			ProtectedTaskIDs:   gate.Definition.ProtectedTaskIDs,
		})
	}
	evidence, err := BuildActivationEvidenceSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.EvidenceSnapshot = &domain.ArtifactDigest{ArtifactID: evidence.ID, Digest: evidence.SHA256}
	envelope, err := BuildActivationPromptEnvelope(snapshot)
	if err != nil {
		t.Fatalf("bounded prompt construction from retained history: %v", err)
	}
	if envelope.Size() > envelope.ByteCap || len(envelope.Facts) > 1+len(snapshot.Triggers)+ActivationBriefSubjectLimit*3 {
		t.Fatalf("unbounded activation prompt: size=%d cap=%d facts=%d", envelope.Size(), envelope.ByteCap, len(envelope.Facts))
	}
	if len(envelope.Inputs) != 1 || envelope.Inputs[0].Digest != evidence.SHA256 {
		t.Fatalf("bounded prompt lost retained evidence custody: %+v", envelope.Inputs)
	}
}

func m5HistoryBundle(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	var manifest strings.Builder
	manifest.WriteString("version: 2\nname: retained-history\nenvironment:\n  project: t3-steward\nroutes:\n  - instance: workerInstance\n    model: worker-model\nsupervision:\n  route:\n    instance: overseerInstance\n    model: overseer-model\n    quota_pool: overseer-pool\n  prompt_file: prompts/overseer.md\n  max_activations: 8\n  max_turns_per_activation: 2\n  activation_deadline: 90m\n  idle_escalation_after: 12h\n  escalation:\n    notify_thread: true\ngates:\n")
	for index := 0; index < 42; index++ {
		fmt.Fprintf(&manifest, "  review-%02d:\n    after: [task-%02d]\n    before: [task-%02d]\n    rubric_file: rubrics/review-%02d.md\n", index, index, index+1, index)
	}
	manifest.WriteString("  review-42:\n    after: [task-42]\n    final: true\n    rubric_file: rubrics/review-42.md\ntasks:\n")
	for index := 0; index < 43; index++ {
		fmt.Fprintf(&manifest, "  task-%02d:\n    prompt_file: prompts/task-%02d.md\n", index, index)
		if index > 0 {
			fmt.Fprintf(&manifest, "    needs: [task-%02d]\n", index-1)
		}
	}
	files := map[string]string{
		"workflow.yaml":       manifest.String(),
		"prompts/overseer.md": "Review retained evidence and the current trigger without rerunning accepted branches.\n",
	}
	for index := 0; index < 43; index++ {
		files[fmt.Sprintf("prompts/task-%02d.md", index)] = fmt.Sprintf("perform retained task %02d\n", index)
		files[fmt.Sprintf("rubrics/review-%02d.md", index)] = fmt.Sprintf("review evidence for task %02d\n", index)
	}
	for name, body := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
