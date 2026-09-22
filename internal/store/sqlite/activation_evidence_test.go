package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func seedActivationEvidenceOwner(t *testing.T, store *Store) ActivationEvidencePublication {
	t.Helper()
	ctx := context.Background()
	run := seedSupervisedRun(t, store, nil)
	seedSupervisionInboxEvent(t, store, "evidence-event", 1)
	actor := domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:" + run.ID, ActivationEpoch: 1}
	if _, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: run.ID, ActivationID: "activation-evidence", RequestID: "evidence-trigger",
		Actor: actor, Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: 1, DispatchIdentity: "supervision:" + run.ID + ":1",
		TransitionedAt: supervisionTestTime,
	}); err != nil {
		t.Fatalf("create activation: %v", err)
	}
	attempt := domain.Attempt{
		ID: "activation-attempt", WorkflowRunID: run.ID, TaskID: "supervision", Number: 1,
		Revision: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
		SupervisionActivationID: "activation-evidence", SupervisionActivationEpoch: 1,
	}
	raw, err := json.Marshal(attempt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO coordinator_attempts(
		id, workflow_run_id, task_id, number, revision, record
	) VALUES (?, ?, ?, ?, ?, ?)`, attempt.ID, attempt.WorkflowRunID, attempt.TaskID, attempt.Number, attempt.Revision, raw); err != nil {
		t.Fatalf("insert activation attempt: %v", err)
	}
	return ActivationEvidencePublication{
		CoordinatorEpoch: 1, ActivationID: "activation-evidence", RunID: run.ID,
		ActivationEpoch: 1, GraphRevision: run.GraphRevision,
		Artifact: domain.Artifact{
			ID: "activation-evidence-sha256-a", WorkflowRunID: run.ID,
			TaskID: "supervision", AttemptID: attempt.ID, Kind: domain.ArtifactInput,
			Name:        "supervision-evidence-activation-evidence.json",
			MediaType:   "application/vnd.t3-steward.supervision-evidence.v1+json",
			SHA256:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			StoragePath: "objects/aa/evidence", Producer: "coordinator/supervision",
			CreatedAt: supervisionTestTime,
		},
	}
}

func TestActivationEvidenceRejectsArtifactOutsideLiveActivationAttempt(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ActivationEvidencePublication)
	}{
		{name: "wrong task", mutate: func(p *ActivationEvidencePublication) { p.Artifact.TaskID = "task-2" }},
		{name: "wrong attempt", mutate: func(p *ActivationEvidencePublication) { p.Artifact.AttemptID = "attempt-1" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
			publication := seedActivationEvidenceOwner(t, store)
			test.mutate(&publication)
			if _, _, err := store.EnsureActivationEvidence(context.Background(), publication); !errors.Is(err, ErrActivationEvidenceConflict) {
				t.Fatalf("wrong owner error = %v, want %v", err, ErrActivationEvidenceConflict)
			}
		})
	}
}

func TestActivationEvidenceFirstFreezeSurvivesRestartAndRejectsCompetitor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	publication := seedActivationEvidenceOwner(t, store)
	retained, created, err := store.EnsureActivationEvidence(context.Background(), publication)
	if err != nil || !created {
		t.Fatalf("first freeze = %#v, %v, want created", retained, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	store = openSupervisionStore(t, path)
	replayed, created, err := store.EnsureActivationEvidence(context.Background(), publication)
	if err != nil || created || replayed.ID != retained.ID || replayed.SHA256 != retained.SHA256 {
		t.Fatalf("restart replay = %#v, created %v, err %v", replayed, created, err)
	}
	competitor := publication
	competitor.Artifact.ID = "activation-evidence-sha256-b"
	competitor.Artifact.SHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, _, err := store.EnsureActivationEvidence(context.Background(), competitor); !errors.Is(err, ErrActivationEvidenceConflict) {
		t.Fatalf("competing publication error = %v, want %v", err, ErrActivationEvidenceConflict)
	}
}

func TestActivationEvidenceConcurrentPublicationFreezesOneDigest(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	first := seedActivationEvidenceOwner(t, store)
	second := first
	second.Artifact.ID = "activation-evidence-sha256-b"
	second.Artifact.SHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	type result struct {
		artifact domain.Artifact
		created  bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, publication := range []ActivationEvidencePublication{first, second} {
		wg.Add(1)
		go func(publication ActivationEvidencePublication) {
			defer wg.Done()
			<-start
			artifact, created, err := store.EnsureActivationEvidence(context.Background(), publication)
			results <- result{artifact: artifact, created: created, err: err}
		}(publication)
	}
	close(start)
	wg.Wait()
	close(results)

	created := 0
	conflicts := 0
	for got := range results {
		if got.created && got.err == nil {
			created++
		} else if errors.Is(got.err, ErrActivationEvidenceConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent result: %+v", got)
		}
	}
	if created != 1 || conflicts != 1 {
		t.Fatalf("created %d conflicts %d, want one each", created, conflicts)
	}
}
