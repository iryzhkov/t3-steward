package backlogadmin

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type graphAuthorizer struct{}

func (graphAuthorizer) Authorize(context.Context, Principal, Action) error { return nil }

func graphFixture(t *testing.T) (*Service, *sqlite.Store) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	store, err := sqlite.OpenMigrated(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	records := sqlite.CoordinatorRecords{Workflows: []domain.Workflow{{ID: "workflow", Name: "test", TaskIDs: []string{"a", "b"}}}}
	for _, name := range []string{"a", "b"} {
		task := domain.Task{ID: name, WorkflowID: "workflow", Name: name, Class: domain.TaskClassSurplus, MaxTurns: 1, PromptArtifactID: "prompt-" + name, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "old"}}}
		a, err := backlog.PrepareGraphInput(filepath.Join(root, "artifacts"), task.PromptArtifactID, "run", name, "prompt "+name, now)
		if err != nil {
			t.Fatal(err)
		}
		records.Tasks = append(records.Tasks, task)
		records.Artifacts = append(records.Artifacts, a)
		records.Attempts = append(records.Attempts, domain.Attempt{ID: "attempt-" + name, WorkflowRunID: "run", TaskID: name, Number: 1, Revision: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, UpdatedAt: now})
	}
	for _, id := range []string{"run", "sibling"} {
		run, err := domain.BindRunSink(domain.WorkflowRun{ID: id, WorkflowID: "workflow", GraphRevision: 1, Revision: 1, Progress: domain.ProgressQueued, CreatedAt: now, UpdatedAt: now}, records.Tasks)
		if err != nil {
			t.Fatal(err)
		}
		records.WorkflowRuns = append(records.WorkflowRuns, run)
	}
	if err = store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	s, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	s.SetClock(func() time.Time { return now.Add(time.Minute) })
	s.SetGraphAmendmentSupport(filepath.Join(root, "artifacts"), func(domain.Workflow, domain.Task) error { return nil })
	artifacts := backlog.CoordinatorArtifactStore{Root: filepath.Join(root, "artifacts"), Catalog: store}
	s.SetArtifactOpener(func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return artifacts.Open(ctx, id)
	})
	return s, store
}
func graphRequest(id, op, target string, revision int64) domain.GraphAmendment {
	return domain.GraphAmendment{ID: id, RunID: "run", Operation: op, TaskID: target, ExpectedRevision: revision, Reason: "test amendment"}
}

func TestGraphAmendmentRevisionReplaySiblingAndClone(t *testing.T) {
	ctx := context.Background()
	s, store := graphFixture(t)
	p := Principal{ID: "operator"}
	model := "new"
	r := graphRequest("change-model", "task-set", "a", 1)
	r.Model = &model
	result, err := s.AmendGraph(ctx, p, r)
	if err != nil {
		t.Fatal(err)
	}
	if result.Graph.Revision != 2 || result.Graph.Parent != 1 || result.Run.Sink.GraphRevision != 2 {
		t.Fatalf("revision binding: %+v", result)
	}
	replay, err := s.AmendGraph(ctx, p, r)
	if err != nil || !replay.Replay || replay.Graph.Digest != result.Graph.Digest {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	changed := r
	changed.Reason = "different"
	if _, err = s.AmendGraph(ctx, p, changed); err == nil {
		t.Fatal("changed replay accepted")
	}
	if _, err = s.AmendGraph(ctx, Principal{ID: "other"}, r); err == nil {
		t.Fatal("changed actor accepted")
	}
	stale := r
	stale.ID = "stale"
	if _, err = s.AmendGraph(ctx, p, stale); !errors.Is(err, sqlite.ErrStaleGraph) {
		t.Fatalf("stale: %v", err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range records.WorkflowRuns {
		task := domain.TasksForRun(run, records.Tasks)[0]
		if run.ID == "sibling" && (run.Graph != nil || task.Routes[0].Model != "old") {
			t.Fatal("sibling definition changed")
		}
	}
	clone := graphRequest("clone", "clone", "", 2)
	copied, err := s.AmendGraph(ctx, p, clone)
	if err != nil {
		t.Fatal(err)
	}
	if copied.Run.ID == "run" || copied.Run.GraphRevision != 1 || copied.Graph.ClonedFrom.RunID != "run" || copied.Graph.ClonedFrom.Revision != 2 {
		t.Fatalf("clone: %+v", copied)
	}
	for _, task := range copied.Graph.Tasks {
		if task.ID == "a" || task.ID == "b" || task.PromptArtifactID == "prompt-a" || task.PromptArtifactID == "prompt-b" {
			t.Fatal("clone reused source identity")
		}
	}
	replay, err = s.AmendGraph(ctx, p, clone)
	if err != nil || !replay.Replay || replay.Run.ID != copied.Run.ID {
		t.Fatalf("clone replay: %+v %v", replay, err)
	}
	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, a := range records.Attempts {
		if a.WorkflowRunID == copied.Run.ID {
			count++
			if a.AssignmentID != "" || a.ThreadID != "" || a.ID == "attempt-a" {
				t.Fatal("clone reused execution")
			}
		}
	}
	if count != 2 {
		t.Fatalf("clone attempts=%d", count)
	}
}

func TestGraphAmendmentAddEdgesCyclesAndTerminalFreeze(t *testing.T) {
	ctx := context.Background()
	s, store := graphFixture(t)
	p := Principal{ID: "operator"}
	add := graphRequest("add", "task-add", "", 1)
	add.Task = &domain.Task{Name: "c", Verification: []string{"git status --porcelain"}, Class: domain.TaskClassSurplus, MaxTurns: 1, Needs: []string{"a"}, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "new"}}}
	add.Prompt = "new task"
	result, err := s.AmendGraph(ctx, p, add)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Graph.Tasks) != 3 || len(result.Run.Sink.Needs) != 3 {
		t.Fatal("sink was not rebound")
	}
	edge := graphRequest("cycle", "edge-add", "a", 2)
	edge.Source = "c"
	if _, err = s.AmendGraph(ctx, p, edge); err == nil {
		t.Fatal("cycle accepted")
	}
	edge.ID = "edge"
	edge.TaskID = "b"
	edge.Source = "c"
	result, err = s.AmendGraph(ctx, p, edge)
	if err != nil {
		t.Fatal(err)
	}
	if result.Graph.Revision != 3 {
		t.Fatal("rejected cycle consumed revision")
	}
	edge.ID = "remove"
	edge.Operation = "edge-remove"
	edge.ExpectedRevision = 3
	if _, err = s.AmendGraph(ctx, p, edge); err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range records.Attempts {
		if a.ID == "attempt-a" {
			a.Progress = domain.ProgressSucceeded
			a.Control = domain.ControlStopped
			if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	model := "changed"
	set := graphRequest("terminal", "task-set", "a", 4)
	set.Model = &model
	if _, err = s.AmendGraph(ctx, p, set); err == nil {
		t.Fatal("terminal task edited")
	}
}

func TestGraphAmendmentConcurrentWritersAndAssignedFreeze(t *testing.T) {
	ctx := context.Background()
	s, store := graphFixture(t)
	p := Principal{ID: "operator"}
	model := "new"
	a := graphRequest("first", "task-set", "a", 1)
	a.Model = &model
	b := a
	b.ID = "second"
	b.TaskID = "b"
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, r := range []domain.GraphAmendment{a, b} {
		wg.Add(1)
		go func(r domain.GraphAmendment) {
			defer wg.Done()
			<-start
			_, err := s.AmendGraph(ctx, p, r)
			results <- err
		}(r)
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("successful concurrent amendments=%d", success)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	attempt := records.Attempts[0]
	attempt.AssignmentID = "offered"
	assignment := domain.Assignment{ID: "offered", AttemptID: attempt.ID, State: domain.AssignmentOffered, DispatchToken: "dispatch", WorkerID: "worker", Epoch: 1}
	if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	r := graphRequest("assigned", "task-set", attempt.TaskID, 2)
	verification := []string{"test -f result.txt"}
	r.Verification = &verification
	if _, err = s.AmendGraph(ctx, p, r); err == nil {
		t.Fatal("offered task verification edited")
	}
}

func TestGraphAmendmentVerificationRejectsIncompleteAddAndRepairsLegacyTask(t *testing.T) {
	ctx := context.Background()
	s, store := graphFixture(t)
	p := Principal{ID: "operator"}
	add := graphRequest("missing-verification", "task-add", "", 1)
	add.Task = &domain.Task{Name: "c", Class: domain.TaskClassSurplus, MaxTurns: 1, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "new"}}}
	add.Prompt = "write result.txt"
	if _, err := s.AmendGraph(ctx, p, add); err == nil {
		t.Fatal("addition without verification accepted")
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Artifacts) != 2 || len(records.Attempts) != 2 {
		t.Fatal("invalid addition published input or attempt")
	}
	for _, run := range records.WorkflowRuns {
		if run.GraphRevision != 1 || run.Graph != nil {
			t.Fatal("invalid addition changed graph")
		}
	}
	commands := []string{"test -s result.txt", "git diff --exit-code"}
	set := graphRequest("repair-verification", "task-set", "a", 1)
	set.Verification = &commands
	result, err := s.AmendGraph(ctx, p, set)
	if err != nil {
		t.Fatal(err)
	}
	if result.Graph.Revision != 2 {
		t.Fatal("repair did not create revision 2")
	}
	commands[0] = "mutated"
	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range records.WorkflowRuns {
		for _, task := range domain.TasksForRun(run, records.Tasks) {
			if task.Name != "a" {
				continue
			}
			if run.ID == "run" && (len(task.Verification) != 2 || task.Verification[0] != "test -s result.txt" || task.Verification[1] != "git diff --exit-code") {
				t.Fatalf("verification not persisted: %+v", task)
			}
			if run.ID == "sibling" && len(task.Verification) != 0 {
				t.Fatal("repair changed original definition")
			}
		}
	}
}

func TestGraphAmendmentInputFailureDoesNotPublish(t *testing.T) {
	ctx := context.Background()
	s, store := graphFixture(t)
	s.graphInputRoot = "/dev/null/unwritable"
	r := graphRequest("input-failure", "task-add", "", 1)
	r.Task = &domain.Task{Name: "c", Verification: []string{"git status --porcelain"}, Class: domain.TaskClassSurplus, MaxTurns: 1, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "new"}}}
	r.Prompt = "prompt"
	if _, err := s.AmendGraph(ctx, Principal{ID: "operator"}, r); err == nil {
		t.Fatal("input publication unexpectedly succeeded")
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Artifacts) != 2 || len(records.Attempts) != 2 {
		t.Fatal("failed input exposed metadata")
	}
	for _, run := range records.WorkflowRuns {
		if run.GraphRevision != 1 || run.Graph != nil {
			t.Fatal("failed input changed graph")
		}
	}
}
