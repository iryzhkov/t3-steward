package backlogadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

type safeIdentityFamily struct {
	name, key string
	result    domain.GraphAmendmentResult
	store     *sqlite.Store
}

func safeIdentityFamilies(t *testing.T) []safeIdentityFamily {
	t.Helper()
	ctx := context.Background()
	rerunService, rerunStore, _ := rerunFixture(t)
	rerun := rerunRequest("safe-rerun", "implement")
	rerun.Prompt = "corrected implementation prompt"
	rerunResult, err := rerunService.AmendGraph(ctx, Principal{ID: "operator"}, rerun)
	if err != nil {
		t.Fatal(err)
	}
	cloneService, cloneStore := graphFixture(t)
	clone := graphRequest("safe-clone", "clone", "", 1)
	cloneResult, err := cloneService.AmendGraph(ctx, Principal{ID: "operator"}, clone)
	if err != nil {
		t.Fatal(err)
	}
	addService, addStore := graphFixture(t)
	add := graphRequest("safe-add", "task-add", "", 1)
	add.Task = &domain.Task{Name: "added", Verification: []string{"true"}, Class: domain.TaskClassSurplus, MaxTurns: 1, Needs: []string{"a"}, Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "new"}}}
	add.Prompt = "added task prompt"
	addResult, err := addService.AmendGraph(ctx, Principal{ID: "operator"}, add)
	if err != nil {
		t.Fatal(err)
	}
	return []safeIdentityFamily{
		{name: "rerun", key: rerun.ID, result: rerunResult, store: rerunStore},
		{name: "clone", key: clone.ID, result: cloneResult, store: cloneStore},
		{name: "graph", key: add.ID, result: addResult, store: addStore},
	}
}

func safeFamilyTasks(f safeIdentityFamily) []domain.Task {
	if f.name != "graph" {
		return f.result.Graph.Tasks
	}
	for _, task := range f.result.Graph.Tasks {
		if task.Name == "added" {
			return []domain.Task{task}
		}
	}
	return nil
}

func assertSafeID(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("identity = %q, want %q", got, want)
	}
	if !domain.PathSafeID(got) {
		t.Errorf("identity is not path safe: %q", got)
	}
}

func assertSafeFamily(t *testing.T, f safeIdentityFamily) {
	t.Helper()
	switch f.name {
	case "rerun":
		assertSafeID(t, f.result.Run.ID, domain.RerunRunID(f.key))
	case "clone":
		assertSafeID(t, f.result.Run.ID, domain.CloneRunID(f.key))
	}
	tasks := safeFamilyTasks(f)
	if len(tasks) == 0 {
		t.Fatal("new task is absent")
	}
	records, err := f.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i, task := range tasks {
		var want string
		switch f.name {
		case "rerun":
			want = domain.RerunTaskID(f.key, i)
		case "clone":
			want = domain.CloneTaskID(f.key, i)
		case "graph":
			want = domain.GraphTaskID(f.key)
		}
		assertSafeID(t, task.ID, want)
		count := 0
		for _, attempt := range records.Attempts {
			if attempt.WorkflowRunID == f.result.Run.ID && attempt.TaskID == task.ID {
				count++
				assertSafeID(t, attempt.ID, domain.FirstAttemptID(f.name, task.ID))
				if attempt.Number != 1 {
					t.Errorf("first attempt number = %d", attempt.Number)
				}
			}
		}
		if count != 1 {
			t.Errorf("first attempts for %s = %d, want 1", task.ID, count)
		}
		if f.name == "graph" {
			assertSafeID(t, task.PromptArtifactID, domain.GraphPromptInputID(f.key))
		}
		if f.name == "rerun" && i == 0 {
			assertSafeID(t, task.PromptArtifactID, domain.RerunPromptInputID(f.key))
		}
	}
	var inputs []domain.Artifact
	for _, artifact := range records.Artifacts {
		if artifact.WorkflowRunID != f.result.Run.ID || artifact.Kind != domain.ArtifactInput {
			continue
		}
		if f.name == "graph" && artifact.TaskID != tasks[0].ID {
			continue
		}
		inputs = append(inputs, artifact)
	}
	if len(inputs) == 0 {
		t.Fatal("minted inputs are absent")
	}
	expected := map[string]bool{}
	switch f.name {
	case "rerun":
		expected[domain.RerunPromptInputID(f.key)] = true
		for i := 1; i < len(inputs); i++ {
			expected[domain.RerunInputID(f.key, i)] = true
		}
	case "clone":
		for i := 0; i < len(inputs); i++ {
			expected[domain.CloneInputID(f.key, i)] = true
		}
	case "graph":
		expected[domain.GraphPromptInputID(f.key)] = true
	}
	for _, input := range inputs {
		if !expected[input.ID] {
			t.Errorf("unexpected input identity %q", input.ID)
		}
		delete(expected, input.ID)
		if !domain.PathSafeID(input.ID) {
			t.Errorf("input identity is not path safe: %q", input.ID)
		}
	}
	if len(expected) != 0 {
		t.Errorf("missing inputs: %v", expected)
	}
}

func TestRerunMintsPathSafeIdentities(t *testing.T) { assertSafeFamily(t, safeIdentityFamilies(t)[0]) }
func TestCloneMintsPathSafeIdentities(t *testing.T) { assertSafeFamily(t, safeIdentityFamilies(t)[1]) }
func TestTaskAddMintsPathSafeIdentities(t *testing.T) {
	assertSafeFamily(t, safeIdentityFamilies(t)[2])
}

func TestWorkspacePathsAreVenvSafe(t *testing.T) {
	type identity struct{ name, run, task, attempt string }
	var ids []identity
	for _, family := range safeIdentityFamilies(t) {
		tasks := safeFamilyTasks(family)
		if len(tasks) == 0 {
			t.Fatal("new task is absent")
		}
		task := tasks[0]
		records, err := family.store.LoadCoordinatorRecords(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		attemptID := ""
		for _, attempt := range records.Attempts {
			if attempt.WorkflowRunID == family.result.Run.ID && attempt.TaskID == task.ID {
				attemptID = attempt.ID
			}
		}
		ids = append(ids, identity{family.name, family.result.Run.ID, task.ID, attemptID})
	}
	scheduledRun, scheduledTask := "scheduled-run-0123456789abcdef", "task-0123456789abcdef"
	ids = append(ids, identity{"schedule", scheduledRun, scheduledTask, domain.ScheduledAttemptID(scheduledRun, scheduledTask)})
	ids = append(ids, identity{"recovery", "run-0123456789abcdef", "task-fedcba9876543210", domain.RecoveryAttemptID(strings.Repeat("a", 64))})
	python, pythonErr := exec.LookPath("python3")
	if pythonErr != nil {
		t.Log("python3 is unavailable; venv step was not run")
	}
	for _, id := range ids {
		t.Run(id.name, func(t *testing.T) {
			for _, component := range []string{id.run, id.task, id.attempt} {
				if !domain.PathSafeID(component) {
					t.Errorf("unsafe workspace component %q", component)
				}
			}
			for _, suffix := range []string{"", "-e2"} {
				path := filepath.Join(t.TempDir(), id.run, id.task, id.attempt+suffix)
				// The temporary root is platform dependent; the identities beneath it are not.
				if strings.Contains(filepath.Join(id.run, id.task, id.attempt+suffix), ":") {
					t.Errorf("workspace identities contain colon: %s", path)
				}
				if !domain.PathSafeID(id.attempt + suffix) {
					t.Errorf("unsafe epoch component %q", id.attempt+suffix)
				}
				if pythonErr == nil {
					if output, err := exec.Command(python, "-m", "venv", "--without-pip", filepath.Join(path, ".venv")).CombinedOutput(); err != nil {
						t.Errorf("venv at %s: %v\n%s", path, err, output)
					}
				}
			}
		})
	}
}

func TestDerivedTaskCommitRefsAreValid(t *testing.T) {
	for _, family := range safeIdentityFamilies(t) {
		for _, task := range safeFamilyTasks(family) {
			ref := backlog.CampaignRef(family.result.Run.ID, task.ID, "change")
			if err := backlog.ValidateRefSyntax(ref); err != nil {
				t.Errorf("%s commit ref %q: %v", family.name, ref, err)
			}
		}
	}
}

func TestDerivedIdentitiesPassWakeSummaryRules(t *testing.T) {
	// Re-state commandSafeRunID and summaryName without coupling to internal/wait.
	runRule := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	taskRule := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	for _, family := range safeIdentityFamilies(t) {
		if !runRule.MatchString(family.result.Run.ID) {
			t.Errorf("wake run rule rejects %q", family.result.Run.ID)
		}
		for _, task := range safeFamilyTasks(family) {
			if !taskRule.MatchString(task.ID) {
				t.Errorf("wake task rule rejects %q", task.ID)
			}
		}
	}
	for _, id := range []string{"scheduled-run-0123456789abcdef", "admin-run-0123456789abcdef"} {
		if !runRule.MatchString(id) {
			t.Errorf("wake run rule rejects %q", id)
		}
	}
	if runRule.MatchString("run:rerun:legacy") {
		t.Fatal("legacy colon run unexpectedly passes wake-summary rule")
	}
}

func TestLegacyColonRunsStillReadRerunAndClone(t *testing.T) {
	ctx := context.Background()
	_, sourceStore, artifactRoot := rerunFixture(t)
	records, err := sourceStore.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const legacyRun = "run:rerun:legacy"
	remap := map[string]string{}
	for i, task := range records.Tasks {
		remap[task.ID] = fmt.Sprintf("task:rerun:legacy:%d", i)
	}
	for i := range records.Tasks {
		task := &records.Tasks[i]
		task.ID = remap[task.ID]
		task.RunID = legacyRun
	}
	for i := range records.Workflows {
		for j, id := range records.Workflows[i].TaskIDs {
			records.Workflows[i].TaskIDs[j] = remap[id]
		}
	}
	for i := range records.Attempts {
		attempt := &records.Attempts[i]
		attempt.TaskID = remap[attempt.TaskID]
		attempt.WorkflowRunID = legacyRun
		attempt.ID = "attempt:" + attempt.TaskID + ":1"
	}
	for i := range records.Artifacts {
		artifact := &records.Artifacts[i]
		artifact.WorkflowRunID = legacyRun
		artifact.TaskID = remap[artifact.TaskID]
		if artifact.AttemptID != "" {
			artifact.AttemptID = "attempt:" + artifact.TaskID + ":1"
		}
	}
	for i := range records.WorkflowRuns {
		run := &records.WorkflowRuns[i]
		run.ID = legacyRun
		run.Sink = nil
		*run, err = domain.BindRunSink(*run, records.Tasks)
		if err != nil {
			t.Fatal(err)
		}
	}
	scheduled := records.WorkflowRuns[0]
	scheduled.ID = "scheduled-run-legacy"
	scheduled.Sink = nil
	scheduledTask := records.Tasks[0]
	scheduledTask.ID = "task-scheduled-legacy"
	scheduledTask.RunID = scheduled.ID
	scheduledTask.Name = "scheduled"
	scheduledTask.Needs = nil
	records.Tasks = append(records.Tasks, scheduledTask)
	scheduled, err = domain.BindRunSink(scheduled, []domain.Task{scheduledTask})
	if err != nil {
		t.Fatal(err)
	}
	records.WorkflowRuns = append(records.WorkflowRuns, scheduled)
	scheduledAttempt := records.Attempts[0]
	scheduledAttempt.WorkflowRunID = scheduled.ID
	scheduledAttempt.TaskID = scheduledTask.ID
	scheduledAttempt.ID = "attempt:" + scheduled.ID + ":" + scheduledAttempt.TaskID + ":1"
	records.Attempts = append(records.Attempts, scheduledAttempt)
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err = store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return records.WorkflowRuns[0].UpdatedAt.Add(2 * time.Hour) })
	service.SetGraphAmendmentSupport(artifactRoot, func(domain.Workflow, domain.Task) error { return nil })
	artifacts := backlog.CoordinatorArtifactStore{Root: artifactRoot, Catalog: store}
	service.SetArtifactOpener(func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return artifacts.Open(ctx, id)
	})
	snapshot := func() string {
		t.Helper()
		loaded, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var kept sqlite.CoordinatorRecords
		for _, run := range loaded.WorkflowRuns {
			if run.ID == legacyRun || run.ID == scheduled.ID {
				kept.WorkflowRuns = append(kept.WorkflowRuns, run)
			}
		}
		for _, task := range loaded.Tasks {
			if strings.HasPrefix(task.ID, "task:rerun:legacy:") || task.ID == scheduledTask.ID {
				kept.Tasks = append(kept.Tasks, task)
			}
		}
		for _, attempt := range loaded.Attempts {
			if attempt.WorkflowRunID == legacyRun || attempt.WorkflowRunID == scheduled.ID {
				kept.Attempts = append(kept.Attempts, attempt)
			}
		}
		for _, artifact := range loaded.Artifacts {
			if artifact.WorkflowRunID == legacyRun {
				kept.Artifacts = append(kept.Artifacts, artifact)
			}
		}
		raw, err := json.Marshal(kept)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	before := snapshot()
	for _, runID := range []string{legacyRun, scheduled.ID} {
		response, err := service.Query(ctx, Query{Version: CurrentReadVersion, Kind: QueryWorkflow, Principal: Principal{ID: "operator"}, WorkflowRunID: runID})
		if err != nil || response.Workflow == nil {
			t.Fatalf("legacy workflow %s: %v", runID, err)
		}
		taskID := records.Tasks[0].ID
		if runID == scheduled.ID {
			taskID = scheduledTask.ID
		}
		ref, err := domain.ParseNodeRef(runID + "/" + taskID)
		if err != nil || ref.RunID != runID || ref.TaskID != taskID {
			t.Fatalf("legacy node reference: %+v %v", ref, err)
		}
		observation, err := domain.ResolveNode(ref, records.WorkflowRuns, records.Tasks, records.Attempts, nil)
		if err != nil {
			t.Fatal(err)
		}
		wantAttempt := "attempt:" + records.Tasks[0].ID + ":1"
		if runID == scheduled.ID {
			wantAttempt = scheduledAttempt.ID
		}
		if observation.AttemptID != wantAttempt {
			t.Errorf("legacy attempt = %q, want %q", observation.AttemptID, wantAttempt)
		}
	}
	rerun := rerunRequest("from-legacy-rerun", "implement")
	rerun.RunID = legacyRun
	rerun.Prompt = "corrected legacy implementation prompt"
	rerunResult, err := service.AmendGraph(ctx, Principal{ID: "operator"}, rerun)
	if err != nil {
		t.Fatal(err)
	}
	assertSafeFamily(t, safeIdentityFamily{"rerun", rerun.ID, rerunResult, store})
	clone := graphRequest("from-legacy-clone", "clone", "", 1)
	clone.RunID = legacyRun
	cloneResult, err := service.AmendGraph(ctx, Principal{ID: "operator"}, clone)
	if err != nil {
		t.Fatal(err)
	}
	assertSafeFamily(t, safeIdentityFamily{"clone", clone.ID, cloneResult, store})
	if after := snapshot(); after != before {
		t.Fatalf("legacy rows changed\nbefore: %s\nafter: %s", before, after)
	}
	if rerunResult.Graph.RerunOf.SourceRunID != legacyRun || cloneResult.Graph.ClonedFrom.RunID != legacyRun {
		t.Fatal("legacy source reference was rewritten")
	}
}
