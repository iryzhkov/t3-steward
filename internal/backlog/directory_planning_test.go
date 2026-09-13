package backlog

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func directoryTestBinding(access directoryresource.Access) directoryresource.Binding {
	return directoryresource.Binding{
		Identity: directoryresource.Identity{
			Registration: directoryresource.Registration{WorkerID: "worker-a", ResourceID: "dataset", Revision: "1", Path: "/srv/data", Writable: true},
			Object:       directoryresource.Object{Device: 1, Inode: 2, BirthSeconds: 100}, MountID: 3,
			Ancestors: []directoryresource.Object{{Device: 1, Inode: 1, BirthSeconds: 99}},
		}, Access: access,
	}
}

func TestDirectoryPlanningReservesWithinBatchAndPinsHost(t *testing.T) {
	for _, access := range []directoryresource.Access{directoryresource.ReadOnly, directoryresource.ReadWrite} {
		t.Run(string(access), func(t *testing.T) {
			first, second := testTask("alpha"), testTask("beta")
			first.DirectoryBindings = []directoryresource.Binding{directoryTestBinding(access)}
			second.DirectoryBindings = []directoryresource.Binding{directoryTestBinding(access)}
			input := plannerInput([]domain.Task{first, second}, []domain.WorkerInventory{plannerWorker("worker-b"), plannerWorker("worker-a")})
			plan, err := BuildPlan(input)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if access == directoryresource.ReadOnly {
				want = 2
			}
			if len(plan.Proposals) != want {
				t.Fatalf("proposals=%+v want=%d", plan.Proposals, want)
			}
			for _, p := range plan.Proposals {
				if p.WorkerID != "worker-a" {
					t.Fatal("wrong host")
				}
			}
			again, err := BuildPlan(input)
			if err != nil || len(again.Proposals) != want {
				t.Fatal("planning leaked ownership into next evaluation")
			}
		})
	}
}

func TestDirectoryOwnershipSurvivesDatabaseRestartCancellationAndExpiredLease(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	owner := testTask("owner")
	owner.DirectoryBindings = []directoryresource.Binding{directoryTestBinding(directoryresource.ReadWrite)}
	state := testDAGState(owner)
	attempt := state.Attempts[0]
	attempt.Progress = domain.ProgressCancelled
	attempt.AssignmentID = "assignment-owner"
	assignment := domain.Assignment{ID: attempt.AssignmentID, AttemptID: attempt.ID, WorkerID: "worker-a", State: domain.AssignmentClaimed, LeaseExpiresAt: plannerTestTime.Add(-time.Hour)}
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: owner.WorkflowID, Project: "project", TaskIDs: []string{owner.ID}}},
		WorkflowRuns: []domain.WorkflowRun{state.Run}, Tasks: []domain.Task{owner}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	store.Close()
	store, err = sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	input, err := BuildCoordinatorPlanInput(CoordinatorPlanningStateInput{
		Now: plannerTestTime, CoordinatorEpoch: 1, MaxWorkerSnapshotAge: time.Minute, MaxQuotaObservationAge: time.Minute, DeadlineRiskWindow: time.Hour, CheckpointMargin: time.Minute, Workflows: loaded.Workflows, WorkflowRuns: loaded.WorkflowRuns,
		Tasks: loaded.Tasks, Attempts: loaded.Attempts, Assignments: loaded.Assignments, DisableQuotaChecks: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(input.DirectoryOwners) != 1 {
		t.Fatalf("ownership lost: %+v", input.DirectoryOwners)
	}
	challenger := testTask("challenger")
	challenger.DirectoryBindings = []directoryresource.Binding{directoryTestBinding(directoryresource.ReadOnly)}
	candidate := PlanningCandidate{Task: challenger, Attempt: domain.Attempt{ID: "challenger-attempt", AdminForceStart: true}, WorkerID: "worker-a"}
	if got := newDirectorySession(input.DirectoryOwners).Evaluate(candidate); len(got) == 0 {
		t.Fatal("cancelled unconfirmed writer released")
	}
	// Missing assignment records also retain uncertainty.
	if got := directoryOwners(loaded.Attempts, nil, loaded.WorkflowRuns, loaded.Tasks); len(got) != 1 {
		t.Fatal("missing evidence released owner")
	}
	loaded.Assignments[0].State = domain.AssignmentCompleted
	if got := directoryOwners(loaded.Attempts, loaded.Assignments, loaded.WorkflowRuns, loaded.Tasks); len(got) != 0 {
		t.Fatal("confirmed settlement retained owner")
	}
}

func TestDirectoryCatalogRejectsChangedIdentityAndEscalation(t *testing.T) {
	allowed := directoryTestBinding(directoryresource.ReadOnly)
	task := testTask("task")
	task.DirectoryBindings = []directoryresource.Binding{allowed}
	project := ProjectDefinition{Name: "project", Type: EnvironmentFresh, SetupProfile: "empty", DirectoryBindings: []directoryresource.Binding{allowed}}
	catalog, err := NewProjectCatalog([]ProjectDefinition{project}, []SetupProfile{{Name: "empty", Commands: []string{"true"}, Timeout: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	workflow := domain.Workflow{ID: task.WorkflowID, Project: "project", Environment: domain.ExecutionEnvironment{Type: EnvironmentFresh, Scope: EnvironmentScopeTask}}
	if _, err := catalog.Resolve(workflow, task); err != nil {
		t.Fatal(err)
	}
	task.DirectoryBindings[0].Access = directoryresource.ReadWrite
	if _, err := catalog.Resolve(workflow, task); err == nil {
		t.Fatal("access escalated")
	}
	task.DirectoryBindings[0] = allowed
	task.DirectoryBindings[0].Identity.Registration.Revision = "2"
	if _, err := catalog.Resolve(workflow, task); err == nil {
		t.Fatal("revision changed")
	}
}
