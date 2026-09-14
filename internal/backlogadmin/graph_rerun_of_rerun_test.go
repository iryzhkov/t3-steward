package backlogadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// terminalize makes a run look finished so that it can be rerun, which is the
// only state PlanRerun accepts.
func terminalize(t *testing.T, store *sqlite.Store, runID string, failed map[string]bool) {
	t.Helper()
	ctx := context.Background()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := commitCampaignTime.Add(3 * time.Hour)
	var update sqlite.CoordinatorRecords
	for _, attempt := range records.Attempts {
		if attempt.WorkflowRunID != runID {
			continue
		}
		attempt.Progress = domain.ProgressSucceeded
		if failed[attempt.TaskID] {
			attempt.Progress = domain.ProgressFailed
		}
		attempt.Control = domain.ControlStopped
		attempt.Revision++
		attempt.UpdatedAt = now
		update.Attempts = append(update.Attempts, attempt)
	}
	for _, run := range records.WorkflowRuns {
		if run.ID != runID {
			continue
		}
		run.Progress = domain.ProgressFailed
		run.Revision++
		run.UpdatedAt = now
		run.CompletedAt = &now
		if run.Sink != nil {
			run.Sink.Progress = domain.ProgressFailed
			run.Sink.CompletedAt = &now
		}
		update.WorkflowRuns = append(update.WorkflowRuns, run)
	}
	if err := store.SaveCoordinatorRecords(ctx, update); err != nil {
		t.Fatal(err)
	}
}

// A rerun of a rerun is the commonest recovery path there is: the fix did not
// work, and the same task has to run again. The second rerun's subtree root
// still carries what the first rerun's reused ancestors produced, and those
// carried inputs name the first rerun's artifacts until they are referenced
// into the new run. Leaving them alone made every rerun of a rerun refuse with
// an internal ownership message.
func TestARerunOfARerunCarriesTheInputsTheFirstRerunCarried(t *testing.T) {
	ctx := context.Background()
	service, store, _ := rerunFixture(t)
	principal := Principal{ID: "operator"}

	first, err := service.AmendGraph(ctx, principal, rerunRequest("rerun-1", "implement"))
	if err != nil {
		t.Fatalf("first rerun: %v", err)
	}
	carried := taskNamed(t, first, "implement").CarriedInputs
	if len(carried) != 1 {
		t.Fatalf("the first rerun carried %+v", carried)
	}

	// The retry fails again, which is why anyone reruns a rerun.
	terminalize(t, store, first.Run.ID, map[string]bool{taskNamed(t, first, "implement").ID: true})
	second, err := service.AmendGraph(ctx, principal, domain.GraphAmendment{
		ID: "rerun-2", RunID: first.Run.ID, Operation: "rerun", TaskID: "implement",
		ExpectedRevision: 1, Reason: "the fix did not hold",
	})
	if err != nil {
		t.Fatalf("rerun of a rerun: %v", err)
	}
	if second.Run.ID == first.Run.ID {
		t.Fatal("the second rerun reused the first run's identity")
	}

	// What the first rerun carried is carried again, by a reference the new run
	// owns rather than one naming the run it came from.
	secondCarried := taskNamed(t, second, "implement").CarriedInputs
	if len(secondCarried) != 1 {
		t.Fatalf("the second rerun carried %+v", secondCarried)
	}
	if secondCarried[0].Name != carried[0].Name || secondCarried[0].Producer != carried[0].Producer {
		t.Fatalf("carried input changed shape: %+v, want the name and producer of %+v", secondCarried[0], carried[0])
	}
	if secondCarried[0].ArtifactID == carried[0].ArtifactID {
		t.Fatalf("the second rerun points at the first run's artifact %q", secondCarried[0].ArtifactID)
	}

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var reference, origin domain.Artifact
	for _, artifact := range records.Artifacts {
		switch artifact.ID {
		case secondCarried[0].ArtifactID:
			reference = artifact
		case carried[0].ArtifactID:
			origin = artifact
		}
	}
	if reference.ID == "" || origin.ID == "" {
		t.Fatalf("artifacts missing: reference %+v origin %+v", reference, origin)
	}
	if reference.WorkflowRunID != second.Run.ID || reference.TaskID != taskNamed(t, second, "implement").ID {
		t.Fatalf("the carried reference is not owned by the new run: %+v", reference)
	}
	// Carried by reference, so the content address is the same and nothing was
	// copied. That is what makes the chain safe to repeat.
	if reference.SHA256 != origin.SHA256 || reference.StoragePath != origin.StoragePath {
		t.Fatalf("the carried reference copied content: %+v vs %+v", reference, origin)
	}
}

// The refusal an unresolvable carried input produces is about the artifact, not
// about internal ownership: a rerun whose carried input has been pruned must
// say what it could not read.
func TestARerunOfARerunRefusesWhenTheCarriedArtifactIsGone(t *testing.T) {
	ctx := context.Background()
	service, store, _ := rerunFixture(t)
	principal := Principal{ID: "operator"}
	first, err := service.AmendGraph(ctx, principal, rerunRequest("rerun-1", "implement"))
	if err != nil {
		t.Fatal(err)
	}
	terminalize(t, store, first.Run.ID, map[string]bool{taskNamed(t, first, "implement").ID: true})
	service.SetArtifactOpener(nil)
	_, err = service.AmendGraph(ctx, principal, domain.GraphAmendment{
		ID: "rerun-2", RunID: first.Run.ID, Operation: "rerun", TaskID: "implement",
		ExpectedRevision: 1, Reason: "the fix did not hold",
	})
	if err == nil || strings.Contains(err.Error(), "ownership mismatch") {
		t.Fatalf("error = %v, want one that names the unreadable artifact", err)
	}
}

func taskNamed(t *testing.T, result domain.GraphAmendmentResult, name string) domain.Task {
	t.Helper()
	for _, task := range result.Graph.Tasks {
		if task.Name == name {
			return task
		}
	}
	t.Fatalf("run %s has no task %q", result.Run.ID, name)
	return domain.Task{}
}
