package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func claimStagingParent(t *testing.T, db *sqlite.Store, records sqlite.CoordinatorRecords) backlog.DeclaredAdmissionRequest {
	t.Helper()
	task, run, parent := records.Tasks[0], records.WorkflowRuns[0], records.Attempts[0]
	parent.Progress, parent.Control = domain.ProgressActive, domain.ControlRunning
	parent.ThreadID, parent.AssignmentID, parent.Revision = "stage-thread", "stage-assignment", 1
	as := domain.Assignment{ID: parent.AssignmentID, DispatchToken: "stage-token", AttemptID: parent.ID, Epoch: 1, State: domain.AssignmentClaimed, ThreadID: parent.ThreadID, Project: records.Workflows[0].Project, TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: run.GraphRevision, Route: domain.ProviderRoute{ProviderInstanceID: "t3-primary", Model: "opus"}}
	if err := db.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{as}}); err != nil {
		t.Fatal(err)
	}
	return backlog.DeclaredAdmissionRequest{RunID: run.ID, TaskID: task.ID, AttemptID: parent.ID}
}

// The BEFORE version executed this same actual configured submission helper,
// retained criteria and claimed-parent freeze, then failed at the absent owner.
func TestReviewChildStagingConfiguredMissingBoundary(t *testing.T) {
	ctx := context.Background()
	_, db, entry, records, bundle := independentDeclaredProduction(t)
	request := claimStagingParent(t, db, records)
	before, err := db.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	audits, err := db.LoadAuditEvents(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	// Actual SQLite reload precedes the configured owner; separate backlog
	// attachment tests cover closing and reopening database ownership.
	cfg := declaredCoordinatorSettings(entry.service.Artifacts.SubmissionRoot)
	cfg.Storage.Artifacts = t.TempDir()
	owner, err := newCoordinatorDeclaredReviewStaging(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().UTC().Add(time.Hour)
	req := backlog.DeclaredChildStageRequest{Parent: request, CheckpointID: "configured", HeadCommit: strings.Repeat("d", 40), Deadline: deadline}
	if err = os.WriteFile(filepath.Join(bundle, "inputs/plan.md"), []byte("untrusted mutable source"), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := owner.StageDeclared(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := result.Preparation.Build(result.Admission.Authority, result.Checkpoint, result.Receipt.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if graph.Workflow.InputManifest.Digest != records.Workflows[0].InputManifest.Digest || len(graph.Artifacts) != len(result.Receipt.Manifest.Entries)+2 || result.Receipt.Criteria.SHA256 != records.Tasks[0].ReviewRequirements.Criteria.SHA256 {
		t.Fatal("whole manifest/criteria custody lost")
	}
	if result.Preparation.Deadline() != deadline || result.Receipt.PromptVersion == "" || result.Receipt.Version != "review-child-stage/v1" {
		t.Fatal("deadline/version lost")
	}
	after, err := db.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	auditAfter, err := db.LoadAuditEvents(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(audits, auditAfter) {
		t.Fatal("staging published phantom graph/artifact/wait/native audit effects")
	}
	againOwner, err := newCoordinatorDeclaredReviewStaging(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	again, err := againOwner.StageDeclared(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Receipt, again.Receipt) || !reflect.DeepEqual(result.Admission, again.Admission) {
		t.Fatal("reconstructed owner changed immutable stage")
	}
}
