package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReviewChildStagingConfiguredCloneRerunCustody(t *testing.T) {
	for _, operation := range []string{"clone", "rerun"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			admin, db, entry, records, bundle := independentDeclaredProduction(t)
			source := records.WorkflowRuns[0]
			original := records.Tasks[0]
			if operation == "rerun" {
				source.Progress = domain.ProgressFailed
				source.Revision++
				a := records.Attempts[0]
				a.Progress = domain.ProgressFailed
				a.Control = domain.ControlStopped
				a.Revision = 1
				if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{source}, Attempts: []domain.Attempt{a}}); err != nil {
					t.Fatal(err)
				}
			}
			principal := backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}}
			request := domain.GraphAmendment{ID: "independent-" + operation, RunID: source.ID, TaskID: original.ID, ExpectedRevision: source.GraphRevision, Operation: operation, Reason: "independent custody"}
			if operation == "clone" {
				request.TaskID = ""
			}
			result, err := admin.AmendGraph(ctx, principal, request)
			if err != nil {
				t.Fatal(err)
			}
			task := result.Graph.Tasks[0]
			d := task.ReviewRequirements
			if d.Criteria.RunID != result.Run.ID || d.Criteria.TaskID != task.ID || d.Criteria.ArtifactID == original.ReviewRequirements.Criteria.ArtifactID {
				t.Fatal("old criteria identity reused")
			}
			// Original submitted mutable file has no authority after retention.
			if err = os.WriteFile(filepath.Join(bundle, "inputs/plan.md"), []byte("untrusted changed source"), 0600); err != nil {
				t.Fatal(err)
			}
			loaded, err := db.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var parent domain.Attempt
			for _, a := range loaded.Attempts {
				if a.WorkflowRunID == result.Run.ID {
					parent = a
				}
			}
			parent.Progress = domain.ProgressActive
			parent.Control = domain.ControlRunning
			parent.ThreadID = "independent-thread"
			parent.AssignmentID = "independent-assignment"
			assignment := domain.Assignment{ID: parent.AssignmentID, DispatchToken: "independent-token", AttemptID: parent.ID, Epoch: 1, State: domain.AssignmentClaimed, ThreadID: parent.ThreadID, Project: "dev-fleet", TaskDigest: domain.TaskDigest(task), TaskRevision: task.DefinitionRevision, GraphRevision: result.Run.GraphRevision, Route: domain.ProviderRoute{ProviderInstanceID: "t3-primary", Model: "opus"}}
			if err = db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			frozen, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: result.Run.ID, TaskID: task.ID, AttemptID: parent.ID})
			if err != nil {
				t.Fatal(err)
			}
			if frozen.Provenance.Criteria.SHA256 != original.ReviewRequirements.Criteria.SHA256 || frozen.Provenance.Criteria.RunID != result.Run.ID {
				t.Fatal("custody/content lost")
			}
			cfg := declaredCoordinatorSettings(entry.service.Artifacts.SubmissionRoot)
			cfg.Storage.Artifacts = t.TempDir()
			owner, e := newCoordinatorDeclaredReviewStaging(cfg, db)
			if e != nil {
				t.Fatal(e)
			}
			req := backlog.DeclaredChildStageRequest{Parent: backlog.DeclaredAdmissionRequest{RunID: result.Run.ID, TaskID: task.ID, AttemptID: parent.ID}, CheckpointID: "cloned", HeadCommit: strings.Repeat("d", 40), Deadline: time.Now().UTC().Add(time.Hour)}
			staged, e := owner.StageDeclared(ctx, req)
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(staged.Admission, frozen) {
				t.Fatal("clone staging lost original authority")
			}
			if _, e = staged.Preparation.Build(staged.Admission.Authority, staged.Checkpoint, staged.Receipt.CreatedAt); e != nil {
				t.Fatal(e)
			}
			repeated, e := owner.StageDeclared(ctx, req)
			if e != nil || !reflect.DeepEqual(staged, repeated) {
				t.Fatal("clone retained stage changed", e)
			}
			// Returned graph/declaration mutation must not affect immutable reload or replay.
			result.Graph.Tasks[0].ReviewRequirements.Members[0].Route = "forged/model"
			again, err := admin.AmendGraph(ctx, principal, request)
			if err != nil {
				t.Fatal(err)
			}
			if !again.Replay || again.Graph.Tasks[0].ReviewRequirements.Members[0].Route == "forged/model" {
				t.Fatal("graph reply aliases persistent authority")
			}
			fresh, err := entry.FreezeDeclared(ctx, backlog.DeclaredAdmissionRequest{RunID: again.Run.ID, TaskID: task.ID, AttemptID: parent.ID})
			if err != nil || !reflect.DeepEqual(frozen, fresh) {
				t.Fatalf("frozen custody replay: %v", err)
			}
		})
	}
}
