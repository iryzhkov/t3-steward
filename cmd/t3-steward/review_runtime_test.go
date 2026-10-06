package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Real materialization and production sqlite path; no manual cancellation,
// workers, quota, transport or provider effects are needed for the runtime pass.
func reviewRuntimeProductionFixture(t *testing.T) (*sqlite.Store, review.FrozenAuthority, sqlite.ReviewMaterialization, string) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlitetest.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	now := time.Now().UTC()
	parent := domain.Attempt{ID: "parent", WorkflowRunID: "parent-run", TaskID: "parent-task", Number: 1, Revision: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, AssignmentID: "parent-assignment", ThreadID: "parent-thread", UpdatedAt: now}
	assignment := domain.Assignment{ID: parent.AssignmentID, AttemptID: parent.ID, WorkerID: "parent-worker", WorkerEpoch: "parent-session", Epoch: 1, State: domain.AssignmentClaimed, Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "sol"}, ThreadID: parent.ThreadID, Project: "repo", LeaseToken: "parent-lease", DispatchToken: "parent-token"}
	task := domain.Task{ID: parent.TaskID, WorkflowID: "parent-workflow", Name: "parent", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{assignment.Route}}
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: parent.WorkflowRunID, WorkflowID: task.WorkflowID, Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}, []domain.Task{task})
	if err != nil {
		t.Fatal(err)
	}
	workflow := domain.Workflow{ID: task.WorkflowID, Name: "parent", Version: 1, Project: "repo", Environment: domain.ExecutionEnvironment{Type: "repository", Scope: "repo"}, Class: domain.TaskClassRequired, TaskIDs: []string{task.ID}, CreatedAt: now}
	if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	raw := []byte("production runtime frozen review criteria")
	digest := func(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }
	req, err := review.NewRequirements(review.RequirementsSpec{Risk: "routine", CriteriaDigest: digest(raw), PolicyDigest: strings.Repeat("a", 64), RequiredReviewers: 2, MinProviderFamilies: 2, Members: []review.MemberRequirement{{ID: "one", Role: "independent", Route: "codex/sol", ProviderFamily: "openai", Tier: "executor", Required: true}, {ID: "two", Role: "independent", Route: "other/model", ProviderFamily: "other", Tier: "executor", Required: true}}})
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := review.NewFrozenAuthority(review.ParentBinding{RunID: parent.WorkflowRunID, TaskID: parent.TaskID, AttemptID: parent.ID, ThreadID: parent.ThreadID, AssignmentID: assignment.ID, AssignmentEpoch: 1, IssuedRevision: parent.Revision, Repository: "repo", BaseCommit: strings.Repeat("b", 40), ExecutorRoute: "codex/sol"}, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.FreezeReviewAuthority(ctx, frozen); err != nil {
		t.Fatal(err)
	}
	manifest, err := pinnedinput.NewManifest([]pinnedinput.Entry{{Name: "inputs/criteria.md", Size: int64(len(raw)), SHA256: digest(raw)}})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := store.AllocateReviewCheckpoint(ctx, frozen, review.Checkpoint{ID: "runtime", HeadCommit: strings.Repeat("c", 40), InputDigest: manifest.Digest})
	if err != nil {
		t.Fatal(err)
	}
	artifact := domain.Artifact{ID: review.ChildInputID(cp, "inputs/criteria.md"), WorkflowRunID: cp.RoundID, Kind: domain.ArtifactInput, Name: "inputs/criteria.md", Size: int64(len(raw)), SHA256: digest(raw), StoragePath: "child/criteria.md", Producer: "submission", CreatedAt: now}
	files := []review.RetainedFile{{Artifact: artifact, Bytes: raw}}
	for _, member := range frozen.Requirements.Members {
		prompt, err := review.ChildPrompt(frozen, cp, member, manifest, artifact.Name)
		if err != nil {
			t.Fatal(err)
		}
		data := []byte(prompt)
		a := domain.Artifact{ID: review.ChildPromptID(cp, member.ID), WorkflowRunID: cp.RoundID, TaskID: cp.MemberTaskID(member.ID), Kind: domain.ArtifactInput, Name: review.ChildPromptName(member.ID), Size: int64(len(data)), SHA256: digest(data), StoragePath: "child/" + member.ID + ".md", Producer: "submission", CreatedAt: now}
		files = append(files, review.RetainedFile{Artifact: a, Bytes: data})
	}
	prepared, err := review.PrepareChild(frozen, cp, artifact.Name, now.Add(time.Hour), files)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := store.MaterializeReviewChild(ctx, frozen, cp, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return store, frozen, receipt, path
}

func TestReviewRuntimeProductionPlannerAutomaticWithoutCapacity(t *testing.T) {
	for _, boundary := range []string{"planner", "cycle"} {
		for _, healthy := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/healthy=%v", boundary, healthy), func(t *testing.T) {
				ctx := context.Background()
				store, frozen, receipt, path := reviewRuntimeProductionFixture(t)
				records, err := store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var parent domain.Attempt
				for _, a := range records.Attempts {
					if a.ID == frozen.Parent.AttemptID {
						parent = a
					}
				}
				parent.Revision++ // a valid advance keeps ORIGINAL issued authority usable.
				if !healthy {
					parent.Progress = domain.ProgressFailed
					parent.Control = domain.ControlStopped
				}
				if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{parent}}); err != nil {
					t.Fatal(err)
				}
				invoke := func() {
					t.Helper()
					if boundary == "planner" {
						planner := coordinatorPlanner{store: store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: 1, maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: time.Hour, deadlineRiskWindow: time.Hour, checkpointMargin: time.Minute}
						if _, err = planner.Tick(ctx, backlog.QuotaBridgeReport{ChecksDisabled: true}); err != nil {
							t.Fatal(err)
						}
					} else {
						var quotaCalls, scheduleCalls, planningCalls, adminCalls, workerCalls int
						var workerReports []backlog.QuotaBridgeReport
						cycle := coordinatorBoundaryCycle{
							projection: store,
							quota:      failingCoordinatorQuotaTicker{calls: &quotaCalls},
							schedules:  recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
							planning:   recordingCoordinatorPlanningTicker{calls: &planningCalls},
							admin:      recordingCoordinatorAdminExecutor{calls: &adminCalls},
							workers:    recordingCoordinatorWorkerTicker{calls: &workerCalls, reports: &workerReports},
							logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
						}
						cycle.TickWithWorkers(ctx)
					}
				}
				invoke()
				got, err := store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, a := range got.Attempts {
					if a.WorkflowRunID == receipt.Graph.Run.ID && (a.Progress != domain.ProgressCancelled) != healthy {
						t.Fatalf("production automatic healthy=%v child=%+v", healthy, a)
					}
					if a.ID == parent.ID && !reflect.DeepEqual(a, parent) {
						t.Fatal("runtime changed parent history")
					}
				}
				if err = store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = sqlitetest.OpenMigrated(path)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				invoke()
				invoke()
				again, err := store.LoadCoordinatorRecords(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, a := range again.Attempts {
					if a.WorkflowRunID == receipt.Graph.Run.ID && !healthy && a.Progress != domain.ProgressCancelled {
						t.Fatal("reopen resurrected child")
					}
				}
			})
		}
	}
}

func TestReviewRuntimeProductionCorruptionStopsBoundary(t *testing.T) {
	ctx := context.Background()
	store, _, receipt, path := reviewRuntimeProductionFixture(t)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err = raw.Exec("UPDATE coordinator_attempts SET workflow_run_id='foreign' WHERE id=?", receipt.Graph.Attempts[0].ID); err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	var quotaCalls, scheduleCalls, planningCalls, adminCalls, workerCalls int
	var workerReports []backlog.QuotaBridgeReport
	cycle := coordinatorBoundaryCycle{projection: store, quota: failingCoordinatorQuotaTicker{calls: &quotaCalls}, schedules: recordingCoordinatorScheduleTicker{calls: &scheduleCalls}, planning: recordingCoordinatorPlanningTicker{calls: &planningCalls}, admin: recordingCoordinatorAdminExecutor{calls: &adminCalls}, workers: recordingCoordinatorWorkerTicker{calls: &workerCalls, reports: &workerReports}, logger: slog.New(slog.NewTextHandler(&log, nil))}
	cycle.TickWithWorkers(ctx)
	if quotaCalls != 0 || scheduleCalls != 0 || planningCalls != 0 || adminCalls != 0 || workerCalls != 0 || !strings.Contains(log.String(), "boundary stopped") || !strings.Contains(log.String(), receipt.Checkpoint.Key()) {
		t.Fatalf("damaged review continued effects or lost actionable context: %s", log.String())
	}
	after, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("corrupt runtime pass mutated durable records")
	}
	c := backlog.FleetCoordinator{Store: store}
	if _, err = c.PlanAndCommit(ctx, backlog.PlanInput{}); err == nil || !strings.Contains(err.Error(), "before planning") {
		t.Fatalf("known damaged child admitted: %v", err)
	}
}

type reviewRuntimeFailingPass struct{ *sqlite.Store }

func (s reviewRuntimeFailingPass) ReconcileMaterializedReviewChildren(context.Context) error {
	return fmt.Errorf("fixture damaged materialized authority")
}
func TestReviewRuntimeAutomaticFailureStopsNewEffects(t *testing.T) {
	ctx := context.Background()
	store, _, _, _ := reviewRuntimeProductionFixture(t)
	c := backlog.FleetCoordinator{Store: reviewRuntimeFailingPass{store}}
	if _, err := c.PlanAndCommit(ctx, backlog.PlanInput{}); err == nil || !strings.Contains(err.Error(), "before planning") {
		t.Fatalf("failed pass admitted planning: %v", err)
	}
	var log bytes.Buffer
	planningCalls, workerCalls := 0, 0
	planning := &recordingCoordinatorPlanningTicker{calls: &planningCalls}
	workers := &recordingCoordinatorWorkerTicker{calls: &workerCalls}
	cycle := coordinatorBoundaryCycle{projection: reviewRuntimeFailingPass{store}, planning: planning, workers: workers, logger: slog.New(slog.NewTextHandler(&log, nil))}
	cycle.TickWithWorkers(ctx)
	if planningCalls != 0 || workerCalls != 0 || !strings.Contains(log.String(), "boundary stopped") {
		t.Fatalf("failed pass continued: planning %+v workers %+v log %s", planning, workers, log.String())
	}
}
