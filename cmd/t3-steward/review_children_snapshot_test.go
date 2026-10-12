package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestCoordinatorTickSettledReviewChildrenPreserveRecordLoads(t *testing.T) {
	for _, workers := range []int{3, 7} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			store, loads, _ := openSnapshotCountingStore(t)
			ctx := context.Background()
			epoch, err := store.AcquireCoordinator(ctx, "settled-review-tick-test")
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			sequence := int64(0)
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			calls := 0
			sessions := &coordinatorWorkerSessions{
				exchange: func(ctx context.Context, id string, _ backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
					calls++
					transport := snapshotIdleTransport{snapshot: domain.WorkerSnapshot{
						WorkerID: id, WorkerEpoch: "epoch", CoordinatorEpoch: epoch,
						Sequence: sequence, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
						Inventory: domain.WorkerInventory{ID: id, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
					}}
					return (backlog.FleetCoordinator{Store: store, Now: func() time.Time { return now }}).ReconcileWorker(
						ctx, transport, snapshotIdleBuilder{}, backlog.WorkerAdmissionPolicy{}, nil, nil, time.Minute, time.Hour)
				},
			}
			for i := 0; i < workers; i++ {
				sessions.workerIDs = append(sessions.workerIDs, fmt.Sprintf("worker-%d", i))
			}
			cycle := coordinatorBoundaryCycle{
				projection: store, supervision: &coordinatorSupervision{store: backlog.CoordinatorSupervisionStore{Store: store}, logger: logger},
				quota: fairnessQuota{}, schedules: backlog.ScheduleTimer{Store: store, CatchUpMax: 1},
				planning: snapshotReadingPlanner{store: store}, admin: fairnessAdmin{},
				workers: sessions, logger: logger,
			}
			tick := func() (int64, int64) {
				t.Helper()
				sequence++
				now = now.Add(time.Second)
				calls = 0
				loads.Store(0)
				logs.Reset()
				cycle.TickWithWorkers(ctx)
				if calls != workers {
					t.Fatalf("worker calls=%d, want %d; logs=%s", calls, workers, &logs)
				}
				var loggedLoads int64
				completions := 0
				decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
				for {
					var entry struct {
						Level       string `json:"level"`
						Message     string `json:"msg"`
						RecordLoads int64  `json:"record_loads"`
					}
					err := decoder.Decode(&entry)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if entry.Level == "ERROR" {
						t.Fatalf("boundary failed: %s", &logs)
					}
					if entry.Message == "backlog-v2 coordinator tick completed" {
						completions++
						loggedLoads = entry.RecordLoads
					}
				}
				if completions != 1 {
					t.Fatalf("tick completion logs=%d, want 1; logs=%s", completions, &logs)
				}
				return loads.Load(), loggedLoads
			}

			baselineActual, baselineLogged := tick()
			want := int64(1 + workers)
			if baselineActual != want || baselineLogged != want {
				t.Fatalf("without checkpoints: actual loads=%d logged loads=%d, want %d", baselineActual, baselineLogged, want)
			}

			// Use separate parents so each real materialization stays within its
			// frozen routine policy's checkpoint limit.
			addSettledReviewSnapshotCheckpoints(t, store, now, 3)
			for i := 0; i < 2; i++ {
				actual, logged := tick()
				if actual != baselineActual || logged != baselineLogged {
					t.Fatalf("settled checkpoints tick %d: actual loads=%d logged loads=%d; without checkpoints actual=%d logged=%d",
						i+1, actual, logged, baselineActual, baselineLogged)
				}
			}
		})
	}
}

func addSettledReviewSnapshotCheckpoints(t *testing.T, store *sqlite.Store, now time.Time, count int) {
	t.Helper()
	ctx := context.Background()
	raw := []byte("settled review snapshot criteria")
	digest := func(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }
	requirements, err := review.NewRequirements(review.RequirementsSpec{
		Risk: "routine", CriteriaDigest: digest(raw), PolicyDigest: fmt.Sprintf("%064x", 1),
		RequiredReviewers: 2, MinProviderFamilies: 2,
		Members: []review.MemberRequirement{
			{ID: "one", Role: "independent", Route: "codex/sol", ProviderFamily: "openai", Tier: "executor", Required: true},
			{ID: "two", Role: "independent", Route: "other/model", ProviderFamily: "other", Tier: "executor", Required: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := pinnedinput.NewManifest([]pinnedinput.Entry{{Name: "inputs/criteria.md", Size: int64(len(raw)), SHA256: digest(raw)}})
	if err != nil {
		t.Fatal(err)
	}
	roundIDs := make(map[string]bool, count)
	for i := 0; i < count; i++ {
		prefix := fmt.Sprintf("settled-parent-%d", i)
		parent := domain.Attempt{
			ID: prefix, WorkflowRunID: prefix + "-run", TaskID: prefix + "-task", Number: 1, Revision: 1,
			Progress: domain.ProgressActive, Control: domain.ControlRunning, AssignmentID: prefix + "-assignment",
			ThreadID: prefix + "-thread", UpdatedAt: now,
		}
		assignment := domain.Assignment{
			ID: parent.AssignmentID, AttemptID: parent.ID, WorkerID: prefix + "-worker", WorkerEpoch: "parent-session",
			Epoch: 1, State: domain.AssignmentClaimed, Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "sol"},
			ThreadID: parent.ThreadID, Project: "repo", LeaseToken: prefix + "-lease", DispatchToken: prefix + "-token",
			LeaseExpiresAt: now.Add(time.Hour),
		}
		task := domain.Task{ID: parent.TaskID, WorkflowID: prefix + "-workflow", Name: prefix, Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{assignment.Route}}
		run, err := domain.BindRunSink(domain.WorkflowRun{
			ID: parent.WorkflowRunID, WorkflowID: task.WorkflowID, Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now,
		}, []domain.Task{task})
		if err != nil {
			t.Fatal(err)
		}
		workflow := domain.Workflow{
			ID: task.WorkflowID, Name: prefix, Version: 1, Project: "repo",
			Environment: domain.ExecutionEnvironment{Type: "repository", Scope: "repo"},
			Class:       domain.TaskClassRequired, TaskIDs: []string{task.ID}, CreatedAt: now,
		}
		if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
			Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task},
			Attempts: []domain.Attempt{parent}, Assignments: []domain.Assignment{assignment},
		}); err != nil {
			t.Fatal(err)
		}
		frozen, err := review.NewFrozenAuthority(review.ParentBinding{
			RunID: parent.WorkflowRunID, TaskID: parent.TaskID, AttemptID: parent.ID, ThreadID: parent.ThreadID,
			AssignmentID: assignment.ID, AssignmentEpoch: 1, IssuedRevision: parent.Revision,
			Repository: "repo", BaseCommit: fmt.Sprintf("%040x", 2), ExecutorRoute: "codex/sol",
		}, requirements)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.FreezeReviewAuthority(ctx, frozen); err != nil {
			t.Fatal(err)
		}
		cp, err := store.AllocateReviewCheckpoint(ctx, frozen, review.Checkpoint{
			ID: "snapshot", HeadCommit: fmt.Sprintf("%040x", 3), InputDigest: manifest.Digest,
		})
		if err != nil {
			t.Fatal(err)
		}
		artifact := domain.Artifact{
			ID: review.ChildInputID(cp, "inputs/criteria.md"), WorkflowRunID: cp.RoundID, Kind: domain.ArtifactInput,
			Name: "inputs/criteria.md", Size: int64(len(raw)), SHA256: digest(raw),
			StoragePath: prefix + "/criteria.md", Producer: "submission", CreatedAt: now,
		}
		files := []review.RetainedFile{{Artifact: artifact, Bytes: raw}}
		for _, member := range frozen.Requirements.Members {
			prompt, err := review.ChildPrompt(frozen, cp, member, manifest, artifact.Name)
			if err != nil {
				t.Fatal(err)
			}
			data := []byte(prompt)
			files = append(files, review.RetainedFile{
				Artifact: domain.Artifact{
					ID: review.ChildPromptID(cp, member.ID), WorkflowRunID: cp.RoundID, TaskID: cp.MemberTaskID(member.ID),
					Kind: domain.ArtifactInput, Name: review.ChildPromptName(member.ID), Size: int64(len(data)), SHA256: digest(data),
					StoragePath: prefix + "/" + member.ID + ".md", Producer: "submission", CreatedAt: now,
				},
				Bytes: data,
			})
		}
		prepared, err := review.PrepareChild(frozen, cp, artifact.Name, now.Add(time.Hour), files)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := store.MaterializeReviewChild(ctx, frozen, cp, prepared)
		if err != nil {
			t.Fatal(err)
		}
		roundIDs[receipt.Graph.Run.ID] = true
		parent.Revision++
		parent.Progress = domain.ProgressFailed
		parent.Control = domain.ControlStopped
		run.Revision++
		run.Progress = domain.ProgressFailed
		assignment.State = domain.AssignmentCompleted
		if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
			Attempts: []domain.Attempt{parent}, WorkflowRuns: []domain.WorkflowRun{run}, Assignments: []domain.Assignment{assignment},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(roundIDs) != count {
		t.Fatalf("materialized rounds=%d, want %d", len(roundIDs), count)
	}
	if err := store.ReconcileMaterializedReviewChildren(ctx); err != nil {
		t.Fatal(err)
	}
	// Finish ordinary sink projection before measuring read-only settled history.
	// Otherwise the first measured tick legitimately writes the newly ended runs.
	if _, err := backlog.ProjectWorkflowRunsWithRetries(ctx, store, now, 0); err != nil {
		t.Fatal(err)
	}
	for roundID := range roundIDs {
		round, err := store.GetReviewRound(ctx, roundID)
		if err != nil {
			t.Fatal(err)
		}
		if !round.Terminal() {
			t.Fatalf("round %s is not settled: %+v", roundID, round)
		}
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	childCounts := make(map[string]int, count)
	for _, attempt := range records.Attempts {
		if roundIDs[attempt.WorkflowRunID] {
			childCounts[attempt.WorkflowRunID]++
			if !attempt.Progress.Terminal() {
				t.Fatalf("child attempt is not terminal: %+v", attempt)
			}
		}
	}
	for roundID := range roundIDs {
		if childCounts[roundID] != 2 {
			t.Fatalf("round %s child attempts=%d, want 2", roundID, childCounts[roundID])
		}
	}
}
