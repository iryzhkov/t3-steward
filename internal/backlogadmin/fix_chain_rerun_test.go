package backlogadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Submit the generated workflow so the rerun tests use its actual dependencies,
// prompts, commit declarations and implicit gate input.
func generatedFixRerunFixture(t *testing.T) (*Service, *sqlite.Store, string) {
	t.Helper()
	ctx := context.Background()
	c := newCommitCampaign(t)
	t.Cleanup(func() {
		// Submission seals the retained workflow directories.
		if err := filepath.Walk(c.artifacts.Root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		}); err != nil {
			t.Error(err)
		}
	})
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	source := campaign.FixSource{RunID: "run", ReviewedCommit: c.provenance.Commit}
	for i := range records.Workflows {
		records.Workflows[i].Class = domain.TaskClassSurplus
		records.Workflows[i].Project = "project"
		records.Workflows[i].Environment = domain.ExecutionEnvironment{Type: "git", Scope: "task", Ref: c.provenance.Commit}
		source.Workflow = records.Workflows[i]
	}
	for i := range records.Tasks {
		task := &records.Tasks[i]
		if task.Name == "implement" {
			source.Producer = *task
		} else {
			task.Outputs = []domain.ArtifactDeclaration{{Name: "review.md"}}
			records.Artifacts = append(records.Artifacts, rerunOutput(t, c.artifacts.Root, task.ID, "review.md", commitCampaignTime))
			source.Review = *task
		}
	}
	for i := range records.Attempts {
		records.Attempts[i].Progress = domain.ProgressSucceeded
	}
	if err = c.store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	unit, err := campaign.GenerateFixChain(source, campaign.FixOptions{
		Name: "fix-rerun", Lineage: campaign.FixLineage{RoundLimit: 4, RoundsDeclared: 2},
		Gate:  &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Minute},
		Brief: []campaign.CompiledFile{{Path: "inputs/fix/brief/prompt.md", Content: []byte("original brief")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, file := range unit.Files {
		path := filepath.Join(dir, filepath.FromSlash(file.Path))
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, file.Content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	submit := backlog.SubmissionService{Store: c.store, StorageRoot: c.artifacts.Root, MaxFiles: campaign.DefaultLimits.MaxFiles, MaxBytes: campaign.DefaultLimits.MaxBytes}
	submitted, err := submit.SubmitDirectory(ctx, backlog.DirectorySubmission{IdempotencyKey: "generated-fix", BundleDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	runID := submitted.Record.RunID
	records, err = c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range records.WorkflowRuns {
		run := &records.WorkflowRuns[i]
		if run.ID == runID {
			run.Progress = domain.ProgressFailed
			completed := commitCampaignTime.Add(time.Hour)
			run.CompletedAt = &completed
		}
	}
	for _, task := range records.Tasks {
		if task.RunID != runID {
			continue
		}
		attemptID := ""
		for i := range records.Attempts {
			attempt := &records.Attempts[i]
			if attempt.WorkflowRunID != runID || attempt.TaskID != task.ID {
				continue
			}
			attempt.Progress, attempt.Control = domain.ProgressSucceeded, domain.ControlStopped
			if task.Name == "review3" {
				attempt.Progress = domain.ProgressFailed
			}
			attemptID = attempt.ID
		}
		if attemptID == "" {
			t.Fatal("missing generated task attempt")
		}
		// fix2 failed once before succeeding. Both attempts retain a commit and log.
		attempts := []string{attemptID}
		if task.Gate != nil {
			failed := domain.Attempt{ID: "failed-fix2", WorkflowRunID: runID, TaskID: task.ID, Number: 1, Revision: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, UpdatedAt: commitCampaignTime}
			records.Attempts = append(records.Attempts, failed)
			for i := range records.Attempts {
				if records.Attempts[i].ID == attemptID {
					records.Attempts[i].Number = 2
				}
			}
			attempts = []string{failed.ID, attemptID}
		}
		for _, id := range attempts {
			outputs := slices.Clone(task.Outputs)
			if task.Gate != nil {
				outputs = append(outputs, domain.ArtifactDeclaration{Name: "gate"}, domain.ArtifactDeclaration{Name: "gate/log.txt"})
			}
			for index, output := range outputs {
				payload := task.Name + "/" + id + "/" + output.Name
				if output.Commit != nil {
					provenance := c.provenance
					provenance.WorkflowRunID, provenance.TaskID, provenance.Name = runID, task.ID, output.Name
					if id == "failed-fix2" {
						provenance.Commit = strings.Repeat("f", 40)
					}
					raw, marshalErr := backlog.MarshalCommitProvenance(provenance)
					if marshalErr != nil {
						t.Fatal(marshalErr)
					}
					payload = string(raw)
				}
				artifact, prepareErr := backlog.PrepareGraphInput(c.artifacts.Root, fmt.Sprintf("%s-output-%d", id, index), runID, task.ID, payload, commitCampaignTime)
				if prepareErr != nil {
					t.Fatal(prepareErr)
				}
				artifact.Kind, artifact.Name, artifact.AttemptID = domain.ArtifactOutput, output.Name, id
				if output.Name == "gate" || output.Name == "gate/log.txt" {
					artifact.Kind = domain.ArtifactGate
				}
				records.Artifacts = append(records.Artifacts, artifact)
			}
		}
	}
	if err = c.store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	return c.service, c.store, runID
}

type fixRerunSnapshot struct {
	Run       domain.WorkflowRun
	Workflows []domain.Workflow
	Tasks     []domain.Task
	Attempts  []domain.Attempt
	Artifacts []domain.Artifact
}

func loadFixRerunSnapshot(ctx context.Context, store *sqlite.Store, runID string) (fixRerunSnapshot, error) {
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return fixRerunSnapshot{}, err
	}
	var snapshot fixRerunSnapshot
	for _, run := range records.WorkflowRuns {
		if run.ID == runID {
			snapshot.Run = run
			snapshot.Tasks = domain.TasksForRun(run, records.Tasks)
			for _, workflow := range records.Workflows {
				if workflow.ID == run.WorkflowID {
					snapshot.Workflows = append(snapshot.Workflows, workflow)
				}
			}
		}
	}
	for _, attempt := range records.Attempts {
		if attempt.WorkflowRunID == runID {
			snapshot.Attempts = append(snapshot.Attempts, attempt)
		}
	}
	for _, artifact := range records.Artifacts {
		if artifact.WorkflowRunID == runID {
			snapshot.Artifacts = append(snapshot.Artifacts, artifact)
		}
	}
	return snapshot, nil
}

func TestGeneratedFixChainPersistsRerunFromEveryNode(t *testing.T) {
	for index, from := range []string{"fix1", "review2", "fix2", "review3"} {
		t.Run(from, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			service, store, runID := generatedFixRerunFixture(t)
			before, err := loadFixRerunSnapshot(ctx, store, runID)
			if err != nil {
				t.Fatal(err)
			}
			request := rerunRequest("generated-rerun", from)
			request.RunID = runID
			result, err := service.AmendGraph(ctx, Principal{ID: "operator"}, request)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"fix1", "review2", "fix2", "review3"}[index:]
			var names []string
			for _, task := range result.Graph.Tasks {
				names = append(names, task.Name)
			}
			slices.Sort(names)
			slices.Sort(want)
			if !reflect.DeepEqual(names, want) {
				t.Fatalf("rerun tasks = %v, want %v", names, want)
			}
			persisted, err := loadFixRerunSnapshot(ctx, store, result.Run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if persisted.Run.Graph == nil || !reflect.DeepEqual(persisted.Run.Graph.Tasks, result.Graph.Tasks) {
				t.Fatal("rerun graph not persisted")
			}
			if len(persisted.Attempts) != len(want) {
				t.Fatalf("attempts = %d, want %d", len(persisted.Attempts), len(want))
			}
			for _, attempt := range persisted.Attempts {
				if attempt.Progress != domain.ProgressBlocked || attempt.Control != domain.ControlUnassigned {
					t.Fatalf("rerun attempt not ready for execution: %+v", attempt)
				}
			}
			for _, task := range result.Graph.Tasks {
				// Every local ancestor input must become a carried reference; inputs from
				// producers still in the subtree must remain ordinary dependency inputs.
				var original domain.Task
				for _, sourceTask := range before.Tasks {
					if sourceTask.Name == task.Name {
						original = sourceTask
					}
				}
				for producer, outputs := range original.DependencyInputs {
					for _, output := range outputs {
						if slices.Contains(want, producer) {
							if !slices.Contains(task.DependencyInputs[producer], output) {
								t.Fatalf("lost executing dependency %s/%s", producer, output)
							}
						} else {
							if !slices.ContainsFunc(task.CarriedInputs, func(input domain.CarriedInput) bool { return input.Producer == producer && input.Name == output }) {
								t.Fatalf("lost reused dependency %s/%s", producer, output)
							}
						}
					}
				}
				for _, need := range task.Needs {
					if !slices.Contains(want, need) {
						t.Fatalf("reused ancestor remains an edge: %s", need)
					}
				}
				for _, input := range task.CarriedInputs {
					if slices.Contains(want, input.Producer) {
						t.Fatalf("rerun subtree output carried: %+v", input)
					}
				}
			}
			if from == "review3" {
				review := result.Graph.Tasks[0]
				found := map[string]bool{}
				for _, carried := range review.CarriedInputs {
					if carried.Producer != "fix2" {
						continue
					}
					if carried.SourceRunID != runID || carried.SourceAttemptID == "" || carried.SourceAttemptID == "failed-fix2" {
						t.Fatalf("wrong provenance: %+v", carried)
					}
					original := domain.Artifact{}
					for _, artifact := range before.Artifacts {
						if artifact.ID == carried.SourceArtifactID {
							original = artifact
						}
					}
					artifact, reader, openErr := service.artifactOpen(ctx, carried.ArtifactID)
					if openErr != nil {
						t.Fatal(openErr)
					}
					raw, readErr := io.ReadAll(reader)
					closeErr := reader.Close()
					if readErr != nil || closeErr != nil {
						t.Fatalf("read carried artifact: %v / %v", readErr, closeErr)
					}
					if original.ID == "" || original.AttemptID != carried.SourceAttemptID || original.SHA256 != artifact.SHA256 || original.StoragePath != artifact.StoragePath {
						t.Fatalf("carried artifact differs from successful source: %+v", carried)
					}
					found[carried.Name] = true
					if carried.Name == "fix" {
						var provenance backlog.CommitProvenance
						if err = json.Unmarshal(raw, &provenance); err != nil {
							t.Fatal(err)
						}
						if provenance.Commit != before.Workflows[0].Environment.Ref {
							t.Fatalf("carried wrong commit: %s", provenance.Commit)
						}
					}
					if carried.Name == "gate/log.txt" && string(raw) != "fix2/"+carried.SourceAttemptID+"/gate/log.txt" {
						t.Fatalf("carried wrong gate log: %s", raw)
					}
				}
				for _, name := range []string{"fix", "artifacts/handoff.md", "artifacts/verification.log", "gate/log.txt"} {
					if !found[name] {
						t.Fatalf("missing carried fix2 output %s", name)
					}
				}
			}
			after, err := loadFixRerunSnapshot(ctx, store, runID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rerun changed source graph, attempts or artifacts")
			}
		})
	}
}
