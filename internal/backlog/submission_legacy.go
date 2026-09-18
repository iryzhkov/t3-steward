package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

type SingleTaskSubmission struct {
	IdempotencyKey string
	Task           Task
}

// SubmitSingleTask preserves the legacy t3-backlog/t3-job fields while routing
// the request through the version 2 immutable submission boundary.
func (s *SubmissionService) SubmitSingleTask(ctx context.Context, request SingleTaskSubmission) (SubmissionResult, error) {
	task := request.Task
	if task.Project == "" {
		return SubmissionResult{}, errors.New("single-task submission project is required")
	}
	if task.Prompt == "" {
		return SubmissionResult{}, errors.New("single-task submission prompt is required")
	}
	if task.Instance == "" || task.Model == "" {
		// The same rule the version 2 intake applies: the coordinator never
		// chooses a route, and a task without one is not dispatchable. It is a
		// conflict of the content so that the legacy source quarantines the file
		// once instead of reporting it on every cycle, and it is permanent because
		// only different content can fix it.
		return SubmissionResult{}, fmt.Errorf("no-route: single-task submission names no provider route (instance %q, model %q); "+
			"set both instance and model to a route an eligible worker advertises (t3-steward models): %w: %w",
			task.Instance, task.Model, ErrPermanentIntake, domain.ErrSubmissionConflict)
	}
	class := domain.TaskClassSurplus
	if !task.Gated() {
		class = domain.TaskClassRequired
	}
	var hosts []string
	if task.Host != "" {
		hosts = []string{task.Host}
	}
	var routes []ManifestRoute
	if task.Host != "" || task.Instance != "" || task.Model != "" || len(task.Options) != 0 {
		routes = []ManifestRoute{{
			Host: task.Host, Instance: task.Instance, Model: task.Model,
			Options: task.Options,
		}}
	}
	manifest := Manifest{
		Version: ManifestVersion,
		Name:    "legacy-single-task",
		Class:   class,
		Environment: ManifestEnvironment{
			Project: task.Project, Type: "git", Scope: EnvironmentScopeTask,
		},
		Placement: ManifestPlacement{Hosts: hosts},
		Routes:    routes,
		Tasks: map[string]ManifestTask{
			"task": {
				PromptFile: "prompts/task.md", Importance: task.Importance,
				Difficulty: task.Difficulty, EstimatedCost: task.EstimatedCost,
				MaxTurns: task.MaxTurns, NotBefore: task.NotBefore,
				Deadline: task.Deadline, Verify: []string{"true"},
			},
		},
	}
	raw, err := yaml.Marshal(manifest)
	if err != nil {
		return SubmissionResult{}, fmt.Errorf("encode single-task workflow: %w", err)
	}
	root, err := os.MkdirTemp("", "t3-steward-single-task-")
	if err != nil {
		return SubmissionResult{}, fmt.Errorf("create single-task bundle: %w", err)
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(filepath.Join(root, "prompts"), 0o700); err != nil {
		return SubmissionResult{}, err
	}
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), raw, 0o600); err != nil {
		return SubmissionResult{}, err
	}
	if err := os.WriteFile(filepath.Join(root, "prompts", "task.md"), []byte(task.Prompt), 0o600); err != nil {
		return SubmissionResult{}, err
	}
	return s.SubmitDirectory(ctx, DirectorySubmission{
		IdempotencyKey: request.IdempotencyKey,
		BundleDir:      root,
	})
}
