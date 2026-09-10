package backlog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// LegacyWorkflow is the coordinator representation of one Markdown backlog
// task. Source remains available to the host-local runner while the v2
// scheduler is introduced incrementally.
type LegacyWorkflow struct {
	Source   Task
	Workflow domain.Workflow
	Run      domain.WorkflowRun
	Task     domain.Task
	Attempt  domain.Attempt
}

// AdaptLegacyTask packages a parsed Markdown task as a one-task workflow.
// The content digest makes edits produce a new immutable definition while an
// unchanged task keeps stable coordinator identifiers across scans.
func AdaptLegacyTask(source Task, raw []byte) LegacyWorkflow {
	digest := sha256.Sum256(raw)
	baseID := fmt.Sprintf("legacy:%s:%x", source.ID, digest[:8])
	workflowID := baseID + ":workflow"
	runID := baseID + ":run"
	taskID := baseID + ":task"
	attemptID := baseID + ":attempt:1"
	createdAt := source.ModTime.UTC()
	if createdAt.IsZero() {
		createdAt = time.Unix(0, 0).UTC()
	}

	class := domain.TaskClassSurplus
	if !source.Gated() {
		class = domain.TaskClassRequired
	}

	progress := domain.ProgressReady
	control := domain.ControlUnassigned
	runProgress := domain.ProgressQueued
	var completedAt *time.Time
	if !source.IsEnabled() {
		progress = domain.ProgressSkipped
		control = domain.ControlStopped
		runProgress = domain.ProgressSkipped
		completedAt = &createdAt
	}

	var hosts []string
	if source.Host != "" {
		hosts = []string{source.Host}
	}
	var routes []domain.ProviderRoute
	if source.Host != "" || source.Instance != "" || source.Model != "" || len(source.Options) != 0 {
		routes = []domain.ProviderRoute{{
			WorkerID:           source.Host,
			ProviderInstanceID: source.Instance,
			Model:              source.Model,
			Options:            source.Options,
		}}
	}

	return LegacyWorkflow{
		Source: source,
		Workflow: domain.Workflow{
			ID:        workflowID,
			Version:   1,
			Name:      source.Title,
			Project:   source.Project,
			Class:     class,
			TaskIDs:   []string{taskID},
			CreatedAt: createdAt,
		},
		Run: domain.WorkflowRun{
			ID:          runID,
			WorkflowID:  workflowID,
			Progress:    runProgress,
			Revision:    1,
			CreatedAt:   createdAt,
			UpdatedAt:   createdAt,
			CompletedAt: completedAt,
		},
		Task: domain.Task{
			ID:               taskID,
			WorkflowID:       workflowID,
			Name:             "task",
			Class:            class,
			PromptArtifactID: baseID + ":prompt",
			Placement:        domain.Placement{Hosts: hosts},
			Routes:           routes,
			Importance:       source.Importance,
			Difficulty:       source.Difficulty,
			EstimatedCost:    source.EstimatedCost,
			MaxTurns:         source.MaxTurns,
			NotBefore:        source.NotBefore,
			Deadline:         source.Deadline,
		},
		Attempt: domain.Attempt{
			ID:            attemptID,
			WorkflowRunID: runID,
			TaskID:        taskID,
			Number:        1,
			Progress:      progress,
			Control:       control,
			UpdatedAt:     createdAt,
			CompletedAt:   completedAt,
		},
	}
}

// ParseWorkflowFile reads one Markdown task and packages it for the
// coordinator without changing the legacy runner representation.
func ParseWorkflowFile(path string) (LegacyWorkflow, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return LegacyWorkflow{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return LegacyWorkflow{}, err
	}
	source, err := parseFile(path, raw, info.ModTime())
	if err != nil {
		return LegacyWorkflow{}, err
	}
	return AdaptLegacyTask(source, raw), nil
}

// LoadLegacyWorkflows parses every visible Markdown task in a directory.
func LoadLegacyWorkflows(dir string) ([]LegacyWorkflow, []error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, []error{err}
	}

	var workflows []LegacyWorkflow
	var errs []error
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		workflow, err := ParseWorkflowFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		workflows = append(workflows, workflow)
	}
	return workflows, errs
}
