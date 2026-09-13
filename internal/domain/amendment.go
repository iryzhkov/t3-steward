package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

type GraphAmendment struct {
	ID               string             `json:"id"`
	RunID            string             `json:"runId"`
	ExpectedRevision int64              `json:"expectedRevision"`
	Reason           string             `json:"reason"`
	Operation        string             `json:"operation"`
	TaskID           string             `json:"taskId,omitempty"`
	Task             *Task              `json:"task,omitempty"`
	Source           string             `json:"source,omitempty"`
	Model            *string            `json:"model,omitempty"`
	Provider         *string            `json:"provider,omitempty"`
	Options          *map[string]string `json:"options,omitempty"`
	Timeout          *time.Duration     `json:"timeout,omitempty"`
	Prompt           string             `json:"prompt,omitempty"`
	Verification     *[]string          `json:"verification,omitempty"`
}
type GraphAmendmentResult struct {
	Run    WorkflowRun     `json:"run"`
	Graph  GraphDefinition `json:"graph"`
	Replay bool            `json:"replay"`
}

var graphName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func ValidateGraphAmendment(r GraphAmendment) error {
	if !graphName.MatchString(r.ID) || r.RunID == "" || r.ExpectedRevision < 1 ||
		strings.TrimSpace(r.Reason) == "" || len(r.Reason) > 4096 {
		return errors.New("amendment requires bounded request ID, run, expected graph revision and reason")
	}
	switch r.Operation {
	case "task-add":
		if r.Task == nil || !graphName.MatchString(r.Task.Name) || r.Task.Name == SinkTaskName ||
			r.Prompt == "" || len(r.Prompt) > 256<<10 {
			return errors.New("task add requires a named task and prompt of at most 256 KiB")
		}
		if r.TaskID != "" || r.Source != "" || r.Model != nil || r.Provider != nil || r.Options != nil || r.Timeout != nil || r.Verification != nil {
			return errors.New("mixed task add fields")
		}
	case "task-set":
		if r.TaskID == "" || r.Task != nil || r.Source != "" || r.Prompt != "" ||
			(r.Model == nil && r.Provider == nil && r.Options == nil && r.Timeout == nil && r.Verification == nil) {
			return errors.New("task set requires target and model/provider/options/timeout/verification")
		}
	case "edge-add", "edge-remove":
		if r.TaskID == "" || r.Source == "" || r.Task != nil || r.Prompt != "" || r.Model != nil || r.Provider != nil || r.Options != nil || r.Timeout != nil || r.Verification != nil {
			return errors.New("edge amendment requires source and target")
		}
	case "clone":
		if r.TaskID != "" || r.Task != nil || r.Source != "" || r.Prompt != "" || r.Model != nil || r.Provider != nil || r.Options != nil || r.Timeout != nil || r.Verification != nil {
			return errors.New("mixed clone fields")
		}
	default:
		return errors.New("unknown graph amendment operation")
	}
	if r.Operation == "task-add" {
		return validateAmendmentVerification(r.Task.Verification)
	}
	if r.Verification != nil {
		return validateAmendmentVerification(*r.Verification)
	}
	return nil
}

func validateAmendmentVerification(commands []string) error {
	if len(commands) == 0 {
		return errors.New("task amendment requires at least one verification command")
	}
	for _, command := range commands {
		if strings.TrimSpace(command) == "" || strings.ContainsRune(command, 0) {
			return errors.New("task amendment has invalid verification command")
		}
	}
	return nil
}

// AmendTasks produces a detached candidate. Authority, immutable artifact custody,
// assignment history and the combined cross-run graph are checked at commit.
func AmendTasks(r GraphAmendment, run WorkflowRun, templates []Task, newID, promptID string) ([]Task, error) {
	raw, err := json.Marshal(TasksForRun(run, templates))
	if err != nil {
		return nil, err
	}
	var tasks []Task
	if err = json.Unmarshal(raw, &tasks); err != nil {
		return nil, err
	}
	if r.Operation == "task-add" {
		var task Task
		raw, err := json.Marshal(r.Task)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &task); err != nil {
			return nil, err
		}
		task.ID, task.WorkflowID, task.RunID, task.PromptArtifactID = newID, run.WorkflowID, run.ID, promptID
		task.DefinitionRevision = run.GraphRevision + 1
		tasks = append(tasks, task)
	} else {
		index := -1
		for i, t := range tasks {
			if t.ID == r.TaskID || t.Name == r.TaskID {
				index = i
				break
			}
		}
		if index < 0 {
			return nil, errors.New("ordinary target task not found")
		}
		task := &tasks[index]
		switch r.Operation {
		case "task-set":
			if len(task.Routes) != 1 && (r.Model != nil || r.Provider != nil || r.Options != nil) {
				return nil, errors.New("route edits require exactly one route")
			}
			if r.Model != nil {
				task.Routes[0].Model = *r.Model
			}
			if r.Provider != nil {
				task.Routes[0].ProviderInstanceID = *r.Provider
				task.Routes[0].QuotaPoolID = ""
			}
			if r.Options != nil {
				task.Routes[0].Options = *r.Options
			}
			if r.Timeout != nil {
				task.Timeout = *r.Timeout
			}
			if r.Verification != nil {
				task.Verification = slices.Clone(*r.Verification)
			}
		case "edge-add", "edge-remove":
			if strings.Contains(r.Source, "/") {
				ref, err := ParseNodeRef(r.Source)
				if err != nil {
					return nil, err
				}
				i := slices.Index(task.ExternalNeeds, ref)
				if r.Operation == "edge-add" {
					if i >= 0 {
						return nil, errors.New("edge already exists")
					}
					task.ExternalNeeds = append(task.ExternalNeeds, ref)
				} else {
					if i < 0 {
						return nil, errors.New("edge does not exist")
					}
					task.ExternalNeeds = slices.Delete(task.ExternalNeeds, i, i+1)
				}
			} else {
				source := ""
				for _, t := range tasks {
					if t.ID == r.Source || t.Name == r.Source {
						source = t.Name
						break
					}
				}
				if source == "" {
					return nil, errors.New("source task not found")
				}
				i := slices.Index(task.Needs, source)
				if r.Operation == "edge-add" {
					if i >= 0 {
						return nil, errors.New("edge already exists")
					}
					task.Needs = append(task.Needs, source)
				} else {
					if i < 0 {
						return nil, errors.New("edge does not exist")
					}
					if len(task.DependencyInputs[source]) > 0 {
						return nil, errors.New("edge supplies required artifacts")
					}
					task.Needs = slices.Delete(task.Needs, i, i+1)
				}
			}
		}
		task.DefinitionRevision = run.GraphRevision + 1
	}
	if err := ValidateGraphTasks(run, tasks); err != nil {
		return nil, err
	}
	slices.SortFunc(tasks, func(a, b Task) int { return strings.Compare(a.ID, b.ID) })
	return tasks, nil
}

func ValidateGraphTasks(run WorkflowRun, tasks []Task) error {
	names := map[string]Task{}
	ids := map[string]bool{}
	if len(tasks) > 1024 {
		return errors.New("graph exceeds 1024 tasks")
	}
	for _, t := range tasks {
		if t.ID == "" || t.WorkflowID != run.WorkflowID || !graphName.MatchString(t.Name) || t.Name == SinkTaskName || ids[t.ID] {
			return errors.New("invalid or duplicate task identity")
		}
		if _, ok := names[t.Name]; ok {
			return errors.New("duplicate task name")
		}
		names[t.Name] = t
		ids[t.ID] = true
		if t.PromptArtifactID == "" || t.MaxTurns < 1 || t.Timeout < 0 || t.Timeout > 7*24*time.Hour || (t.Class != TaskClassRequired && t.Class != TaskClassSurplus) || len(t.Routes) == 0 {
			return fmt.Errorf("task %s has invalid execution limits, class, prompt or routes", t.Name)
		}
		for _, route := range t.Routes {
			if strings.TrimSpace(route.Model) == "" || strings.TrimSpace(route.ProviderInstanceID) == "" {
				return errors.New("route requires provider and model")
			}
		}
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visiting[name] {
			return errors.New("local dependency cycle")
		}
		if done[name] {
			return nil
		}
		t, ok := names[name]
		if !ok {
			return fmt.Errorf("missing dependency %s", name)
		}
		visiting[name] = true
		seen := map[string]bool{}
		for _, need := range t.Needs {
			if seen[need] {
				return errors.New("duplicate dependency")
			}
			seen[need] = true
			if err := visit(need); err != nil {
				return err
			}
		}
		for source, outputs := range t.DependencyInputs {
			if !seen[source] {
				return errors.New("artifact input requires dependency edge")
			}
			for _, name := range outputs {
				found := false
				for _, out := range names[source].Outputs {
					if out.Name == name {
						found = true
					}
				}
				if !found {
					return errors.New("required dependency output is undeclared")
				}
			}
		}
		delete(visiting, name)
		done[name] = true
		return nil
	}
	for name := range names {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}
