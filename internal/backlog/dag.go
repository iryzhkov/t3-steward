package backlog

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// DAGState is the complete mutable progress projection for one workflow run.
type DAGState struct {
	External map[string]domain.NodeObservation
	Run      domain.WorkflowRun
	Tasks    []domain.Task
	Attempts []domain.Attempt
}

// CompletionResult is the coordinator's reconciled view of an attempt's final turn.
// A task succeeds only when the agent explicitly reported success and verification passed.
type CompletionResult struct {
	ExplicitSuccess    bool
	VerificationPassed bool
	Failure            string
}

// DAGExecution applies deterministic workflow progress transitions without dispatching work
// or performing persistence. Callers persist Snapshot atomically after a successful mutation.
type DAGExecution struct {
	state      DAGState
	taskByID   map[string]int
	taskByName map[string]int
	attempts   map[string][]int
}

// NewDAGExecution validates and reconciles a workflow run projection.
func NewDAGExecution(state DAGState) (*DAGExecution, error) {
	execution := &DAGExecution{state: cloneDAGState(state)}
	if err := execution.indexAndValidate(); err != nil {
		return nil, err
	}
	execution.refresh(time.Time{}, false)
	return execution, nil
}

// Snapshot returns a detached, deterministically ordered copy of the execution state.
func (e *DAGExecution) Snapshot() DAGState {
	state := cloneDAGState(e.state)
	sort.Slice(state.Tasks, func(i, j int) bool {
		if state.Tasks[i].Name == state.Tasks[j].Name {
			return state.Tasks[i].ID < state.Tasks[j].ID
		}
		return state.Tasks[i].Name < state.Tasks[j].Name
	})
	sort.Slice(state.Attempts, func(i, j int) bool {
		left, right := state.Attempts[i], state.Attempts[j]
		if left.TaskID == right.TaskID {
			if left.Number == right.Number {
				return left.ID < right.ID
			}
			return left.Number < right.Number
		}
		return left.TaskID < right.TaskID
	})
	return state
}

// StartAttempt moves a dependency-ready attempt into active execution.
func (e *DAGExecution) StartAttempt(attemptID string, now time.Time) error {
	index, err := e.attemptIndex(attemptID)
	if err != nil {
		return err
	}
	attempt := &e.state.Attempts[index]
	if attempt.Progress != domain.ProgressReady {
		return fmt.Errorf("start attempt %q: progress is %q, want %q", attemptID, attempt.Progress, domain.ProgressReady)
	}
	attempt.Progress = domain.ProgressActive
	attempt.Control = domain.ControlRunning
	attempt.StartedAt = timePointer(now)
	attempt.UpdatedAt = now
	e.refresh(now, true)
	return nil
}

// CompleteAttempt records the final result of an active attempt. Missing explicit
// success and failed verification are failures and never release dependencies.
func (e *DAGExecution) CompleteAttempt(attemptID string, result CompletionResult, now time.Time) error {
	index, err := e.attemptIndex(attemptID)
	if err != nil {
		return err
	}
	attempt := &e.state.Attempts[index]
	if attempt.Progress != domain.ProgressActive && attempt.Progress != domain.ProgressVerifying {
		return fmt.Errorf("complete attempt %q: progress is %q, want active or verifying", attemptID, attempt.Progress)
	}

	attempt.Control = domain.ControlStopped
	attempt.UpdatedAt = now
	attempt.CompletedAt = timePointer(now)
	switch {
	case !result.ExplicitSuccess:
		attempt.Progress = domain.ProgressFailed
		attempt.Failure = firstNonEmpty(result.Failure, "missing explicit success")
	case !result.VerificationPassed:
		attempt.Progress = domain.ProgressFailed
		attempt.Failure = firstNonEmpty(result.Failure, "verification failed")
	default:
		attempt.Progress = domain.ProgressSucceeded
		attempt.Failure = ""
	}
	e.refresh(now, true)
	return nil
}

// CancelTask cancels a task's unfinished current attempt and every unfinished
// descendant. Successful tasks and their artifacts are preserved.
func (e *DAGExecution) CancelTask(taskID string, now time.Time) error {
	if e.state.Run.Sink != nil && e.state.Run.Sink.Progress.Terminal() {
		return fmt.Errorf("run sink is final; submit a new workflow")
	}
	taskIndex, ok := e.taskByID[taskID]
	if !ok {
		return fmt.Errorf("cancel task %q: unknown task", taskID)
	}
	names := e.descendantNames(e.state.Tasks[taskIndex].Name)
	changed := false
	for _, name := range names {
		task := e.state.Tasks[e.taskByName[name]]
		attempt := &e.state.Attempts[e.currentAttemptIndex(task.ID)]
		if attempt.Progress == domain.ProgressSucceeded || attempt.Progress.Terminal() {
			continue
		}
		attempt.Progress = domain.ProgressCancelled
		attempt.Control = domain.ControlStopped
		attempt.UpdatedAt = now
		attempt.CompletedAt = timePointer(now)
		attempt.Failure = ""
		changed = true
	}
	if !changed {
		return fmt.Errorf("cancel task %q: task and descendants are already terminal", taskID)
	}
	e.refresh(now, true)
	return nil
}

// SkipTask marks one unfinished task as intentionally skipped without releasing
// dependent tasks.
func (e *DAGExecution) SkipTask(taskID string, now time.Time) error {
	if e.state.Run.Sink != nil && e.state.Run.Sink.Progress.Terminal() {
		return fmt.Errorf("run sink is final; submit a new workflow")
	}
	taskIndex, ok := e.taskByID[taskID]
	if !ok {
		return fmt.Errorf("skip task %q: unknown task", taskID)
	}
	attempt := &e.state.Attempts[e.currentAttemptIndex(e.state.Tasks[taskIndex].ID)]
	if attempt.Progress == domain.ProgressSucceeded || attempt.Progress == domain.ProgressSkipped {
		return fmt.Errorf("skip task %q: progress is %q", taskID, attempt.Progress)
	}
	attempt.Progress = domain.ProgressSkipped
	attempt.Control = domain.ControlStopped
	attempt.UpdatedAt = now
	attempt.CompletedAt = timePointer(now)
	attempt.Failure = ""
	e.refresh(now, true)
	return nil
}

// RetryTask creates a new unassigned attempt for a failed or cancelled task.
// Successful dependency attempts are reused and are never rerun.
func (e *DAGExecution) RetryTask(taskID, attemptID string, now time.Time) error {
	if e.state.Run.Sink != nil && e.state.Run.Sink.Progress.Terminal() {
		return fmt.Errorf("run sink is final; submit a new workflow")
	}
	taskIndex, ok := e.taskByID[taskID]
	if !ok {
		return fmt.Errorf("retry task %q: unknown task", taskID)
	}
	if attemptID == "" {
		return fmt.Errorf("retry task %q: empty attempt ID", taskID)
	}
	if _, err := e.attemptIndex(attemptID); err == nil {
		return fmt.Errorf("retry task %q: duplicate attempt ID %q", taskID, attemptID)
	}
	current := e.state.Attempts[e.currentAttemptIndex(taskID)]
	if current.Progress != domain.ProgressFailed && current.Progress != domain.ProgressCancelled {
		return fmt.Errorf("retry task %q: progress is %q, want failed or cancelled", taskID, current.Progress)
	}
	attempt := domain.Attempt{
		ID:            attemptID,
		WorkflowRunID: e.state.Run.ID,
		TaskID:        e.state.Tasks[taskIndex].ID,
		Number:        current.Number + 1,
		Progress:      domain.ProgressBlocked,
		Control:       domain.ControlUnassigned,
		UpdatedAt:     now,
	}
	e.state.Attempts = append(e.state.Attempts, attempt)
	index := len(e.state.Attempts) - 1
	e.attempts[taskID] = append(e.attempts[taskID], index)
	e.refresh(now, true)
	return nil
}

func (e *DAGExecution) indexAndValidate() error {
	if e.state.Run.ID == "" || e.state.Run.WorkflowID == "" {
		return fmt.Errorf("workflow run ID and workflow ID are required")
	}
	e.taskByID = make(map[string]int, len(e.state.Tasks))
	e.taskByName = make(map[string]int, len(e.state.Tasks))
	for index, task := range e.state.Tasks {
		if task.ID == "" || task.Name == "" {
			return fmt.Errorf("task at index %d has an empty ID or name", index)
		}
		if task.WorkflowID != e.state.Run.WorkflowID {
			return fmt.Errorf("task %q belongs to workflow %q, want %q", task.Name, task.WorkflowID, e.state.Run.WorkflowID)
		}
		if _, exists := e.taskByID[task.ID]; exists {
			return fmt.Errorf("duplicate task ID %q", task.ID)
		}
		if _, exists := e.taskByName[task.Name]; exists {
			return fmt.Errorf("duplicate task name %q", task.Name)
		}
		e.taskByID[task.ID] = index
		e.taskByName[task.Name] = index
	}
	for _, task := range e.state.Tasks {
		seen := make(map[string]struct{}, len(task.Needs))
		for _, dependency := range task.Needs {
			if dependency == task.Name {
				return fmt.Errorf("task %q depends on itself", task.Name)
			}
			if _, exists := e.taskByName[dependency]; !exists {
				return fmt.Errorf("task %q needs missing task %q", task.Name, dependency)
			}
			if _, exists := seen[dependency]; exists {
				return fmt.Errorf("task %q repeats dependency %q", task.Name, dependency)
			}
			seen[dependency] = struct{}{}
		}
	}
	if err := e.validateAcyclic(); err != nil {
		return err
	}

	e.attempts = make(map[string][]int, len(e.state.Tasks))
	attemptIDs := make(map[string]struct{}, len(e.state.Attempts))
	numbers := make(map[string]map[int]struct{}, len(e.state.Tasks))
	for index, attempt := range e.state.Attempts {
		if attempt.ID == "" {
			return fmt.Errorf("attempt at index %d has an empty ID", index)
		}
		if _, exists := attemptIDs[attempt.ID]; exists {
			return fmt.Errorf("duplicate attempt ID %q", attempt.ID)
		}
		attemptIDs[attempt.ID] = struct{}{}
		if attempt.WorkflowRunID != e.state.Run.ID {
			return fmt.Errorf("attempt %q belongs to run %q, want %q", attempt.ID, attempt.WorkflowRunID, e.state.Run.ID)
		}
		if _, exists := e.taskByID[attempt.TaskID]; !exists {
			return fmt.Errorf("attempt %q refers to missing task %q", attempt.ID, attempt.TaskID)
		}
		if attempt.Number < 1 {
			return fmt.Errorf("attempt %q has invalid number %d", attempt.ID, attempt.Number)
		}
		if numbers[attempt.TaskID] == nil {
			numbers[attempt.TaskID] = make(map[int]struct{})
		}
		if _, exists := numbers[attempt.TaskID][attempt.Number]; exists {
			return fmt.Errorf("task %q has duplicate attempt number %d", attempt.TaskID, attempt.Number)
		}
		numbers[attempt.TaskID][attempt.Number] = struct{}{}
		e.attempts[attempt.TaskID] = append(e.attempts[attempt.TaskID], index)
	}
	for _, task := range e.state.Tasks {
		indexes := e.attempts[task.ID]
		if len(indexes) == 0 {
			return fmt.Errorf("task %q has no attempt", task.Name)
		}
		sort.Slice(indexes, func(i, j int) bool {
			return e.state.Attempts[indexes[i]].Number < e.state.Attempts[indexes[j]].Number
		})
	}
	return nil
}

func (e *DAGExecution) validateAcyclic() error {
	const (
		visiting = 1
		visited  = 2
	)
	state := make(map[string]int, len(e.state.Tasks))
	var visit func(string) error
	visit = func(name string) error {
		switch state[name] {
		case visiting:
			return fmt.Errorf("workflow graph contains a cycle at task %q", name)
		case visited:
			return nil
		}
		state[name] = visiting
		task := e.state.Tasks[e.taskByName[name]]
		dependencies := append([]string(nil), task.Needs...)
		sort.Strings(dependencies)
		for _, dependency := range dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[name] = visited
		return nil
	}
	names := make([]string, 0, len(e.taskByName))
	for name := range e.taskByName {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

func (e *DAGExecution) refresh(now time.Time, touch bool) {
	for _, task := range e.state.Tasks {
		if e.taskSucceeded(task.ID) {
			continue
		}
		attempt := &e.state.Attempts[e.currentAttemptIndex(task.ID)]
		if attempt.Progress != domain.ProgressBlocked && attempt.Progress != domain.ProgressReady {
			continue
		}
		blockers := make([]string, 0, len(task.Needs))
		for _, dependency := range task.Needs {
			dependencyTask := e.state.Tasks[e.taskByName[dependency]]
			if !e.taskSucceeded(dependencyTask.ID) {
				blockers = append(blockers, dependency)
			}
		}
		for _, ref := range task.ExternalNeeds {
			obs, ok := e.state.External[ref.String()]
			if !ok || obs.ExitCode != 0 {
				blockers = append(blockers, ref.String())
			}
		}
		sort.Strings(blockers)
		if len(blockers) == 0 {
			attempt.Progress = domain.ProgressReady
			attempt.Failure = ""
		} else {
			attempt.Progress = domain.ProgressBlocked
			attempt.Failure = "waiting for dependencies: " + strings.Join(blockers, ", ")
		}
	}

	progress := e.runProgress()
	if e.state.Run.Sink != nil {
		if e.state.Run.Sink.Progress.Terminal() {
			return
		}
		if progress.Terminal() {
			progress = domain.ProgressActive
		}
	}
	e.state.Run.Progress = progress
	if touch {
		e.state.Run.Revision++
		e.state.Run.UpdatedAt = now
	}
	if progress.Terminal() {
		if touch {
			e.state.Run.CompletedAt = timePointer(now)
		}
	} else {
		e.state.Run.CompletedAt = nil
	}
}

func (e *DAGExecution) runProgress() domain.ProgressState {
	allSucceeded := len(e.state.Tasks) != 0
	anyActive, anyReady, anyNeedsInput := false, false, false
	anyFailed, anyCancelled, anySkipped := false, false, false
	for _, task := range e.state.Tasks {
		if e.taskSucceeded(task.ID) {
			continue
		}
		allSucceeded = false
		switch e.state.Attempts[e.currentAttemptIndex(task.ID)].Progress {
		case domain.ProgressActive, domain.ProgressVerifying:
			anyActive = true
		case domain.ProgressReady, domain.ProgressQueued:
			anyReady = true
		case domain.ProgressNeedsInput:
			anyNeedsInput = true
		case domain.ProgressFailed:
			anyFailed = true
		case domain.ProgressCancelled:
			anyCancelled = true
		case domain.ProgressSkipped:
			anySkipped = true
		}
	}
	switch {
	case allSucceeded:
		return domain.ProgressSucceeded
	case anyActive:
		return domain.ProgressActive
	case anyReady:
		return domain.ProgressReady
	case anyNeedsInput:
		return domain.ProgressNeedsInput
	case anyFailed:
		return domain.ProgressFailed
	case anyCancelled:
		return domain.ProgressCancelled
	case anySkipped:
		return domain.ProgressSkipped
	default:
		return domain.ProgressBlocked
	}
}

func (e *DAGExecution) descendantNames(root string) []string {
	selected := map[string]struct{}{root: {}}
	changed := true
	for changed {
		changed = false
		for _, task := range e.state.Tasks {
			if _, exists := selected[task.Name]; exists {
				continue
			}
			for _, dependency := range task.Needs {
				if _, exists := selected[dependency]; exists {
					selected[task.Name] = struct{}{}
					changed = true
					break
				}
			}
		}
	}
	names := make([]string, 0, len(selected))
	for name := range selected {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (e *DAGExecution) taskSucceeded(taskID string) bool {
	for _, index := range e.attempts[taskID] {
		if e.state.Attempts[index].Progress == domain.ProgressSucceeded {
			return true
		}
	}
	return false
}

func (e *DAGExecution) currentAttemptIndex(taskID string) int {
	indexes := e.attempts[taskID]
	return indexes[len(indexes)-1]
}

func (e *DAGExecution) attemptIndex(id string) (int, error) {
	for index := range e.state.Attempts {
		if e.state.Attempts[index].ID == id {
			return index, nil
		}
	}
	return 0, fmt.Errorf("unknown attempt %q", id)
}

func cloneDAGState(state DAGState) DAGState {
	cloned := state
	cloned.External = make(map[string]domain.NodeObservation, len(state.External))
	for key, value := range state.External {
		cloned.External[key] = value
	}
	cloned.Run.Sink = domain.CloneSink(state.Run.Sink)
	cloned.Run.InputArtifactIDs = append([]string(nil), state.Run.InputArtifactIDs...)
	cloned.Tasks = append([]domain.Task(nil), state.Tasks...)
	cloned.Attempts = append([]domain.Attempt(nil), state.Attempts...)
	for index := range cloned.Tasks {
		source := state.Tasks[index]
		task := &cloned.Tasks[index]
		task.Needs = append([]string(nil), source.Needs...)
		task.ExternalNeeds = append([]domain.NodeRef(nil), source.ExternalNeeds...)
		task.InputArtifactIDs = append([]string(nil), source.InputArtifactIDs...)
		task.DependencyInputs = cloneStringSlices(source.DependencyInputs)
		task.Outputs = append([]domain.ArtifactDeclaration(nil), source.Outputs...)
		task.Verification = append([]string(nil), source.Verification...)
		task.Placement.Hosts = append([]string(nil), source.Placement.Hosts...)
		task.Placement.Capabilities = append([]string(nil), source.Placement.Capabilities...)
		task.Routes = append([]domain.ProviderRoute(nil), source.Routes...)
		for routeIndex := range task.Routes {
			task.Routes[routeIndex].Options = cloneStringMap(source.Routes[routeIndex].Options)
		}
		task.ResourceLocks = append([]string(nil), source.ResourceLocks...)
	}
	return cloned
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
