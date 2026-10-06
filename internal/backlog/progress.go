package backlog

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ProgressFilter selects a read-only mirror. Since is an inclusive UTC timestamp.
type ProgressFilter struct {
	RunIDs []string  `json:"runIds,omitempty"`
	Owner  string    `json:"owner,omitempty"`
	Since  time.Time `json:"since,omitempty"`
}
type ProgressDocument struct {
	SchemaVersion int           `json:"schemaVersion"`
	GeneratedAt   time.Time     `json:"generatedAt"`
	Runs          []ProgressRun `json:"runs"`
}
type ProgressRun struct {
	ID           string               `json:"id"`
	Name         string               `json:"name"`
	State        domain.ProgressState `json:"state"`
	Done         int                  `json:"done"`
	Total        int                  `json:"total"`
	CurrentTask  string               `json:"currentTask"`
	CurrentState domain.ProgressState `json:"currentState"`
	LastChange   time.Time            `json:"lastChange"`
	Tasks        []ProgressTask       `json:"tasks"`
}
type ProgressTask struct {
	ID             string               `json:"id"`
	Name           string               `json:"name"`
	State          domain.ProgressState `json:"state"`
	Control        domain.ControlState  `json:"control"`
	Route          string               `json:"route"`
	Effort         string               `json:"effort"`
	ReviewVerdicts string               `json:"reviewVerdicts"`
	Outputs        []string             `json:"outputs"`
	Attempts       int                  `json:"attempts"`
}

// taskProgress is also used by the ledger's boundary record. The actual
// assignment, never the declared candidate route, supplies route and effort.
func (v ledgerRunView) taskProgress(task domain.Task, attempt domain.Attempt, rounds []review.Round, reported bool) ProgressTask {
	p := ProgressTask{ID: task.ID, Name: task.Name, State: domain.ProgressQueued, Outputs: []string{}, ReviewVerdicts: ledgerVerdicts(rounds, reported, task.ID)}
	if attempt.ID == "" {
		return p
	}
	p.State, p.Control = attempt.Progress, attempt.Control
	if assignment, ok := v.assignments[attempt.AssignmentID]; ok && attempt.AssignmentID != "" {
		p.Route = assignment.Route.ProviderInstanceID + "/" + assignment.Route.Model
		p.Effort = assignment.Route.Options["effort"]
	}
	for _, a := range v.artifacts[attempt.ID] {
		p.Outputs = append(p.Outputs, a.Name)
	}
	return p
}

func progressCurrentPriority(state domain.ProgressState) int {
	switch state {
	case domain.ProgressActive, domain.ProgressVerifying:
		return 3
	case domain.ProgressWaitingExternal, domain.ProgressNeedsInput:
		return 2
	case domain.ProgressReady, domain.ProgressBlocked:
		return 1
	default:
		return 0
	}
}

func progressTerminal(state domain.ProgressState) bool {
	switch state {
	case domain.ProgressSucceeded, domain.ProgressFailed, domain.ProgressCancelled, domain.ProgressSkipped:
		return true
	}
	return false
}

// BuildProgress reads only supplied facts. Explicit IDs bypass the default
// recent-terminal window; owner and since still apply. Runs are newest first.
func BuildProgress(records sqlite.CoordinatorRecords, rounds []review.Round, reported bool, waits []domain.NodeWait, filter ProgressFilter, now time.Time) (ProgressDocument, error) {
	doc := ProgressDocument{SchemaVersion: 1, GeneratedAt: now.UTC(), Runs: []ProgressRun{}}
	workflows := map[string]domain.Workflow{}
	for _, w := range records.Workflows {
		workflows[w.ID] = w
	}
	requested := map[string]bool{}
	for _, id := range filter.RunIDs {
		requested[id] = false
	}
	for _, run := range records.WorkflowRuns {
		if len(requested) > 0 {
			if _, ok := requested[run.ID]; !ok {
				continue
			}
			requested[run.ID] = true
		}
		if filter.Owner != "" {
			owned := false
			for _, w := range waits {
				if w.Request.Target.RunID == run.ID && w.Request.Target.TaskID == domain.SinkTaskName && w.Request.ThreadID == filter.Owner {
					owned = true
					break
				}
			}
			if !owned {
				continue
			}
		}
		view := newLedgerRunView(records, workflows[run.WorkflowID], run)
		last := run.UpdatedAt
		if last.IsZero() {
			last = run.CreatedAt
		}
		if run.CompletedAt != nil && run.CompletedAt.After(last) {
			last = *run.CompletedAt
		}
		if run.Sink != nil && run.Sink.CompletedAt != nil && run.Sink.CompletedAt.After(last) {
			last = *run.Sink.CompletedAt
		}
		latest := map[string]domain.Attempt{}
		counts := map[string]int{}
		for _, a := range view.attempts {
			if _, ok := view.taskByID[a.TaskID]; !ok {
				continue
			}
			counts[a.TaskID]++
			if old, ok := latest[a.TaskID]; !ok || a.Number > old.Number || (a.Number == old.Number && a.ID > old.ID) {
				latest[a.TaskID] = a
			}
			if a.UpdatedAt.After(last) {
				last = a.UpdatedAt
			}
		}
		ownRounds := []review.Round{}
		for _, r := range rounds {
			if r.WorkflowRunID == run.ID {
				ownRounds = append(ownRounds, r)
				if r.UpdatedAt.After(last) {
					last = r.UpdatedAt
				}
			}
		}
		if !filter.Since.IsZero() && last.Before(filter.Since) {
			continue
		}
		terminalAt := run.UpdatedAt
		if run.CompletedAt != nil {
			terminalAt = *run.CompletedAt
		}
		if run.Sink != nil && run.Sink.CompletedAt != nil {
			terminalAt = *run.Sink.CompletedAt
		}
		if len(requested) == 0 && filter.Since.IsZero() && progressTerminal(run.Progress) && terminalAt.Before(now.Add(-24*time.Hour)) {
			continue
		}
		p := ProgressRun{ID: run.ID, Name: view.workflow.Name, State: run.Progress, Total: len(view.tasks), LastChange: last.UTC(), Tasks: []ProgressTask{}}
		for _, t := range view.tasks {
			a := latest[t.ID]
			task := view.taskProgress(t, a, ownRounds, reported)
			task.Attempts = counts[t.ID]
			p.Tasks = append(p.Tasks, task)
			if progressTerminal(task.State) {
				p.Done++
			} else if p.CurrentTask == "" || (progressCurrentPriority(task.State) > progressCurrentPriority(p.CurrentState)) {
				p.CurrentTask = t.Name
				p.CurrentState = task.State
			}
		}
		doc.Runs = append(doc.Runs, p)
	}
	for id, found := range requested {
		if !found {
			return doc, fmt.Errorf("campaign run %q not found", id)
		}
	}
	sort.Slice(doc.Runs, func(i, j int) bool {
		if !doc.Runs[i].LastChange.Equal(doc.Runs[j].LastChange) {
			return doc.Runs[i].LastChange.After(doc.Runs[j].LastChange)
		}
		return doc.Runs[i].ID < doc.Runs[j].ID
	})
	return doc, nil
}

// progressLine prevents control characters and bounds every text row to 120
// Unicode code points. JSON retains complete values and no result bodies.
func progressLine(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	r := []rune(s)
	if len(r) > 120 {
		s = string(r[:119]) + "…"
	}
	return s
}
func RenderProgress(out io.Writer, doc ProgressDocument) error {
	if len(doc.Runs) == 0 {
		_, err := fmt.Fprintln(out, "No campaign runs match.")
		return err
	}
	for _, run := range doc.Runs {
		current := run.CurrentTask
		if current == "" {
			current = "-"
		} else {
			current += ":" + string(run.CurrentState)
		}
		line := fmt.Sprintf("%s %s %s %d/%d current=%s changed=%s", run.ID, run.Name, run.State, run.Done, run.Total, current, run.LastChange.Format(time.RFC3339))
		if _, err := fmt.Fprintln(out, progressLine(line)); err != nil {
			return err
		}
		for _, t := range run.Tasks {
			route, effort := t.Route, t.Effort
			if route == "" {
				route = "-"
			}
			if effort == "" {
				effort = "-"
			}
			line = fmt.Sprintf("  %s %s route=%s effort=%s attempts=%d outputs=%s review=%s", t.Name, t.State, route, effort, t.Attempts, strings.Join(t.Outputs, ","), t.ReviewVerdicts)
			if _, err := fmt.Fprintln(out, progressLine(line)); err != nil {
				return err
			}
		}
	}
	return nil
}
