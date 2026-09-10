// Package backlog runs queued tasks as T3 threads when the quota forecast
// says nobody will miss the credits: outside the user's usual hours, with
// enough headroom before the next reset, one task per provider at a time.
package backlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Status of a task.
type Status string

const (
	StatusPending    Status = "pending"
	StatusRunning    Status = "running"
	StatusNeedsInput Status = "needs-input"
	StatusDone       Status = "done"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
	StatusDisabled   Status = "disabled"
	// StatusForwarded means another host's runner owns the task now.
	StatusForwarded Status = "forwarded"
)

// Task is one backlog item as authored in a markdown file with YAML
// frontmatter.
type Task struct {
	ID   string `yaml:"-"`
	Path string `yaml:"-"`
	// Project is the T3 project title or id: the workspace the agent
	// works in.
	Project string `yaml:"project"`
	// Host is the machine whose T3 server runs the task (an SSH alias).
	// Empty means the configured default; a task for another host is
	// forwarded into that host's backlog directory.
	Host  string `yaml:"host"`
	Title string `yaml:"title"`
	// Importance 1..5 orders tasks; Difficulty 1..5 seeds the cost and
	// duration estimates.
	Importance int `yaml:"importance"`
	Difficulty int `yaml:"difficulty"`
	// Model and Instance override the project's default model selection.
	Model    string `yaml:"model"`
	Instance string `yaml:"instance"`
	// Options are provider options such as effort or contextWindow.
	Options   map[string]string `yaml:"options"`
	NotBefore *time.Time        `yaml:"not_before"`
	Deadline  *time.Time        `yaml:"deadline"`
	MaxTurns  int               `yaml:"max_turns"`
	Enabled   *bool             `yaml:"enabled"`
	// Gate false runs the task at not_before regardless of the forecast
	// (quota health is still required).
	Gate *bool `yaml:"gate"`
	// EstimatedCost overrides the difficulty seed, in percent of the short
	// window.
	EstimatedCost *float64 `yaml:"estimated_cost"`
	// Prompt is the markdown body.
	Prompt string `yaml:"-"`
	// ModTime detects edits.
	ModTime time.Time `yaml:"-"`
}

// State is the runner's record of a task, persisted in the store.
type State struct {
	ID            string     `json:"id"`
	Status        Status     `json:"status"`
	ThreadID      string     `json:"threadId,omitempty"`
	Turns         int        `json:"turns"`
	EstimatedCost float64    `json:"estimatedCost"`
	MeasuredCost  float64    `json:"measuredCost"`
	EstimatedMins float64    `json:"estimatedMinutes"`
	MeasuredMins  float64    `json:"measuredMinutes"`
	DispatchedAt  *time.Time `json:"dispatchedAt,omitempty"`
	TurnStartedAt *time.Time `json:"turnStartedAt,omitempty"`
	CompletedAt   *time.Time `json:"completedAt,omitempty"`
	LastTurnID    string     `json:"lastTurnId,omitempty"`
	Reason        string     `json:"reason,omitempty"`
	FileModTime   time.Time  `json:"fileModTime"`
	UpdatedAt     time.Time  `json:"updatedAt"`
}

// Seeds by difficulty: percent of the short window and minutes per turn.
var (
	costSeed = map[int]float64{1: 5, 2: 10, 3: 20, 4: 35, 5: 50}
	minsSeed = map[int]float64{1: 15, 2: 30, 3: 60, 4: 90, 5: 150}
)

// SeedCost returns the difficulty's initial cost estimate.
func SeedCost(difficulty int) float64 {
	if v, ok := costSeed[difficulty]; ok {
		return v
	}
	return costSeed[3]
}

// SeedMinutes returns the difficulty's initial duration estimate.
func SeedMinutes(difficulty int) float64 {
	if v, ok := minsSeed[difficulty]; ok {
		return v
	}
	return minsSeed[3]
}

// IsEnabled reports whether the task may run.
func (t Task) IsEnabled() bool { return t.Enabled == nil || *t.Enabled }

// Gated reports whether the forecast gate applies.
func (t Task) Gated() bool { return t.Gate == nil || *t.Gate }

// ParseFile reads one task file.
func ParseFile(path string) (Task, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Task{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return Task{}, err
	}
	return parseFile(path, raw, info.ModTime())
}

func parseFile(path string, raw []byte, modTime time.Time) (Task, error) {
	t, err := Parse(raw)
	if err != nil {
		return Task{}, fmt.Errorf("%s: %w", path, err)
	}
	t.Path = path
	t.ID = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	t.ModTime = modTime
	if t.Title == "" {
		for _, line := range strings.Split(t.Prompt, "\n") {
			line = strings.TrimSpace(strings.TrimLeft(line, "# "))
			if line != "" {
				if len(line) > 72 {
					line = line[:72]
				}
				t.Title = line
				break
			}
		}
	}
	if t.Title == "" {
		t.Title = t.ID
	}
	return t, nil
}

// Parse decodes frontmatter and body.
func Parse(raw []byte) (Task, error) {
	var t Task
	body := raw
	if bytes.HasPrefix(raw, []byte("---")) {
		rest := raw[3:]
		rest = bytes.TrimLeft(rest, " \t")
		if !bytes.HasPrefix(rest, []byte("\n")) && !bytes.HasPrefix(rest, []byte("\r\n")) {
			return t, errors.New("frontmatter must start with --- on its own line")
		}
		end := bytes.Index(rest, []byte("\n---"))
		if end < 0 {
			return t, errors.New("frontmatter is not closed with ---")
		}
		front := rest[:end]
		body = rest[end+4:]
		if err := yaml.Unmarshal(front, &t); err != nil {
			return t, fmt.Errorf("frontmatter: %w", err)
		}
	}
	t.Prompt = strings.TrimSpace(string(body))
	if t.Project == "" {
		return t, errors.New("project is required")
	}
	if t.Prompt == "" {
		return t, errors.New("task body (the prompt) is empty")
	}
	if t.Importance == 0 {
		t.Importance = 3
	}
	if t.Difficulty == 0 {
		t.Difficulty = 3
	}
	if t.Importance < 1 || t.Importance > 5 || t.Difficulty < 1 || t.Difficulty > 5 {
		return t, errors.New("importance and difficulty must be between 1 and 5")
	}
	if t.MaxTurns == 0 {
		t.MaxTurns = 3
	}
	return t, nil
}

// LoadDir parses every *.md file in the directory.
func LoadDir(dir string) ([]Task, []error) {
	workflows, errs := LoadLegacyWorkflows(dir)
	tasks := make([]Task, 0, len(workflows))
	for _, workflow := range workflows {
		tasks = append(tasks, workflow.Source)
	}
	return tasks, errs
}

// Order sorts pending tasks: deadlines within a day first, then
// importance descending, then cheaper first, then id.
func Order(tasks []Task, states map[string]*State, now time.Time) {
	urgent := func(t Task) bool {
		return t.Deadline != nil && t.Deadline.Sub(now) < 24*time.Hour
	}
	cost := func(t Task) float64 {
		if st, ok := states[t.ID]; ok && st.EstimatedCost > 0 {
			return st.EstimatedCost
		}
		if t.EstimatedCost != nil {
			return *t.EstimatedCost
		}
		return SeedCost(t.Difficulty)
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		a, b := tasks[i], tasks[j]
		if urgent(a) != urgent(b) {
			return urgent(a)
		}
		if a.Importance != b.Importance {
			return a.Importance > b.Importance
		}
		if ca, cb := cost(a), cost(b); ca != cb {
			return ca < cb
		}
		return a.ID < b.ID
	})
}
