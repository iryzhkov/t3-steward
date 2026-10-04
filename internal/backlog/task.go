// Package backlog owns persisted workflow, task, worker and scheduling authority.
package backlog

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Task is one backlog item as authored in a markdown file with YAML
// frontmatter.
type Task struct {
	ID   string `yaml:"-"`
	Path string `yaml:"-"`
	// Project is the T3 project title or id: the workspace the agent
	// works in.
	Project string `yaml:"project"`
	// Host is retained frontmatter metadata; it never enables SSH forwarding.
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
