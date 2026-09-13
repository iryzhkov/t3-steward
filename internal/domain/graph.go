package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// GraphDefinition is an immutable run-local snapshot. Workflow submission
// definitions remain templates; an amendment never changes a sibling run.
type GraphDefinition struct {
	RunID      string          `json:"runId"`
	Revision   int64           `json:"revision"`
	Parent     int64           `json:"parent"`
	Actor      string          `json:"actor"`
	Reason     string          `json:"reason"`
	RequestID  string          `json:"requestId"`
	Digest     string          `json:"digest"`
	CreatedAt  time.Time       `json:"createdAt"`
	Tasks      []Task          `json:"tasks"`
	ClonedFrom *GraphReference `json:"clonedFrom,omitempty"`
}

type GraphReference struct {
	RunID    string `json:"runId"`
	Revision int64  `json:"revision"`
}

func TasksForRun(run WorkflowRun, templates []Task) []Task {
	if run.Graph != nil {
		return run.Graph.Tasks
	}
	result := []Task{}
	for _, task := range templates {
		if task.WorkflowID == run.WorkflowID && (task.RunID == "" || task.RunID == run.ID) {
			result = append(result, task)
		}
	}
	return result
}

func TaskForAttempt(attempt Attempt, runs []WorkflowRun, templates []Task) (Task, bool) {
	for _, run := range runs {
		if run.ID == attempt.WorkflowRunID {
			for _, task := range TasksForRun(run, templates) {
				if task.ID == attempt.TaskID {
					return task, true
				}
			}
			return Task{}, false
		}
	}
	return Task{}, false
}

// TasksWithGraphAdditions supplies identities for fleet-wide indexes. Definition
// selection must still use TasksForRun or TaskForAttempt.
func TasksWithGraphAdditions(runs []WorkflowRun, templates []Task) []Task {
	result := append([]Task(nil), templates...)
	seen := map[string]bool{}
	for _, task := range result {
		seen[task.ID] = true
	}
	for _, run := range runs {
		if run.Graph != nil {
			for _, task := range run.Graph.Tasks {
				if !seen[task.ID] {
					result = append(result, task)
					seen[task.ID] = true
				}
			}
		}
	}
	return result
}

func TaskDigest(task Task) string {
	raw, _ := json.Marshal(task)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func GraphDigest(tasks []Task) string {
	raw, _ := json.Marshal(tasks)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
