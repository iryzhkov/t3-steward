package main

import (
	"encoding/json"
	"fmt"
)

type run struct {
	ID         int64  `json:"id"`
	Attempt    int    `json:"run_attempt"`
	SHA        string `json:"head_sha"`
	Branch     string `json:"head_branch"`
	Event      string `json:"event"`
	Path       string `json:"path"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}
type job struct {
	ID         int64  `json:"id"`
	RunID      int64  `json:"run_id"`
	Attempt    int    `json:"run_attempt"`
	SHA        string `json:"head_sha"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

func selectRun(data []byte, sha string) (run, error) {
	var response struct {
		Runs []run `json:"workflow_runs"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return run{}, err
	}
	var selected run
	for _, r := range response.Runs {
		if r.SHA == sha && r.Branch == "main" && r.Event == "push" && r.Path == ".github/workflows/ci.yml" && r.ID > selected.ID {
			selected = r
		}
	}
	return selected, nil
}

// decision returns pending until the bounded wait expires. Only the current
// attempt's jobs count; skipped jobs never count as successful.
func decision(r run, data []byte, timedOut bool) (bool, error) {
	label := fmt.Sprintf("run %d attempt %d", r.ID, r.Attempt)
	if r.ID == 0 {
		label = "run none (no matching push CI on main)"
	}
	pending := func(reason string) (bool, error) {
		if timedOut {
			return false, fmt.Errorf("%s: timeout: %s", label, reason)
		}
		return false, nil
	}
	if r.ID == 0 {
		return pending("missing run")
	}
	if r.Status != "completed" {
		return pending("run is " + r.Status)
	}
	if r.Conclusion != "success" {
		return false, fmt.Errorf("%s concluded %q", label, r.Conclusion)
	}
	var pages []struct {
		Jobs  []job `json:"jobs"`
		Total int   `json:"total_count"`
	}
	if err := json.Unmarshal(data, &pages); err != nil {
		return false, fmt.Errorf("%s jobs: %w", label, err)
	}
	var jobs []job
	total := 0
	for _, page := range pages {
		jobs = append(jobs, page.Jobs...)
		if page.Total > total {
			total = page.Total
		}
	}
	if len(jobs) == 0 || len(jobs) != total {
		return false, fmt.Errorf("%s: incomplete job list (%d of %d)", label, len(jobs), total)
	}
	seen := map[int64]bool{}
	for _, j := range jobs {
		if j.ID == 0 || seen[j.ID] || j.RunID != r.ID || j.Attempt != r.Attempt || j.SHA != r.SHA {
			return false, fmt.Errorf("%s: job %q has mismatched identity", label, j.Name)
		}
		seen[j.ID] = true
		if j.Status != "completed" {
			return pending("job " + j.Name + " is " + j.Status)
		}
		if j.Conclusion != "success" {
			return false, fmt.Errorf("%s: job %q concluded %q", label, j.Name, j.Conclusion)
		}
	}
	return true, nil
}
