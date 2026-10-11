package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testSHA = "0123456789abcdef0123456789abcdef01234567"

func goodRun() run {
	return run{ID: 42, Attempt: 2, SHA: testSHA, Branch: "main", Event: "push", Path: ".github/workflows/ci.yml", Status: "completed", Conclusion: "success"}
}
func goodJobs(r run) []job {
	names := []string{"test (ubuntu-26.04)", "test (macos-latest)", "race", "staticcheck", "cross-build"}
	var jobs []job
	for i, name := range names {
		jobs = append(jobs, job{ID: int64(i + 1), RunID: r.ID, Attempt: r.Attempt, SHA: r.SHA, Name: name, Status: "completed", Conclusion: "success"})
	}
	return jobs
}
func recordedRuns(t *testing.T, runs ...run) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"workflow_runs": runs})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func recordedJobs(t *testing.T, jobs []job) []byte {
	t.Helper()
	// Two pages exercise the same --paginate --slurp shape used by the workflow.
	pages := []map[string]any{{"total_count": len(jobs), "jobs": jobs[:len(jobs)/2]}, {"total_count": len(jobs), "jobs": jobs[len(jobs)/2:]}}
	data, err := json.Marshal(pages)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func TestExactCIRecordedJSON(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*run, *[]job)
		missing bool
		timeout bool
		wantOK  bool
		wantErr string
	}{
		{name: "success", wantOK: true},
		{name: "missing run", missing: true, timeout: true, wantErr: "missing run"},
		{name: "different SHA", mutate: func(r *run, _ *[]job) { r.SHA = strings.Repeat("a", 40) }, timeout: true, wantErr: "missing run"},
		{name: "PR only", mutate: func(r *run, _ *[]job) { r.Event = "pull_request" }, timeout: true, wantErr: "missing run"},
		{name: "other branch", mutate: func(r *run, _ *[]job) { r.Branch = "feature" }, timeout: true, wantErr: "missing run"},
		{name: "other workflow", mutate: func(r *run, _ *[]job) { r.Path = ".github/workflows/nightly.yml" }, timeout: true, wantErr: "missing run"},
		{name: "run failed", mutate: func(r *run, _ *[]job) { r.Conclusion = "failure" }, wantErr: "failure"},
		{name: "run cancelled", mutate: func(r *run, _ *[]job) { r.Conclusion = "cancelled" }, wantErr: "cancelled"},
		{name: "failed job", mutate: func(_ *run, j *[]job) { (*j)[1].Conclusion = "failure" }, wantErr: "failure"},
		{name: "cancelled job", mutate: func(_ *run, j *[]job) { (*j)[1].Conclusion = "cancelled" }, wantErr: "cancelled"},
		{name: "skipped job", mutate: func(_ *run, j *[]job) { (*j)[1].Conclusion = "skipped" }, wantErr: "skipped"},
		{name: "unfinished pending", mutate: func(r *run, _ *[]job) { r.Status = "in_progress" }},
		{name: "unfinished timeout", mutate: func(r *run, _ *[]job) { r.Status = "in_progress" }, timeout: true, wantErr: "timeout"},
		{name: "job timeout", mutate: func(_ *run, j *[]job) { (*j)[1].Status = "in_progress" }, timeout: true, wantErr: "timeout"},
		{name: "wrong job SHA", mutate: func(_ *run, j *[]job) { (*j)[1].SHA = "other" }, wantErr: "identity"},
		{name: "previous attempt", mutate: func(_ *run, j *[]job) { (*j)[1].Attempt = 1 }, wantErr: "identity"},
		{name: "wrong run jobs", mutate: func(_ *run, j *[]job) { (*j)[1].RunID = 9 }, wantErr: "identity"},
		{name: "no jobs", mutate: func(_ *run, j *[]job) { *j = nil }, wantErr: "incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := goodRun()
			jobs := goodJobs(r)
			if tc.mutate != nil {
				tc.mutate(&r, &jobs)
			}
			runs := []run{r}
			if tc.missing {
				runs = nil
			}
			selected, err := selectRun(recordedRuns(t, runs...), testSHA)
			if err != nil {
				t.Fatal(err)
			}
			ok, err := decision(selected, recordedJobs(t, jobs), tc.timeout)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err=%v want %q", err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "run ") {
				t.Fatalf("missing run identity: %v", err)
			}
		})
	}
}
func TestLatestMatchingRunAndCompleteJobs(t *testing.T) {
	old := goodRun()
	latest := old
	latest.ID++
	latest.Status = "in_progress"
	selected, err := selectRun(recordedRuns(t, old, latest), testSHA)
	if err != nil || selected.ID != latest.ID {
		t.Fatalf("selected=%+v err=%v", selected, err)
	}
	if ok, err := decision(selected, nil, true); ok || err == nil {
		t.Fatalf("older success bypassed newer run: %v %v", ok, err)
	}
	if _, err := selectRun([]byte("{"), testSHA); err == nil {
		t.Fatal("malformed runs accepted")
	}
	r := goodRun()
	if _, err := decision(r, []byte("{"), false); err == nil {
		t.Fatal("malformed jobs accepted")
	}
	jobs := goodJobs(r)
	jobs[1] = jobs[0]
	if _, err := decision(r, recordedJobs(t, jobs), false); err == nil {
		t.Fatal("duplicate jobs accepted")
	}
	if _, err := decision(r, []byte(`[{"total_count":5,"jobs":[]}]`), false); err == nil {
		t.Fatal("truncated jobs accepted")
	}
}
func TestVerifyPublishedAssets(t *testing.T) {
	for _, kind := range []string{"success", "missing", "differs", "extra", "no manifest", "empty manifest", "duplicate", "path traversal"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, body string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write("archive.tar.gz", "archive")
			checksum := fmt.Sprintf("%x  archive.tar.gz\n", sha256.Sum256([]byte("archive")))
			switch kind {
			case "missing":
				if err := os.Remove(filepath.Join(dir, "archive.tar.gz")); err != nil {
					t.Fatal(err)
				}
			case "differs":
				write("archive.tar.gz", "corrupt")
			case "extra":
				write("extra.tar.gz", "extra")
			case "empty manifest":
				checksum = ""
			case "duplicate":
				checksum += checksum
			case "path traversal":
				checksum = fmt.Sprintf("%x  ../escape\n", sha256.Sum256(nil))
			}
			if kind != "no manifest" {
				write("checksums.txt", checksum)
			}
			err := verifyAssets(dir)
			if (err == nil) != (kind == "success") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
