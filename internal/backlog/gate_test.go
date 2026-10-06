package backlog

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestH2GateCacheMissForCommandsToolsAndAge(t *testing.T) {
	for _, change := range []string{"commands", "tools", "timeout", "age"} {
		t.Run(change, func(t *testing.T) {
			dir := h2GateRepository(t)
			storage := t.TempDir()
			now := time.Now().UTC()
			f := AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour, Now: func() time.Time { return now }}
			req := h2GateRequest(dir, "first")
			first, err := f.Finalize(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, first.StorageDir)
			switch change {
			case "commands":
				req.Task.Gate.Commands = []string{"printf different"}
			case "tools":
				f.GateToolchainIdentity = "new-toolchain"
			case "timeout":
				req.Task.Gate.Timeout = 2 * time.Second
			case "age":
				now = now.Add(2 * time.Hour)
			}
			req.Attempt.ID = "second"
			second, err := f.Finalize(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, second.StorageDir)
			if h2ReadGate(t, storage, second).Cached {
				t.Fatal("changed cache identity/age reused result")
			}
		})
	}
}
func TestH2GateBoundsOutputAndRetainsPointer(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	req := h2GateRequest(dir, "large")
	req.Task.Gate.Commands = []string{"head -c 2000000 /dev/zero | tr '\\000' x"}
	f := AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}}
	result, err := f.Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	report := h2ReadGate(t, storage, result)
	if !report.Passed || !report.LogTruncated || report.LogArtifact != "gate/log.txt" {
		t.Fatalf("report=%+v", report)
	}
	for _, a := range result.Artifacts {
		if a.Name == "gate/log.txt" {
			raw := readStoredArtifact(t, storage, a)
			if len(raw) > gateLogLimit+256 || !strings.Contains(string(raw), "structured result: gate") {
				t.Fatalf("bad bounded log length %d", len(raw))
			}
			return
		}
	}
	t.Fatal("missing bounded log")
}
func TestH2GateDependencyIsWorkerEvidence(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	req := h2GateRequest(dir, "producer")
	result, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	consumer := domain.Task{ID: "consumer", Name: "review", Needs: []string{"build"}, DependencyInputs: map[string][]string{"build": {"gate", "gate/log.txt"}}}
	workspace := t.TempDir()
	cleanupImmutable(t, filepath.Join(workspace, ".t3", "dependencies"))
	_, err = MaterializeDependencies(workspace, storage, "run-1", consumer, []domain.Task{req.Task, consumer}, result.Artifacts)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(workspace, ".t3", "dependencies", "build", "gate", "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report GateReport
	if err = json.Unmarshal(raw, &report); err != nil || !report.Passed {
		t.Fatalf("gate dependency=%s err=%v", raw, err)
	}
}

func TestH2GateConcurrentCacheAndCorruptReplay(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	type response struct {
		result FinalizedAttempt
		err    error
	}
	responses := make(chan response, 2)
	for _, id := range []string{"concurrent-a", "concurrent-b"} {
		go func(id string) {
			f := AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}
			result, err := f.Finalize(context.Background(), h2GateRequest(dir, id))
			responses <- response{result, err}
		}(id)
	}
	cached := 0
	var key string
	for i := 0; i < 2; i++ {
		response := <-responses
		if response.err != nil {
			t.Fatal(response.err)
		}
		cleanupImmutable(t, response.result.StorageDir)
		r := h2ReadGate(t, storage, response.result)
		key = r.CacheKey
		if r.Cached {
			cached++
		}
	}
	if cached != 1 {
		t.Fatalf("concurrent identical gates reused=%d", cached)
	}
	// Interrupted/corrupt cache records are misses, never success evidence.
	if err := os.WriteFile(filepath.Join(storage, "gate-cache", key+".json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}).Finalize(context.Background(), h2GateRequest(dir, "replay"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	if h2ReadGate(t, storage, result).Cached {
		t.Fatal("corrupt cache reused")
	}
}
func TestH2GatePostconditionCannotCacheMutation(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	req := h2GateRequest(dir, "mutating")
	req.Task.Gate.Commands = []string{"printf mutation >> source.txt"}
	result, err := (AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}).Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	report := h2ReadGate(t, storage, result)
	if report.Passed || report.Cached || report.Failure == nil || report.Failure.Command != "git status" {
		t.Fatalf("mutation=%+v", report)
	}
}

func TestH2GateMetadataDoesNotRunRepositoryHooks(t *testing.T) {
	for _, kind := range []string{"fsmonitor", "filter"} {
		t.Run(kind, func(t *testing.T) {
			dir := h2GateRepository(t)
			outside := t.TempDir()
			marker := filepath.Join(outside, "executed")
			hook := filepath.Join(outside, "hook")
			if err := os.WriteFile(hook, []byte("#!/bin/sh\nprintf executed > '"+marker+"'\ncat\n"), 0700); err != nil {
				t.Fatal(err)
			}
			config := []string{"config", "core.fsmonitor", hook}
			if kind == "filter" {
				writeTestFile(t, dir, ".gitattributes", "source.txt filter=fixture\n")
				c := exec.Command("git", "add", ".gitattributes")
				c.Dir = dir
				if out, err := c.CombinedOutput(); err != nil {
					t.Fatalf("%s %v", out, err)
				}
				c = exec.Command("git", "commit", "-qm", "attributes")
				c.Dir = dir
				if out, err := c.CombinedOutput(); err != nil {
					t.Fatalf("%s %v", out, err)
				}
				config = []string{"config", "filter.fixture.clean", hook}
				writeTestFile(t, dir, "source.txt", "dirty source")
			}
			c := exec.Command("git", config...)
			c.Dir = dir
			if out, err := c.CombinedOutput(); err != nil {
				t.Fatalf("%s %v", out, err)
			}
			req := h2GateRequest(dir, "metadata")
			f := AttemptFinalizer{StorageRoot: t.TempDir(), Processes: &directRunner{}}
			result, err := f.Finalize(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			cleanupImmutable(t, result.StorageDir)
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("worker metadata executed repository %s hook outside containment", kind)
			}
		})
	}
}

func h2GateRepository(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"config", "user.name", "fixture"}, {"config", "user.email", "fixture@example.invalid"}} {
		c := exec.Command("git", args...)
		c.Dir = dir
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("git: %s %v", out, err)
		}
	}
	writeTestFile(t, dir, "source.txt", "source")
	h2Commit(t, dir)
	return dir
}
func h2Commit(t *testing.T, dir string) {
	t.Helper()
	for _, args := range [][]string{{"add", "source.txt"}, {"commit", "-qm", "fixture"}} {
		c := exec.Command("git", args...)
		c.Dir = dir
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("git: %s %v", out, err)
		}
	}
}
func h2GateRequest(dir, id string) AttemptFinalization {
	task := artifactTestTask()
	task.Outputs = nil
	task.Verification = []string{"printf verified"}
	task.Gate = &domain.TaskGate{Commands: []string{"printf gated"}, Timeout: time.Second}
	attempt := artifactTestAttempt()
	attempt.ID = id
	return AttemptFinalization{Task: task, Attempt: attempt, WorkspaceDir: dir, ExplicitSuccess: true}
}
func h2ReadGate(t *testing.T, storage string, result FinalizedAttempt) GateReport {
	t.Helper()
	for _, a := range result.Artifacts {
		if a.Name == "gate" {
			var report GateReport
			if err := json.Unmarshal(readStoredArtifact(t, storage, a), &report); err != nil {
				t.Fatal(err)
			}
			return report
		}
	}
	t.Fatal("missing gate result")
	return GateReport{}
}
func TestH2GateAfterVerifyAndDurableCache(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	runner := &directRunner{}
	finalizer := AttemptFinalizer{StorageRoot: storage, Processes: runner, GateCacheAge: time.Hour}
	first, err := finalizer.Finalize(context.Background(), h2GateRequest(dir, "first"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, first.StorageDir)
	report := h2ReadGate(t, storage, first)
	if !report.Passed || report.Cached || len(report.TreeHash) != 40 || len(report.Commands) != 1 || report.Commands[0].Command != "printf gated" {
		t.Fatalf("gate: %+v", report)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("calls=%d", len(runner.calls))
	}
	// A new finalizer simulates restart; verification still runs, gate reuses durable evidence.
	restartRunner := &directRunner{}
	restart := AttemptFinalizer{StorageRoot: storage, Processes: restartRunner, GateCacheAge: time.Hour}
	second, err := restart.Finalize(context.Background(), h2GateRequest(dir, "second"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, second.StorageDir)
	cached := h2ReadGate(t, storage, second)
	if !cached.Cached || cached.OriginalAttempt != "first" || len(restartRunner.calls) != 1 {
		t.Fatalf("cache=%+v calls=%d", cached, len(restartRunner.calls))
	}
	for _, a := range second.Artifacts {
		if a.Name == "gate/log.txt" {
			if !strings.Contains(string(readStoredArtifact(t, storage, a)), "gated") {
				t.Fatal("cached log lost")
			}
			return
		}
	}
	t.Fatal("missing cached gate log")
}
func TestH2GateMissAndFailureRetainsLog(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	finalizer := AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}
	req := h2GateRequest(dir, "first")
	first, err := finalizer.Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, first.StorageDir)
	writeTestFile(t, dir, "source.txt", "changed")
	h2Commit(t, dir)
	req.Attempt.ID = "changed"
	second, err := finalizer.Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, second.StorageDir)
	if h2ReadGate(t, storage, second).Cached {
		t.Fatal("tree change hit cache")
	}
	req.Attempt.ID = "failed"
	req.Task.Gate.Commands = []string{"printf failure-output; exit 7"}
	failed, err := finalizer.Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, failed.StorageDir)
	report := h2ReadGate(t, storage, failed)
	if failed.Completion.VerificationPassed || report.Passed || report.Failure == nil || report.Failure.ExitCode != 7 {
		t.Fatalf("failed=%+v report=%+v", failed.Completion, report)
	}
	for _, a := range failed.Artifacts {
		if a.Name == "gate/log.txt" && strings.Contains(string(readStoredArtifact(t, storage, a)), "failure-output") {
			return
		}
	}
	t.Fatal("failure log not retained")
}
func TestH2GateSkipFailedVerifyAndTimeout(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	runner := &directRunner{}
	finalizer := AttemptFinalizer{StorageRoot: storage, Processes: runner}
	req := h2GateRequest(dir, "verify-fail")
	req.Task.Verification = []string{"exit 4"}
	failed, err := finalizer.Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, failed.StorageDir)
	if len(runner.calls) != 1 {
		t.Fatalf("gate ran after failed verify")
	}
	for _, a := range failed.Artifacts {
		if a.Name == "gate" {
			t.Fatal("gate evidence on skipped gate")
		}
	}
	req = h2GateRequest(dir, "timeout")
	req.Task.Gate.Timeout = 20 * time.Millisecond
	req.Task.Gate.Commands = []string{"exec sleep 1"}
	timed, err := finalizer.Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, timed.StorageDir)
	report := h2ReadGate(t, storage, timed)
	if report.Passed || report.Failure == nil || report.Failure.ExitCode != 124 {
		t.Fatalf("timeout=%+v", report)
	}
}
func TestH2GateDirtyTrackedTreeCannotReuseCache(t *testing.T) {
	dir := h2GateRepository(t)
	storage := t.TempDir()
	finalizer := AttemptFinalizer{StorageRoot: storage, Processes: &directRunner{}, GateCacheAge: time.Hour}
	first, err := finalizer.Finalize(context.Background(), h2GateRequest(dir, "clean"))
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, first.StorageDir)
	if err = os.WriteFile(filepath.Join(dir, "source.txt"), []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	req := h2GateRequest(dir, "dirty")
	result, err := finalizer.Finalize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, result.StorageDir)
	report := h2ReadGate(t, storage, result)
	if report.Cached || report.Passed {
		t.Fatalf("dirty tree accepted cached evidence: %+v", report)
	}
}
