package backlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAttemptFinalizerCapturesOutputsAndVerification(t *testing.T) {
	workspace := t.TempDir()
	storage := t.TempDir()
	writeTestFile(t, workspace, "dist/result.txt", "verified output\n")

	task := artifactTestTask()
	attempt := artifactTestAttempt()
	finalizer := testFinalizer(storage)
	result, err := finalizer.Finalize(context.Background(), AttemptFinalization{
		Task: task, Attempt: attempt, WorkspaceDir: workspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("finalize attempt: %v", err)
	}
	cleanupImmutable(t, result.StorageDir)
	if !result.Completion.ExplicitSuccess || !result.Completion.VerificationPassed || result.Completion.Failure != "" {
		t.Fatalf("completion = %#v, want verified success", result.Completion)
	}
	if len(result.Artifacts) != 2 {
		t.Fatalf("artifact count = %d, want 2", len(result.Artifacts))
	}

	output := findArtifact(t, result.Artifacts, domain.ArtifactOutput)
	wantHash := sha256.Sum256([]byte("verified output\n"))
	if output.Name != "dist/result.txt" || output.SHA256 != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("output artifact = %#v", output)
	}
	if output.Size != int64(len("verified output\n")) || output.MediaType != "text/plain; charset=utf-8" {
		t.Fatalf("output metadata = %#v", output)
	}
	assertStoredContent(t, storage, output, "verified output\n")
	assertReadOnly(t, filepath.Join(storage, filepath.FromSlash(output.StoragePath)))

	verification := findArtifact(t, result.Artifacts, domain.ArtifactVerification)
	raw := readStoredArtifact(t, storage, verification)
	var report VerificationReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode verification report: %v", err)
	}
	if report.Command != "test -f dist/result.txt && printf checked" || report.ExitCode != 0 || report.Output != "checked" {
		t.Fatalf("verification report = %#v", report)
	}
	if result.StorageDir != filepath.Join(storage, "runs", "run-1", "task-build", "attempt-1") {
		t.Fatalf("storage dir = %q", result.StorageDir)
	}
}

func TestFinalizedVerificationReleasesDAGDependency(t *testing.T) {
	workspace := t.TempDir()
	storage := t.TempDir()
	writeTestFile(t, workspace, "dist/result.txt", "done")

	producer := artifactTestTask()
	producer.WorkflowID = "workflow"
	consumer := testTask("consumer", "build")
	execution := newTestDAG(t, producer, consumer)
	mustStart(t, execution, "build-1")

	var active domain.Attempt
	for _, attempt := range execution.Snapshot().Attempts {
		if attempt.ID == "build-1" {
			active = attempt
		}
	}
	result, err := testFinalizer(storage).Finalize(context.Background(), AttemptFinalization{
		Task: producer, Attempt: active, WorkspaceDir: workspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("finalize producer: %v", err)
	}
	cleanupImmutable(t, result.StorageDir)
	if err := execution.CompleteAttempt("build-1", result.Completion, dagTestTime.Add(time.Minute)); err != nil {
		t.Fatalf("complete producer: %v", err)
	}
	assertAttemptProgress(t, execution, "consumer-1", domain.ProgressReady)
}

func TestAttemptFinalizerMissingOutputIsTaskFailure(t *testing.T) {
	workspace := t.TempDir()
	storage := t.TempDir()
	task := artifactTestTask()
	task.Verification = []string{"printf ran > should-not-be-skipped"}
	task.Outputs = []domain.ArtifactDeclaration{{Name: "absent.txt"}}

	result, err := testFinalizer(storage).Finalize(context.Background(), AttemptFinalization{
		Task: task, Attempt: artifactTestAttempt(), WorkspaceDir: workspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("finalize attempt: %v", err)
	}
	cleanupImmutable(t, result.StorageDir)
	if result.Completion.VerificationPassed {
		t.Fatal("missing output passed verification")
	}
	if !strings.Contains(result.Completion.Failure, "missing declared output: absent.txt") {
		t.Fatalf("failure = %q", result.Completion.Failure)
	}
	if _, err := os.Stat(filepath.Join(workspace, "should-not-be-skipped")); err != nil {
		t.Fatalf("verification did not run before output capture: %v", err)
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0].Kind != domain.ArtifactVerification {
		t.Fatalf("artifacts = %#v, want retained verification report", result.Artifacts)
	}
}

func TestAttemptFinalizerRetainsFailedVerification(t *testing.T) {
	workspace := t.TempDir()
	storage := t.TempDir()
	writeTestFile(t, workspace, "dist/result.txt", "candidate")
	task := artifactTestTask()
	task.Verification = []string{
		"printf first",
		"printf broken >&2; exit 7",
		"printf should-not-run",
	}

	result, err := testFinalizer(storage).Finalize(context.Background(), AttemptFinalization{
		Task: task, Attempt: artifactTestAttempt(), WorkspaceDir: workspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("finalize attempt: %v", err)
	}
	cleanupImmutable(t, result.StorageDir)
	if result.Completion.VerificationPassed || !strings.Contains(result.Completion.Failure, "failed (7)") {
		t.Fatalf("completion = %#v, want verification failure", result.Completion)
	}
	if len(result.Artifacts) != 3 {
		t.Fatalf("artifact count = %d, want output and two reports", len(result.Artifacts))
	}
	var reports []VerificationReport
	for _, artifact := range result.Artifacts {
		if artifact.Kind != domain.ArtifactVerification {
			continue
		}
		var report VerificationReport
		if err := json.Unmarshal(readStoredArtifact(t, storage, artifact), &report); err != nil {
			t.Fatalf("decode report: %v", err)
		}
		reports = append(reports, report)
	}
	if len(reports) != 2 || reports[1].ExitCode != 7 || reports[1].Output != "broken" {
		t.Fatalf("reports = %#v", reports)
	}
}

func TestAttemptFinalizerMissingExplicitSuccessRunsNothing(t *testing.T) {
	workspace := t.TempDir()
	storage := t.TempDir()
	task := artifactTestTask()
	task.Verification = []string{"touch should-not-exist"}

	result, err := testFinalizer(storage).Finalize(context.Background(), AttemptFinalization{
		Task: task, Attempt: artifactTestAttempt(), WorkspaceDir: workspace,
	})
	if err != nil {
		t.Fatalf("finalize attempt: %v", err)
	}
	if result.Completion.Failure != "missing explicit success" || len(result.Artifacts) != 0 {
		t.Fatalf("result = %#v", result)
	}
	if _, err := os.Stat(filepath.Join(workspace, "should-not-exist")); !os.IsNotExist(err) {
		t.Fatalf("verification ran without explicit success: %v", err)
	}
}

func TestAttemptFinalizerRejectsEscapingOutputSymlink(t *testing.T) {
	workspace := t.TempDir()
	storage := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "result.txt")); err != nil {
		t.Fatal(err)
	}
	task := artifactTestTask()
	task.Outputs = []domain.ArtifactDeclaration{{Name: "result.txt"}}
	task.Verification = nil

	_, err := testFinalizer(storage).Finalize(context.Background(), AttemptFinalization{
		Task: task, Attempt: artifactTestAttempt(), WorkspaceDir: workspace, ExplicitSuccess: true,
	})
	if err == nil || !strings.Contains(err.Error(), "symlink escapes") {
		t.Fatalf("error = %v, want escaping symlink rejection", err)
	}
}

func TestMaterializeDependenciesSelectsAndVerifiesOutputs(t *testing.T) {
	storage := t.TempDir()
	workspace := t.TempDir()
	first := storedOutputArtifact(t, storage, "producer-id", "attempt-p", "reports/one.txt", "one")
	second := storedOutputArtifact(t, storage, "producer-id", "attempt-p", "reports/two.txt", "two")
	unselected := storedOutputArtifact(t, storage, "producer-id", "attempt-p", "other.txt", "other")
	task := domain.Task{
		ID: "consumer-id", Name: "consumer",
		Needs:            []string{"producer"},
		DependencyInputs: map[string][]string{"producer": {"reports/two.txt", "reports/one.txt"}},
	}
	tasks := []domain.Task{{ID: "producer-id", Name: "producer"}, task}

	paths, err := MaterializeDependencies(workspace, storage, "run-1", task, tasks, []domain.Artifact{second, unselected, first})
	if err != nil {
		t.Fatalf("materialize dependencies: %v", err)
	}
	cleanupImmutable(t, filepath.Join(workspace, ".t3", "dependencies"))
	wantPaths := []string{
		".t3/dependencies/producer/reports/one.txt",
		".t3/dependencies/producer/reports/two.txt",
	}
	if strings.Join(paths, "|") != strings.Join(wantPaths, "|") {
		t.Fatalf("paths = %#v, want %#v", paths, wantPaths)
	}
	for _, pair := range []struct {
		path    string
		content string
	}{{wantPaths[0], "one"}, {wantPaths[1], "two"}} {
		raw, err := os.ReadFile(filepath.Join(workspace, filepath.FromSlash(pair.path)))
		if err != nil {
			t.Fatalf("read %s: %v", pair.path, err)
		}
		if string(raw) != pair.content {
			t.Fatalf("%s content = %q", pair.path, raw)
		}
		assertReadOnly(t, filepath.Join(workspace, filepath.FromSlash(pair.path)))
	}
	if _, err := os.Stat(filepath.Join(workspace, ".t3", "dependencies", "producer", "other.txt")); !os.IsNotExist(err) {
		t.Fatalf("unselected artifact materialized: %v", err)
	}
}

func TestMaterializeDependenciesRejectsChecksumMismatchWithoutPartialPublish(t *testing.T) {
	storage := t.TempDir()
	workspace := t.TempDir()
	artifact := storedOutputArtifact(t, storage, "producer-id", "attempt-p", "result.txt", "content")
	artifact.SHA256 = strings.Repeat("0", 64)
	task := domain.Task{
		ID: "consumer-id", Name: "consumer",
		Needs:            []string{"producer"},
		DependencyInputs: map[string][]string{"producer": {"result.txt"}},
	}

	_, err := MaterializeDependencies(
		workspace,
		storage,
		"run-1",
		task,
		[]domain.Task{{ID: "producer-id", Name: "producer"}, task},
		[]domain.Artifact{artifact},
	)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("error = %v, want checksum mismatch", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, ".t3", "dependencies")); !os.IsNotExist(err) {
		t.Fatalf("partial dependency tree published: %v", err)
	}
}

func TestMaterializeDependenciesRejectsAgentControlledMetadataSymlink(t *testing.T) {
	storage := t.TempDir()
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspace, ".t3")); err != nil {
		t.Fatal(err)
	}
	artifact := storedOutputArtifact(t, storage, "producer-id", "attempt-p", "result.txt", "content")
	task := domain.Task{
		ID: "consumer-id", Name: "consumer",
		Needs:            []string{"producer"},
		DependencyInputs: map[string][]string{"producer": {"result.txt"}},
	}
	_, err := MaterializeDependencies(
		workspace,
		storage,
		"run-1",
		task,
		[]domain.Task{{ID: "producer-id", Name: "producer"}, task},
		[]domain.Artifact{artifact},
	)
	if err == nil || !strings.Contains(err.Error(), ".t3 is not a real directory") {
		t.Fatalf("error = %v, want metadata symlink rejection", err)
	}
	if entries, readErr := os.ReadDir(outside); readErr != nil || len(entries) != 0 {
		t.Fatalf("outside directory changed: entries=%v error=%v", entries, readErr)
	}
}

func TestMaterializeDependenciesRejectsMissingArtifact(t *testing.T) {
	task := domain.Task{
		ID: "consumer-id", Name: "consumer",
		Needs:            []string{"producer"},
		DependencyInputs: map[string][]string{"producer": {"missing.txt"}},
	}
	_, err := MaterializeDependencies(
		t.TempDir(),
		t.TempDir(),
		"run-1",
		task,
		[]domain.Task{{ID: "producer-id", Name: "producer"}, task},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), `missing output "missing.txt"`) {
		t.Fatalf("error = %v, want missing output", err)
	}
}

func artifactTestTask() domain.Task {
	return domain.Task{
		ID: "task-build", WorkflowID: "workflow-1", Name: "build",
		Outputs:      []domain.ArtifactDeclaration{{Name: "dist/result.txt"}},
		Verification: []string{"test -f dist/result.txt && printf checked"},
	}
}

func artifactTestAttempt() domain.Attempt {
	return domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-build", Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlRunning,
	}
}

func testFinalizer(storage string) AttemptFinalizer {
	nextID := 0
	return AttemptFinalizer{
		StorageRoot: storage,
		Now: func() time.Time {
			return time.Date(2026, 9, 10, 15, 0, nextID, 0, time.UTC)
		},
		NewID: func(kind string) string {
			nextID++
			return kind + "-" + string(rune('0'+nextID))
		},
	}
}

func storedOutputArtifact(t *testing.T, storage, taskID, attemptID, name, content string) domain.Artifact {
	t.Helper()
	storagePath := filepath.ToSlash(filepath.Join("runs", "run-1", taskID, attemptID, "artifacts", "outputs", name))
	fullPath := filepath.Join(storage, filepath.FromSlash(storagePath))
	writeTestFile(t, storage, filepath.ToSlash(strings.TrimPrefix(fullPath, storage+string(filepath.Separator))), content)
	hash := sha256.Sum256([]byte(content))
	return domain.Artifact{
		ID:            "artifact-" + strings.ReplaceAll(name, "/", "-"),
		WorkflowRunID: "run-1", TaskID: taskID, AttemptID: attemptID,
		Kind: domain.ArtifactOutput, Name: filepath.ToSlash(name), MediaType: mediaType(name),
		Size: int64(len(content)), SHA256: hex.EncodeToString(hash[:]), StoragePath: storagePath,
		Producer: taskID, CreatedAt: dagTestTime,
	}
}

func findArtifact(t *testing.T, artifacts []domain.Artifact, kind domain.ArtifactKind) domain.Artifact {
	t.Helper()
	for _, artifact := range artifacts {
		if artifact.Kind == kind {
			return artifact
		}
	}
	t.Fatalf("artifact kind %q not found in %#v", kind, artifacts)
	return domain.Artifact{}
}

func readStoredArtifact(t *testing.T, storage string, artifact domain.Artifact) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(artifact.StoragePath)))
	if err != nil {
		t.Fatalf("read artifact %s: %v", artifact.Name, err)
	}
	return raw
}

func assertStoredContent(t *testing.T, storage string, artifact domain.Artifact, want string) {
	t.Helper()
	if got := string(readStoredArtifact(t, storage, artifact)); got != want {
		t.Fatalf("artifact %s content = %q, want %q", artifact.Name, got, want)
	}
}

func assertReadOnly(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if info.Mode().Perm()&0o222 != 0 {
		t.Fatalf("%s mode = %o, want read-only", path, info.Mode().Perm())
	}
}

func cleanupImmutable(t *testing.T, path string) {
	t.Helper()
	if path == "" {
		return
	}
	t.Cleanup(func() {
		if err := removeIngestedTree(path); err != nil && !os.IsNotExist(err) {
			t.Errorf("clean immutable tree %s: %v", path, err)
		}
	})
}

func writeTestFile(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent for %s: %v", relative, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", relative, err)
	}
}
