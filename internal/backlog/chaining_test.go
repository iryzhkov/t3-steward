package backlog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestChainingManifestOffline(t *testing.T) {
	raw := []byte("version: 2\nname: successor\nenvironment: {project: repo, ref: 'campaign:source-run/producer/repair'}\nroutes: [{instance: codex, model: m}]\ntasks:\n  consumer:\n    prompt_file: prompt.md\n    inputs_from_run:\n      source-run/producer: [report.txt]\n")
	m, err := ParseManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	task := m.Tasks["consumer"]
	if len(task.Needs) != 1 || task.Needs[0] != "source-run/producer" || len(task.InputsFrom["source-run/producer"]) != 2 {
		t.Fatalf("lowered task: %+v", task)
	}
	for _, bad := range []string{"campaign:run/task", "campaign:run/__sink/name", "campaign:run/task/../name", "campaign:run/task/.lock"} {
		if _, _, err := ParseCampaignReference(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	raw = []byte(strings.Replace(string(raw), "source-run/producer: [report.txt]", "producer: [report.txt]", 1))
	if _, err := ParseManifest(raw); err == nil {
		t.Fatal("accepted local inputs_from_run")
	}
}

func TestSuccessorCampaignStartsAtRetainedCommit(t *testing.T) {
	repository, storage := newGitFixture(t), t.TempDir()
	producer, consumer := newCommitWorker(t, storage), newCommitWorker(t, storage)
	produced := produceCommit(t, producer, repository, storage, produceOptions{})
	// The commit exists only in the producer checkout/store and the retained bundle.
	if got := gitOutput(t, repository, "rev-parse", "HEAD"); got != produced.base {
		t.Fatal("producer pushed its commit")
	}
	namespace := externalProducerNamespace("run-1", domain.Task{ID: "task-producer", Name: "producer"})
	task := workspaceTask("consumer", "consumer")
	task.Needs = []string{namespace}
	task.DependencyInputs = map[string][]string{namespace: {"repair"}}
	request := workspaceRequest(repository, "campaign:run-1/producer/repair", task, "attempt-successor")
	request.WorkflowRunID, request.Attempt.WorkflowRunID = "run-2", "run-2"
	record := produced.record
	record.WorkflowRunID, record.TaskID, record.AttemptID = "run-2", "task-producer", ""
	request.DependencyTasks = []domain.Task{{ID: "task-producer", Name: namespace, Outputs: []domain.ArtifactDeclaration{{Name: "repair", Commit: &domain.CommitOutput{}}}}}
	request.DependencyArtifacts = []domain.Artifact{record}
	request.DependencySources = map[string]DependencySource{namespace: {WorkflowRunID: "run-1", TaskID: "task-producer", AttemptID: "attempt-1"}}
	request.CommitBundles = map[string]CommitBundleDelivery{produced.provenance.Ref: produced.delivery(t, nil)}
	prepared, err := consumer.preparer.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, prepared.RootDir)
	if prepared.Commit != produced.commit || gitOutput(t, prepared.WorkspaceDir, "rev-parse", "HEAD") != produced.commit {
		t.Fatal("successor did not start at exact commit")
	}
	if got := readTestFile(t, prepared.DependenciesDir, namespace+"/repair"); !strings.Contains(got, CampaignCommitRecordVersion) {
		t.Fatal("provenance missing")
	}
	// A damaged pinned input is refused by preparation, before setup or an agent.
	request.Attempt.ID = "attempt-corrupt"
	request.DependencyArtifacts[0].SHA256 = strings.Repeat("0", 64)
	if _, err := consumer.preparer.Prepare(context.Background(), request); err == nil || !strings.Contains(err.Error(), "repair") {
		t.Fatalf("corrupt input error = %v", err)
	}
}

func TestInputsFromRunRetainsDigestAndRefusesChangedBytes(t *testing.T) {
	store, manifest, target := externalInputFixture(domain.ProgressSucceeded)
	manifest.Tasks["consumer"] = ManifestTask{InputsFromRun: map[string][]string{"source-run/producer": {"report.txt"}}}
	applyChainingDefaults(&manifest)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source/report.txt"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	store.records.Artifacts[0].SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte("changed")))
	ingester := BundleIngester{Store: store, StorageRoot: root}
	if err := ingester.retainExternalInputs(context.Background(), manifest, &target, "target-run"); err != nil {
		t.Fatal(err)
	}
	if target.Artifacts[0].SHA256 != store.records.Artifacts[0].SHA256 {
		t.Fatal("submission did not pin digest")
	}
	if err := os.WriteFile(filepath.Join(root, "source/report.txt"), []byte("damaged"), 0600); err != nil {
		t.Fatal(err)
	}
	task := target.Tasks[0]
	request := workspaceRequest(newGitFixture(t), "main", task, "attempt-damaged")
	request.WorkflowRunID, request.Attempt.WorkflowRunID = "target-run", "target-run"
	request.DependencyArtifacts = target.Artifacts
	marker := filepath.Join(root, "setup-started")
	request.Environment.Setup.Commands = []string{"touch " + marker}
	_, err := newCommitWorker(t, root).preparer.Prepare(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "report.txt") || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("worker mismatch error = %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("setup ran with damaged input")
	}
	err = ingester.retainExternalInputs(context.Background(), manifest, &target, "target-run")
	if err == nil || !strings.Contains(err.Error(), "source-run/producer/report.txt") || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("error = %v", err)
	}
}
