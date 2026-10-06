package main

import (
	"slices"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// campaign check asks about a commit consumer with the capability placement
// will require of it, so a worker that could never obtain the commit is
// reported with that capability named instead of being counted as a candidate.
func TestCampaignCheckAsksForTheCommitBundleCapabilityOfACommitConsumer(t *testing.T) {
	plan := campaign.Plan{
		Environment: campaign.Environment{Project: "t3-steward", Type: "git", Ref: "main"},
		Tasks: []campaign.Task{
			{Name: "implement", Commits: []campaign.Commit{{Name: "repair"}}, Outputs: []string{"notes.md"}},
			{Name: "review", InputsFrom: []campaign.Binding{{Producer: "implement", Artifacts: []string{"repair"}}}},
			{Name: "summarize", InputsFrom: []campaign.Binding{{Producer: "implement", Artifacts: []string{"notes.md"}}}},
		},
	}
	request, err := campaignViabilityRequest(plan, 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range request.Tasks {
		want := task.Name == "review"
		if got := slices.Contains(task.Capabilities, workerproto.PackageCapabilityCommitBundle); got != want {
			t.Fatalf("%s capabilities = %v, want commit bundle capability %v", task.Name, task.Capabilities, want)
		}
	}
}

// The bundle travels to the consuming worker with the package's other objects,
// over the same download, so no new channel is involved. A bundle the package
// names as omitted adds nothing to the download.
func TestExecutionPackageObjectsIncludeCommitBundles(t *testing.T) {
	bundle := workerproto.ArtifactObject{ID: "bundle-1", Path: workerproto.CommitBundlePath("run-1", "task-producer", "repair")}
	objects := executionPackageObjects(workerproto.ExecutionPackage{
		Prompt: workerproto.ArtifactObject{ID: "prompt-1"},
		CommitBundles: []workerproto.CommitBundleInput{
			{WorkflowRunID: "run-1", TaskID: "task-producer", Name: "repair", Bundle: &bundle},
			{WorkflowRunID: "run-1", TaskID: "task-producer", Name: "followup", Omitted: "over the total"},
		},
	})
	if len(objects) != 2 || objects[1] != bundle {
		t.Fatalf("delivered objects = %+v", objects)
	}
}
