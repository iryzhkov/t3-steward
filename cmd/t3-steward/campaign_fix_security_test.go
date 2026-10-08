package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
)

func TestCampaignFixSecurityArtifactBinding(t *testing.T) {
	valid := backlogadmin.ArtifactMetadata{ID: "artifact", WorkflowRunID: "run", TaskID: "p", AttemptID: "latest", Kind: domain.ArtifactOutput, Name: "implementation"}
	task := backlogadmin.TaskDetail{Task: domain.Task{ID: "p", RunID: "run", Name: "producer"}, Attempt: &domain.Attempt{ID: "latest"}}
	for _, change := range []struct {
		name string
		edit func(*backlogadmin.ArtifactMetadata)
	}{
		{"missing-attempt", func(m *backlogadmin.ArtifactMetadata) { m.AttemptID = "" }},
		{"previous-attempt", func(m *backlogadmin.ArtifactMetadata) { m.AttemptID = "previous" }},
		{"wrong-run", func(m *backlogadmin.ArtifactMetadata) { m.WorkflowRunID = "other" }},
		{"wrong-task", func(m *backlogadmin.ArtifactMetadata) { m.TaskID = "other" }},
		{"missing-id", func(m *backlogadmin.ArtifactMetadata) { m.ID = "" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			m := valid
			change.edit(&m)
			task.Artifacts = []backlogadmin.Artifact{{Metadata: m}}
			if _, err := fixArtifact(task, "implementation"); err == nil {
				t.Fatal("unbound output accepted")
			}
		})
	}
	task.Artifacts = []backlogadmin.Artifact{{Metadata: valid}, {Metadata: valid}}
	if _, err := fixArtifact(task, "implementation"); err == nil {
		t.Fatal("ambiguous outputs accepted")
	}
	task.Artifacts = []backlogadmin.Artifact{{Metadata: valid}}
	if _, err := fixArtifact(task, "implementation"); err != nil {
		t.Fatal(err)
	}
}

func TestCampaignFixSecurityInputCannotStandInForOutput(t *testing.T) {
	task := backlogadmin.TaskDetail{Task: domain.Task{ID: "p", RunID: "run", Name: "producer"}, Attempt: &domain.Attempt{ID: "latest"}, Artifacts: []backlogadmin.Artifact{{Metadata: backlogadmin.ArtifactMetadata{ID: "artifact", WorkflowRunID: "run", TaskID: "p", Name: "implementation", AttemptID: "latest", Kind: domain.ArtifactInput}}}}
	if got, err := fixArtifact(task, "implementation"); err == nil {
		t.Fatalf("accepted input as declared output: %+v", got)
	}
}

func TestCampaignFixSecurityUnboundCommitArtifact(t *testing.T) {
	cli, _ := fixCommandFixture(t)
	old := cli.detail
	cli.detail = func(ctx context.Context, run string) (backlogadmin.WorkflowDetail, error) {
		d, err := old(ctx, run)
		d.Tasks[0].Artifacts[0].Metadata.AttemptID = ""
		return d, err
	}
	got, err := cli.resolve(context.Background(), campaignFixArgs{node: domain.NodeRef{RunID: "run", TaskID: "review"}})
	if err == nil {
		t.Fatalf("accepted commit artifact with no attempt provenance: %+v", got.Commit)
	}
}
