package main

import (
	"bytes"
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCampaignCommitExportFetchIntoFreshRepository(t *testing.T) {
	fixture := newCandidateFixture(t)
	// The fixture's origin has only the base; the candidate remains local.
	candidateGit(t, "-C", fixture.root, "update-ref", "refs/heads/exported-repair", fixture.head)
	bundle := filepath.Join(t.TempDir(), "repair.bundle")
	candidateGit(t, "-C", fixture.root, "bundle", "create", bundle, "refs/heads/exported-repair", "^"+fixture.base)
	raw, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(t.TempDir(), "fresh")
	candidateGit(t, "clone", "-q", fixture.remote, repository)
	var out bytes.Buffer
	c := campaignCLI{stdout: &out, exportCommit: func(_ context.Context, request backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error) {
		if request.Branch != "exported-repair" {
			t.Fatalf("request = %+v", request)
		}
		return backlogadmin.ArtifactContent{
			Provenance: &backlog.CommitProvenance{WorkflowRunID: "run", TaskID: "task", Name: "repair", Commit: fixture.head, Base: fixture.base},
			Metadata:   backlogadmin.ArtifactMetadata{Size: int64(len(raw)), SHA256: exportCLIHash(raw)}, Content: io.NopCloser(bytes.NewReader(raw)),
		}, nil
	}}
	args := []string{"commit", "export", "run/task/repair", "--into", repository, "--branch", "exported-repair"}
	if err := c.run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if got := candidateGit(t, "-C", repository, "rev-parse", "refs/heads/exported-repair"); got != fixture.head {
		t.Fatalf("head = %s", got)
	}
	for _, want := range []string{fixture.head, fixture.base, exportCLIHash(raw), "fetched refs/heads/exported-repair"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output = %s", out.String())
		}
	}
	if err := c.run(context.Background(), args); err == nil {
		t.Fatal("overwrote branch")
	}
	if err := c.run(context.Background(), []string{"commit", "export", "run/task/repair", "--into", repository, "--bundle", bundle}); err == nil {
		t.Fatal("accepted mutually exclusive options")
	}
}
