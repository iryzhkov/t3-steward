package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestCampaignCommitExportWritesDigestVerifiedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "repair.bundle")
	raw := []byte("verified coordinator bundle")
	var out bytes.Buffer
	var got backlogadmin.CommitExportRequest
	c := campaignCLI{stdout: &out, exportCommit: func(_ context.Context, r backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error) {
		got = r
		return backlogadmin.ArtifactContent{Provenance: &backlog.CommitProvenance{WorkflowRunID: "run-1", TaskID: "task-1", Name: "repair", Commit: strings.Repeat("a", 40), Base: strings.Repeat("b", 40)}, Metadata: backlogadmin.ArtifactMetadata{Size: int64(len(raw)), SHA256: exportCLIHash(raw)}, Content: io.NopCloser(bytes.NewReader(raw))}, nil
	}}
	if err := c.run(context.Background(), []string{"commit", "export", "run-1/task-1/repair", "--bundle", path}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, raw) {
		t.Fatalf("data=%q err=%v", data, err)
	}
	if got.Branch != "repair" || !strings.Contains(out.String(), strings.Repeat("a", 40)) || !strings.Contains(out.String(), exportCLIHash(raw)) {
		t.Fatalf("request=%+v output=%q", got, out.String())
	}
	if err := c.run(context.Background(), []string{"commit", "export", "run-1/task-1/repair", "--bundle", path}); err == nil {
		t.Fatal("overwrote existing destination")
	}
}

func TestCampaignCommitExportRefusesBadArgumentsBeforeTransport(t *testing.T) {
	for _, args := range [][]string{
		{"commit", "export", "run/task/name"},
		{"commit", "export", "run/task", "--bundle", "out"},
		{"commit", "export", "run/task/name", "--bundle", "out", "--branch", "../escape"},
		{"commit", "export", "run/task/name", "--bundle", "out", "--branch", ""},
		{"commit", "export", "run/task/name", "--bundle", "out", "--bundle", "other"},
		{"commit", "export", "run/task/name", "--bundle", "out", "--json"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			c := campaignCLI{exportCommit: func(context.Context, backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error) {
				t.Fatal("transport reached")
				return backlogadmin.ArtifactContent{}, errors.New("unexpected")
			}}
			if err := c.run(context.Background(), args); err == nil {
				t.Fatal("bad args accepted")
			}
		})
	}
}

func TestCampaignCommitExportCorruptionDoesNotPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.bundle")
	c := campaignCLI{exportCommit: func(context.Context, backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error) {
		return backlogadmin.ArtifactContent{Provenance: &backlog.CommitProvenance{WorkflowRunID: "run", TaskID: "task", Name: "name", Commit: strings.Repeat("a", 40), Base: strings.Repeat("b", 40)}, Metadata: backlogadmin.ArtifactMetadata{Size: 5, SHA256: strings.Repeat("a", 64)}, Content: io.NopCloser(strings.NewReader("bad"))}, nil
	}}
	if err := c.run(context.Background(), []string{"commit", "export", "run/task/name", "--bundle", path}); err == nil {
		t.Fatal("corrupt export accepted")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial destination: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary file leak: %v %v", entries, err)
	}
}

func TestCampaignCommitExportHelp(t *testing.T) {
	var out bytes.Buffer
	if handled, err := admitCampaignHelp(&out, []string{"commit", "export", "--help", "full"}); err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	for _, word := range []string{"--bundle", "--branch", "prerequisite", "read-only"} {
		if !strings.Contains(out.String(), word) {
			t.Fatalf("missing %s: %s", word, out.String())
		}
	}
}
