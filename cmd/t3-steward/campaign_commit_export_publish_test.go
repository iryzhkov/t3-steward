package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// A destination filesystem without hard links must still receive the bundle,
// and must still never have an existing file overwritten.
func TestCampaignCommitExportPublishesWithoutHardLinks(t *testing.T) {
	original := linkCommitExport
	linkCommitExport = func(string, string) error {
		return &os.LinkError{Op: "link", Err: syscall.EPERM}
	}
	t.Cleanup(func() { linkCommitExport = original })

	raw := []byte("verified coordinator bundle")
	var beforeReturn func()
	c := campaignCLI{stdout: io.Discard, exportCommit: func(context.Context, backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error) {
		if beforeReturn != nil {
			beforeReturn()
		}
		return backlogadmin.ArtifactContent{Provenance: &backlog.CommitProvenance{WorkflowRunID: "run-1", TaskID: "task-1", Name: "repair", Commit: strings.Repeat("a", 40), Base: strings.Repeat("b", 40)}, Metadata: backlogadmin.ArtifactMetadata{Size: int64(len(raw)), SHA256: exportCLIHash(raw)}, Content: io.NopCloser(bytes.NewReader(raw))}, nil
	}}

	directory := t.TempDir()
	path := filepath.Join(directory, "repair.bundle")
	if err := c.run(context.Background(), []string{"commit", "export", "run-1/task-1/repair", "--bundle", path}); err != nil {
		t.Fatalf("export without hard links: %v", err)
	}
	if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, raw) {
		t.Fatalf("data=%q err=%v", data, err)
	}
	if entries, err := os.ReadDir(directory); err != nil || len(entries) != 1 {
		t.Fatalf("temporary file leak: %v %v", entries, err)
	}

	// A destination created while the bundle was in flight is left untouched.
	racing := filepath.Join(directory, "racing.bundle")
	beforeReturn = func() {
		if err := os.WriteFile(racing, []byte("someone else"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.run(context.Background(), []string{"commit", "export", "run-1/task-1/repair", "--bundle", racing}); err == nil {
		t.Fatal("overwrote a concurrently created destination")
	}
	if data, err := os.ReadFile(racing); err != nil || string(data) != "someone else" {
		t.Fatalf("concurrent destination changed: %q %v", data, err)
	}
}
