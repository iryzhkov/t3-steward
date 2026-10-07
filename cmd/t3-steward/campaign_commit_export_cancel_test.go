package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// An interrupted export (the CLI turns SIGINT and SIGTERM into cancellation)
// must end promptly even while the bundle stream is stalled, and must leave
// neither the destination nor its temporary file behind.
func TestCampaignCommitExportCancellationLeavesNoFiles(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "repair.bundle")
	stalled, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := campaignCLI{stdout: io.Discard, exportCommit: func(context.Context, backlogadmin.CommitExportRequest) (backlogadmin.ArtifactContent, error) {
		go func() {
			_, _ = writer.Write([]byte("partial"))
			cancel()
		}()
		return backlogadmin.ArtifactContent{Provenance: &backlog.CommitProvenance{WorkflowRunID: "run-1", TaskID: "task-1", Name: "repair", Commit: strings.Repeat("a", 40), Base: strings.Repeat("b", 40)}, Metadata: backlogadmin.ArtifactMetadata{Size: 1 << 20, SHA256: strings.Repeat("c", 64)}, Content: stalled}, nil
	}}
	done := make(chan error, 1)
	go func() { done <- c.run(ctx, []string{"commit", "export", "run-1/task-1/repair", "--bundle", path}) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled export returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled export still blocked on a stalled stream")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cancelled export left files: %v %v", entries, err)
	}
}
