package backlog

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMalformedRecognizedCommitCannotBecomeOrdinaryDependency(t *testing.T) {
	dir := t.TempDir()
	p := CommitProvenance{Version: CampaignCommitRecordVersion, WorkflowRunID: "r", TaskID: "t", Name: "candidate", Repository: "repo", Base: strings.Repeat("a", 40), Commit: strings.Repeat("a", 40), Ref: "refs/heads/spoof"}
	raw, _ := json.Marshal(p)
	if err := os.WriteFile(filepath.Join(dir, "candidate"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	preparer := WorkspacePreparer{}
	if err := preparer.resolveDependencyCommits(context.Background(), dir, t.TempDir(), WorkspacePreparation{WorkflowRunID: "r"}, nil); err == nil {
		t.Fatal("malformed commit silently became ordinary input")
	}
}
