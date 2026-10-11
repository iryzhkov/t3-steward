package backlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExpandedExportRefusesUnrelatedBaseAndPrerequisite(t *testing.T) {
	repository, storage := newGitFixture(t), t.TempDir()
	produced := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	gitRun(t, repository, "checkout", "-b", "unrelated")
	writeGitFile(t, repository, "unrelated.txt", "unrelated\n")
	gitRun(t, repository, "add", "unrelated.txt")
	gitRun(t, repository, "commit", "-m", "unrelated")
	unrelated := gitOutput(t, repository, "rev-parse", "HEAD")
	raw, err := os.ReadFile(filepath.Join(storage, produced.bundle.StoragePath))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("base", func(t *testing.T) {
		p := produced.provenance
		p.Base = unrelated
		exported, err := ExportCommitBundle(context.Background(), p, "repair", bytes.NewReader(raw), DefaultCommitBundleMaxBytes)
		if err == nil {
			exported.Close()
			t.Fatal("export accepted unrelated recorded base")
		}
		if !strings.Contains(err.Error(), "does not descend") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("prerequisite", func(t *testing.T) {
		damaged := bytes.Replace(raw, []byte("-"+produced.base+" "), []byte("-"+unrelated+" "), 1)
		if bytes.Equal(damaged, raw) {
			t.Fatal("fixture has no prerequisite to replace")
		}
		p := produced.provenance
		binding := *p.Bundle
		binding.SHA256, binding.Size = fmt.Sprintf("%x", sha256.Sum256(damaged)), int64(len(damaged))
		p.Bundle = &binding
		exported, err := ExportCommitBundle(context.Background(), p, "repair", bytes.NewReader(damaged), DefaultCommitBundleMaxBytes)
		if err == nil {
			exported.Close()
			t.Fatal("export accepted unrelated prerequisite even with matching digest")
		}
		if !strings.Contains(err.Error(), "ancestry") {
			t.Fatalf("error = %v", err)
		}
	})
}
