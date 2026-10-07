package backlog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestExportCommitBundleRejectsUnsupportedCapabilities(t *testing.T) {
	storage := t.TempDir()
	repository := newGitFixture(t)
	p := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	raw, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(p.bundle.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	p.provenance.Bundle = nil
	for _, header := range []string{
		"# v3 git bundle\n@unsupported-security-capability=1\n",
		"# v3 git bundle\n@object-format=sha256\n",
		"# v3 git bundle\n@object-format=sha1\n@object-format=sha1\n",
		"# v2 git bundle\n@object-format=sha1\n",
		"# v3 git bundle\n",
	} {
		source := bytes.Replace(raw, []byte("# v2 git bundle\n"), []byte(header), 1)
		result, err := ExportCommitBundle(context.Background(), p.provenance, "repair", bytes.NewReader(source), DefaultCommitBundleMaxBytes)
		if err == nil {
			result.Close()
			t.Errorf("accepted unsupported bundle header %q", header)
		}
	}
	source := bytes.Replace(raw, []byte("# v2 git bundle\n"), []byte("# v3 git bundle\n@object-format=sha1\n"), 1)
	result, err := ExportCommitBundle(context.Background(), p.provenance, "repair", bytes.NewReader(source), DefaultCommitBundleMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	gitRun(t, repository, "bundle", "verify", result.Path)
}

func TestExportBranchRejectsNestedLockComponents(t *testing.T) {
	for _, branch := range []string{"allow.lock/bypass", "nested/deeper.lock/leaf"} {
		if err := ValidateExportBranch(branch); err == nil {
			t.Errorf("accepted invalid branch %q", branch)
		}
	}
}
