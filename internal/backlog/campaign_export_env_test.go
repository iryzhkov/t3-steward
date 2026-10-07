package backlog

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The verifier repository must see only the objects in the received pack. An
// inherited object-store variable would let cat-file find the declared commit
// elsewhere and accept a pack that does not contain it.
func TestExportCommitBundleIgnoresInheritedGitObjectStores(t *testing.T) {
	storage := t.TempDir()
	repository := newGitFixture(t)
	p := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	raw, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(p.bundle.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	retained := filepath.Join(t.TempDir(), "retained.bundle")
	if err := os.WriteFile(retained, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	// Give the fixture repository the declared commit, as a worker cache would.
	gitRun(t, repository, "fetch", "--quiet", retained, p.provenance.Ref+":refs/heads/declared")

	// A forged bundle: the genuine header, but a pack holding only one blob.
	blob := gitOutput(t, repository, "rev-parse", p.base+":version.txt")
	packObjects := exec.Command("git", "-C", repository, "pack-objects", "--stdout", "-q")
	packObjects.Stdin = bytes.NewReader([]byte(blob + "\n"))
	pack, err := packObjects.Output()
	if err != nil {
		t.Fatal(err)
	}
	end := bytes.Index(raw, []byte("\n\n"))
	forged := append(append([]byte(nil), raw[:end+2]...), pack...)
	provenance := p.provenance
	provenance.Bundle = nil

	objects := filepath.Join(repository, ".git", "objects")
	for name, value := range map[string]string{
		"GIT_ALTERNATE_OBJECT_DIRECTORIES": objects,
		"GIT_OBJECT_DIRECTORY":             objects,
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			b, err := ExportCommitBundle(context.Background(), provenance, "forged", bytes.NewReader(forged), DefaultCommitBundleMaxBytes)
			if err == nil {
				b.Close()
				t.Fatal("accepted a pack without the declared commit")
			}
		})
	}
}
