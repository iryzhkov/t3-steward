package backlog

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// largeLines is a multi-kilobyte file body; changing one line of it makes Git
// store the new blob as a delta against the base blob.
func largeLines(changed int) string {
	var body strings.Builder
	for line := 1; line <= 20000; line++ {
		if line == changed {
			body.WriteString("changed\n")
			continue
		}
		fmt.Fprintf(&body, "%d\n", line)
	}
	return body.String()
}

// Git bundle packs are thin: a commit that modifies an existing file is sent as
// a delta against an object only the prerequisite holds. Export must accept
// such a pack on both source paths, and the result must verify at the base.
func TestExportCommitBundleAcceptsThinPackOfRealisticCommit(t *testing.T) {
	ctx := context.Background()
	storage := t.TempDir()
	repository := newGitFixture(t)
	writeGitFile(t, repository, "large.txt", largeLines(0))
	gitRun(t, repository, "add", "large.txt")
	gitRun(t, repository, "commit", "-m", "large base file")
	producer := newCommitWorker(t, storage)
	p := produceCommit(t, producer, repository, storage, produceOptions{
		edit: func(t *testing.T, workspace string) {
			writeGitFile(t, workspace, "large.txt", largeLines(777))
			gitRun(t, workspace, "add", "large.txt")
		},
	})
	if p.bundle == nil {
		t.Fatal("producer retained no commit bundle")
	}
	retained, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(p.bundle.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}

	// The worker transport regenerates the bundle from the producer's ref store.
	transportPath, err := producer.refs.ExportPublishedBundle(ctx, p.provenance, DefaultCommitBundleMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(filepath.Dir(transportPath))
	transport, err := os.ReadFile(transportPath)
	if err != nil {
		t.Fatal(err)
	}

	workerProvenance := p.provenance
	workerProvenance.Bundle = nil
	for _, source := range []struct {
		name       string
		provenance CommitProvenance
		raw        []byte
	}{{"retained", p.provenance, retained}, {"worker", workerProvenance, transport}} {
		t.Run(source.name, func(t *testing.T) {
			assertThinPack(t, source.raw, p.base)
			b, err := ExportCommitBundle(ctx, source.provenance, "fix/thin", bytes.NewReader(source.raw), DefaultCommitBundleMaxBytes)
			if err != nil {
				t.Fatalf("export thin bundle: %v", err)
			}
			defer b.Close()
			clone := filepath.Join(t.TempDir(), "clone")
			gitRun(t, filepath.Dir(clone), "clone", "--quiet", "--no-checkout", repository, clone)
			gitRun(t, clone, "reset", "--quiet", "--soft", p.base)
			gitRun(t, clone, "bundle", "verify", b.Path)
			gitRun(t, clone, "fetch", "--quiet", b.Path, "refs/heads/fix/thin:refs/heads/fix/thin")
			if got := gitOutput(t, clone, "rev-parse", "refs/heads/fix/thin"); got != p.commit {
				t.Fatalf("exported branch=%s want %s", got, p.commit)
			}
		})
	}

	// Tolerating thin packs must not mean tolerating damaged ones.
	for name, mutate := range map[string]func([]byte) []byte{
		"pack body": func(raw []byte) []byte {
			broken := append([]byte(nil), raw...)
			broken[len(broken)-60] ^= 0x40
			return broken
		},
		"checksum": func(raw []byte) []byte {
			broken := append([]byte(nil), raw...)
			broken[len(broken)-1] ^= 1
			return broken
		},
		"trailing data": func(raw []byte) []byte { return append(append([]byte(nil), raw...), []byte("junk")...) },
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			b, err := ExportCommitBundle(ctx, workerProvenance, "fix/thin", bytes.NewReader(mutate(transport)), DefaultCommitBundleMaxBytes)
			if err == nil {
				b.Close()
				t.Fatal("accepted damaged thin pack")
			}
		})
	}
}

// assertThinPack proves the fixture exercises the defect: Git itself cannot
// index the pack without the base objects.
func assertThinPack(t *testing.T, raw []byte, base string) {
	t.Helper()
	end := bytes.Index(raw, []byte("\n\n"))
	if end < 0 {
		t.Fatal("bundle has no header terminator")
	}
	if !bytes.Contains(raw[:end], []byte("-"+base)) {
		t.Fatalf("bundle lacks base prerequisite %s", base)
	}
	repo := filepath.Join(t.TempDir(), "empty.git")
	gitRun(t, filepath.Dir(repo), "init", "--bare", "--quiet", repo)
	command := exec.Command("git", "--git-dir", repo, "index-pack", "--stdin")
	command.Stdin = bytes.NewReader(raw[end+2:])
	if out, err := command.CombinedOutput(); err == nil || !strings.Contains(string(out), "unresolved delta") {
		t.Fatalf("fixture pack is not thin: %v: %s", err, out)
	}
}
