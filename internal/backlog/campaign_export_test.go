package backlog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExportCommitBundleSingleBranchAndPrerequisite(t *testing.T) {
	storage := t.TempDir()
	repository := newGitFixture(t)
	producer := newCommitWorker(t, storage)
	p := produceCommit(t, producer, repository, storage, produceOptions{})
	raw, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(p.bundle.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	for _, branch := range []string{"repair", "fix/h3"} {
		b, err := ExportCommitBundle(context.Background(), p.provenance, branch, bytes.NewReader(raw), DefaultCommitBundleMaxBytes)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		gitRun(t, repository, "bundle", "verify", b.Path)
		heads := gitOutput(t, repository, "bundle", "list-heads", b.Path)
		if heads != p.commit+" refs/heads/"+branch {
			t.Fatalf("heads=%q", heads)
		}
		hs, prerequisites, err := readBundleHeader(b.Path)
		if err != nil || len(hs) != 1 || len(prerequisites) != 1 || prerequisites[0] != p.base {
			t.Fatalf("header=%v prerequisites=%v err=%v", hs, prerequisites, err)
		}
		sum, size := hashExportTest(t, b.Path)
		if sum != b.SHA256 || size != b.Size {
			t.Fatalf("digest/size mismatch")
		}
	}
}

func TestExportCommitBundleRefusesMalformedAndCorruptSources(t *testing.T) {
	storage := t.TempDir()
	repository := newGitFixture(t)
	p := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	raw, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(p.bundle.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, branch string
		raw          []byte
		limit        int64
	}{
		{"branch", "../escape", raw, DefaultCommitBundleMaxBytes},
		{"option", "-bad", raw, DefaultCommitBundleMaxBytes},
		{"empty", "", raw, DefaultCommitBundleMaxBytes},
		{"huge", strings.Repeat("a", 1025), raw, DefaultCommitBundleMaxBytes},
		{"limit", "repair", raw, int64(len(raw) - 1)},
		{"corrupt", "repair", append(append([]byte(nil), raw[:len(raw)-1]...), raw[len(raw)-1]^1), DefaultCommitBundleMaxBytes},
		{"truncated", "repair", raw[:len(raw)-8], DefaultCommitBundleMaxBytes},
		{"wrong commit", "repair", bytes.Replace(raw, []byte(p.commit), []byte(strings.Repeat("a", 40)), 1), DefaultCommitBundleMaxBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := ExportCommitBundle(context.Background(), p.provenance, tc.branch, bytes.NewReader(tc.raw), tc.limit)
			if err == nil {
				b.Close()
				t.Fatal("accepted invalid export")
			}
		})
	}
	// Without retained metadata, the worker transport still must reject a corrupt pack.
	p.provenance.Bundle = nil
	broken := append([]byte(nil), raw...)
	broken[len(broken)-1] ^= 1
	b, err := ExportCommitBundle(context.Background(), p.provenance, "repair", bytes.NewReader(broken), DefaultCommitBundleMaxBytes)
	if err == nil {
		b.Close()
		t.Fatal("accepted corrupt transport pack")
	}
}

func hashExportTest(t *testing.T, path string) (string, int64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmtExportHash(data), int64(len(data))
}
