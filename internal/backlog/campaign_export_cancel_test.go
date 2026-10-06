package backlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Cancelling an export while Git verifies the pack is a cancellation, not a
// corrupt bundle, so callers can tell the two apart.
func TestExportCommitBundleReportsCancellationDuringVerification(t *testing.T) {
	storage := t.TempDir()
	repository := newGitFixture(t)
	p := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	raw, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(p.bundle.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	// A git that stalls in unpack-objects and is otherwise the real one.
	bin := t.TempDir()
	script := "#!/bin/sh\nfor a in \"$@\"; do [ \"$a\" = unpack-objects ] && exec sleep 30; done\nexec " + git + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := ExportCommitBundle(ctx, p.provenance, "repair", bytes.NewReader(raw), DefaultCommitBundleMaxBytes)
	if err == nil {
		b.Close()
		t.Fatal("export succeeded with a stalled verifier")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation reported as %v", err)
	}
}

// Bytes after the pack checksum are refused whatever their length, including
// lengths Git does not read into its buffer before it stops.
func TestExportCommitBundleRefusesTrailingDataOfAnyLength(t *testing.T) {
	storage := t.TempDir()
	repository := newGitFixture(t)
	p := produceCommit(t, newCommitWorker(t, storage), repository, storage, produceOptions{})
	raw, err := os.ReadFile(filepath.Join(storage, filepath.FromSlash(p.bundle.StoragePath)))
	if err != nil {
		t.Fatal(err)
	}
	provenance := p.provenance
	provenance.Bundle = nil
	// unpack-objects echoes only the trailing bytes already in its 4096-byte
	// buffer, so a pack whose length is a multiple of 4096 hid them entirely.
	aligned, alignedProvenance := alignedPackBundle(t)
	forged := append(append([]byte(nil), aligned...), []byte("SMUGGLED TRAILING DATA")...)
	if b, err := ExportCommitBundle(context.Background(), alignedProvenance, "repair", bytes.NewReader(forged), DefaultCommitBundleMaxBytes); err == nil {
		b.Close()
		t.Fatal("accepted data after a 4096-byte-aligned pack")
	}
	if b, err := ExportCommitBundle(context.Background(), alignedProvenance, "repair", bytes.NewReader(aligned), DefaultCommitBundleMaxBytes); err != nil {
		t.Fatalf("refused the aligned bundle itself: %v", err)
	} else {
		b.Close()
	}
	for length := 1; length <= 1<<20; length *= 2 {
		for _, extra := range []int{length - 1, length, length + 1} {
			if extra <= 0 {
				continue
			}
			forged := append(append([]byte(nil), raw...), bytes.Repeat([]byte{'j'}, extra)...)
			b, err := ExportCommitBundle(context.Background(), provenance, "repair", bytes.NewReader(forged), DefaultCommitBundleMaxBytes)
			if err == nil {
				b.Close()
				t.Fatalf("accepted %d bytes after the pack", extra)
			}
		}
	}
}

// alignedPackBundle builds a bundle whose pack is exactly 4096 bytes by
// resizing one incompressible file until the pack lands on the boundary. A
// size whose pack straddles it is retried with a longer commit message.
func alignedPackBundle(t *testing.T) ([]byte, CommitProvenance) {
	t.Helper()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	ref := CampaignRef("run-aligned", "task-aligned", "out")
	size, message := 3900, "noise"
	tried := map[int]bool{}
	for attempt := 0; attempt < 80; attempt++ {
		if tried[size] {
			message += " " + fmt.Sprint(attempt)
			tried = map[int]bool{}
		}
		tried[size] = true
		data := make([]byte, size)
		rand.New(rand.NewSource(int64(size))).Read(data)
		if err := os.WriteFile(filepath.Join(repository, "noise"), data, 0o644); err != nil {
			t.Fatal(err)
		}
		gitRun(t, repository, "add", "noise")
		gitRun(t, repository, "commit", "-q", "-m", message)
		commit := gitOutput(t, repository, "rev-parse", "HEAD")
		gitRun(t, repository, "update-ref", ref, commit)
		path := filepath.Join(t.TempDir(), "aligned.bundle")
		gitRun(t, repository, "bundle", "create", "-q", path, "^"+base, ref)
		gitRun(t, repository, "reset", "-q", "--hard", base)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		packLength := len(raw) - bytes.Index(raw, []byte("\n\n")) - 2
		if packLength == 4096 {
			return raw, CommitProvenance{WorkflowRunID: "run-aligned", TaskID: "task-aligned", Name: "out", Base: base, Commit: commit, Ref: ref}
		}
		t.Logf("noise of %d bytes gives a %d-byte pack", size, packLength)
		step := 4096 - packLength
		if step > 64 || step < -64 {
			step /= 2
		}
		if step == 0 {
			step = 1
		}
		size += step
	}
	t.Fatal("could not build a 4096-byte pack")
	return nil, CommitProvenance{}
}
