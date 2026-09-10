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

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

type fakeArtifactService struct {
	content   backlogadmin.ArtifactContent
	err       error
	id        string
	principal backlogadmin.Principal
}

func (f *fakeArtifactService) OpenArtifact(_ context.Context, principal backlogadmin.Principal, id string) (backlogadmin.ArtifactContent, error) {
	f.id, f.principal = id, principal
	return f.content, f.err
}

func TestArtifactGetSafelyRendersApprovedText(t *testing.T) {
	fake := &fakeArtifactService{content: backlogadmin.ArtifactContent{
		Metadata: backlogadmin.ArtifactMetadata{ID: "artifact-1", MediaType: "text/markdown", Size: 13},
		Content:  io.NopCloser(strings.NewReader("hello\x1b[31m\n")),
	}}
	var out bytes.Buffer
	principal := backlogadmin.Principal{ID: "operator"}
	cli := backlogAdminCLI{artifacts: fake, principal: principal, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"artifact", "get", "artifact-1"}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "hello\\x1b[31m\n" || fake.id != "artifact-1" || fake.principal.ID != principal.ID {
		t.Fatalf("output = %q, fake = %#v", out.String(), fake)
	}
}

func TestSafeTerminalTextEscapesUnicodeControlsAndReplacesInvalidUTF8(t *testing.T) {
	raw := append([]byte("before\u009b31m after "), 0xff)
	got := string(safeTerminalText(raw))
	if want := "before\\x9b31m after \ufffd"; got != want {
		t.Fatalf("safe terminal text = %q, want %q", got, want)
	}
}

func TestArtifactGetRequiresDownloadForUnsafeMediaAndNeverOverwrites(t *testing.T) {
	content := "<html>agent output</html>"
	newFake := func() *fakeArtifactService {
		return &fakeArtifactService{content: backlogadmin.ArtifactContent{
			Metadata: backlogadmin.ArtifactMetadata{ID: "artifact-html", MediaType: "text/html", Size: int64(len(content))},
			Content:  io.NopCloser(strings.NewReader(content)),
		}}
	}
	var out bytes.Buffer
	cli := backlogAdminCLI{artifacts: newFake(), principal: backlogadmin.Principal{ID: "operator"}, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"artifact", "get", "artifact-html"}); err == nil || !strings.Contains(err.Error(), "requires --output") {
		t.Fatalf("inline error = %v", err)
	}
	destination := filepath.Join(t.TempDir(), "artifact.html")
	cli.artifacts = newFake()
	if err := cli.runBacklog(context.Background(), []string{"artifact", "get", "artifact-html", "--output", destination}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(destination)
	if err != nil || string(raw) != content {
		t.Fatalf("download = %q, %v", raw, err)
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("download mode = %v, %v", info, err)
	}
	cli.artifacts = newFake()
	if err := cli.runBacklog(context.Background(), []string{"artifact", "get", "artifact-html", "--output", destination}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("overwrite error = %v", err)
	}
}

func TestArtifactGetRejectsOversizedInlineContent(t *testing.T) {
	fake := &fakeArtifactService{content: backlogadmin.ArtifactContent{
		Metadata: backlogadmin.ArtifactMetadata{ID: "artifact-large", MediaType: "text/plain", Size: maxInlineArtifactBytes + 1},
		Content:  io.NopCloser(strings.NewReader("small backing stream")),
	}}
	cli := backlogAdminCLI{artifacts: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: io.Discard}
	err := cli.runBacklog(context.Background(), []string{"artifact", "get", "artifact-large"})
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("size error = %v", err)
	}
}

func TestArtifactDownloadRejectsSymlinkDirectory(t *testing.T) {
	content := "download"
	fake := &fakeArtifactService{content: backlogadmin.ArtifactContent{
		Metadata: backlogadmin.ArtifactMetadata{ID: "artifact-1", MediaType: "application/octet-stream", Size: int64(len(content))},
		Content:  io.NopCloser(strings.NewReader(content)),
	}}
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	linkDirectory := filepath.Join(root, "linked")
	if err := os.Symlink(realDirectory, linkDirectory); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(realDirectory, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(linkDirectory, "nested", "artifact.bin")
	cli := backlogAdminCLI{artifacts: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: io.Discard}
	err := cli.runBacklog(context.Background(), []string{"artifact", "get", "artifact-1", "--output", destination})
	if err == nil || !strings.Contains(err.Error(), "real directory") {
		t.Fatalf("symlink directory error = %v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination exists: %v", err)
	}
}

func TestOpenCheckedOutputDirectoryRejectsDirectorySwap(t *testing.T) {
	base := t.TempDir()
	outputDirectory := filepath.Join(base, "output")
	replacementDirectory := filepath.Join(base, "replacement")
	if err := os.Mkdir(outputDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(replacementDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	checked, err := os.Lstat(outputDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(outputDirectory, filepath.Join(base, "original")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(replacementDirectory, outputDirectory); err != nil {
		t.Fatal(err)
	}
	root, err := openCheckedOutputDirectory(outputDirectory, checked)
	if root != nil {
		root.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "changed during validation") {
		t.Fatalf("directory swap error = %v", err)
	}
}

func TestArtifactDownloadRemovesStagingFileAfterWriteFailure(t *testing.T) {
	directory := t.TempDir()
	destination := filepath.Join(directory, "artifact.bin")
	err := downloadArtifact(&failingArtifactReader{}, destination)
	if err == nil || !strings.Contains(err.Error(), "write artifact output") {
		t.Fatalf("write failure = %v", err)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("staging files remain: %#v", entries)
	}
}

type failingArtifactReader struct{}

func (*failingArtifactReader) Read([]byte) (int, error) {
	return 0, errors.New("injected read failure")
}
