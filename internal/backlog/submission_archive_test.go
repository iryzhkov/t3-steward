package backlog

import (
	"archive/tar"
	"bytes"
	"context"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestSubmissionServiceArchiveReplayAndUnsafeRefusal(t *testing.T) {
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	service := &SubmissionService{
		StorageRoot: storage, Store: store, MaxBytes: 1 << 20, MaxFiles: 16,
	}
	archive := submissionTar(t, map[string]string{
		"workflow.yaml":      "version: 2\nname: archive\nenvironment: {project: t3-steward}\ntasks:\n  inspect: {prompt_file: prompts/inspect.md}\n",
		"prompts/inspect.md": "inspect",
	})
	request := ArchiveSubmission{IdempotencyKey: "archive-request", Archive: bytes.NewReader(archive)}
	first, err := service.SubmitArchive(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Archive = bytes.NewReader(archive)
	replay, err := service.SubmitArchive(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || replay.Record.WorkflowID != first.Record.WorkflowID {
		t.Fatalf("archive replay = %#v, first = %#v", replay, first)
	}

	unsafe := submissionTar(t, map[string]string{
		"../workflow.yaml": "unsafe",
	})
	_, err = service.SubmitArchive(context.Background(), ArchiveSubmission{
		IdempotencyKey: "unsafe-archive", Archive: bytes.NewReader(unsafe),
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe entry path") {
		t.Fatalf("unsafe archive error = %v", err)
	}
}

func submissionTar(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		content := []byte(files[name])
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
