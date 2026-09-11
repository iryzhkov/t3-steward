package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

type fakeSubmissionService struct {
	request backlogadmin.LocalSubmissionRequest
	raw     []byte
	size    int64
}

func (f *fakeSubmissionService) SubmitArchive(
	_ context.Context,
	request backlogadmin.LocalSubmissionRequest,
	archive io.Reader,
	size int64,
) (backlogadmin.LocalSubmissionResponse, error) {
	raw, err := io.ReadAll(archive)
	if err != nil {
		return backlogadmin.LocalSubmissionResponse{}, err
	}
	f.request, f.raw, f.size = request, raw, size
	return backlogadmin.LocalSubmissionResponse{
		Key: request.IdempotencyKey, Digest: "abc", WorkflowID: "workflow-1",
		RunID: "run-1", State: "accepted", AcceptedAt: "2026-09-10T12:00:00Z",
	}, nil
}

func TestBacklogSubmissionStreamsRegularArchiveAndRendersResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bundle.tar")
	raw := []byte("archive bytes")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	fake := &fakeSubmissionService{}
	var output bytes.Buffer
	cli := backlogAdminCLI{submissions: fake, stdout: &output}
	if err := cli.runBacklog(context.Background(), []string{
		"submit", path, "--idempotency-key", "request-1",
	}); err != nil {
		t.Fatal(err)
	}
	if fake.request.IdempotencyKey != "request-1" ||
		fake.size != int64(len(raw)) ||
		!bytes.Equal(fake.raw, raw) {
		t.Fatalf("request=%+v size=%d raw=%q", fake.request, fake.size, fake.raw)
	}
	for _, want := range []string{"submission request-1", "workflow=workflow-1", "state=accepted"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("output %q does not contain %q", output.String(), want)
		}
	}
}

func TestBacklogSubmissionRejectsUnsafeInputAndArguments(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "bundle.tar")
	if err := os.WriteFile(target, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.tar")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	cli := backlogAdminCLI{submissions: &fakeSubmissionService{}, stdout: io.Discard}
	for name, args := range map[string][]string{
		"missing":        nil,
		"unknown option": {target, "--unknown"},
		"duplicate key":  {target, "--idempotency-key", "one", "--idempotency-key", "two"},
		"symlink":        {link},
	} {
		t.Run(name, func(t *testing.T) {
			if err := cli.runSubmission(context.Background(), args); err == nil {
				t.Fatal("unsafe submission succeeded")
			}
		})
	}
}
