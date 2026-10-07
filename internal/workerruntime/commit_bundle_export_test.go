package workerruntime

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type fakePublishedExporter struct {
	backlog.CampaignRefStore
	calls int
}

func (f *fakePublishedExporter) ExportPublishedBundle(ctx context.Context, p backlog.CommitProvenance, limit int64) (string, error) {
	f.calls++
	dir, err := os.MkdirTemp("", "t3-export-test-")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "commit.bundle")
	return path, os.WriteFile(path, []byte("bundle bytes"), 0600)
}
func TestCommitBundleStreamAuthenticationAndReplay(t *testing.T) {
	service := testWorkerService(t)
	fake := &fakePublishedExporter{}
	service.Exchange.Runtime.config.CampaignRefs = fake
	p := backlog.CommitProvenance{Version: backlog.CampaignCommitRecordVersion, WorkflowRunID: "run-1", TaskID: "task-1", Name: "implementation", Repository: "repo", Base: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Commit: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Ref: backlog.CampaignRef("run-1", "task-1", "implementation"), CreatedAt: runtimeTestNow}
	raw, err := backlog.MarshalCommitProvenance(p)
	if err != nil {
		t.Fatal(err)
	}
	request := signedWorkerRequest(t, workerproto.MessageCommitBundle, 1, workerproto.CommitBundleRequest{Provenance: raw, MaxBytes: 1024})
	for i := 0; i < 2; i++ {
		var output bytes.Buffer
		if err := service.ServeArtifactSendEnvelope(context.Background(), request, bufio.NewReader(bytes.NewReader(nil)), &output); err != nil {
			t.Fatal(err)
		}
		envelope, reader, err := ReadStreamEnvelope(&output, service.Codec)
		if err != nil {
			t.Fatal(err)
		}
		var metadata workerproto.CommitBundleResponse
		if err := workerproto.DecodePayload(envelope, workerproto.MessageCommitBundle, &metadata); err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		sum := sha256.Sum256(data)
		if err != nil || string(data) != "bundle bytes" || metadata.Size != int64(len(data)) || metadata.SHA256 != fmt.Sprintf("%x", sum) {
			t.Fatalf("metadata %v data %q err %v", metadata, data, err)
		}
	}
	bad := request
	bad.RequestID = "bad"
	bad.Authentication.Signature = "invalid"
	var output bytes.Buffer
	if err := service.ServeArtifactSendEnvelope(context.Background(), bad, bufio.NewReader(bytes.NewReader(nil)), &output); err == nil {
		t.Fatal("accepted invalid signature")
	}
	if fake.calls != 2 {
		t.Fatalf("unauthenticated request reached exporter, calls=%d", fake.calls)
	}
}
