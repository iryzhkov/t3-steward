package backlog

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A log artifact is either the thread archive or preflight evidence. Preflight
// runs before the provider session exists, so it can never be the thread
// archive, and a contract that recognised only the archive rejected every
// preflight log. A live batch stalled on exactly that: the worker produced the
// evidence and the coordinator refused the whole result in a retry loop.
func TestResultArtifactAcceptsPreflightEvidenceAndTheThreadArchive(t *testing.T) {
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1"}
	task := domain.Task{ID: "task-1"}
	manifest := workerproto.ArtifactTransferManifest{WorkerID: "homelab"}
	now := time.Date(2026, 9, 14, 6, 0, 0, 0, time.UTC)
	digest := strings.Repeat("a", 64)

	accepted := map[string]workerproto.ArtifactObject{
		"thread archive": {
			ID: "thread-archive-attempt-1", Path: "results/thread.json", Kind: string(domain.ArtifactLog),
			MediaType: "application/json", Size: 12, SHA256: digest,
		},
		"preflight log": {
			ID: "preflight-0123456789abcdef", Path: "results/preflight/baseline_build.log",
			Kind: string(domain.ArtifactLog), MediaType: "text/plain; charset=utf-8", Size: 12, SHA256: digest,
		},
	}
	for name, object := range accepted {
		t.Run(name, func(t *testing.T) {
			artifact, err := resultArtifact(object, manifest, attempt, task, now)
			if err != nil {
				t.Fatalf("%s was refused: %v", name, err)
			}
			if artifact.Kind != domain.ArtifactLog {
				t.Fatalf("kind = %q", artifact.Kind)
			}
		})
	}

	// Widening the contract must not make the kind a way to import arbitrary
	// content under a name nobody declared.
	refused := map[string]workerproto.ArtifactObject{
		"archive under the wrong name": {
			ID: "thread-archive-attempt-1", Path: "results/elsewhere.json", Kind: string(domain.ArtifactLog),
			MediaType: "application/json", Size: 12, SHA256: digest,
		},
		"preflight name without a preflight id": {
			ID: "arbitrary-1", Path: "results/preflight/baseline.log", Kind: string(domain.ArtifactLog),
			MediaType: "text/plain; charset=utf-8", Size: 12, SHA256: digest,
		},
		"preflight id outside the preflight tree": {
			ID: "preflight-0123456789abcdef", Path: "results/secrets.txt", Kind: string(domain.ArtifactLog),
			MediaType: "text/plain; charset=utf-8", Size: 12, SHA256: digest,
		},
	}
	for name, object := range refused {
		t.Run(name, func(t *testing.T) {
			if _, err := resultArtifact(object, manifest, attempt, task, now); err == nil {
				t.Fatalf("%s was accepted", name)
			}
		})
	}
}
