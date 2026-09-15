package main

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

func TestWorkerExchangeCommandAcceptsAtMostOneFixedOperation(t *testing.T) {
	if err := run([]string{"worker-exchange", "control", "extra"}); err == nil ||
		!strings.Contains(err.Error(), "at most one fixed operation") {
		t.Fatalf("extra operation error = %v", err)
	}
	if err := cmdWorkerExchange(globalFlags{}, "other"); err == nil ||
		!strings.Contains(err.Error(), "operation must be") {
		t.Fatalf("unknown operation error = %v", err)
	}
}

// A worker is reached at one address for control and for both artifact
// directions, and OpenSSH picks the authorized_keys line by key, so a forced
// command that pins an operation confines that worker to one of the three and
// it can never both run work and take delivery of its inputs. The endpoint
// therefore accepts an invocation with no operation word.
func TestWorkerExchangeAcceptsNoOperationWord(t *testing.T) {
	// Reaching the configuration refusal proves the empty word was accepted;
	// an unknown word is refused before the configuration is read at all.
	if err := cmdWorkerExchange(globalFlags{}, ""); err == nil ||
		!strings.Contains(err.Error(), "explicit operator-controlled --config") {
		t.Fatalf("no operation word: %v", err)
	}
	if err := run([]string{"worker-exchange"}); err == nil ||
		!strings.Contains(err.Error(), "explicit operator-controlled --config") {
		t.Fatalf("bare invocation: %v", err)
	}
}

// The endpoint and the runtime agree on what each operation word means, and on
// which message each one carries. The mapping itself is proved end to end in
// internal/workerruntime, against real artifact streams.
func TestWorkerExchangeOperationWordsMatchTheRuntime(t *testing.T) {
	if workerOperationControl != workerruntime.OperationControl ||
		workerOperationArtifactReceive != workerruntime.OperationArtifactReceive ||
		workerOperationArtifactSend != workerruntime.OperationArtifactSend {
		t.Fatal("the endpoint and the runtime disagree about the operation words")
	}
	for kind, want := range map[workerproto.MessageType]string{
		workerproto.MessageArtifactDownload: workerOperationArtifactReceive,
		workerproto.MessageArtifactUpload:   workerOperationArtifactSend,
		workerproto.MessageCommands:         workerOperationControl,
		workerproto.MessageSnapshot:         workerOperationControl,
		workerproto.MessageArtifactPoll:     workerOperationControl,
	} {
		if got := workerruntime.OperationFor(kind); got != want {
			t.Fatalf("OperationFor(%q) = %q, want %q", kind, got, want)
		}
	}
	// The coordinator's own words are the same three, so what it asks for and
	// what a pinned forced command accepts cannot drift apart.
	if coordinatorWorkerControlOperation != workerOperationControl ||
		coordinatorWorkerArtifactSendOperation != workerOperationArtifactSend ||
		coordinatorWorkerArtifactReceiveOperation != workerOperationArtifactReceive {
		t.Fatal("the coordinator client and the worker endpoint disagree about the operation words")
	}
}

func TestWorkerExchangeRequiresExplicitLocalConfig(t *testing.T) {
	if err := run([]string{"worker-exchange", "control"}); err == nil ||
		!strings.Contains(err.Error(), "explicit operator-controlled --config") {
		t.Fatalf("implicit config error = %v", err)
	}
}

func TestWorkerExchangeRejectsAuthorityFlags(t *testing.T) {
	for name, flags := range map[string]globalFlags{
		"dry run":    {dryRun: true},
		"no dry run": {noDryRun: true},
		"log level":  {logLevel: "debug"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := cmdWorkerExchange(flags, workerOperationControl); err == nil ||
				!strings.Contains(err.Error(), "only the local --config override") {
				t.Fatalf("authority flag error = %v", err)
			}
		})
	}
}
