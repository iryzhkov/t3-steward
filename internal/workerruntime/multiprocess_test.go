package workerruntime

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestLocalCoordinatorStubWorkerMultiProcess(t *testing.T) {
	if os.Getenv("T3_WORKER_HELPER") == "1" {
		runWorkerHelper(t)
		return
	}
	root := t.TempDir()
	request, err := workerproto.NewEnvelope(
		workerproto.MessageSnapshot, "session-1", "request-1", "coordinator", "normandy",
		9, "worker-1", 1, runtimeTestNow, runtimeTestNow.Add(time.Minute), workerproto.SnapshotRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.SignEnvelope(&request, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=TestLocalCoordinatorStubWorkerMultiProcess")
	command.Env = append(os.Environ(), "T3_WORKER_HELPER=1", "T3_WORKER_ROOT="+root)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	responseRead, responseWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer responseRead.Close()
	command.ExtraFiles = []*os.File{responseWrite}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	responseWrite.Close()
	codec := workerproto.Codec{MaxBytes: 1 << 20}
	if err := codec.Encode(stdin, request); err != nil {
		t.Fatal(err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	var response workerproto.Envelope
	if err := codec.Decode(responseRead, &response); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if response.Type != workerproto.MessageObservations || response.InReplyTo != request.RequestID {
		t.Fatalf("response = %+v", response)
	}
	if err := workerproto.VerifyEnvelopeSignature(response, []byte("worker-response-secret")); err != nil {
		t.Fatal(err)
	}
	var observations workerproto.Observations
	if err := workerproto.DecodePayload(response, workerproto.MessageObservations, &observations); err != nil {
		t.Fatal(err)
	}
	if observations.Snapshot.WorkerID != "normandy" || observations.Snapshot.CoordinatorEpoch != 9 ||
		observations.Snapshot.WorkerEpoch != "worker-1" || !observations.Snapshot.Connected {
		t.Fatalf("snapshot = %+v", observations.Snapshot)
	}
}

func runWorkerHelper(t *testing.T) {
	t.Helper()
	root := os.Getenv("T3_WORKER_ROOT")
	driver := &fakeDriver{workspace: root + "/workspace"}
	runtime := newTestRuntime(t, root, driver)
	server, err := workerproto.NewServer(workerproto.ServerConfig{
		CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 9, WorkerEpoch: "worker-1",
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key", PeerSecret: []byte("coordinator-secret"),
		SignerPrincipal: "ssh:normandy", SignerKeyID: "worker-key", SignerSecret: []byte("worker-response-secret"),
		Allowed: map[workerproto.MessageType]bool{workerproto.MessageSnapshot: true},
		Now:     func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	codec := workerproto.Codec{MaxBytes: 1 << 20}
	output := os.NewFile(3, "worker-response")
	if output == nil {
		t.Fatal("worker response descriptor is missing")
	}
	defer output.Close()
	if err := ServeOne(
		context.Background(), os.Stdin, output, codec,
		Exchange{Runtime: runtime, Server: server},
	); err != nil {
		t.Fatal(err)
	}
}
