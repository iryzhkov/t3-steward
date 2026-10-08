package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// busyCatalogWorker serves a catalog host on a Unix socket, the way the worker
// daemon does, accepts an initial catalog, and leaves one running and one
// parked attempt in its journal.
func busyCatalogWorker(t *testing.T) (*CatalogHost, CatalogProjection, func(workerproto.Envelope) (workerproto.Envelope, error)) {
	t.Helper()
	host, projection := catalogHostFixture(t)
	dir, err := os.MkdirTemp("", "t3cat-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	listener, err := net.Listen("unix", filepath.Join(dir, "worker.sock"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ServeWorkerListener(ctx, listener, 24<<20, time.Minute, 4, host.HandleFrame) }()
	t.Cleanup(func() { cancel(); <-done })
	frames := workerproto.FrameCodec{MaxBytes: 24 << 20}
	codec := workerproto.Codec{MaxBytes: 8 << 20}
	exchange := func(request workerproto.Envelope) (workerproto.Envelope, error) {
		conn, err := net.Dial("unix", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := frames.Write(conn, mustEncodeEnvelope(t, codec, request)); err != nil {
			t.Fatal(err)
		}
		raw, err := frames.Read(conn)
		if err != nil {
			return workerproto.Envelope{}, err
		}
		var response workerproto.Envelope
		if err := codec.Decode(bytes.NewReader(raw), &response); err != nil {
			t.Fatal(err)
		}
		if err := workerproto.VerifyEnvelopeSignature(response, testProtocolCredentials().WorkerSecret); err != nil {
			t.Fatalf("catalog response is not signed by the worker: %v", err)
		}
		return response, nil
	}
	response, err := exchange(catalogEnvelope(t, "initial", CatalogRequest{Projection: projection}))
	if err != nil || response.Type != MessageCatalog {
		t.Fatalf("initial catalog: %+v %v", response, err)
	}
	for id, phase := range map[string]Phase{"running": PhaseRunning, "parked": PhaseWaiting} {
		record := AttemptRecord{Phase: phase}
		record.Assignment.ID = id
		record.Package.Package.Identity.AssignmentID = id
		record.Package.Package.Environment.CatalogRevision = projection.Revision
		if err := host.service.Exchange.Runtime.journal.update(func(s *journalState) error { s.Attempts[id] = record; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	return host, projection, exchange
}

func TestBusyWorkerAcceptsCapacityOnlyCatalogChange(t *testing.T) {
	host, projection, exchange := busyCatalogWorker(t)
	service := host.service
	settings, err := projection.Settings(host.Bootstrap, host.Home)
	if err != nil {
		t.Fatal(err)
	}
	worker := settings.Workers["normandy"]
	worker.CPUClass = "high"
	worker.Executors.Slots = 6
	worker.Executors.CPUUnits = 9
	worker.Executors.MemoryMB = 30000
	settings.Workers["normandy"] = worker
	changed, err := BuildCatalogProjection(settings, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	if changed.Revision == projection.Revision {
		t.Fatal("capacity change did not change the catalog revision")
	}
	response, err := exchange(catalogEnvelope(t, "capacity", CatalogRequest{Projection: changed, ExpectedRevision: projection.Revision}))
	if err != nil {
		t.Fatalf("capacity-only catalog change ended the stream: %v", err)
	}
	var accepted map[string]string
	if err := workerproto.DecodePayload(response, MessageCatalog, &accepted); err != nil || accepted["revision"] != changed.Revision {
		t.Fatalf("capacity-only catalog change refused: %+v %v", response, err)
	}
	if host.service != service {
		t.Fatal("capacity-only catalog change replaced the busy worker's runtime")
	}
	runtime := host.service.Exchange.Runtime
	inventory := runtime.desiredInventory
	if inventory.CatalogRevision != changed.Revision || inventory.CPUClass != "high" ||
		inventory.Allocatable.ExecutorSlots != 6 || inventory.Allocatable.CPUUnits != 9 || inventory.Allocatable.MemoryMB != 30000 {
		t.Fatalf("inventory did not adopt the new capacity: %+v", inventory)
	}
	state, err := runtime.journal.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.Attempts["running"].Phase != PhaseRunning || state.Attempts["parked"].Phase != PhaseWaiting {
		t.Fatalf("assignments disturbed: %+v", state.Attempts)
	}
	driver := runtime.driver.(*LocalDriver)
	if !driver.acceptsCatalogRevision(projection.Revision) || !driver.acceptsCatalogRevision(changed.Revision) || driver.acceptsCatalogRevision("other") {
		t.Fatal("driver must execute packages of both the old and the new capacity revision, and nothing else")
	}

	restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
	if err := restarted.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.unusable != nil || restarted.retained.Projection.Revision != changed.Revision {
		t.Fatalf("restart lost the adopted catalog: %v", restarted.unusable)
	}
	if !restarted.service.Exchange.Runtime.driver.(*LocalDriver).acceptsCatalogRevision(projection.Revision) {
		t.Fatal("restart forgot that packages of the previous capacity revision stay executable")
	}
}

// Every catalog revision a live attempt's package names stays executable, no
// matter how many capacity-only changes the worker adopts while the attempt
// lives, before and after a restart.
func TestBusyCapacityChangesKeepLiveRevision(t *testing.T) {
	host, original, exchange := busyCatalogWorker(t)
	if err := host.service.Exchange.Runtime.journal.update(func(s *journalState) error {
		record := s.Attempts["parked"]
		record.Phase = PhaseClaimed
		s.Attempts["parked"] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	settings, err := original.Settings(host.Bootstrap, host.Home)
	if err != nil {
		t.Fatal(err)
	}
	previous := original.Revision
	for i := 1; i <= maxPreviousCatalogRevisions+1; i++ {
		worker := settings.Workers["normandy"]
		worker.Executors.CPUUnits = float64(9 + i)
		settings.Workers["normandy"] = worker
		changed, err := BuildCatalogProjection(settings, "normandy")
		if err != nil {
			t.Fatal(err)
		}
		response, err := exchange(catalogEnvelope(t, fmt.Sprintf("resize-%d", i), CatalogRequest{Projection: changed, ExpectedRevision: previous}))
		if err != nil || response.Type != MessageCatalog {
			t.Fatalf("resize %d: %+v %v", i, response, err)
		}
		previous = changed.Revision
	}
	if !host.service.Exchange.Runtime.driver.(*LocalDriver).acceptsCatalogRevision(original.Revision) {
		t.Fatalf("capacity changes evicted live package revision %s", original.Revision)
	}
	restarted := &CatalogHost{Home: host.Home, Bootstrap: host.Bootstrap, Options: host.Options}
	if err := restarted.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !restarted.service.Exchange.Runtime.driver.(*LocalDriver).acceptsCatalogRevision(original.Revision) {
		t.Fatalf("capacity changes evicted live package revision %s after restart", original.Revision)
	}
}

// Predecessors no live attempt names are bounded history; a pinned one is
// never dropped to make room.
func TestAppendPreviousRevisionKeepsPinnedRevisions(t *testing.T) {
	var history []string
	for i := 0; i < maxPreviousCatalogRevisions+5; i++ {
		history = appendPreviousRevision(history, fmt.Sprintf("r%d", i), fmt.Sprintf("r%d", i+1), map[string]bool{"r0": true})
	}
	if len(history) != maxPreviousCatalogRevisions+1 {
		t.Fatalf("history length = %d, want %d unpinned plus the pinned one", len(history), maxPreviousCatalogRevisions+1)
	}
	if !slices.Contains(history, "r0") {
		t.Fatalf("pinned revision dropped: %v", history)
	}
	last := fmt.Sprintf("r%d", maxPreviousCatalogRevisions+4)
	if history[len(history)-1] != last || slices.Contains(history, "r1") {
		t.Fatalf("unpinned history did not keep the newest: %v", history)
	}
}

func TestBusyWorkerRefusesIncompatibleCatalogChangeExplicitly(t *testing.T) {
	host, projection, exchange := busyCatalogWorker(t)
	settings, err := projection.Settings(host.Bootstrap, host.Home)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range settings.Projects {
		project.T3Project = ""
		settings.Projects["managed"] = project
		break
	}
	worker := settings.Workers["normandy"]
	worker.Executors.CPUUnits = 9
	settings.Workers["normandy"] = worker
	changed, err := BuildCatalogProjection(settings, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	response, err := exchange(catalogEnvelope(t, "projects", CatalogRequest{Projection: changed, ExpectedRevision: projection.Revision}))
	if err != nil {
		t.Fatalf("refused catalog change ended the stream instead of answering: %v", err)
	}
	if response.Type != workerproto.MessageError {
		t.Fatalf("incompatible catalog change accepted on a busy worker: %+v", response)
	}
	var refusal workerproto.ProtocolError
	if err := workerproto.DecodePayload(response, workerproto.MessageError, &refusal); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`worker "normandy" has 2 active or parked assignments`, "catalog change touches executor capacity, projects", "retry when idle"} {
		if !strings.Contains(refusal.Message, want) {
			t.Fatalf("refusal %q does not say %q", refusal.Message, want)
		}
	}
	if host.retained.Projection.Revision != projection.Revision {
		t.Fatal("refused catalog was retained")
	}
	// The stream stays usable after a refusal: the same catalog fence is
	// answered again rather than dropped.
	if response, err = exchange(catalogEnvelope(t, "again", CatalogRequest{Projection: changed, ExpectedRevision: projection.Revision})); err != nil || response.Type != workerproto.MessageError {
		t.Fatalf("second refusal: %+v %v", response, err)
	}
}

func setListenerLogOutput(w io.Writer) func() {
	previous := listenerLog
	logger := slog.New(slog.NewTextHandler(w, nil))
	listenerLog = func() *slog.Logger { return logger }
	return func() { listenerLog = previous }
}

// stopTestListener joins every peer before the test reads its log or restores
// listenerLog. It is safe both as a defer on early failure and on the success path.
func stopTestListener(conn *net.Conn, cancel context.CancelFunc, done <-chan error) func() {
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		if *conn != nil {
			(*conn).Close()
		}
		cancel()
		<-done
	}
}

// Only a refusal the handler marks as answered is sent. A handler that fails
// after writing part of its reply, as an artifact stream can, must end the
// stream so the peer retries, never deliver the part as a complete frame.
func TestWorkerListenerSendsOnlyAnsweredRefusals(t *testing.T) {
	var logged bytes.Buffer
	restore := setListenerLogOutput(&logged)
	defer restore()
	dir, err := os.MkdirTemp("", "t3cat-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	listener, err := net.Listen("unix", filepath.Join(dir, "worker.sock"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ServeWorkerListener(ctx, listener, 1<<20, time.Minute, 1, func(_ context.Context, raw []byte) ([]byte, error) {
			if string(raw) == "refuse" {
				return []byte("signed refusal"), &AnsweredError{Err: errors.New("busy")}
			}
			return []byte("header\nPARTIAL"), errors.New("artifact send: short object")
		})
	}()
	var conn net.Conn
	stop := stopTestListener(&conn, cancel, done)
	defer stop()
	frames := workerproto.FrameCodec{MaxBytes: 1 << 20}
	conn, err = net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := frames.Write(conn, []byte("refuse")); err != nil {
		t.Fatal(err)
	}
	if reply, err := frames.Read(conn); err != nil || string(reply) != "signed refusal" {
		t.Fatalf("answered refusal = %q, %v", reply, err)
	}
	if err := frames.Write(conn, []byte("stream")); err != nil {
		t.Fatal(err)
	}
	if reply, err := frames.Read(conn); err == nil {
		t.Fatalf("a failed handler's partial reply was delivered: %q", reply)
	}
	// The log is read once the listener has stopped writing it.
	stop()
	if !strings.Contains(logged.String(), "short object") || !strings.Contains(logged.String(), "busy") {
		t.Fatalf("listener did not log both outcomes: %q", logged.String())
	}
}

// A frame larger than the listener accepts is logged rather than dropped
// without a trace.
func TestWorkerListenerLogsAnOversizeFrame(t *testing.T) {
	var logged bytes.Buffer
	restore := setListenerLogOutput(&logged)
	defer restore()
	dir, err := os.MkdirTemp("", "t3cat-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	listener, err := net.Listen("unix", filepath.Join(dir, "worker.sock"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	handlerCalled := false
	go func() {
		done <- ServeWorkerListener(ctx, listener, 16, time.Minute, 1, func(context.Context, []byte) ([]byte, error) {
			handlerCalled = true
			return []byte("ok"), nil
		})
	}()
	var conn net.Conn
	stop := stopTestListener(&conn, cancel, done)
	defer stop()
	conn, err = net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// The server may reject the header before the client finishes the body.
	if err := (workerproto.FrameCodec{MaxBytes: 1 << 20}).Write(conn, bytes.Repeat([]byte("x"), 64)); err != nil && !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatal(err)
	}
	_, readErr := (workerproto.FrameCodec{MaxBytes: 1 << 20}).Read(conn)
	stop()
	if readErr == nil {
		t.Fatal("oversize frame was answered")
	}
	if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, syscall.ECONNRESET) {
		t.Fatalf("unexpected oversize rejection read error: %v", readErr)
	}
	if handlerCalled {
		t.Fatal("oversize frame reached the handler")
	}
	if !strings.Contains(logged.String(), "stream frame exceeds limit") {
		t.Fatalf("oversize frame not logged: %q", logged.String())
	}
}
