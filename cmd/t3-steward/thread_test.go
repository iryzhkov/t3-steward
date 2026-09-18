package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fakeThreadStopper struct {
	thread *domain.Thread
	stops  []string
}

func (f *fakeThreadStopper) GetThread(context.Context, string) (*domain.Thread, error) {
	return f.thread, nil
}

func (f *fakeThreadStopper) StopThread(_ context.Context, t domain.Thread, mode t3control.StopMode) error {
	f.stops = append(f.stops, string(mode)+":"+t.ID)
	return nil
}

// thread stop dispatches the interrupt for a running turn and, with
// --session, the session stop as well, and says what it sent (S-16).
func TestThreadStopDispatchesInterruptAndSession(t *testing.T) {
	running := &domain.Thread{ID: "thread-1", Title: "orphan", Running: true, TurnID: "turn-1", TurnState: "running"}
	fake := &fakeThreadStopper{thread: running}
	var out bytes.Buffer
	if err := stopThread(context.Background(), fake, "thread-1", false, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Join(fake.stops, ",") != "interrupt:thread-1" || !strings.Contains(out.String(), "dispatched: thread.turn.interrupt") {
		t.Fatalf("stops=%v out=%q", fake.stops, out.String())
	}
	fake.stops = nil
	out.Reset()
	if err := stopThread(context.Background(), fake, "thread-1", true, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Join(fake.stops, ",") != "interrupt:thread-1,session-stop:thread-1" || !strings.Contains(out.String(), "dispatched: thread.session.stop") {
		t.Fatalf("stops=%v out=%q", fake.stops, out.String())
	}
	// An idle thread gets no interrupt; the session stop is still sent when asked.
	fake.thread = &domain.Thread{ID: "thread-1", Title: "idle"}
	fake.stops = nil
	out.Reset()
	if err := stopThread(context.Background(), fake, "thread-1", true, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Join(fake.stops, ",") != "session-stop:thread-1" || !strings.Contains(out.String(), "no running turn") {
		t.Fatalf("stops=%v out=%q", fake.stops, out.String())
	}
	fake.thread = nil
	if err := stopThread(context.Background(), fake, "thread-2", false, &out); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing thread error = %v", err)
	}
	if _, _, err := parseThreadStop([]string{"--session"}); err == nil {
		t.Fatal("missing thread id accepted")
	}
	if id, session, err := parseThreadStop([]string{"thread-9", "--session"}); err != nil || id != "thread-9" || !session {
		t.Fatalf("parse = %q %v %v", id, session, err)
	}
}
