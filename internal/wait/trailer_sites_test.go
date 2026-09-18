package wait

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// firstLine is the wake message's first line, which must be the trailer.
func firstLine(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	return line
}

// An interactive shell wake starts with the trailer, and the prose follows a
// blank line.
func TestInteractiveWakeBeginsWithTheTrailer(t *testing.T) {
	store := &memStore{waits: map[string]Wait{}}
	control := &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1"}}}
	runner := New(store, control, nil)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	runner.SetClock(func() time.Time { return now })
	runner.Exec = func(context.Context, Wait) (string, int, error) { return "ready", 0, nil }
	started := now
	_ = store.SaveWait(context.Background(), Wait{
		ID: "w1", ThreadID: "t1", Name: "deploy", Command: []string{"check"},
		Every: 30 * time.Second, Timeout: time.Hour, Wake: WakeEach,
		Status: StatusWaiting, CreatedAt: now, LastRunAt: &started, Runs: 1, LastExit: 1,
	})
	now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, nil)
	runner.Tick(context.Background(), nil, nil)
	if len(control.texts) != 1 {
		t.Fatalf("texts = %v", control.texts)
	}
	text := control.texts[0]
	if !strings.HasPrefix(text, "t3-steward-wait kind=shell outcome=met wait=w1") {
		t.Fatalf("the wake does not begin with the trailer: %q", firstLine(text))
	}
	parsed, _ := ParseWakeTrailer(text)
	if parsed["exit"] != "0" {
		t.Fatalf("a shell wake carries no exit: %v", parsed)
	}
	if !strings.Contains(text, "\n\nWait finished (T3 steward)") {
		t.Fatalf("today's prose does not follow a blank line: %q", text)
	}
	if store.waits["w1"].Outcome != "met" || store.waits["w1"].Kind != domain.WaitKindShell {
		t.Fatalf("the stored wait does not carry kind and outcome: %+v", store.waits["w1"])
	}
}

// The wake a parked task resumes with starts with the trailer too.
func TestTaskWakeBeginsWithTheTrailer(t *testing.T) {
	runner, store, control, now := taskWaitRunner(t, domain.ProgressWaitingExternal)
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.texts) != 1 {
		t.Fatalf("texts = %v", control.texts)
	}
	text := control.texts[0]
	if !strings.HasPrefix(text, "t3-steward-wait kind=shell outcome=met wait=tw-1") {
		t.Fatalf("the task wake does not begin with the trailer: %q", firstLine(text))
	}
	if !strings.Contains(text, "\n\nThe steward is waking this task") {
		t.Fatalf("today's prose does not follow a blank line: %q", text)
	}
	if result := store.taskWaits["tw-1"].Result; result == nil || result.Outcome != domain.TaskWaitMet {
		t.Fatalf("the settled task wait carries no outcome: %+v", store.taskWaits["tw-1"])
	}
}

// A node wake, which is also what a campaign notification is, starts with the
// node trailer and names the result verb for a terminal run.
func TestNodeWakeBeginsWithTheTrailerAndNamesTheResult(t *testing.T) {
	now := time.Now()
	store := &nativeMemory{w: domain.NodeWait{
		Request: domain.NodeWaitRequest{ID: "nw-campaign-key", ThreadID: "thread", Name: "run-1/__sink", Target: domain.NodeRef{RunID: "run-1", TaskID: "sink:run-1"}},
		Host:    "host", SettledAt: &now, DeliveryID: "token", Delivery: "pending",
		Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: "run-1", TaskID: "sink:run-1"}, RunRevision: 9, Progress: domain.ProgressSucceeded, ExitCode: 0, Reason: "succeeded"},
	}}
	control := &nativeControl{}
	runner := New(store, control, nil)
	runner.NodeHost = "host"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 || len(control.texts) != 1 {
		t.Fatalf("sends=%d texts=%v", control.sends, control.texts)
	}
	text := control.texts[0]
	if !strings.HasPrefix(text, "t3-steward-wait kind=node outcome=met wait=nw-campaign-key") {
		t.Fatalf("the node wake does not begin with the trailer: %q", firstLine(text))
	}
	parsed, _ := ParseWakeTrailer(text)
	for key, want := range map[string]string{
		"run": "run-1", "task": "sink:run-1", "revision": "9", "progress": "succeeded",
		"result": "t3-steward task result run-1",
	} {
		if parsed[key] != want {
			t.Fatalf("%s = %q, want %q (trailer %q)", key, parsed[key], want, firstLine(text))
		}
	}
	if !strings.Contains(text, "\n\nWait finished (T3 steward)") {
		t.Fatalf("today's prose does not follow a blank line: %q", text)
	}
}
