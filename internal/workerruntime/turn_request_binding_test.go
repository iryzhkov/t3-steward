package workerruntime

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// fakeTurnThread is one task thread as T3 0.0.38 projects it: the user
// messages that request turns, the latest turn, the provider session and the
// thread activities. Times are minutes after base.
type fakeTurnThread struct {
	messages   []int // user message times
	turn       *fakeTurn
	session    *fakeSession
	activities []fakeActivity
}

type fakeTurn struct {
	id        string
	state     string
	requested int
}

type fakeSession struct {
	status    string
	updated   int
	lastError string
}

type fakeActivity struct {
	id, kind, detail string
	at               int
}

var fakeTurnBase = time.Date(2026, 10, 2, 5, 0, 0, 0, time.UTC)

func fakeTurnTime(minute int) string {
	return fakeTurnBase.Add(time.Duration(minute) * time.Minute).Format("2006-01-02T15:04:05.000Z")
}

// fakeTurnT3 serves the shell snapshot and thread detail of one thread whose
// state the test changes between observations.
type fakeTurnT3 struct {
	mu       sync.Mutex
	threadID string
	thread   fakeTurnThread
	exports  int
}

func (f *fakeTurnT3) set(thread fakeTurnThread) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thread = thread
}

func (f *fakeTurnT3) json() (shell, detail map[string]any) {
	thread := f.thread
	var latestTurn, session any
	if thread.turn != nil {
		turn := map[string]any{"turnId": thread.turn.id, "state": thread.turn.state, "requestedAt": fakeTurnTime(thread.turn.requested),
			"startedAt": fakeTurnTime(thread.turn.requested), "completedAt": nil}
		if thread.turn.state != "running" {
			turn["completedAt"] = fakeTurnTime(thread.turn.requested + 1)
		}
		latestTurn = turn
	}
	if thread.session != nil {
		var lastError any
		if thread.session.lastError != "" {
			lastError = thread.session.lastError
		}
		session = map[string]any{"threadId": f.threadID, "status": thread.session.status, "providerInstanceId": "claude",
			"runtimeMode": "full-access", "activeTurnId": nil, "lastError": lastError, "updatedAt": fakeTurnTime(thread.session.updated)}
	}
	var latestUser any
	messages := []map[string]any{}
	for index, at := range thread.messages {
		messages = append(messages, map[string]any{"id": "message-" + string(rune('a'+index)), "role": "user", "text": "work", "createdAt": fakeTurnTime(at)})
		latestUser = fakeTurnTime(at)
	}
	if thread.turn != nil && thread.turn.state == "completed" {
		messages = append(messages, map[string]any{"id": "answer", "role": "assistant", "text": "done", "createdAt": fakeTurnTime(thread.turn.requested + 1)})
	}
	activities := []map[string]any{}
	for _, activity := range thread.activities {
		activities = append(activities, map[string]any{"id": activity.id, "tone": "error", "kind": activity.kind, "summary": "Provider turn start failed",
			"payload": map[string]any{"detail": activity.detail}, "turnId": nil, "createdAt": fakeTurnTime(activity.at)})
	}
	common := map[string]any{"id": f.threadID, "title": "task", "modelSelection": map[string]any{"instanceId": "claude", "model": "claude-sol"},
		"runtimeMode": "full-access", "interactionMode": "default", "latestTurn": latestTurn, "session": session,
		"archivedAt": nil, "deletedAt": nil, "updatedAt": fakeTurnTime(10)}
	shell = map[string]any{"projectId": "project-1", "createdAt": fakeTurnTime(0), "settledAt": nil, "latestUserMessageAt": latestUser,
		"hasPendingApprovals": false, "hasPendingUserInput": false, "hasActionableProposedPlan": false}
	detail = map[string]any{"messages": messages, "activities": activities}
	for key, value := range common {
		shell[key], detail[key] = value, value
	}
	return shell, detail
}

func newFakeTurnT3(t *testing.T, threadID string) (*fakeTurnT3, *t3control.Control) {
	t.Helper()
	fake := &fakeTurnT3{threadID: threadID}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		shell, detail := fake.json()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/api/orchestration/shell":
			_ = json.NewEncoder(w).Encode(map[string]any{"snapshotSequence": 1, "projects": []any{}, "threads": []any{shell}})
		case req.Method == http.MethodGet && req.URL.Path == "/api/orchestration/threads/"+threadID:
			fake.exports++
			_ = json.NewEncoder(w).Encode(map[string]any{"thread": detail})
		default:
			http.Error(w, "unexpected request "+req.Method+" "+req.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return fake, t3control.New(t3api.New(server.URL, t3api.StaticToken("test"), 5*time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), false)
}

func refusal(id string, at int) fakeActivity {
	return fakeActivity{id: id, kind: "provider.turn.start.failed", detail: "refused " + id, at: at}
}

// observeTurn is one collection gate observation, and the archive the
// coordinator would judge if collection went ahead.
func observeTurn(t *testing.T, driver *LocalDriver, control *t3control.Control) (backlog.DispatchThreadState, backlog.DispatchThreadState, string, string) {
	t.Helper()
	pkg := testPackage()
	threadState, err := driver.ObserveThread(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	turnState, identity, err := driver.ObserveThreadTurn(context.Background(), pkg)
	if err != nil && !strings.Contains(err.Error(), "deferred") {
		t.Fatal(err)
	}
	archive, err := control.ExportThread(context.Background(), pkg.Identity.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	reason, err := backlog.ResultCompletionFailure(archive, pkg.Identity.ThreadID, "")
	if err != nil {
		t.Fatal(err)
	}
	return threadState, turnState, identity, reason
}

// Sol review of #41: a session error with no projected turn ended the attempt
// even when it belonged to an earlier start request. Terminal evidence is
// bound to the current request, which is T3's latest user message: until a
// turn adopts it, or a refusal or session failure at or after it is
// projected, the request is unresolved and collection waits.
func TestTerminalEvidenceIsBoundToTheCurrentStartRequest(t *testing.T) {
	pkg := testPackage()
	fake, control := newFakeTurnT3(t, pkg.Identity.ThreadID)
	driver := &LocalDriver{T3: control}

	// A slow start: the request is sent and nothing else is projected yet.
	fake.set(fakeTurnThread{messages: []int{0}})
	if thread, turn, _, _ := observeTurn(t, driver, control); thread != backlog.DispatchThreadActive || turn != backlog.DispatchThreadActive {
		t.Fatalf("a start with no session and no turn yet: thread=%q turn=%q", thread, turn)
	}
	// The first request was refused, and the task's start was sent again: the
	// old session error and the old refusal are still projected.
	fake.set(fakeTurnThread{messages: []int{0, 5},
		session:    &fakeSession{status: "error", updated: 0, lastError: "refused first"},
		activities: []fakeActivity{refusal("refused-first", 0)}})
	if thread, turn, _, _ := observeTurn(t, driver, control); thread != backlog.DispatchThreadActive || turn != backlog.DispatchThreadActive {
		t.Fatalf("an earlier request's refusal ended the current one: thread=%q turn=%q", thread, turn)
	}
	// The new request is refused in turn: the attempt ends, once, on it.
	fake.set(fakeTurnThread{messages: []int{0, 5},
		session:    &fakeSession{status: "error", updated: 5, lastError: "refused second"},
		activities: []fakeActivity{refusal("refused-first", 0), refusal("refused-second", 5)}})
	thread, turn, identity, reason := observeTurn(t, driver, control)
	if thread != backlog.DispatchThreadStopped || turn != backlog.DispatchThreadStopped || identity != "turn-start-failed:refused-second" ||
		reason != "T3 refused to start the provider turn: refused refused-second" {
		t.Fatalf("current refusal: thread=%q turn=%q identity=%q reason=%q", thread, turn, identity, reason)
	}
	// The session failed for the current request with no refusal activity
	// (T3 recorded only the session error): terminal, named by that error.
	fake.set(fakeTurnThread{messages: []int{0, 5}, session: &fakeSession{status: "error", updated: 5, lastError: "provider gone"}})
	if thread, turn, identity, reason := observeTurn(t, driver, control); thread != backlog.DispatchThreadStopped || turn != backlog.DispatchThreadStopped ||
		!strings.HasPrefix(identity, "session-error:") || reason != "provider turn did not complete successfully: provider gone" {
		t.Fatalf("current session error: thread=%q turn=%q identity=%q reason=%q", thread, turn, identity, reason)
	}
}

// A refused wake or resume leaves the previous turn completed as T3's latest
// turn. The collection identity is the refusal, not that turn, so the failure
// is one failure however often it is observed, after a worker restart, and in
// the coordinator's own judgement of the archive; a later request that a turn
// adopts supersedes the refusal.
func TestARefusedLaterRequestIsCollectedOnTheRefusal(t *testing.T) {
	pkg := testPackage()
	fake, control := newFakeTurnT3(t, pkg.Identity.ThreadID)
	driver := &LocalDriver{T3: control}
	completed := &fakeTurn{id: "turn-1", state: "completed", requested: 0}
	ready := &fakeSession{status: "ready", updated: 1}

	// The wake was sent and not yet adopted: unresolved, not the old turn.
	fake.set(fakeTurnThread{messages: []int{0, 5}, turn: completed, session: ready})
	if thread, turn, _, _ := observeTurn(t, driver, control); thread != backlog.DispatchThreadActive || turn != backlog.DispatchThreadActive {
		t.Fatalf("an unadopted wake read as the finished earlier turn: thread=%q turn=%q", thread, turn)
	}
	// The wake was refused.
	fake.set(fakeTurnThread{messages: []int{0, 5}, turn: completed,
		session: &fakeSession{status: "error", updated: 5, lastError: "refused wake"}, activities: []fakeActivity{refusal("wake", 5)}})
	for observation := 0; observation < 3; observation++ {
		observer := driver
		if observation == 2 {
			observer = &LocalDriver{T3: control} // a restarted worker
		}
		thread, turn, identity, reason := observeTurn(t, observer, control)
		if thread != backlog.DispatchThreadStopped || turn != backlog.DispatchThreadStopped || identity != "turn-start-failed:wake" ||
			reason != "T3 refused to start the provider turn: refused wake" {
			t.Fatalf("observation %d: thread=%q turn=%q identity=%q reason=%q", observation, thread, turn, identity, reason)
		}
	}
	// A later request that a turn adopted supersedes the refusal.
	fake.set(fakeTurnThread{messages: []int{0, 5, 9}, turn: &fakeTurn{id: "turn-2", state: "completed", requested: 9},
		session: &fakeSession{status: "ready", updated: 10}, activities: []fakeActivity{refusal("wake", 5)}})
	if thread, turn, identity, reason := observeTurn(t, driver, control); thread != backlog.DispatchThreadStopped || turn != backlog.DispatchThreadStopped ||
		identity != "turn-2" || reason != "" {
		t.Fatalf("superseded refusal: thread=%q turn=%q identity=%q reason=%q", thread, turn, identity, reason)
	}
}
