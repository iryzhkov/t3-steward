package workerruntime

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"

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
	// lagging, when set, is what the thread detail still shows while the
	// shell already shows thread: T3 projects the two separately.
	lagging *fakeTurnThread
	// editDetail, when set, changes the thread detail as it is served, so a
	// test can give it a shape the projection never produces.
	editDetail func(map[string]any)
	// settled is set by a settlement dispatch and shown by the shell.
	settled bool
	exports int
	// commands are the dispatched command types, in order.
	commands []string
}

func (f *fakeTurnT3) set(thread fakeTurnThread) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thread = thread
	f.lagging = nil
}

func (f *fakeTurnT3) setLagging(shell, detail fakeTurnThread) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thread = shell
	f.lagging = &detail
}

func (f *fakeTurnT3) json() (shell, detail map[string]any) {
	shell, _ = f.project(f.thread)
	if f.lagging != nil {
		_, detail = f.project(*f.lagging)
	} else {
		_, detail = f.project(f.thread)
	}
	if f.settled {
		shell["settledAt"], shell["settledOverride"] = fakeTurnTime(20), "settled"
	}
	if f.editDetail != nil {
		f.editDetail(detail)
	}
	return shell, detail
}

func (f *fakeTurnT3) project(thread fakeTurnThread) (shell, detail map[string]any) {
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
		case req.Method == http.MethodPost && req.URL.Path == "/api/orchestration/dispatch":
			var command struct {
				Type string `json:"type"`
			}
			_ = json.NewDecoder(req.Body).Decode(&command)
			fake.commands = append(fake.commands, command.Type)
			if command.Type == "thread.settle" {
				fake.settled = true
			}
			_, _ = io.WriteString(w, `{"sequence":2}`)
		default:
			http.Error(w, "unexpected request "+req.Method+" "+req.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return fake, t3control.New(t3api.New(server.URL, t3api.StaticToken("test"), testtiming.Bound(5*time.Second)), slog.New(slog.NewTextHandler(io.Discard, nil)), false)
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
	if thread != backlog.DispatchThreadStopped || turn != backlog.DispatchThreadStopped || identity != requestIdentity(5) ||
		reason != "T3 refused to start the provider turn: refused refused-second" {
		t.Fatalf("current refusal: thread=%q turn=%q identity=%q reason=%q", thread, turn, identity, reason)
	}
	// The session failed for the current request with no refusal activity
	// (T3 recorded only the session error): terminal, named by that error.
	fake.set(fakeTurnThread{messages: []int{0, 5}, session: &fakeSession{status: "error", updated: 5, lastError: "provider gone"}})
	if thread, turn, identity, reason := observeTurn(t, driver, control); thread != backlog.DispatchThreadStopped || turn != backlog.DispatchThreadStopped ||
		identity != requestIdentity(5) || reason != "provider turn did not complete successfully: provider gone" {
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
		if thread != backlog.DispatchThreadStopped || turn != backlog.DispatchThreadStopped || identity != requestIdentity(5) ||
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

// requestIdentity is the collection identity of the start request sent at
// minute: one identity per request, whatever evidence ends it.
func requestIdentity(minute int) string {
	return "turn-request:" + fakeTurnBase.Add(time.Duration(minute)*time.Minute).UTC().Format(time.RFC3339Nano)
}

// Review round 2 of #41: the shell and the thread detail are projected
// separately. When the shell already shows a retried request (R2) and the
// detail still shows only the first (R1) with its refusal, that refusal is
// not evidence about R2 and nothing is collected; once the detail shows R2
// refused, the attempt fails once, on R2.
func TestARefusalCountsOnlyOnceTheDetailShowsTheShellsRequest(t *testing.T) {
	pkg := testPackage()
	fake, control := newFakeTurnT3(t, pkg.Identity.ThreadID)
	driver := &LocalDriver{T3: control}
	first := fakeTurnThread{messages: []int{0}, session: &fakeSession{status: "error", updated: 0, lastError: "refused R1"},
		activities: []fakeActivity{refusal("r1", 0)}}
	fake.setLagging(fakeTurnThread{messages: []int{0, 5}, session: first.session}, first)
	if thread, turn, _, _ := observeTurn(t, driver, control); thread != backlog.DispatchThreadActive || turn != backlog.DispatchThreadActive {
		t.Fatalf("the detail's R1 refusal ended the shell's R2: thread=%q turn=%q", thread, turn)
	}
	// The detail is behind even for a session the shell already shows failed
	// for R2: T3's refusal of R2 may yet be projected with its reason.
	fake.setLagging(fakeTurnThread{messages: []int{0, 5}, session: &fakeSession{status: "error", updated: 5, lastError: "refused R2"}}, first)
	if thread, turn, _, _ := observeTurn(t, driver, control); thread != backlog.DispatchThreadActive || turn != backlog.DispatchThreadActive {
		t.Fatalf("collected before the detail showed R2: thread=%q turn=%q", thread, turn)
	}
	caughtUp := fakeTurnThread{messages: []int{0, 5}, session: &fakeSession{status: "error", updated: 5, lastError: "refused R2"},
		activities: []fakeActivity{refusal("r1", 0), refusal("r2", 5)}}
	fake.set(caughtUp)
	thread, turn, identity, reason := observeTurn(t, driver, control)
	if thread != backlog.DispatchThreadStopped || turn != backlog.DispatchThreadStopped || identity != requestIdentity(5) ||
		reason != "T3 refused to start the provider turn: refused r2" {
		t.Fatalf("R2 refused: thread=%q turn=%q identity=%q reason=%q", thread, turn, identity, reason)
	}
}

// One durable identity per request: a session-only failure keeps its identity
// when T3's refusal activity is projected later, and a refused first or later
// request keeps it after the thread is settled, instead of falling back to the
// earlier turn's.
func TestARefusedRequestKeepsOneIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		turn *fakeTurn
	}{
		{name: "first request"},
		{name: "later request", turn: &fakeTurn{id: "turn-1", state: "completed", requested: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pkg := testPackage()
			fake, control := newFakeTurnT3(t, pkg.Identity.ThreadID)
			driver := &LocalDriver{T3: control}
			sessionOnly := fakeTurnThread{messages: []int{0, 5}, turn: tc.turn, session: &fakeSession{status: "error", updated: 5, lastError: "refused"}}
			fake.set(sessionOnly)
			_, turn, before, _ := observeTurn(t, driver, control)
			if turn != backlog.DispatchThreadStopped || before != requestIdentity(5) {
				t.Fatalf("session-only failure: turn=%q identity=%q", turn, before)
			}
			withActivity := sessionOnly
			withActivity.activities = []fakeActivity{refusal("late", 5)}
			fake.set(withActivity)
			if _, turn, after, _ := observeTurn(t, driver, control); turn != backlog.DispatchThreadStopped || after != before {
				t.Fatalf("the delayed refusal changed the identity: %q then %q", before, after)
			}
			fake.mu.Lock()
			fake.settled = true
			fake.mu.Unlock()
			if _, turn, settled, _ := observeTurn(t, driver, control); turn != backlog.DispatchThreadStopped || settled != before {
				t.Fatalf("settlement changed the identity: %q then %q", before, settled)
			}
		})
	}
}

// A settled thread whose retried start request is unresolved may still start
// that turn. A stop does not take the settlement shortcut: it stops the
// session first, and only then settles.
func TestStopStopsASettledThreadWithAnUnresolvedRequest(t *testing.T) {
	pkg := testPackage()
	fake, control := newFakeTurnT3(t, pkg.Identity.ThreadID)
	driver := &LocalDriver{T3: control, Config: LocalDriverConfig{StopTimeout: time.Second}}
	fake.set(fakeTurnThread{messages: []int{0, 5}, turn: &fakeTurn{id: "turn-1", state: "completed", requested: 0},
		session: &fakeSession{status: "ready", updated: 1}})
	fake.mu.Lock()
	fake.settled = true
	fake.mu.Unlock()
	if err := driver.StopThread(context.Background(), pkg); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.commands) < 2 || fake.commands[0] != "thread.session.stop" || fake.commands[len(fake.commands)-1] != "thread.settle" {
		t.Fatalf("commands = %v, want the session stopped before the settlement", fake.commands)
	}
}

// Through collection: a task whose turn-1 was collected cleanly, then woken,
// and whose wake T3 refused. Collection binds to the wake, not to turn-1's
// recorded clean read, so the attempt fails on the refusal; a restarted
// worker collecting again (journal replay) reaches the same failure, and the
// coordinator's judgement of each published archive agrees.
func TestCollectingARefusedWakeNeverReusesTheEarlierTurn(t *testing.T) {
	repository, commit := makeGitRepository(t)
	pkg := testPackage()
	pkg.Environment.Repository = "https://example.com/steward.git"
	pkg.Environment.Ref = commit
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	fake, control := newFakeTurnT3(t, pkg.Identity.ThreadID)
	root := t.TempDir()
	publisher := &recordingPublisher{}
	newDriver := func() *LocalDriver {
		driver, err := NewLocalDriver(LocalDriver{
			Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
			Catalog: catalog,
			Workspace: backlog.WorkspacePreparer{
				Cache:     staticRepositoryCache{path: repository},
				Processes: successfulProcessRunner{},
			},
			Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}, Now: func() time.Time { return runtimeTestNow }, NewID: func(string) string { return "verification-1" }},
			Source:    mapArtifactSource{pkg.Prompt.ID: []byte("prompt")},
			Publisher: publisher, T3: control, Now: func() time.Time { return runtimeTestNow },
		})
		if err != nil {
			t.Fatal(err)
		}
		return driver
	}
	driver := newDriver()
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup unseals the captured trees so the temporary directory can go.
	t.Cleanup(func() { _ = driver.Cleanup(context.Background(), pkg, workspace) })
	// Turn-1 finished and was collected cleanly; its read is recorded.
	fake.set(fakeTurnThread{messages: []int{0}, turn: &fakeTurn{id: "turn-1", state: "completed", requested: 0}, session: &fakeSession{status: "ready", updated: 1}})
	if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if len(publisher.results) != 1 || !publisher.results[0].Finalized.Completion.ExplicitSuccess {
		t.Fatalf("turn-1 collection = %+v", publisher.results)
	}
	// The task is woken and T3 refuses the wake; the session idles out, so
	// a fresh read of turn-1 alone would look unfinished.
	fake.set(fakeTurnThread{messages: []int{0, 5}, turn: &fakeTurn{id: "turn-1", state: "completed", requested: 0},
		session: &fakeSession{status: "error", updated: 5, lastError: "refused wake"}, activities: []fakeActivity{refusal("wake", 5)}})
	for collection, collector := range []*LocalDriver{driver, newDriver()} {
		if err := collector.Collect(context.Background(), pkg, workspace); err != nil {
			t.Fatal(err)
		}
		result := publisher.results[len(publisher.results)-1]
		if result.Finalized.Completion.ExplicitSuccess {
			t.Fatalf("collection %d reused turn-1's clean read for a refused wake", collection)
		}
		reason, err := backlog.ResultCompletionFailure(result.ThreadArchive, pkg.Identity.ThreadID, result.FinalMessage)
		if err != nil || reason != "T3 refused to start the provider turn: refused wake" {
			t.Fatalf("collection %d: coordinator reason=%q err=%v", collection, reason, err)
		}
	}
}
