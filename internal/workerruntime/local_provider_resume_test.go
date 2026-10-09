package workerruntime

import (
	"context"
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
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// capacityT3 is a T3 server whose only thread was working when its provider
// went to capacity: T3 left the latest turn running, moved the session to
// error with the provider's message, and recorded a runtime.error activity on
// the turn. sessionStatus overrides the session's status. Every POST body is
// recorded.
func capacityT3(t *testing.T, threadID, sessionStatus string) (*t3control.Control, func() []string) {
	t.Helper()
	const detail = "Selected model is at capacity. Please try a different model."
	lastError := `"` + detail + `"`
	if sessionStatus != "error" {
		lastError = "null"
	}
	session := `{"threadId":"` + threadID + `","status":"` + sessionStatus + `","providerName":"codex","providerInstanceId":"codex","runtimeMode":"full-access","activeTurnId":"turn-1","lastError":` + lastError + `,"updatedAt":"2026-10-07T05:40:00.000Z"}`
	turn := `{"turnId":"turn-1","state":"running","requestedAt":"2026-10-07T05:00:00.000Z","startedAt":"2026-10-07T05:00:01.000Z","completedAt":null}`
	shell := `{"snapshotSequence":7,"projects":[],"threads":[{"id":"` + threadID + `","projectId":"project-1","title":"implement",` +
		`"modelSelection":{"instanceId":"codex","model":"gpt-6.1-sol"},"runtimeMode":"full-access","interactionMode":"default",` +
		`"latestTurn":` + turn + `,"createdAt":"2026-10-07T05:00:00.000Z","updatedAt":"2026-10-07T05:40:00.000Z","archivedAt":null,"settledAt":null,"deletedAt":null,` +
		`"session":` + session + `,"latestUserMessageAt":"2026-10-07T05:00:00.000Z","hasPendingApprovals":false,"hasPendingUserInput":false,"hasActionableProposedPlan":false}]}`
	thread := `{"thread":{"id":"` + threadID + `","title":"implement","modelSelection":{"instanceId":"codex","model":"gpt-6.1-sol"},` +
		`"latestTurn":` + turn + `,"archivedAt":null,"deletedAt":null,"updatedAt":"2026-10-07T05:40:00.000Z","session":` + session + `,` +
		`"messages":[{"id":"message-1","role":"user","text":"implement the unit","createdAt":"2026-10-07T05:00:00.000Z"}],` +
		`"activities":[{"id":"activity-1","tone":"error","kind":"runtime.error","summary":"Runtime error",` +
		`"payload":{"message":"` + detail + `"},"turnId":"turn-1","createdAt":"2026-10-07T05:40:00.000Z"}]}}`
	var mu sync.Mutex
	var posts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		defer mu.Unlock()
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/api/orchestration/shell":
			_, _ = io.WriteString(w, shell)
		case req.Method == http.MethodGet && req.URL.Path == "/api/orchestration/threads/"+threadID:
			_, _ = io.WriteString(w, thread)
		case req.Method == http.MethodPost:
			body, _ := io.ReadAll(req.Body)
			posts = append(posts, string(body))
			_, _ = io.WriteString(w, `{"sequence":8}`)
		default:
			http.Error(w, "unexpected request "+req.Method+" "+req.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	recorded := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), posts...)
	}
	return t3control.New(t3api.New(server.URL, t3api.StaticToken("test"), 5*time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), false), recorded
}

// Field defect 2026-10-07 (feedback 148): the model went to capacity under a
// task's turn, T3 left the turn running under a session in error, and the
// attempt read as active and running for good. The worker now reads that turn
// as ended, classifies the capacity error from T3's own record, and resumes
// the same thread.
func TestRunningTurnUnderAFailedSessionIsAProviderErrorTheWorkerResumes(t *testing.T) {
	pkg := testPackage()
	control, posts := capacityT3(t, pkg.Identity.ThreadID, "error")
	driver := &LocalDriver{T3: control}
	state, turnID, err := driver.ObserveThreadTurn(context.Background(), pkg)
	if err != nil || state != backlog.DispatchThreadStopped || turnID != "turn-1" {
		t.Fatalf("state=%q turn=%q err=%v, want the running turn ended", state, turnID, err)
	}
	failure, provider, err := driver.ProviderTurnError(context.Background(), pkg)
	if err != nil || !provider || failure.Kind != domain.ProviderErrorCapacity || !strings.Contains(failure.Detail, "at capacity") {
		t.Fatalf("failure=%+v provider=%v err=%v", failure, provider, err)
	}
	if err := driver.ResumeAfterProviderError(context.Background(), pkg, "dispatch-1/provider-resume/turn-1", "Read continuation.md and continue."); err != nil {
		t.Fatal(err)
	}
	sent := posts()
	if len(sent) == 0 || !strings.Contains(strings.Join(sent, "\n"), "Read continuation.md and continue.") {
		t.Fatalf("the resume message did not reach T3: %q", sent)
	}
}

// The same running turn under a healthy session is still running.
func TestRunningTurnUnderAHealthySessionStaysActive(t *testing.T) {
	pkg := testPackage()
	control, _ := capacityT3(t, pkg.Identity.ThreadID, "running")
	driver := &LocalDriver{T3: control}
	state, _, err := driver.ObserveThreadTurn(context.Background(), pkg)
	if err != nil || state != backlog.DispatchThreadActive {
		t.Fatalf("state=%q err=%v, want active", state, err)
	}
}
