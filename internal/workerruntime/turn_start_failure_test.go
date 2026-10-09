package workerruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	t3control "github.com/iryzhkov/t3-steward/internal/control/t3"
	"github.com/iryzhkov/t3-steward/internal/t3api"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// refusedTurnStartT3 is a T3 server whose only thread had its first turn
// refused, exactly as T3 0.0.38 projects it: no latest turn, the session in
// error with the reason as its last error, and a provider.turn.start.failed
// activity carrying the same reason.
func refusedTurnStartT3(t *testing.T, threadID string) *t3control.Control {
	t.Helper()
	const detail = `ProviderValidationError: Provider validation failed in ProviderService.sendTurn: Expected a value with a length of at most 120000 at [\"input\"]`
	session := `{"threadId":"` + threadID + `","status":"error","providerName":"claudeAgent","providerInstanceId":"claude","runtimeMode":"full-access","activeTurnId":null,"lastError":"` + detail + `","updatedAt":"2026-10-02T05:00:01.000Z"}`
	shell := `{"snapshotSequence":7,"projects":[],"threads":[{"id":"` + threadID + `","projectId":"project-1","title":"review",` +
		`"modelSelection":{"instanceId":"claude","model":"claude-sol"},"runtimeMode":"full-access","interactionMode":"default",` +
		`"latestTurn":null,"createdAt":"2026-10-02T05:00:00.000Z","updatedAt":"2026-10-02T05:00:01.000Z","archivedAt":null,"settledAt":null,"deletedAt":null,` +
		`"session":` + session + `,"latestUserMessageAt":"2026-10-02T05:00:00.000Z","hasPendingApprovals":false,"hasPendingUserInput":false,"hasActionableProposedPlan":false}]}`
	thread := `{"thread":{"id":"` + threadID + `","title":"review","modelSelection":{"instanceId":"claude","model":"claude-sol"},` +
		`"latestTurn":null,"archivedAt":null,"deletedAt":null,"updatedAt":"2026-10-02T05:00:01.000Z","session":` + session + `,` +
		`"messages":[{"id":"message-1","role":"user","text":"a very long prompt","createdAt":"2026-10-02T05:00:00.000Z"}],` +
		`"activities":[{"id":"activity-1","tone":"error","kind":"provider.turn.start.failed","summary":"Provider turn start failed",` +
		`"payload":{"detail":"` + detail + `"},"turnId":null,"createdAt":"2026-10-02T05:00:00.000Z"}]}}`
	var mu sync.Mutex
	settled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		defer mu.Unlock()
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/api/orchestration/shell":
			if settled {
				_, _ = io.WriteString(w, strings.Replace(shell, `"settledAt":null,`, `"settledAt":"2026-10-02T05:10:00.000Z","settledOverride":"settled",`, 1))
				return
			}
			_, _ = io.WriteString(w, shell)
		case req.Method == http.MethodGet && req.URL.Path == "/api/orchestration/threads/"+threadID:
			_, _ = io.WriteString(w, thread)
		case req.Method == http.MethodPost && req.URL.Path == "/api/orchestration/dispatch":
			// The only command collection sends is the settlement.
			settled = true
			_, _ = io.WriteString(w, `{"sequence":8}`)
		default:
			http.Error(w, "unexpected request "+req.Method+" "+req.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return t3control.New(t3api.New(server.URL, t3api.StaticToken("test"), 5*time.Second), slog.New(slog.NewTextHandler(io.Discard, nil)), false)
}

// Field defect 2026-10-02: T3 refused a task's first turn and the attempt sat
// "running" for over an hour, because a thread with no turn and an errored
// session read as a turn that had not started yet. The worker now sees the
// refusal, collects the attempt, and the failure carries T3's reason.
func TestRefusedFirstTurnStartFailsTheAttemptWithT3sReason(t *testing.T) {
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
	root := t.TempDir()
	publisher := &recordingPublisher{}
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Catalog: catalog,
		Workspace: backlog.WorkspacePreparer{
			Cache:     staticRepositoryCache{path: repository},
			Processes: successfulProcessRunner{},
		},
		Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}, Now: func() time.Time { return runtimeTestNow }, NewID: func(string) string { return "verification-1" }},
		Source:    mapArtifactSource{pkg.Prompt.ID: []byte("prompt")},
		Publisher: publisher, T3: refusedTurnStartT3(t, pkg.Identity.ThreadID), Now: func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}

	if state, err := driver.ObserveThread(context.Background(), pkg); err != nil || state != backlog.DispatchThreadStopped {
		t.Fatalf("a refused turn start must read as a stopped thread: state=%q err=%v", state, err)
	}
	state, turnID, err := driver.ObserveThreadTurn(context.Background(), pkg)
	if err != nil || state != backlog.DispatchThreadStopped || turnID == "" {
		t.Fatalf("a refused turn start must have a turn identity collection can bind to: state=%q turn=%q err=%v", state, turnID, err)
	}
	if again, err := func() (string, error) {
		_, id, err := driver.ObserveThreadTurn(context.Background(), pkg)
		return id, err
	}(); err != nil || again != turnID {
		t.Fatalf("the refused turn's identity is not stable: %q then %q (%v)", turnID, again, err)
	}

	started := time.Now()
	if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > testtiming.Bound(3*time.Second) {
		t.Fatalf("collection waited %s for an assistant message a refused turn can never write", elapsed)
	}
	if len(publisher.results) != 1 {
		t.Fatalf("published=%d, want one failed result", len(publisher.results))
	}
	completion := publisher.results[0].Finalized.Completion
	want := `T3 refused to start the provider turn: ProviderValidationError: Provider validation failed in ProviderService.sendTurn: Expected a value with a length of at most 120000 at ["input"]`
	if completion.ExplicitSuccess {
		t.Fatalf("a refused turn start was published as a success: %+v", completion)
	}
	// The coordinator judges the published archive (evaluateResultEvidence),
	// and that judgement is the attempt's recorded failure. It must reach T3's
	// reason from the published bytes.
	if reason, err := backlog.ResultCompletionFailure(publisher.results[0].ThreadArchive, pkg.Identity.ThreadID, publisher.results[0].FinalMessage); err != nil || reason != want {
		t.Fatalf("coordinator re-evaluation reason=%q err=%v", reason, err)
	}
}
