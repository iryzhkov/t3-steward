package backlog

import (
	"encoding/json"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// providerArchive is a thread detail as T3 exports it, reduced to what the
// classification reads.
type providerArchive struct {
	turnState     string
	sessionStatus string
	lastError     string
	runtimeError  string
	refusal       string
	pendingInput  bool
}

func (a providerArchive) bytes(t *testing.T) []byte {
	t.Helper()
	thread := map[string]any{"id": "thread-1", "hasPendingUserInput": a.pendingInput}
	if a.turnState != "" {
		turn := map[string]any{"turnId": "turn-1", "state": a.turnState,
			"requestedAt": "2026-10-07T05:00:00Z", "startedAt": "2026-10-07T05:00:01Z"}
		if a.turnState == "completed" || a.turnState == "error" {
			turn["completedAt"] = "2026-10-07T05:30:00Z"
		}
		thread["latestTurn"] = turn
	}
	if a.sessionStatus != "" {
		session := map[string]any{"threadId": "thread-1", "status": a.sessionStatus}
		if a.lastError != "" {
			session["lastError"] = a.lastError
		}
		thread["session"] = session
	}
	thread["messages"] = []any{map[string]any{"role": "user", "createdAt": "2026-10-07T05:00:00Z"}}
	var activities []any
	if a.runtimeError != "" {
		activities = append(activities, map[string]any{"id": "activity-1", "kind": "runtime.error", "turnId": "turn-1",
			"payload": map[string]any{"message": a.runtimeError}, "createdAt": "2026-10-07T05:30:00Z"})
	}
	if a.refusal != "" {
		activities = append(activities, map[string]any{"id": "activity-2", "kind": TurnStartFailedActivity,
			"payload": map[string]any{"detail": a.refusal}, "createdAt": "2026-10-07T05:00:00Z"})
		delete(thread, "latestTurn")
	}
	thread["activities"] = activities
	raw, err := json.Marshal(map[string]any{"thread": thread})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Each provider-side condition is recognised wherever T3 records it: the
// turn's runtime error, a refused turn start, a session left in error under a
// completed or a running turn.
func TestClassifyProviderTurnEndRecognisesProviderErrors(t *testing.T) {
	for _, test := range []struct {
		name    string
		archive providerArchive
		want    domain.ProviderErrorKind
	}{
		{"capacity", providerArchive{turnState: "error", sessionStatus: "error", runtimeError: "Selected model is at capacity. Please try a different model."}, domain.ProviderErrorCapacity},
		{"overload", providerArchive{turnState: "error", sessionStatus: "ready", runtimeError: `API Error: 529 {"type":"overloaded_error"}`}, domain.ProviderErrorOverload},
		{"rate limit", providerArchive{turnState: "error", sessionStatus: "ready", runtimeError: "429 Too Many Requests: rate limit reached for requests"}, domain.ProviderErrorRateLimit},
		{"server error", providerArchive{turnState: "error", sessionStatus: "ready", runtimeError: "stream disconnected before completion: 502 Bad Gateway"}, domain.ProviderErrorServer},
		{"internal server error", providerArchive{turnState: "error", sessionStatus: "error", runtimeError: "Internal server error"}, domain.ProviderErrorServer},
		{"session not ready after a completed turn", providerArchive{turnState: "completed", sessionStatus: "error"}, domain.ProviderErrorSessionNotReady},
		{"session error names its cause", providerArchive{turnState: "completed", sessionStatus: "error", lastError: "model overloaded"}, domain.ProviderErrorOverload},
		{"running turn under a failed session", providerArchive{turnState: "running", sessionStatus: "error", runtimeError: "Selected model is at capacity"}, domain.ProviderErrorCapacity},
		{"running turn under a silent failed session", providerArchive{turnState: "running", sessionStatus: "error"}, domain.ProviderErrorSessionNotReady},
		{"refused start for a session not ready", providerArchive{sessionStatus: "error", refusal: "provider session is not ready"}, domain.ProviderErrorSessionNotReady},
		{"no turn and a failed session", providerArchive{sessionStatus: "error", lastError: "503 Service Unavailable"}, domain.ProviderErrorServer},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure, provider, err := ClassifyProviderTurnEnd(test.archive.bytes(t), "thread-1")
			if err != nil || !provider || failure.Kind != test.want {
				t.Fatalf("failure=%+v provider=%v err=%v, want %s", failure, provider, err, test.want)
			}
		})
	}
}

// Everything else that ends a turn is the task's own ending and is never a
// provider error: the agent ending its turn, a stop, a thread waiting for
// input, a failure naming no provider condition, and a request the provider
// refused for what it asked.
func TestClassifyProviderTurnEndLeavesTheTasksOwnEndingsAlone(t *testing.T) {
	for _, test := range []struct {
		name    string
		archive providerArchive
	}{
		{"agent ended its turn", providerArchive{turnState: "completed", sessionStatus: "ready"}},
		{"interrupted by a stop", providerArchive{turnState: "interrupted", sessionStatus: "ready"}},
		{"running turn in a healthy session", providerArchive{turnState: "running", sessionStatus: "running"}},
		{"waiting for input", providerArchive{turnState: "error", sessionStatus: "error", runtimeError: "model overloaded", pendingInput: true}},
		{"failure naming no provider condition", providerArchive{turnState: "error", sessionStatus: "ready", runtimeError: "tool execution failed: go test exited 1"}},
		{"turn error with no detail", providerArchive{turnState: "error", sessionStatus: "ready"}},
		{"prompt over the input limit", providerArchive{sessionStatus: "error", refusal: "ProviderValidationError: prompt is too long: 230000 tokens > 200000 maximum"}},
		{"policy refusal", providerArchive{turnState: "error", sessionStatus: "error", runtimeError: "500: the request was refused under the usage policy"}},
		{"plan exhausted", providerArchive{turnState: "error", sessionStatus: "error", runtimeError: "Claude usage limit reached; rate limit resets at 5pm"}},
		{"credentials", providerArchive{turnState: "completed", sessionStatus: "error", lastError: "401 Unauthorized: invalid x-api-key"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure, provider, err := ClassifyProviderTurnEnd(test.archive.bytes(t), "thread-1")
			if err != nil || provider {
				t.Fatalf("failure=%+v provider=%v err=%v, want the task's own ending", failure, provider, err)
			}
		})
	}
	// Another thread's archive says nothing about this one.
	archive := providerArchive{turnState: "error", sessionStatus: "error", runtimeError: "at capacity"}.bytes(t)
	if _, provider, err := ClassifyProviderTurnEnd(archive, "thread-2"); err != nil || provider {
		t.Fatalf("mismatched thread classified: provider=%v err=%v", provider, err)
	}
	if _, _, err := ClassifyProviderTurnEnd([]byte("{"), "thread-1"); err == nil {
		t.Fatal("an invalid archive was classified")
	}
}
