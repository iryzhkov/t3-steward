package backlog

import (
	"strings"
	"testing"
)

func TestResultCompletionRequiresStructuredSuccess(t *testing.T) {
	archive := `{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`
	for _, test := range []struct {
		name, old, replacement, summary string
		success                         bool
	}{
		{name: "implicit completion", summary: "Finished.", success: true},
		{name: "empty summary", success: true},
		{name: "legacy done", summary: "BACKLOG STATUS: done", success: true},
		{name: "provider error cannot be overridden", old: `"state":"completed"`, replacement: `"state":"error"`, summary: "BACKLOG STATUS: done"},
		{name: "interrupted", old: `"state":"completed"`, replacement: `"state":"interrupted"`},
		{name: "running", old: `"state":"completed"`, replacement: `"state":"running"`},
		{name: "unknown turn", old: `"state":"completed"`, replacement: `"state":""`},
		{name: "wrong thread", old: `"id":"thread-1"`, replacement: `"id":"other"`},
		{name: "no turn identity", old: `"turnId":"turn-1"`, replacement: `"turnId":""`},
		{name: "missing completion", old: `"completedAt":"2026-09-13T05:01:00Z"`, replacement: `"completedAt":null`},
		{name: "backwards time", old: "2026-09-13T05:01:00Z", replacement: "2026-09-13T04:00:00Z"},
		{name: "active session", old: `"activeTurnId":null`, replacement: `"activeTurnId":"turn-2"`},
		{name: "session error", old: `"lastError":null`, replacement: `"lastError":"provider rejected request"`},
		{name: "session not ready", old: `"status":"ready"`, replacement: `"status":"running"`},
		{name: "pending approval", old: `"id":"thread-1"`, replacement: `"id":"thread-1","hasPendingApprovals":true`},
		{name: "pending input", old: `"id":"thread-1"`, replacement: `"id":"thread-1","hasPendingUserInput":true`},
		{name: "background work", old: `"id":"thread-1"`, replacement: `"id":"thread-1","backgroundLiveness":"working"`},
		{name: "continue marker", summary: "BACKLOG STATUS: continue"},
		{name: "input marker", summary: "BACKLOG STATUS: needs-input"},
		{name: "worker failure", summary: "BACKLOG STATUS: failed\nsetup failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := archive
			if test.old != "" {
				data = strings.Replace(data, test.old, test.replacement, 1)
			}
			reason, err := ResultCompletionFailure([]byte(data), "thread-1", test.summary)
			if err != nil || (reason == "") != test.success {
				t.Fatalf("reason=%q err=%v", reason, err)
			}
		})
	}
	if reason, err := ResultCompletionFailure([]byte("{}"), "thread-1", "BACKLOG STATUS: done"); err != nil || reason == "" {
		t.Fatalf("missing archive: %q %v", reason, err)
	}
	if _, err := ResultCompletionFailure([]byte("invalid"), "thread-1", ""); err == nil {
		t.Fatal("invalid archive accepted")
	}
}
