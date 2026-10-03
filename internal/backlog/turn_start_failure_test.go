package backlog

import (
	"strings"
	"testing"
)

// turnStartRefusal is the detail T3 0.0.38 recorded for the field defect of
// 2026-10-02: a first turn whose prompt was over the provider input limit.
const turnStartRefusal = `ProviderValidationError: Provider validation failed in ProviderService.sendTurn: Expected a value with a length of at most 120000 at [\"input\"]`

// A thread whose first turn T3 refused has no latest turn, a session in error
// and a provider.turn.start.failed activity. The result names T3's reason
// rather than a generic provider failure.
func TestResultCompletionNamesARefusedFirstTurnStart(t *testing.T) {
	archive := `{"thread":{"id":"thread-1","latestTurn":null,` +
		`"session":{"threadId":"thread-1","status":"error","activeTurnId":null,"lastError":"` + turnStartRefusal + `"},` +
		`"activities":[{"id":"activity-1","tone":"error","kind":"provider.turn.start.failed","summary":"Provider turn start failed",` +
		`"payload":{"detail":"` + turnStartRefusal + `"},"turnId":null,"createdAt":"2026-10-02T05:00:00.000Z"}]}}`
	reason, err := ResultCompletionFailure([]byte(archive), "thread-1", "")
	if err != nil {
		t.Fatal(err)
	}
	want := `T3 refused to start the provider turn: ProviderValidationError: Provider validation failed in ProviderService.sendTurn: Expected a value with a length of at most 120000 at ["input"]`
	if reason != want {
		t.Fatalf("reason=%q\nwant  %q", reason, want)
	}
	failure, ok, err := LatestTurnStartFailure([]byte(archive))
	if err != nil || !ok || failure.ActivityID != "activity-1" || !strings.Contains(failure.Detail, "at most 120000") {
		t.Fatalf("failure=%+v ok=%v err=%v", failure, ok, err)
	}
}

// A turn the steward sends to a thread that already ran one (a wake, a quota
// resume) can be refused too. The latest turn is then the earlier completed
// one, and judging it alone would call the task a success.
func TestResultCompletionRefusesATurnStartFailedAfterTheLatestTurn(t *testing.T) {
	base := `{"thread":{"id":"thread-1",` +
		`"latestTurn":{"turnId":"turn-1","state":"completed","requestedAt":"2026-10-02T04:59:00Z","startedAt":"2026-10-02T05:00:00Z","completedAt":"2026-10-02T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null},` +
		`"activities":[{"id":"activity-1","tone":"error","kind":"provider.turn.start.failed","payload":{"detail":"refused"},"turnId":null,"createdAt":"CREATED"}]}}`
	later := strings.Replace(base, "CREATED", "2026-10-02T05:30:00Z", 1)
	if reason, err := ResultCompletionFailure([]byte(later), "thread-1", "done"); err != nil || reason != "T3 refused to start the provider turn: refused" {
		t.Fatalf("a refused later turn read as %q err=%v", reason, err)
	}
	// A refusal that came before the turn which then ran and completed is
	// history: that turn is the result.
	earlier := strings.Replace(base, "CREATED", "2026-10-02T04:58:00Z", 1)
	if reason, err := ResultCompletionFailure([]byte(earlier), "thread-1", "done"); err != nil || reason != "" {
		t.Fatalf("an earlier refusal failed a completed later turn: %q err=%v", reason, err)
	}
	if _, ok, err := LatestTurnStartFailure([]byte(earlier)); err != nil || ok {
		t.Fatalf("an earlier refusal is reported as current: ok=%v err=%v", ok, err)
	}
}

// A refusal is evidence about the request it refused only. Once a later user
// message asks for a turn again (a retried start, a wake), the old refusal is
// history even before any turn adopts the new request.
func TestARefusalIsSupersededByALaterRequest(t *testing.T) {
	archive := `{"thread":{"id":"thread-1","latestTurn":null,` +
		`"session":{"threadId":"thread-1","status":"error","activeTurnId":null,"lastError":"old"},` +
		`"messages":[{"id":"m1","role":"user","createdAt":"2026-10-02T05:00:00.000Z"},{"id":"m2","role":"user","createdAt":"RETRY"}],` +
		`"activities":[{"id":"a1","kind":"provider.turn.start.failed","payload":{"detail":"old"},"turnId":null,"createdAt":"2026-10-02T05:00:00.000Z"}]}}`
	if _, ok, err := LatestTurnStartFailure([]byte(strings.Replace(archive, "RETRY", "2026-10-02T05:05:00.000Z", 1))); err != nil || ok {
		t.Fatalf("a refusal of an earlier request is current: ok=%v err=%v", ok, err)
	}
	if failure, ok, err := LatestTurnStartFailure([]byte(strings.Replace(archive, "RETRY", "2026-10-02T04:59:00.000Z", 1))); err != nil || !ok || failure.ActivityID != "a1" {
		t.Fatalf("the refusal of the latest request is not current: %+v ok=%v err=%v", failure, ok, err)
	}
}

// A turn that ended in error says why when T3 recorded a reason, from the
// turn's runtime error or, failing that, the session's last error.
func TestResultCompletionNamesTheProviderErrorOfAFailedTurn(t *testing.T) {
	archive := `{"thread":{"id":"thread-1",` +
		`"latestTurn":{"turnId":"turn-1","state":"error","requestedAt":"2026-10-02T04:59:00Z","startedAt":"2026-10-02T05:00:00Z","completedAt":"2026-10-02T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"error","activeTurnId":null,"lastError":"session went away"},` +
		`"activities":ACTIVITIES}}`
	withRuntime := strings.Replace(archive, "ACTIVITIES", `[{"id":"a","tone":"error","kind":"runtime.error","payload":{"message":"overloaded_error"},"turnId":"turn-1","createdAt":"2026-10-02T05:00:30Z"}]`, 1)
	if reason, err := ResultCompletionFailure([]byte(withRuntime), "thread-1", ""); err != nil || reason != "provider turn did not complete successfully: overloaded_error" {
		t.Fatalf("runtime error reason=%q err=%v", reason, err)
	}
	sessionOnly := strings.Replace(archive, "ACTIVITIES", `[]`, 1)
	if reason, err := ResultCompletionFailure([]byte(sessionOnly), "thread-1", ""); err != nil || reason != "provider turn did not complete successfully: session went away" {
		t.Fatalf("session error reason=%q err=%v", reason, err)
	}
}
