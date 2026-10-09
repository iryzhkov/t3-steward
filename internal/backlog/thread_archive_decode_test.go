package backlog

import (
	"errors"
	"strings"
	"testing"
)

// The activity fixtures below are T3 v0.0.45 thread activities, built from
// its OrchestrationThreadActivity schema: id, tone, kind, summary, payload,
// turnId, sequence and createdAt, where payload is whatever the producing
// kind records. The field defect of 2026-10-08 was a payload.detail that is
// an object, which the archive decoder took for a string, so every collection
// of the thread was deferred for hours.
const (
	// v045ProviderErrorActivity is a provider failure inside a turn ("model at
	// capacity"), whose detail is the provider's structured error.
	v045ProviderErrorActivity = `{"id":"activity-provider-error","tone":"error","kind":"provider.error","summary":"Provider error",` +
		`"payload":{"message":"model at capacity","detail":{"type":"overloaded_error","status":529,"retryable":true,"error":{"message":"model at capacity"}}},` +
		`"turnId":"turn-1","sequence":41,"createdAt":"2026-10-08T03:10:00.000Z"}`
	// v045ContextWindowActivity is the context-window report of a long turn,
	// whose detail is the usage record.
	v045ContextWindowActivity = `{"id":"activity-context","tone":"info","kind":"context-window.updated","summary":"Context window",` +
		`"payload":{"detail":{"usedTokens":191034,"maxTokens":200000,"compacted":false},"usage":[1,2,3]},` +
		`"turnId":"turn-1","sequence":42,"createdAt":"2026-10-08T03:11:00.000Z"}`
	// v045ToolActivity has a payload that is not an object at all.
	v045ToolActivity = `{"id":"activity-tool","tone":"tool","kind":"tool.completed","summary":"Ran a command",` +
		`"payload":["exit",0],"turnId":"turn-1","sequence":43,"createdAt":"2026-10-08T03:12:00.000Z"}`
)

func v045Archive(latestTurn, session string, activities ...string) string {
	return `{"thread":{"id":"thread-1","latestTurn":` + latestTurn + `,"session":` + session +
		`,"messages":[{"id":"m1","role":"user","text":"work","createdAt":"2026-10-08T03:00:00.000Z"}],` +
		`"activities":[` + strings.Join(activities, ",") + `]}}`
}

const (
	v045CompletedTurn = `{"turnId":"turn-1","state":"completed","requestedAt":"2026-10-08T03:00:00Z","startedAt":"2026-10-08T03:00:01Z","completedAt":"2026-10-08T03:20:00Z"}`
	v045ReadySession  = `{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}`
)

// Activities whose payload Steward does not interpret are read whatever their
// shape, and the judgement is the one the rest of the archive makes.
func TestActivitiesOfAnyPayloadShapeDoNotRejectTheArchive(t *testing.T) {
	archive := []byte(v045Archive(v045CompletedTurn, v045ReadySession, v045ProviderErrorActivity, v045ContextWindowActivity, v045ToolActivity))
	reason, err := ResultCompletionFailure(archive, "thread-1", "done")
	if err != nil || reason != "" {
		t.Fatalf("a completed turn with object-valued activity details: reason=%q err=%v", reason, err)
	}
	if _, ok, err := LatestTurnStartFailure(archive); err != nil || ok {
		t.Fatalf("refusal: ok=%v err=%v", ok, err)
	}
	if thread, latest, err := ArchiveCurrentRequest(archive); err != nil || thread != "thread-1" || latest == nil {
		t.Fatalf("current request: thread=%q latest=%v err=%v", thread, latest, err)
	}
	warnings, err := ThreadArchiveWarnings(archive)
	if err != nil || len(warnings) != 0 {
		t.Fatalf("activities Steward does not interpret produced warnings %q (err %v)", warnings, err)
	}
	// A failed turn with the same activities is still judged failed.
	failed := strings.Replace(v045CompletedTurn, `"completed"`, `"error"`, 1)
	if reason, err := ResultCompletionFailure([]byte(v045Archive(failed, v045ReadySession, v045ProviderErrorActivity, v045ContextWindowActivity)), "thread-1", ""); err != nil ||
		reason != "provider turn did not complete successfully" {
		t.Fatalf("failed turn: reason=%q err=%v", reason, err)
	}
}

// The fields Steward does interpret, a refused start's detail and a runtime
// error's message, are read as text when they are strings. In any other shape
// their JSON is the text, so the provider's account still reaches the
// failure, and a warning records the unexpected shape.
func TestInterpretedActivityFieldsOfAnUnexpectedShapeAreWarnings(t *testing.T) {
	refusal := `{"id":"activity-refused","tone":"error","kind":"provider.turn.start.failed","summary":"Provider turn start failed",` +
		`"payload":{"detail":{"type":"overloaded_error","message":"model at capacity"}},"turnId":null,"sequence":44,"createdAt":"2026-10-08T03:30:00.000Z"}`
	archive := []byte(v045Archive(v045CompletedTurn, `{"threadId":"thread-1","status":"error","activeTurnId":null,"lastError":"model at capacity"}`,
		v045ProviderErrorActivity, v045ContextWindowActivity, refusal))
	reason, err := ResultCompletionFailure(archive, "thread-1", "")
	want := TurnStartRefusedFailure + `: {"type":"overloaded_error","message":"model at capacity"}`
	if err != nil || reason != want {
		t.Fatalf("reason=%q err=%v\nwant   %q", reason, err, want)
	}
	warnings, err := ThreadArchiveWarnings(archive)
	if err != nil || len(warnings) != 1 || !strings.Contains(warnings[0], `"activity-refused"`) || !strings.Contains(warnings[0], "payload.detail") {
		t.Fatalf("warnings=%q err=%v", warnings, err)
	}

	runtime := `{"id":"activity-runtime","tone":"error","kind":"runtime.error","summary":"Runtime error",` +
		`"payload":{"message":{"code":"context_length_exceeded"},"detail":{"usedTokens":200000}},"turnId":"turn-1","sequence":45,"createdAt":"2026-10-08T03:19:00.000Z"}`
	failed := strings.Replace(v045CompletedTurn, `"completed"`, `"error"`, 1)
	reason, err = ResultCompletionFailure([]byte(v045Archive(failed, v045ReadySession, v045ContextWindowActivity, runtime)), "thread-1", "")
	if want := `provider turn did not complete successfully: {"code":"context_length_exceeded"}`; err != nil || reason != want {
		t.Fatalf("runtime error: reason=%q err=%v", reason, err)
	}
}

// An activity entry that is not an object, or whose identity fields are not
// strings, is skipped rather than refused: it cannot be a refusal or a runtime
// error Steward would act on.
func TestMalformedActivityEntriesAreSkipped(t *testing.T) {
	archive := []byte(v045Archive(v045CompletedTurn, v045ReadySession, `"not an activity"`, `42`,
		`{"id":7,"kind":"provider.turn.start.failed","payload":{"detail":"refused"},"createdAt":"2026-10-08T03:30:00.000Z"}`))
	reason, err := ResultCompletionFailure(archive, "thread-1", "")
	if err != nil || reason != "" {
		t.Fatalf("reason=%q err=%v", reason, err)
	}
	warnings, err := ThreadArchiveWarnings(archive)
	if err != nil || len(warnings) != 3 {
		t.Fatalf("warnings=%q err=%v", warnings, err)
	}
}

// A field Steward interprets in a shape it cannot decode, or bytes that are
// not JSON, is a typed error every reader returns the same way.
func TestAnUndecodableArchiveIsATypedError(t *testing.T) {
	for _, archive := range []string{
		"not json",
		strings.Replace(v045Archive(v045CompletedTurn, v045ReadySession), `"turnId":"turn-1"`, `"turnId":{"id":"turn-1"}`, 1),
	} {
		_, err := ResultCompletionFailure([]byte(archive), "thread-1", "")
		var invalid *ThreadArchiveInvalidError
		if !errors.As(err, &invalid) || !strings.Contains(err.Error(), "result import thread archive is invalid") {
			t.Fatalf("archive %q: error %v is not a ThreadArchiveInvalidError", archive, err)
		}
		if _, _, err := LatestTurnStartFailure([]byte(archive)); !errors.As(err, &invalid) {
			t.Fatalf("refusal reader: %v", err)
		}
		if _, _, err := ArchiveCurrentRequest([]byte(archive)); !errors.As(err, &invalid) {
			t.Fatalf("request reader: %v", err)
		}
		failure := ThreadArchiveInvalidFailure(invalid)
		if !IsThreadArchiveInvalidFailure(failure) || !strings.HasPrefix(failure, "infrastructure failure thread-archive-invalid: ") {
			t.Fatalf("failure %q", failure)
		}
	}
}
