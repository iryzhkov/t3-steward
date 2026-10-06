package backlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// compactionFixtures are the thread archives the completion tests of this
// package judge, each one a shape T3 has served: a clean turn, a refused first
// turn start, a refusal after an earlier completed turn, a refusal superseded
// by a later request, a failed turn with a runtime error, and a thread whose
// decision rests on several competing refusals, user messages and errors.
var compactionFixtures = map[string]string{
	"clean turn":        `{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`,
	"session not ready": `{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"stopped","activeTurnId":null,"lastError":null}}}`,
	"pending input":     `{"thread":{"id":"thread-1","hasPendingUserInput":true,"backgroundLiveness":"monitoring","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`,
	"refused first turn": `{"thread":{"id":"thread-1","latestTurn":null,` +
		`"session":{"threadId":"thread-1","status":"error","activeTurnId":null,"lastError":"` + turnStartRefusal + `"},` +
		`"activities":[{"id":"activity-1","tone":"error","kind":"provider.turn.start.failed","summary":"Provider turn start failed",` +
		`"payload":{"detail":"` + turnStartRefusal + `"},"turnId":null,"createdAt":"2026-10-02T05:00:00.000Z"}]}}`,
	"refusal after completed turn": `{"thread":{"id":"thread-1",` +
		`"latestTurn":{"turnId":"turn-1","state":"completed","requestedAt":"2026-10-02T04:59:00Z","startedAt":"2026-10-02T05:00:00Z","completedAt":"2026-10-02T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null},` +
		`"activities":[{"id":"activity-1","tone":"error","kind":"provider.turn.start.failed","payload":{"detail":"refused"},"turnId":null,"createdAt":"2026-10-02T05:30:00Z"}]}}`,
	"refusal superseded by request": `{"thread":{"id":"thread-1","latestTurn":null,` +
		`"session":{"threadId":"thread-1","status":"error","activeTurnId":null,"lastError":"old"},` +
		`"messages":[{"id":"m1","role":"user","createdAt":"2026-10-02T05:00:00.000Z"},{"id":"m2","role":"user","createdAt":"2026-10-02T05:05:00.000Z"}],` +
		`"activities":[{"id":"a1","kind":"provider.turn.start.failed","payload":{"detail":"old"},"turnId":null,"createdAt":"2026-10-02T05:00:00.000Z"}]}}`,
	"failed turn runtime error": `{"thread":{"id":"thread-1",` +
		`"latestTurn":{"turnId":"turn-1","state":"error","requestedAt":"2026-10-02T04:59:00Z","startedAt":"2026-10-02T05:00:00Z","completedAt":"2026-10-02T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"error","activeTurnId":null,"lastError":"session went away"},` +
		`"activities":[{"id":"a","tone":"error","kind":"runtime.error","payload":{"message":"overloaded_error"},"turnId":"turn-1","createdAt":"2026-10-02T05:00:30Z"}]}}`,
	"competing evidence": `{"snapshotSequence":41,"thread":{"id":"thread-1","title":"x",` +
		`"latestTurn":{"turnId":"turn-2","state":"error","requestedAt":"2026-10-02T06:00:00Z","startedAt":"2026-10-02T06:00:01Z","completedAt":"2026-10-02T06:10:00Z"},` +
		`"session":{"threadId":"thread-1","status":"error","activeTurnId":null,"lastError":"session lost"},` +
		`"messages":[{"id":"u1","role":"user","text":"first","createdAt":"2026-10-02T05:00:00Z"},` +
		`{"id":"u3","role":"user","text":"third","createdAt":"2026-10-02T07:00:00Z"},` +
		`{"id":"x1","role":"assistant","text":"the real answer","createdAt":"2026-10-02T06:09:00Z"},` +
		`{"id":"u2","role":"user","text":"second","createdAt":"2026-10-02T06:00:00Z"},` +
		`{"id":"x2","role":"assistant","text":"","createdAt":"2026-10-02T06:09:30Z"},` +
		`{"id":"bad","role":"user","createdAt":"not a time"}],` +
		`"activities":[{"id":"r1","kind":"provider.turn.start.failed","payload":{"detail":"first refusal"},"turnId":null,"createdAt":"2026-10-02T07:01:00Z"},` +
		`{"id":"r2","kind":"provider.turn.start.failed","payload":{"detail":"tied refusal"},"turnId":null,"createdAt":"2026-10-02T07:01:00Z"},` +
		`{"id":"r0","kind":"provider.turn.start.failed","payload":{"detail":"old refusal"},"turnId":null,"createdAt":"2026-10-02T04:00:00Z"},` +
		`{"id":"e1","kind":"runtime.error","payload":{"message":"overloaded"},"turnId":"turn-2","createdAt":"2026-10-02T06:05:00Z"},` +
		`{"id":"e2","kind":"runtime.error","payload":{"message":"   "},"turnId":"turn-2","createdAt":"2026-10-02T06:06:00Z"},` +
		`{"id":"e0","kind":"runtime.error","payload":{"message":"earlier turn"},"turnId":"turn-1","createdAt":"2026-10-02T05:06:00Z"}]}}`,
}

// inflateArchive appends filler messages and activities after the fixture's
// own entries, so that the evidence a decision reads lies outside any tail a
// compaction keeps. Filler never changes a decision: assistant messages with
// text and tool activities, both before every decision timestamp.
func inflateArchive(t *testing.T, archive string, count int) []byte {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal([]byte(archive), &root); err != nil {
		t.Fatal(err)
	}
	thread := root["thread"].(map[string]any)
	messages, _ := thread["messages"].([]any)
	activities, _ := thread["activities"].([]any)
	for index := 0; index < count; index++ {
		messages = append(messages, map[string]any{
			"id": fmt.Sprintf("filler-%d", index), "role": "assistant", "turnId": "turn-0",
			"text": strings.Repeat("progress ", 40), "createdAt": "2026-01-01T00:00:00Z",
		})
		activities = append(activities, map[string]any{
			"id": fmt.Sprintf("tool-%d", index), "kind": "tool.completed", "turnId": "turn-0",
			"payload": map[string]any{"detail": strings.Repeat("output ", 40)}, "createdAt": "2026-01-01T00:00:00Z",
		})
	}
	thread["messages"], thread["activities"] = messages, activities
	thread["proposedPlans"] = []any{strings.Repeat("plan ", 2000)}
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type archiveDecisions struct {
	Reasons       []string
	Refusal       TurnStartFailure
	Refused       bool
	ThreadID      string
	LatestRequest string
}

// decisionsOf is every judgement this package makes from a thread archive,
// over the summaries and pauses a collection can pair it with.
func decisionsOf(t *testing.T, archive []byte) archiveDecisions {
	t.Helper()
	var decisions archiveDecisions
	for _, summary := range []string{"", "Finished.", "BACKLOG STATUS: done", "BACKLOG STATUS: continue", "BACKLOG STATUS: failed\nsetup failed"} {
		for _, pause := range []string{"", "claudeAgent/claude/seven_day at 97%"} {
			for _, threadID := range []string{"thread-1", "other"} {
				reason, err := ResultCompletionFailureWithPause(archive, threadID, summary, pause)
				if err != nil {
					t.Fatal(err)
				}
				decisions.Reasons = append(decisions.Reasons, reason)
			}
		}
	}
	for _, recorded := range []int{0, 1} {
		outcome, reason, err := ActivationTurnOutcome(archive, "thread-1", "done", recorded)
		if err != nil {
			t.Fatal(err)
		}
		decisions.Reasons = append(decisions.Reasons, string(outcome)+"|"+reason)
	}
	var err error
	if decisions.Refusal, decisions.Refused, err = LatestTurnStartFailure(archive); err != nil {
		t.Fatal(err)
	}
	var latest any
	decisions.ThreadID, latest, err = ArchiveCurrentRequest(archive)
	if err != nil {
		t.Fatal(err)
	}
	decisions.LatestRequest = fmt.Sprint(latest)
	return decisions
}

func truncationOf(t *testing.T, archive []byte) ThreadArchiveTruncation {
	t.Helper()
	var root struct {
		Truncation *ThreadArchiveTruncation `json:"stewardTruncation"`
	}
	if err := json.Unmarshal(archive, &root); err != nil || root.Truncation == nil {
		t.Fatalf("compacted archive has no truncation marker: %v", err)
	}
	return *root.Truncation
}

// A compacted archive reaches the same completion decisions as the full one,
// fits its budget and records what it was cut from.
func TestCompactedThreadArchiveKeepsEveryCompletionDecision(t *testing.T) {
	const budget = 16 << 10
	for name, fixture := range compactionFixtures {
		t.Run(name, func(t *testing.T) {
			full := inflateArchive(t, fixture, 400)
			if len(full) <= budget {
				t.Fatalf("fixture is not over budget: %d", len(full))
			}
			compacted, err := CompactThreadArchive(full, budget, "thread-archives/attempt-1/full.json")
			if err != nil {
				t.Fatal(err)
			}
			if len(compacted) > budget {
				t.Fatalf("compacted archive is %d bytes, over %d", len(compacted), budget)
			}
			if want, got := decisionsOf(t, full), decisionsOf(t, compacted); !reflect.DeepEqual(want, got) {
				t.Fatalf("decisions changed\nfull      %+v\ncompacted %+v", want, got)
			}
			sum := sha256.Sum256(full)
			marker := truncationOf(t, compacted)
			if marker.OriginalSize != int64(len(full)) || marker.OriginalSHA256 != hex.EncodeToString(sum[:]) ||
				marker.RetainedPath != "thread-archives/attempt-1/full.json" || marker.OmittedMessages == 0 || marker.OmittedActivities == 0 {
				t.Fatalf("marker=%+v", marker)
			}
			// A second compaction of the same input is byte-identical, which
			// the admission check and the publication rely on.
			again, err := CompactThreadArchive(full, budget, "thread-archives/attempt-1/full.json")
			if err != nil || !bytes.Equal(again, compacted) {
				t.Fatalf("compaction is not deterministic: %v", err)
			}
		})
	}
}

// The final assistant message is kept even when it is far from the tail.
func TestCompactedThreadArchiveKeepsTheFinalAssistantMessage(t *testing.T) {
	archive := `{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null},"messages":[` +
		`{"id":"final","role":"assistant","text":"FINAL ANSWER","createdAt":"2026-09-13T05:00:59Z"}` +
		strings.Repeat(`,{"id":"tool","role":"tool","text":"`+strings.Repeat("t", 300)+`","createdAt":"2026-09-13T05:00:10Z"}`, 300) + `]}}`
	compacted, err := CompactThreadArchive([]byte(archive), 8<<10, "retained.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(compacted), "FINAL ANSWER") {
		t.Fatal("final assistant message dropped")
	}
}

// Message bodies are the last thing given up: an archive whose required
// messages alone are over budget keeps their decision fields and the final
// assistant text, and says so.
func TestCompactedThreadArchiveDropsBodiesOnlyAsALastResort(t *testing.T) {
	archive := `{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null},"messages":[` +
		`{"id":"ask","role":"user","text":"` + strings.Repeat("q", 20000) + `","createdAt":"2026-09-13T04:59:59Z"},` +
		`{"id":"final","role":"assistant","text":"` + strings.Repeat("a", 3000) + `","createdAt":"2026-09-13T05:00:59Z"}]}}`
	compacted, err := CompactThreadArchive([]byte(archive), 4<<10, "retained.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(compacted) > 4<<10 || !truncationOf(t, compacted).MessageBodiesOmitted {
		t.Fatalf("size=%d marker=%+v", len(compacted), truncationOf(t, compacted))
	}
	if !strings.Contains(string(compacted), strings.Repeat("a", 3000)) || strings.Contains(string(compacted), strings.Repeat("q", 100)) {
		t.Fatal("the last resort must keep the final text and only the final text")
	}
	if want, got := decisionsOf(t, []byte(archive)), decisionsOf(t, compacted); !reflect.DeepEqual(want, got) {
		t.Fatalf("decisions changed\nfull      %+v\ncompacted %+v", want, got)
	}
}

// Fields of the latest turn and the session that no decision reads are not
// evidence, so they cannot keep an archive from compacting. The independent
// review of round 1 found 2,000 bytes of latestTurn.padding refused.
func TestCompactThreadArchiveProjectsUnusedTurnAndSessionFields(t *testing.T) {
	padding := strings.Repeat("x", 2000)
	full := strings.Replace(compactionFixtures["clean turn"], `"turnId":"turn-1"`, `"padding":"`+padding+`","turnId":"turn-1"`, 1)
	full = strings.Replace(full, `"threadId":"thread-1"`, `"providerOptions":{"padding":"`+padding+`"},"threadId":"thread-1"`, 1)
	compacted, err := CompactThreadArchive([]byte(full), 1024, "retained.json")
	if err != nil {
		t.Fatalf("unused nested fields defeat compaction: %v", err)
	}
	if len(compacted) > 1024 || strings.Contains(string(compacted), padding) {
		t.Fatalf("unused nested fields kept: %d bytes", len(compacted))
	}
	if want, got := decisionsOf(t, []byte(full)), decisionsOf(t, compacted); !reflect.DeepEqual(want, got) {
		t.Fatalf("decisions changed\nfull      %+v\ncompacted %+v", want, got)
	}
	if got, err := ResultCompletionFailure(compacted, "thread-1", ""); err != nil || got != "" {
		t.Fatalf("decision=%q err=%v", got, err)
	}
	marker := truncationOf(t, compacted)
	if !marker.MessageBodiesOmitted || !reflect.DeepEqual(marker.OmittedFields, []string{"thread.latestTurn.padding", "thread.session.providerOptions"}) {
		t.Fatalf("marker=%+v", marker)
	}
}

// The last resort leaves out optional bodies but keeps the final assistant
// message's text. The independent review of round 1 found a short final
// answer dropped beside a 20,000-byte user request.
func TestCompactedThreadArchiveKeepsTheFinalTextInTheLastResort(t *testing.T) {
	var root map[string]any
	if err := json.Unmarshal([]byte(compactionFixtures["clean turn"]), &root); err != nil {
		t.Fatal(err)
	}
	root["thread"].(map[string]any)["messages"] = []any{
		map[string]any{"id": "ask", "role": "user", "text": strings.Repeat("q", 20000), "createdAt": "2026-09-13T04:59:59Z"},
		map[string]any{"id": "final", "role": "assistant", "text": "FINAL ANSWER", "createdAt": "2026-09-13T05:00:59Z"},
	}
	full, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	compacted, err := CompactThreadArchive(full, 4096, "retained.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(compacted), "FINAL ANSWER") {
		t.Fatal("short final assistant message lost when a large user body forces the last resort")
	}
	if strings.Contains(string(compacted), strings.Repeat("q", 100)) || !truncationOf(t, compacted).MessageBodiesOmitted {
		t.Fatalf("optional body kept: %s", compacted)
	}
	if want, got := decisionsOf(t, full), decisionsOf(t, compacted); !reflect.DeepEqual(want, got) {
		t.Fatalf("decisions changed\nfull      %+v\ncompacted %+v", want, got)
	}
}

// An archive already within budget is returned unchanged, and one that cannot
// be judged or cannot fit is refused rather than replaced with something else.
func TestCompactThreadArchiveRefusesWhatItCannotKeepWhole(t *testing.T) {
	small := []byte(compactionFixtures["clean turn"])
	if out, err := CompactThreadArchive(small, int64(len(small)), "retained.json"); err != nil || !bytes.Equal(out, small) {
		t.Fatalf("archive within budget changed: %v", err)
	}
	if _, err := CompactThreadArchive([]byte("not json"+strings.Repeat(" ", 100)), 10, "retained.json"); err == nil {
		t.Fatal("invalid archive compacted")
	}
	if _, err := CompactThreadArchive([]byte(`{"thread":"text","pad":"`+strings.Repeat("p", 100)+`"}`), 50, "retained.json"); err == nil {
		t.Fatal("archive the completion check cannot read compacted")
	}
	if _, err := CompactThreadArchive(inflateArchive(t, compactionFixtures["competing evidence"], 10), 64, "retained.json"); err == nil {
		t.Fatal("archive compacted below the size of its decision evidence")
	}
	// Required content that is itself over budget is refused, never cut: a
	// session error a decision reports and a final assistant text.
	longError := strings.Replace(compactionFixtures["failed turn runtime error"], `"session went away"`, `"`+strings.Repeat("e", 5000)+`"`, 1)
	if _, err := CompactThreadArchive([]byte(longError), 4<<10, "retained.json"); !errors.Is(err, ErrThreadArchiveEvidenceTooLarge) {
		t.Fatalf("session error over budget: %v", err)
	}
	longFinal := strings.Replace(compactionFixtures["clean turn"], `}}`, `},"messages":[{"id":"final","role":"assistant","text":"`+strings.Repeat("a", 5000)+`","createdAt":"2026-09-13T05:00:59Z"}]}`, 1)
	if _, err := CompactThreadArchive([]byte(longFinal), 4<<10, "retained.json"); !errors.Is(err, ErrThreadArchiveEvidenceTooLarge) {
		t.Fatalf("final assistant text over budget: %v", err)
	}
}
