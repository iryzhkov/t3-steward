package backlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ActivationTurnOutcome reports how one supervision activation's turn ended.
//
// An activation is not a task, and its turn completion is not a task result. It
// publishes no outputs, releases no dependents and satisfies no verification.
// The one thing it establishes is whether the overseer process ran to the end
// of its turn without the provider failing or leaving work in flight, which is
// why the provider-level check is the same one tasks use.
//
// What it deliberately cannot establish is a decision. A supervisor process
// exiting successfully is not gate acceptance: acceptance exists only where the
// coordinator recorded a structured decision under a live lease, a matching
// epoch and an expected revision. That count is passed in by the caller from
// its own decision records, never read out of the transcript, so no amount of
// agreeable prose in the summary can turn a no-decision activation into a
// decided one.
//
// The returned reason is empty when the turn itself was clean; it explains the
// provider-level failure otherwise. An activation whose turn failed still ends
// with an outcome, because the activation record must say how it ended.
func ActivationTurnOutcome(archive []byte, threadID, summary string, recordedDecisions int) (domain.ActivationOutcome, string, error) {
	reason, err := ResultCompletionFailure(archive, threadID, summary)
	if err != nil {
		return domain.ActivationOutcomeNone, "", err
	}
	if recordedDecisions > 0 {
		// The decisions are already durable, so the turn's own ending cannot
		// retract them; a failed turn after a recorded decision is still decided.
		return domain.ActivationOutcomeDecided, reason, nil
	}
	return domain.ActivationOutcomeNoDecision, reason, nil
}

// SessionNotReadyFailure is the reason a result is refused when the provider
// session did not end its turn cleanly.
const SessionNotReadyFailure = "provider session is not ready without an active turn or error"

// ResultCompletionFailureWithPause is ResultCompletionFailure for an attempt
// whose thread was paused by the quota watchdog on the worker host. The
// refusal of a session that is not ready stands, but it names the pause,
// so that "paused by quota watchdog: claudeAgent/claude/seven_day at 97%"
// is read instead of an unexplained session failure. pauseReason is the
// worker's pause summary; empty means no pause was recorded.
func ResultCompletionFailureWithPause(archive []byte, threadID, summary, pauseReason string) (string, error) {
	reason, err := ResultCompletionFailure(archive, threadID, summary)
	if err != nil || reason != SessionNotReadyFailure || strings.TrimSpace(pauseReason) == "" {
		return reason, err
	}
	return "paused by quota watchdog: " + strings.TrimSpace(pauseReason) + "; " + reason, nil
}

// TurnStartFailedActivity is the thread activity kind T3 appends when it
// refuses to start a provider turn: the provider rejected the turn input (as
// with a prompt over the input limit), the user message was not found, or the
// provider session could not take the turn. T3 then leaves the session in
// error with no active turn and starts nothing, so no turn ever reaches a
// terminal state the worker would otherwise wait for. Observed in T3 0.0.38
// (compat.MinServerVersion..compat.MaxServerVersion).
const TurnStartFailedActivity = "provider.turn.start.failed"

// runtimeErrorActivity is the activity T3 records for a provider runtime error
// inside a turn; its payload carries the provider's message.
const runtimeErrorActivity = "runtime.error"

// TurnStartFailure is a turn start T3 refused that no later turn superseded.
type TurnStartFailure struct {
	ActivityID string
	Detail     string
	CreatedAt  time.Time
}

// threadArchive is the part of T3's thread detail a completion judgement
// reads.
type threadArchive struct {
	Thread struct {
		ID         string `json:"id"`
		LatestTurn *struct {
			TurnID      string     `json:"turnId"`
			State       string     `json:"state"`
			RequestedAt *time.Time `json:"requestedAt"`
			StartedAt   *time.Time `json:"startedAt"`
			CompletedAt *time.Time `json:"completedAt"`
		} `json:"latestTurn"`
		Session *struct {
			ThreadID     string  `json:"threadId"`
			Status       string  `json:"status"`
			ActiveTurnID *string `json:"activeTurnId"`
			LastError    *string `json:"lastError"`
		} `json:"session"`
		Messages []struct {
			Role      string `json:"role"`
			CreatedAt string `json:"createdAt"`
		} `json:"messages"`
		Activities          []archiveActivity `json:"activities"`
		HasPendingApprovals bool              `json:"hasPendingApprovals"`
		HasPendingUserInput bool              `json:"hasPendingUserInput"`
		BackgroundLiveness  *string           `json:"backgroundLiveness"`
	} `json:"thread"`
}

// archiveActivity is one entry of a thread's activity log. T3 appends
// activities of many kinds, and each kind gives its payload its own shape: a
// provider error or a context-window activity carries an object as
// payload.detail where a refused turn start carries a string (observed with
// T3 0.0.45). Steward reads the text of two fields of two kinds and nothing
// else, so an activity never makes the archive unreadable: its payload is kept
// as raw JSON and read field by field, and an entry that is not even an object
// is kept as one Steward cannot interpret.
type archiveActivity struct {
	ID        string
	Kind      string
	TurnID    *string
	CreatedAt string
	Payload   json.RawMessage
	// Unreadable records that the entry is not an object with string
	// identity fields; it is then skipped by every judgement.
	Unreadable bool
}

// UnmarshalJSON decodes an activity without ever failing: a shape Steward
// does not expect is recorded, not refused.
func (a *archiveActivity) UnmarshalJSON(data []byte) error {
	*a = archiveActivity{}
	var fields struct {
		ID        json.RawMessage `json:"id"`
		Kind      json.RawMessage `json:"kind"`
		TurnID    json.RawMessage `json:"turnId"`
		CreatedAt json.RawMessage `json:"createdAt"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		a.Unreadable = true
		return nil
	}
	ok := true
	a.ID, ok = optionalJSONString(fields.ID, ok)
	a.Kind, ok = optionalJSONString(fields.Kind, ok)
	a.CreatedAt, ok = optionalJSONString(fields.CreatedAt, ok)
	if turn := bytes.TrimSpace(fields.TurnID); len(turn) != 0 && !bytes.Equal(turn, []byte("null")) {
		var id string
		if json.Unmarshal(turn, &id) == nil {
			a.TurnID = &id
		} else {
			ok = false
		}
	}
	a.Payload = fields.Payload
	a.Unreadable = !ok
	return nil
}

// optionalJSONString decodes an absent, null or string value; ok turns false
// for any other shape.
func optionalJSONString(raw json.RawMessage, ok bool) (string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", ok
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return "", false
	}
	return text, ok
}

// payloadText is the text of one field of the activity's payload, which
// Steward interprets. A string is its text; an absent or null field, or an
// absent or null payload, has none. Any other shape is rendered as compact
// JSON, so the provider's account still reaches the failure reason, and
// warning says so. A payload that is not an object has no fields to read, and
// warning says that instead.
func (a archiveActivity) payloadText(field string) (text, warning string) {
	if len(bytes.TrimSpace(a.Payload)) == 0 {
		return "", ""
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(a.Payload, &payload) != nil {
		var compact bytes.Buffer
		if json.Compact(&compact, a.Payload) != nil {
			compact.Reset()
			compact.WriteString("invalid JSON")
		}
		return "", fmt.Sprintf("activity %q (%s) has a payload that is not an object (%s); its payload.%s is not read", a.ID, a.Kind, providerDetail(compact.String()), field)
	}
	if payload == nil {
		return "", ""
	}
	raw := bytes.TrimSpace(payload[field])
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", ""
	}
	if json.Unmarshal(raw, &text) == nil {
		return text, ""
	}
	var compact bytes.Buffer
	if json.Compact(&compact, raw) != nil {
		return "", ""
	}
	return compact.String(), fmt.Sprintf("activity %q (%s) has a payload.%s that is not a string; it is read as its JSON text", a.ID, a.Kind, field)
}

// activityText is payloadText without the warning, for the judgements;
// ThreadArchiveWarnings reports the warnings.
func activityText(activity archiveActivity, field string) string {
	text, _ := activity.payloadText(field)
	return text
}

// interpretedField is the payload field Steward reads from an activity of
// kind, or empty for a kind it does not interpret.
func interpretedField(kind string) string {
	switch kind {
	case TurnStartFailedActivity:
		return "detail"
	case runtimeErrorActivity:
		return "message"
	}
	return ""
}

// ThreadArchiveInvalidReason is the reason an attempt fails with when its
// thread archive cannot be decoded at all. It is an infrastructure failure:
// the archive is evidence about the provider turn, not the task's result, so
// the declared outputs and commits are still collected.
const ThreadArchiveInvalidReason = "thread-archive-invalid"

// ThreadArchiveInvalidError reports a thread archive whose fields Steward
// interprets cannot be decoded. The same bytes fail the same way every time.
type ThreadArchiveInvalidError struct {
	// Context names the reader, such as "result import".
	Context string
	Err     error
}

func (e *ThreadArchiveInvalidError) Error() string {
	prefix := "thread archive is invalid: "
	if e.Context != "" {
		prefix = e.Context + " " + prefix
	}
	return prefix + e.Err.Error()
}

func (e *ThreadArchiveInvalidError) Unwrap() error { return e.Err }

// ThreadArchiveInvalidFailure is the attempt failure for err, a thread
// archive that cannot be decoded: class infrastructure, reason
// thread-archive-invalid.
func ThreadArchiveInvalidFailure(err error) string {
	return "infrastructure failure " + ThreadArchiveInvalidReason + ": " + providerDetail(err.Error()) +
		"; the declared outputs and commits were collected and the archive is kept as the attempt's thread log"
}

// IsThreadArchiveInvalidFailure reports whether failure names an undecodable
// thread archive.
func IsThreadArchiveInvalidFailure(failure string) bool {
	return strings.Contains(failure, "infrastructure failure "+ThreadArchiveInvalidReason+": ")
}

func decodeThreadArchive(archive []byte, context string) (threadArchive, error) {
	var snapshot threadArchive
	if err := json.Unmarshal(archive, &snapshot); err != nil {
		return threadArchive{}, &ThreadArchiveInvalidError{Context: context, Err: err}
	}
	return snapshot, nil
}

// ThreadArchiveWarnings lists what a completion judgement of archive read in
// a shape it did not expect: an interpreted activity field that is not a
// string, or an activity of an interpreted kind that is not an object. None of
// them stops the judgement; they are reported so that a change in T3's
// activity schema is seen before it matters.
func ThreadArchiveWarnings(archive []byte) ([]string, error) {
	snapshot, err := decodeThreadArchive(archive, "")
	if err != nil {
		return nil, err
	}
	var warnings []string
	for index, activity := range snapshot.Thread.Activities {
		if activity.Unreadable {
			warnings = append(warnings, fmt.Sprintf("activity %d has fields of an unexpected shape and is not interpreted", index))
			continue
		}
		field := interpretedField(activity.Kind)
		if field == "" {
			continue
		}
		if _, warning := activity.payloadText(field); warning != "" {
			warnings = append(warnings, warning)
		}
	}
	return warnings, nil
}

// maxProviderDetail bounds the provider text a failure reason carries; T3's
// own details are short, and the reason is printed on one line.
const maxProviderDetail = 2000

func providerDetail(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > maxProviderDetail {
		text = strings.ToValidUTF8(text[:maxProviderDetail], "") + "..."
	}
	return text
}

// latestUserMessageAt is the time of the thread's latest user message, which
// is the current turn start request: T3 gives a turn, and a refusal of its
// start, the time of the message that requested it.
func (a threadArchive) latestUserMessageAt() *time.Time {
	var latest *time.Time
	for _, message := range a.Thread.Messages {
		if message.Role != "user" {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, message.CreatedAt)
		if err == nil && (latest == nil || at.After(*latest)) {
			latest = &at
		}
	}
	return latest
}

// latestTurnStartFailure finds the refusal of the current start request: the
// newest refused turn start, unless a turn was requested after it or a later
// user message asked for a new turn. A refusal a later request superseded is
// history, whether or not that request has a turn yet.
func (a threadArchive) latestTurnStartFailure() (TurnStartFailure, bool) {
	var latest TurnStartFailure
	found := false
	for _, activity := range a.Thread.Activities {
		if activity.Unreadable || activity.Kind != TurnStartFailedActivity {
			continue
		}
		created, err := time.Parse(time.RFC3339Nano, activity.CreatedAt)
		if err != nil {
			continue
		}
		if !found || !created.Before(latest.CreatedAt) {
			latest = TurnStartFailure{ActivityID: activity.ID, Detail: providerDetail(activityText(activity, "detail")), CreatedAt: created}
			found = true
		}
	}
	if !found {
		return TurnStartFailure{}, false
	}
	if turn := a.Thread.LatestTurn; turn != nil && turn.TurnID != "" {
		since := turn.RequestedAt
		if since == nil {
			since = turn.StartedAt
		}
		if since == nil || !latest.CreatedAt.After(*since) {
			return TurnStartFailure{}, false
		}
	}
	if request := a.latestUserMessageAt(); request != nil && latest.CreatedAt.Before(*request) {
		return TurnStartFailure{}, false
	}
	if latest.Detail == "" {
		latest.Detail = "T3 recorded no detail"
	}
	return latest, true
}

// failedTurnDetail is what T3 recorded about why the latest turn failed: the
// turn's own runtime error, or else the session's last error.
func (a threadArchive) failedTurnDetail() string {
	turn := a.Thread.LatestTurn
	if turn != nil && turn.TurnID != "" {
		detail := ""
		for _, activity := range a.Thread.Activities {
			if !activity.Unreadable && activity.Kind == runtimeErrorActivity && activity.TurnID != nil && *activity.TurnID == turn.TurnID {
				if text := providerDetail(activityText(activity, "message")); text != "" {
					detail = text
				}
			}
		}
		if detail != "" {
			return detail
		}
	}
	if session := a.Thread.Session; session != nil && session.LastError != nil {
		return providerDetail(*session.LastError)
	}
	return ""
}

// LatestTurnStartFailure reports the turn start T3 refused for the current
// start request of the thread in archive: no later turn or user message
// superseded it.
func LatestTurnStartFailure(archive []byte) (TurnStartFailure, bool, error) {
	snapshot, err := decodeThreadArchive(archive, "")
	if err != nil {
		return TurnStartFailure{}, false, err
	}
	failure, ok := snapshot.latestTurnStartFailure()
	return failure, ok, nil
}

// ArchiveCurrentRequest reports the thread id of the archive and its latest
// user message time, the current start request as the archive projects it.
// The worker compares it with the shell's current request, because T3
// projects the two separately and the detail can lag.
func ArchiveCurrentRequest(archive []byte) (string, *time.Time, error) {
	snapshot, err := decodeThreadArchive(archive, "")
	if err != nil {
		return "", nil, err
	}
	return snapshot.Thread.ID, snapshot.latestUserMessageAt(), nil
}

// TurnStartRefusedFailure is the prefix of the reason an attempt fails with
// when T3 refused to start its turn.
const TurnStartRefusedFailure = "T3 refused to start the provider turn"

// ResultCompletionFailure validates provider completion independently of prose.
// A legacy done marker is optional and never overrides unsuccessful execution.
func ResultCompletionFailure(archive []byte, threadID, summary string) (string, error) {
	snapshot, err := decodeThreadArchive(archive, "result import")
	if err != nil {
		return "", err
	}
	if reason, failed := backlogFailedReason(summary); failed {
		return reason, nil
	}
	for _, line := range strings.Split(summary, "\n") {
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "backlog status: continue", "backlog status: needs-input":
			return "agent reported unfinished work: " + strings.TrimSpace(line), nil
		}
	}
	thread := snapshot.Thread
	if threadID == "" || thread.ID != threadID {
		return "thread completion identity is missing or mismatched", nil
	}
	// A refused start is checked before the latest turn: after a refusal of a
	// later turn (a wake, a resume) the latest turn is an earlier one that may
	// well have completed.
	if refused, ok := snapshot.latestTurnStartFailure(); ok {
		return TurnStartRefusedFailure + ": " + refused.Detail, nil
	}
	turn := thread.LatestTurn
	if turn == nil || turn.TurnID == "" || turn.State != "completed" {
		if detail := snapshot.failedTurnDetail(); detail != "" {
			return "provider turn did not complete successfully: " + detail, nil
		}
		return "provider turn did not complete successfully", nil
	}
	if turn.StartedAt == nil || turn.CompletedAt == nil || turn.StartedAt.IsZero() || turn.CompletedAt.IsZero() || turn.CompletedAt.Before(*turn.StartedAt) {
		return "provider completion timestamps are missing or invalid", nil
	}
	session := thread.Session
	if session == nil || session.ThreadID != threadID || session.Status != "ready" || session.ActiveTurnID != nil || (session.LastError != nil && *session.LastError != "") {
		return SessionNotReadyFailure, nil
	}
	if thread.HasPendingApprovals || thread.HasPendingUserInput || (thread.BackgroundLiveness != nil && *thread.BackgroundLiveness == "working") {
		return "thread still has pending input, approval, or background work", nil
	}
	return "", nil
}
