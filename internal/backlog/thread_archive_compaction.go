package backlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ThreadArchiveTruncationKey is the top-level key under which a compacted
// thread archive records what it was cut from. T3 serves no such key, so its
// presence is what tells a reader the archive is not the whole thread.
const ThreadArchiveTruncationKey = "stewardTruncation"

// ThreadArchiveTruncation is the marker a compacted thread archive carries.
type ThreadArchiveTruncation struct {
	// OriginalSize and OriginalSHA256 identify the full archive T3 exported.
	OriginalSize   int64  `json:"originalSize"`
	OriginalSHA256 string `json:"originalSha256"`
	// RetainedPath is where the worker keeps the full archive, relative to
	// its artifact root. That copy is never uploaded.
	RetainedPath string `json:"retainedPath"`
	// OmittedMessages and OmittedActivities count the entries of the
	// thread's messages and activities that were left out.
	OmittedMessages   int `json:"omittedMessages"`
	OmittedActivities int `json:"omittedActivities"`
	// OmittedEntries counts the entries left out of the thread's other lists.
	OmittedEntries map[string]int `json:"omittedEntries,omitempty"`
	// OmittedFields names the fields left out whole: top-level fields beside
	// the thread, and, in the last resort, thread fields and fields of the
	// latest turn and the session that no decision reads.
	OmittedFields []string `json:"omittedFields,omitempty"`
	// MessageBodiesOmitted says the kept messages and activities hold only
	// the fields a completion decision reads, and the final assistant
	// message its text besides. It is set only when nothing larger would fit.
	MessageBodiesOmitted bool `json:"messageBodiesOmitted,omitempty"`
}

// ErrThreadArchiveEvidenceTooLarge is returned when the evidence a completion
// decision reads does not fit the budget on its own. The archive is then
// refused, never cut below what a decision needs.
var ErrThreadArchiveEvidenceTooLarge = errors.New("thread archive: completion evidence exceeds the size limit")

// compactionTails are the numbers of trailing list entries a compaction tries
// to keep, largest first.
var compactionTails = []int{256, 128, 64, 32, 16, 8, 4, 2, 1, 0}

// decisionThreadFields are the thread fields a completion decision reads
// besides messages and activities. The last-resort compaction keeps only these.
var decisionThreadFields = map[string]bool{
	"id": true, "latestTurn": true, "session": true,
	"hasPendingApprovals": true, "hasPendingUserInput": true, "backgroundLiveness": true,
}

// decisionObjectFields are, for the thread fields that are objects, the
// fields of them threadArchive decodes. The last-resort compaction keeps only
// these, so data a decision never reads cannot keep an archive over budget.
var decisionObjectFields = map[string][]string{
	"latestTurn": {"turnId", "state", "requestedAt", "startedAt", "completedAt"},
	"session":    {"threadId", "status", "activeTurnId", "lastError"},
}

// CompactThreadArchive returns archive unchanged when it is at most maxBytes,
// and otherwise a smaller archive of the same thread that is.
//
// The compacted archive keeps every thread field, the latest user message,
// the final assistant message, the turn start refusal and the runtime error
// the completion decision would take, and as long a tail of messages,
// activities and the thread's other lists as fits. Fields beside the thread
// are left out. Only if that cannot fit with no tail at all are the thread,
// latest turn and session fields no decision reads left out and the kept
// entries cut to the fields a decision reads, which for the final assistant
// message include its text. Every judgement ResultCompletionFailure,
// LatestTurnStartFailure and ArchiveCurrentRequest make is the same for the
// compacted archive as for the full one. The archive records its original
// size and digest and retainedPath under ThreadArchiveTruncationKey.
//
// The result depends only on its arguments, so a size check and the
// publication it admits see the same bytes. An archive the completion check
// cannot read is refused, as is one whose decision evidence and final
// assistant text alone are over maxBytes.
func CompactThreadArchive(archive []byte, maxBytes int64, retainedPath string) ([]byte, error) {
	if int64(len(archive)) <= maxBytes {
		return archive, nil
	}
	var snapshot threadArchive
	if err := json.Unmarshal(archive, &snapshot); err != nil {
		return nil, fmt.Errorf("compact thread archive: %w", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(archive, &root); err != nil {
		return nil, fmt.Errorf("compact thread archive: %w", err)
	}
	var thread map[string]json.RawMessage
	if raw, ok := root["thread"]; ok {
		if err := json.Unmarshal(raw, &thread); err != nil {
			return nil, fmt.Errorf("compact thread archive: %w", err)
		}
	}
	sum := sha256.Sum256(archive)
	compaction := archiveCompaction{
		root: root, thread: thread, lists: map[string][]json.RawMessage{},
		marker: ThreadArchiveTruncation{
			OriginalSize: int64(len(archive)), OriginalSHA256: hex.EncodeToString(sum[:]), RetainedPath: retainedPath,
		},
	}
	for key, raw := range thread {
		if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '[' {
			continue
		}
		var entries []json.RawMessage
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, fmt.Errorf("compact thread archive: %s: %w", key, err)
		}
		compaction.lists[key] = entries
	}
	var messages map[int]bool
	messages, compaction.finalAssistant = requiredMessages(compaction.lists["messages"])
	compaction.required = map[string]map[int]bool{
		"messages":   messages,
		"activities": snapshot.requiredActivities(),
	}
	for _, minimal := range []bool{false, true} {
		for _, tail := range compactionTails {
			if minimal && tail != 0 {
				continue
			}
			out, err := compaction.build(tail, minimal)
			if err != nil {
				return nil, err
			}
			if int64(len(out)) <= maxBytes {
				return out, nil
			}
		}
	}
	return nil, ErrThreadArchiveEvidenceTooLarge
}

// archiveCompaction is one archive taken apart for compaction.
type archiveCompaction struct {
	root   map[string]json.RawMessage
	thread map[string]json.RawMessage
	// lists holds the thread's list fields, entry by entry.
	lists map[string][]json.RawMessage
	// required holds, per list, the indexes of the entries a decision reads.
	required map[string]map[int]bool
	// finalAssistant is the index in messages of the final assistant
	// message, whose text every compaction keeps; -1 when there is none.
	finalAssistant int
	marker         ThreadArchiveTruncation
}

// build assembles the archive that keeps the last tail entries of every
// thread list besides the required ones. minimal also leaves out the thread
// fields, latest turn and session fields, and entry fields no decision reads.
func (c archiveCompaction) build(tail int, minimal bool) ([]byte, error) {
	marker := c.marker
	marker.MessageBodiesOmitted = minimal
	marker.OmittedEntries = map[string]int{}
	for key := range c.root {
		if key != "thread" {
			marker.OmittedFields = append(marker.OmittedFields, key)
		}
	}
	var thread map[string]json.RawMessage
	if c.thread != nil {
		thread = make(map[string]json.RawMessage, len(c.thread))
	}
	for key, raw := range c.thread {
		entries, isList := c.lists[key]
		decision := key == "messages" || key == "activities" || decisionThreadFields[key]
		if minimal && !decision {
			marker.OmittedFields = append(marker.OmittedFields, "thread."+key)
			continue
		}
		if !isList {
			if keep, ok := decisionObjectFields[key]; ok && minimal {
				var omitted []string
				raw, omitted = projectObject(raw, keep)
				for _, field := range omitted {
					marker.OmittedFields = append(marker.OmittedFields, "thread."+key+"."+field)
				}
			}
			thread[key] = raw
			continue
		}
		kept := make([]json.RawMessage, 0, tail+len(c.required[key]))
		for index, entry := range entries {
			if !c.required[key][index] && index < len(entries)-tail {
				continue
			}
			if minimal {
				var err error
				keepText := key == "messages" && index == c.finalAssistant
				if entry, err = decisionFields(key, entry, keepText); err != nil {
					return nil, err
				}
			}
			kept = append(kept, entry)
		}
		raw, err := json.Marshal(kept)
		if err != nil {
			return nil, err
		}
		thread[key] = raw
		omitted := len(entries) - len(kept)
		switch key {
		case "messages":
			marker.OmittedMessages = omitted
		case "activities":
			marker.OmittedActivities = omitted
		default:
			if omitted != 0 {
				marker.OmittedEntries["thread."+key] = omitted
			}
		}
	}
	sort.Strings(marker.OmittedFields)
	out := map[string]any{ThreadArchiveTruncationKey: marker}
	if _, ok := c.root["thread"]; ok {
		if thread == nil {
			out["thread"] = json.RawMessage("null")
		} else {
			out["thread"] = thread
		}
	}
	return json.Marshal(out)
}

// projectObject cuts a JSON object to the fields named in keep and returns
// the names of the fields it left out, sorted. A field matches a name as
// encoding/json matches it, ignoring case. A value that is not an object,
// such as null, is returned unchanged.
func projectObject(raw json.RawMessage, keep []string) (json.RawMessage, []string) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return raw, nil
	}
	kept, omitted := keepFields(fields, keep)
	if len(omitted) == 0 {
		return raw, nil
	}
	out, err := json.Marshal(kept)
	if err != nil {
		return raw, nil
	}
	return out, omitted
}

// keepFields splits fields into those named in keep, ignoring case as
// encoding/json does, and the sorted names of the rest.
func keepFields(fields map[string]json.RawMessage, keep []string) (map[string]json.RawMessage, []string) {
	kept := make(map[string]json.RawMessage, len(keep))
	var omitted []string
	for name, raw := range fields {
		matched := false
		for _, want := range keep {
			if strings.EqualFold(name, want) {
				matched = true
				break
			}
		}
		if matched {
			kept[name] = raw
		} else {
			omitted = append(omitted, name)
		}
	}
	sort.Strings(omitted)
	return kept, omitted
}

// decisionFields cuts one message or activity to the fields a completion
// decision reads, and keepText also keeps a message's text.
func decisionFields(list string, entry json.RawMessage, keepText bool) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(entry, &fields); err != nil || fields == nil {
		return entry, nil
	}
	keep := []string{"id", "role", "turnId", "createdAt"}
	if keepText {
		keep = append(keep, "text")
	}
	if list == "activities" {
		keep = []string{"id", "kind", "turnId", "createdAt"}
		var payload struct {
			Detail  json.RawMessage `json:"detail,omitempty"`
			Message json.RawMessage `json:"message,omitempty"`
		}
		if raw, ok := fields["payload"]; ok && json.Unmarshal(raw, &payload) == nil {
			raw, err := json.Marshal(payload)
			if err != nil {
				return nil, err
			}
			fields["payload"] = raw
			keep = append(keep, "payload")
		}
	}
	cut, _ := keepFields(fields, keep)
	return json.Marshal(cut)
}

// requiredMessages are the indexes of the latest user message, which is the
// current start request, and of the final assistant message, which is also
// returned on its own (-1 when there is none).
func requiredMessages(entries []json.RawMessage) (map[int]bool, int) {
	required := map[int]bool{}
	latestUser, finalAssistant := -1, -1
	var latest time.Time
	for index, entry := range entries {
		var message struct {
			Role      string          `json:"role"`
			Text      json.RawMessage `json:"text"`
			CreatedAt json.RawMessage `json:"createdAt"`
		}
		if json.Unmarshal(entry, &message) != nil {
			continue
		}
		switch message.Role {
		case "user":
			var created string
			if json.Unmarshal(message.CreatedAt, &created) != nil {
				continue
			}
			if at, err := time.Parse(time.RFC3339Nano, created); err == nil && (latestUser < 0 || at.After(latest)) {
				latestUser, latest = index, at
			}
		case "assistant":
			var text string
			if json.Unmarshal(message.Text, &text) == nil && text != "" {
				finalAssistant = index
			}
		}
	}
	for _, index := range []int{latestUser, finalAssistant} {
		if index >= 0 {
			required[index] = true
		}
	}
	return required, finalAssistant
}

// requiredActivities are the indexes of the activities a completion decision
// takes: the newest turn start refusal, which latestTurnStartFailure would
// report, and the latest turn's last runtime error with a message, which
// failedTurnDetail would report.
func (a threadArchive) requiredActivities() map[int]bool {
	required := map[int]bool{}
	refusal, runtimeError := -1, -1
	var newest time.Time
	var latestTurn string
	if a.Thread.LatestTurn != nil {
		latestTurn = a.Thread.LatestTurn.TurnID
	}
	for index, activity := range a.Thread.Activities {
		switch activity.Kind {
		case TurnStartFailedActivity:
			if created, err := time.Parse(time.RFC3339Nano, activity.CreatedAt); err == nil && (refusal < 0 || !created.Before(newest)) {
				refusal, newest = index, created
			}
		case runtimeErrorActivity:
			if latestTurn != "" && activity.TurnID != nil && *activity.TurnID == latestTurn && providerDetail(activity.Payload.Message) != "" {
				runtimeError = index
			}
		}
	}
	for _, index := range []int{refusal, runtimeError} {
		if index >= 0 {
			required[index] = true
		}
	}
	return required
}
