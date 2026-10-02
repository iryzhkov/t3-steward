package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// An ask is the one documented way for a steward task to put a question to its
// owner. It is a task-bound wait of kind ask: registering it parks the attempt
// exactly as any other task-bound wait does, and it settles only when an answer
// is recorded or its deadline passes. The question, its options and the answer
// are structured, so the resumed task reads a machine answer rather than prose.

// AskAnswerSchema names the answer document a resumed task receives, both in
// its wake message and as AskAnswerFile in its workspace.
const AskAnswerSchema = "ask-answer/v1"

// AskAnswerFile is the workspace file the answer is written to before the
// resumed turn starts.
const AskAnswerFile = "ask-answer.json"

// Ask limits. AskUserQuestion, the tool the relay thread answers through,
// takes two to four options with short labels and always offers free text in
// addition, so the same bounds hold here: an ask that the relay cannot put to
// the owner verbatim is refused at registration rather than truncated later.
const (
	AskMinOptions        = 2
	AskMaxOptions        = 4
	AskMaxOptionBytes    = 120
	AskMaxQuestionBytes  = 2000
	AskMaxContextBytes   = 16 * 1024
	AskMaxFreeTextBytes  = 4000
	AskDefaultMaxWaiting = 7 * 24 * time.Hour
)

// AskOnDeadline says what an ask becomes when its deadline passes unanswered.
type AskOnDeadline string

const (
	// AskDeadlineDefault settles the ask with its declared default options.
	AskDeadlineDefault AskOnDeadline = "default"
	// AskDeadlineFail settles the ask as timed out; the resumed task is told
	// there was no answer and must end failed.
	AskDeadlineFail AskOnDeadline = "fail"
)

// AskRequires names an authority an answer must carry beyond an ordinary
// answer. Empty is the ordinary ask, answerable in T3 or from the CLI.
type AskRequires string

// AskRequiresApprover keeps the signed approver path: only a frame signed by a
// configured approver may answer, and an answer given in T3 is refused.
const AskRequiresApprover AskRequires = "approver"

// AskAnswerSource says where an answer came from.
type AskAnswerSource string

const (
	// AskSourceT3 is an answer given to the relay thread's question card.
	AskSourceT3 AskAnswerSource = "t3"
	// AskSourceCLI is an answer given with `t3-steward ask answer`.
	AskSourceCLI AskAnswerSource = "cli"
	// AskSourceDeadlineDefault is the declared default, applied by the
	// coordinator when the deadline passed unanswered.
	AskSourceDeadlineDefault AskAnswerSource = "deadline-default"
)

// AskRequest is the immutable question of an ask, plus the coordinator-owned
// record of the relay thread that puts it to the owner.
type AskRequest struct {
	Question string   `json:"question"`
	Options  []string `json:"options"`
	// Multi allows more than one option to be chosen.
	Multi bool `json:"multi,omitempty"`
	// Context is supporting text the task attached with --context, shown to
	// the owner beside the question. ContextName is the file it came from.
	Context     string `json:"context,omitempty"`
	ContextName string `json:"contextName,omitempty"`
	// OnDeadline and Default decide what the deadline does. A task that gave
	// no --deadline gets AskDefaultMaxWaiting and fail.
	OnDeadline AskOnDeadline `json:"onDeadline"`
	Default    []string      `json:"default,omitempty"`
	Requires   AskRequires   `json:"requires,omitempty"`

	// Relay is the thread that shows the question in the owner's T3, recorded
	// by the steward of the worker running the task. It is absent until that
	// steward has opened one, and always absent for an approver ask.
	Relay *AskRelay `json:"relay,omitempty"`
}

// AskRelayState is the lifecycle of a relay thread.
type AskRelayState string

const (
	// AskRelayOpening is recorded before the thread is created, so a steward
	// that crashes between creating it and recording it leaves evidence that a
	// thread may exist rather than silently opening a second one.
	AskRelayOpening AskRelayState = "opening"
	// AskRelayOpen is a relay thread whose question turn was started.
	AskRelayOpen AskRelayState = "open"
	// AskRelayArchived is a relay thread archived after the ask settled.
	AskRelayArchived AskRelayState = "archived"
	// AskRelayFailed is a relay the steward could not open; the ask stays
	// answerable from the CLI and its deadline still applies.
	AskRelayFailed AskRelayState = "failed"
)

// AskRelay is the coordinator's record of one relay thread.
type AskRelay struct {
	WorkerID   string        `json:"workerId"`
	ThreadID   string        `json:"threadId"`
	State      AskRelayState `json:"state"`
	Reason     string        `json:"reason,omitempty"`
	UpdatedAt  time.Time     `json:"updatedAt"`
	ArchivedAt *time.Time    `json:"archivedAt,omitempty"`
}

// AskAnswer is the ask-answer/v1 document: what was chosen, by whom, where and
// when. The resumed task receives exactly this document.
type AskAnswer struct {
	Schema        string          `json:"schema"`
	AskID         string          `json:"askId"`
	WorkflowRunID string          `json:"workflowRunId"`
	TaskID        string          `json:"taskId"`
	Question      string          `json:"question"`
	Options       []string        `json:"options"`
	FreeText      string          `json:"freeText,omitempty"`
	Source        AskAnswerSource `json:"source"`
	AnsweredBy    string          `json:"answeredBy,omitempty"`
	ThreadID      string          `json:"threadId,omitempty"`
	AnsweredAt    time.Time       `json:"answeredAt"`
}

// ErrAskApproverRequired is the refusal of an answer to an approver ask that
// did not come through the signed approver path.
var ErrAskApproverRequired = errors.New("this ask requires the signed approver path")

// ErrAskAlreadyAnswered is the refusal of a second, different answer.
var ErrAskAlreadyAnswered = errors.New("this ask is already answered")

// ErrAskSettled is the refusal of an answer to an ask whose deadline settled it.
var ErrAskSettled = errors.New("this ask has already settled")

// ErrAskAnswerRefused marks a refusal the coordinator will repeat for the same
// answer: it is not an option, the thread is not the relay's, the task is no
// longer waiting. A caller retries anything else.
var ErrAskAnswerRefused = errors.New("ask answer refused")

// IsAskAnswerRefusal reports whether err is a refusal that a retry of the
// same answer cannot change. Over the admin transport only the message
// survives, so the sentinels are matched by their text as well.
func IsAskAnswerRefusal(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrAskAnswerRefused) || errors.Is(err, ErrAskApproverRequired) {
		return true
	}
	text := err.Error()
	return strings.Contains(text, ErrAskAnswerRefused.Error()) || strings.Contains(text, ErrAskApproverRequired.Error())
}

func askText(value string) bool {
	return strings.TrimSpace(value) != "" && value == strings.TrimSpace(value)
}

// Validate checks the immutable question before anything is parked.
func (r AskRequest) Validate() error {
	if !askText(r.Question) || len(r.Question) > AskMaxQuestionBytes {
		return fmt.Errorf("the question must be trimmed, non-empty and at most %d bytes", AskMaxQuestionBytes)
	}
	if len(r.Options) < AskMinOptions || len(r.Options) > AskMaxOptions {
		return fmt.Errorf("an ask needs %d to %d --option values (the owner can always type a free-text answer instead); got %d",
			AskMinOptions, AskMaxOptions, len(r.Options))
	}
	seen := map[string]bool{}
	for _, option := range r.Options {
		if !askText(option) || len(option) > AskMaxOptionBytes || strings.ContainsAny(option, "\n\r") {
			return fmt.Errorf("option %q must be one trimmed, non-empty line of at most %d bytes", option, AskMaxOptionBytes)
		}
		if seen[option] {
			return fmt.Errorf("option %q is given twice", option)
		}
		seen[option] = true
	}
	if len(r.Context) > AskMaxContextBytes {
		return fmt.Errorf("the context must be at most %d bytes", AskMaxContextBytes)
	}
	switch r.Requires {
	case "", AskRequiresApprover:
	default:
		return fmt.Errorf("--requires %q is not known; the only requirement is approver", r.Requires)
	}
	switch r.OnDeadline {
	case AskDeadlineDefault:
		if len(r.Default) == 0 {
			return errors.New("a default on deadline needs --default OPTION")
		}
	case AskDeadlineFail:
		if len(r.Default) != 0 {
			return errors.New("--default and --on-deadline fail exclude each other")
		}
	default:
		return fmt.Errorf("the deadline outcome %q must be default or fail", r.OnDeadline)
	}
	if r.Requires == AskRequiresApprover && len(r.Default) != 0 {
		// A default is an answer nobody gave. An approver ask accepts only the
		// approver's signed answer, so a default would be a way around it.
		return errors.New("--requires approver excludes --default: an approver ask is answered by the approver or times out unanswered")
	}
	if len(r.Default) != 0 {
		if err := r.CheckAnswer(r.Default, ""); err != nil {
			return fmt.Errorf("--default: %w", err)
		}
	}
	return nil
}

// TruncateUTF8 cuts text to at most limit bytes without splitting a
// character.
func TruncateUTF8(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// NormalizeAnswer is the one form an answer is stored and compared in: a
// non-nil option list and trimmed free text. A replay that differs only by a
// nil list or surrounding spaces is the same answer.
func NormalizeAnswer(options []string, freeText string) ([]string, string) {
	normalized := make([]string, 0, len(options))
	normalized = append(normalized, options...)
	return normalized, strings.TrimSpace(freeText)
}

// SameQuestion reports whether two requests ask the same thing, for a replayed
// registration. The relay record is coordinator state and is not compared.
func (r AskRequest) SameQuestion(other AskRequest) bool {
	return r.Question == other.Question && strings.Join(r.Options, "\x00") == strings.Join(other.Options, "\x00") &&
		r.Multi == other.Multi && r.Context == other.Context && r.OnDeadline == other.OnDeadline &&
		strings.Join(r.Default, "\x00") == strings.Join(other.Default, "\x00") && r.Requires == other.Requires
}

// CheckAnswer validates the chosen options and free text of an answer against
// the question. At least one of them must be present.
func (r AskRequest) CheckAnswer(options []string, freeText string) error {
	if len(options) == 0 && strings.TrimSpace(freeText) == "" {
		return errors.New("an answer needs at least one --option or free text")
	}
	if len(options) > 1 && !r.Multi {
		return errors.New("this ask takes one option; it was not asked with --multi")
	}
	if len(freeText) > AskMaxFreeTextBytes {
		return fmt.Errorf("the free-text answer must be at most %d bytes", AskMaxFreeTextBytes)
	}
	known := map[string]bool{}
	for _, option := range r.Options {
		known[option] = true
	}
	chosen := map[string]bool{}
	for _, option := range options {
		if !known[option] {
			return fmt.Errorf("%q is not one of the options: %s", option, strings.Join(quoteAll(r.Options), ", "))
		}
		if chosen[option] {
			return fmt.Errorf("option %q is chosen twice", option)
		}
		chosen[option] = true
	}
	return nil
}

func quoteAll(values []string) []string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = fmt.Sprintf("%q", value)
	}
	return quoted
}

// askWakeText is the part of a wake message that carries an ask's outcome:
// the ask-answer/v1 document when there is an answer, otherwise the
// instruction that follows from --on-deadline fail or a cancellation.
func askWakeText(w TaskWait) string {
	var builder strings.Builder
	if w.AskAnswer != nil {
		raw, err := json.MarshalIndent(w.AskAnswer, "", "  ")
		if err != nil {
			raw = []byte("{}")
		}
		if w.AskAnswer.Source == AskSourceDeadlineDefault {
			builder.WriteString("\nNo answer arrived before the deadline, so the default you declared applies.")
		} else {
			builder.WriteString("\nYour question was answered.")
		}
		fmt.Fprintf(&builder, " Answer: %s\n", w.AskAnswer.Summary())
		fmt.Fprintf(&builder, "The %s document follows; the same document is in %s at the root of the task workspace.\n\n%s\n\n",
			AskAnswerSchema, AskAnswerFile, raw)
		return builder.String()
	}
	if w.Result != nil && w.Result.Outcome == TaskWaitTimedOut {
		builder.WriteString("\nNo answer arrived before the deadline, and this question was asked with --on-deadline fail. ")
		builder.WriteString("End the task now as failed, with the reason \"no answer\": do not guess an answer and do not write the declared outputs.\n\n")
		return builder.String()
	}
	builder.WriteString("\nThe question was withdrawn without an answer. Do not guess one: end the task, or ask again if the decision is still needed.\n\n")
	return builder.String()
}

// Summary is the answer in one line: the chosen options, then the free text.
func (a AskAnswer) Summary() string {
	parts := []string{}
	if len(a.Options) != 0 {
		parts = append(parts, strings.Join(a.Options, ", "))
	}
	if text := strings.TrimSpace(a.FreeText); text != "" {
		parts = append(parts, "free text: "+text)
	}
	return strings.Join(parts, "; ")
}
