package domain

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// UserInputEventKind is one of the two thread activities a provider's native
// question tool produces in T3.
type UserInputEventKind string

const (
	UserInputRequested UserInputEventKind = "requested"
	UserInputResolved  UserInputEventKind = "resolved"
)

// UserInputOption is one choice of a native question.
type UserInputOption struct {
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

// UserInputQuestion is one question of a native question card. For Claude's
// AskUserQuestion, ID is the question text itself.
type UserInputQuestion struct {
	ID          string            `json:"id"`
	Header      string            `json:"header,omitempty"`
	Question    string            `json:"question"`
	Options     []UserInputOption `json:"options,omitempty"`
	MultiSelect bool              `json:"multiSelect,omitempty"`
}

// UserInputEvent is a user-input.requested or user-input.resolved activity of
// a T3 thread, decoded. Answers are keyed by question ID; their values are
// what the answering UI sent, a string or a list of strings.
type UserInputEvent struct {
	Kind       UserInputEventKind  `json:"kind"`
	ActivityID string              `json:"activityId"`
	RequestID  string              `json:"requestId"`
	TurnID     string              `json:"turnId,omitempty"`
	Questions  []UserInputQuestion `json:"questions,omitempty"`
	Answers    map[string]any      `json:"answers,omitempty"`
	At         time.Time           `json:"at"`
}

// AnswerValues returns the answer given to one question as a list of strings:
// the entry keyed by its ID, or the only entry when the card had one question.
// A list answer is returned as given; a string answer is one value.
func (e UserInputEvent) AnswerValues(question UserInputQuestion) []string {
	value, found := e.Answers[question.ID]
	if !found && len(e.Answers) == 1 {
		for _, only := range e.Answers {
			value, found = only, true
		}
	}
	if !found {
		return nil
	}
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []string{typed}
	case []any:
		var values []string
		for _, item := range typed {
			if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
				values = append(values, text)
			}
		}
		return values
	case []string:
		return typed
	default:
		return []string{fmt.Sprint(typed)}
	}
}

// SplitAnswer maps answer values onto the options of an ask: a value equal to
// an option is that option; a single value that is the options joined with
// ", " (how AskUserQuestion reports a multi-select) is those options; anything
// else is free text. Options keep the ask's order.
func (r AskRequest) SplitAnswer(values []string) (options []string, freeText string) {
	known := map[string]int{}
	for index, option := range r.Options {
		known[option] = index
	}
	chosen := map[string]bool{}
	var free []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if _, ok := known[value]; ok {
			chosen[value] = true
			continue
		}
		parts := strings.Split(value, ", ")
		all := len(parts) > 1
		for _, part := range parts {
			if _, ok := known[strings.TrimSpace(part)]; !ok {
				all = false
			}
		}
		if all {
			for _, part := range parts {
				chosen[strings.TrimSpace(part)] = true
			}
			continue
		}
		if value != "" {
			free = append(free, value)
		}
	}
	for option := range chosen {
		options = append(options, option)
	}
	sort.Slice(options, func(i, j int) bool { return known[options[i]] < known[options[j]] })
	return options, strings.Join(free, "\n")
}
