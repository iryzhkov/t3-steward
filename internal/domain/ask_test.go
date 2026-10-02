package domain

import (
	"strings"
	"testing"
	"time"
)

func TestAskRequestValidation(t *testing.T) {
	valid := AskRequest{Question: "Pick one", Options: []string{"alpha", "beta"}, OnDeadline: AskDeadlineDefault, Default: []string{"alpha"}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*AskRequest){
		"one option":          func(r *AskRequest) { r.Options = []string{"alpha"} },
		"five options":        func(r *AskRequest) { r.Options = []string{"a", "b", "c", "d", "e"} },
		"repeated option":     func(r *AskRequest) { r.Options = []string{"a", "a"}; r.Default = []string{"a"} },
		"multi-line option":   func(r *AskRequest) { r.Options[1] = "b\nc" },
		"empty question":      func(r *AskRequest) { r.Question = " " },
		"unknown default":     func(r *AskRequest) { r.Default = []string{"gamma"} },
		"two defaults":        func(r *AskRequest) { r.Default = []string{"alpha", "beta"} },
		"default and fail":    func(r *AskRequest) { r.OnDeadline = AskDeadlineFail },
		"default without one": func(r *AskRequest) { r.Default = nil },
		"unknown requirement": func(r *AskRequest) { r.Requires = "owner" },
	} {
		request := valid
		request.Options = append([]string(nil), valid.Options...)
		change(&request)
		if err := request.Validate(); err == nil {
			t.Errorf("%s: accepted %+v", name, request)
		}
	}
	multi := valid
	multi.Multi, multi.Default = true, []string{"alpha", "beta"}
	if err := multi.Validate(); err != nil {
		t.Fatalf("two defaults with --multi: %v", err)
	}
	if err := multi.CheckAnswer([]string{"alpha", "beta"}, ""); err != nil {
		t.Fatalf("two options for a --multi ask: %v", err)
	}
	if err := valid.CheckAnswer(nil, "something else entirely"); err != nil {
		t.Fatalf("a free-text answer: %v", err)
	}
	if err := valid.CheckAnswer(nil, ""); err == nil {
		t.Fatal("an empty answer was accepted")
	}
}

func TestAskRegistrationNamesNoRelayAndNoOrTimeout(t *testing.T) {
	ask := AskRequest{Question: "Pick one", Options: []string{"alpha", "beta"}, OnDeadline: AskDeadlineFail}
	registration := TaskWaitRegistration{RequestID: "r", WorkflowRunID: "run", TaskID: "task", AttemptID: "a",
		ThreadID: "thread", Wake: WakeEach, MaxDuration: time.Hour, Kind: WaitKindAsk, Ask: &ask}
	if err := registration.Validate(); err != nil {
		t.Fatal(err)
	}
	withRelay := ask
	withRelay.Relay = &AskRelay{ThreadID: "x"}
	registration.Ask = &withRelay
	if err := registration.Validate(); err == nil || !strings.Contains(err.Error(), "relay") {
		t.Fatalf("a registration naming a relay: %v", err)
	}
	registration.Ask, registration.OrTimeout = &ask, true
	if err := registration.Validate(); err == nil {
		t.Fatal("an ask with --or-timeout was accepted")
	}
	registration.OrTimeout, registration.Kind = false, WaitKindShell
	if err := registration.Validate(); err == nil {
		t.Fatal("a shell wait carrying a question was accepted")
	}
}
