package main

import (
	"github.com/iryzhkov/t3-steward/internal/config"
	"testing"
)

func TestCallerSessionProvidersAndAmbiguity(t *testing.T) {
	for _, key := range []string{"CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "OPENCODE_SESSION_ID"} {
		t.Run(key, func(t *testing.T) {
			got, err := callerSession(func(k string) string {
				if k == key {
					return "session-123"
				}
				return ""
			})
			if err != nil || got != "session-123" {
				t.Fatalf("%q %v", got, err)
			}
		})
	}
	if _, err := callerSession(func(string) string { return "" }); err == nil {
		t.Fatal("missing context accepted")
	}
	if _, err := callerSession(func(k string) string { return k }); err == nil {
		t.Fatal("ambiguous context accepted")
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "claude")
	t.Setenv("CODEX_THREAD_ID", "codex")
	got, err := resolveThread(config.Config{}, "explicit")
	if err != nil || got != "explicit" {
		t.Fatalf("override: %q %v", got, err)
	}
}
