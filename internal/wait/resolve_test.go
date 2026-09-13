package wait

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveThreadNativeAndCanonicalEvents(t *testing.T) {
	for _, line := range []string{
		`[2026-09-13T08:00:00Z] NTIVE: {"provider":"codex","threadId":"t3-thread","payload":{"threadId":"provider-session"}}`,
		`[2026-09-13T08:00:00Z] CANON: {"provider":"opencode","payload":{"providerThreadId": "provider-session"}}`,
		`{"session_id": "provider-session"}`,
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "events.t3-thread.log"), []byte(line+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := ResolveThread(dir, "provider-session")
		if err != nil || got != "t3-thread" {
			t.Fatalf("%q %v", got, err)
		}
		if _, err := ResolveThread(dir, "t3-thread"); err == nil {
			t.Fatal("top-level T3 identity mistaken for provider identity")
		}
	}
	dir := t.TempDir()
	line := `{"provider":"codex","payload":{"output":"quoted provider-session","nested":{"providerThreadId":"provider-session"}}}`
	if err := os.WriteFile(filepath.Join(dir, "events.unrelated.log"), []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveThread(dir, "provider-session"); err == nil {
		t.Fatal("tool output treated as caller identity")
	}
}

func TestResolveThreadRejectsAmbiguityAndDeduplicatesRotation(t *testing.T) {
	dir := t.TempDir()
	write := func(name string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{\"providerThreadId\":\"ses_123\"}\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("events.thread-a.log")
	write("events.thread-a.log.1")
	got, err := ResolveThread(dir, "ses_123")
	if err != nil || got != "thread-a" {
		t.Fatalf("%q %v", got, err)
	}
	write("events.thread-b.log")
	if _, err := ResolveThread(dir, "ses_123"); err == nil {
		t.Fatal("ambiguous thread accepted")
	}
	if _, err := ResolveThread(dir, "ses_missing"); err == nil {
		t.Fatal("unknown session accepted")
	}
}
