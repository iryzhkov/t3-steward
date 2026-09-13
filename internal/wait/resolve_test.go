package wait

import (
	"os"
	"path/filepath"
	"testing"
)

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
