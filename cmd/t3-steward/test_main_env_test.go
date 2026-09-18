package main

import (
	"os"
	"strings"
	"testing"
)

// TestMainPinsGoCachesOutsideScratchHome guards F-9: tests in this package spawn
// "go test" subprocesses, which inherit the scratch HOME that TestMain installs.
// Without explicit cache locations the Go toolchain derives GOMODCACHE, GOCACHE
// and GOPATH from that HOME, downloads the module graph again beneath it, and
// leaves a read-only tree that RemoveAll cannot delete. TestMain must therefore
// pin all three to their pre-override values before it overrides HOME.
func TestMainPinsGoCachesOutsideScratchHome(t *testing.T) {
	home := os.Getenv("HOME")
	if home == "" {
		t.Fatal("TestMain did not install a scratch HOME")
	}
	for _, key := range []string{"GOMODCACHE", "GOCACHE", "GOPATH"} {
		value := os.Getenv(key)
		if value == "" {
			t.Errorf("%s is not pinned; a spawned go test would derive it beneath the scratch HOME %s", key, home)
			continue
		}
		if strings.HasPrefix(value, home+string(os.PathSeparator)) || value == home {
			t.Errorf("%s=%s lies beneath the scratch HOME %s", key, value, home)
		}
	}
}
