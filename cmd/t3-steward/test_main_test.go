package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// CLI tests must never discover the operator's coordinator, credentials or
// provider sessions. Individual tests can still install explicit fixtures.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "t3-steward-cli-tests-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for key, value := range map[string]string{
		"HOME":            dir,
		"XDG_CONFIG_HOME": filepath.Join(dir, ".config"),
		"XDG_STATE_HOME":  filepath.Join(dir, ".local/state"),
		"T3CODE_HOME":     filepath.Join(dir, ".t3"),
	} {
		if err := os.Setenv(key, value); err != nil {
			os.RemoveAll(dir)
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
