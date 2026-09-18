package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CLI tests must never discover the operator's coordinator, credentials or
// provider sessions. Individual tests can still install explicit fixtures.
//
// Some tests spawn "go test" subprocesses that inherit this environment. The Go
// toolchain derives GOMODCACHE, GOCACHE and GOPATH from HOME when they are not
// set, so the caches are pinned to their real locations before HOME is moved;
// otherwise every run downloads the module graph again beneath the scratch
// directory and leaves a read-only tree that RemoveAll cannot delete.
func TestMain(m *testing.M) {
	caches, err := goCacheLocations()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "t3-steward-cli-tests-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	values := map[string]string{
		"HOME":            dir,
		"XDG_CONFIG_HOME": filepath.Join(dir, ".config"),
		"XDG_STATE_HOME":  filepath.Join(dir, ".local/state"),
		"T3CODE_HOME":     filepath.Join(dir, ".t3"),
	}
	for key, value := range caches {
		values[key] = value
	}
	for key, value := range values {
		if err := os.Setenv(key, value); err != nil {
			removeScratchHome(dir)
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	code := m.Run()
	removeScratchHome(dir)
	os.Exit(code)
}

// goCacheLocations returns GOMODCACHE, GOCACHE and GOPATH as the toolchain
// resolves them with the real HOME still in place.
func goCacheLocations() (map[string]string, error) {
	keys := []string{"GOMODCACHE", "GOCACHE", "GOPATH"}
	output, err := exec.Command("go", append([]string{"env"}, keys...)...).Output()
	if err != nil {
		return nil, fmt.Errorf("resolve go cache locations: %w", err)
	}
	lines := strings.Split(strings.TrimRight(string(output), "\n"), "\n")
	if len(lines) != len(keys) {
		return nil, fmt.Errorf("resolve go cache locations: expected %d lines, got %q", len(keys), output)
	}
	locations := make(map[string]string, len(keys))
	for i, key := range keys {
		if lines[i] == "" {
			return nil, fmt.Errorf("resolve go cache locations: %s is empty", key)
		}
		locations[key] = lines[i]
	}
	return locations, nil
}

func removeScratchHome(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		fmt.Fprintf(os.Stderr, "remove scratch HOME %s: %v\n", dir, err)
	}
}
