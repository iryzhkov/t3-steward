package backlog

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// The worker's Git commands keep automatic maintenance in the foreground, so
// no detached git maintenance run outlives a command and writes into a
// workspace the worker is sealing or removing. The setting reaches Git as
// command-line configuration, ahead of any repository or global file.
func TestWorkerGitCommandsKeepMaintenanceInTheForeground(t *testing.T) {
	dir := t.TempDir()
	gitRun(t, dir, "init", "--quiet")
	gitRun(t, dir, "config", "gc.autoDetach", "true")
	gitRun(t, dir, "config", "maintenance.autoDetach", "true")
	for _, key := range []string{"gc.autoDetach", "maintenance.autoDetach"} {
		raw, err := runLoggedCommandOutput(context.Background(), nil, "", "git", "-C", dir, "config", "--show-scope", "--get", key)
		if err != nil {
			t.Fatalf("git config --get %s: %v\n%s", key, err, raw)
		}
		if got := strings.Fields(string(raw)); !slices.Equal(got, []string{"command", "false"}) {
			t.Fatalf("%s = %q, want false from the command line over the repository's true", key, raw)
		}
	}
}

// The settings follow any configuration entries the worker inherited, rather
// than replacing them.
func TestGitForegroundMaintenanceAppendsToInheritedConfiguration(t *testing.T) {
	for _, test := range []struct {
		inherited string
		want      []string
	}{
		{"", []string{"GIT_CONFIG_KEY_0=gc.autoDetach", "GIT_CONFIG_VALUE_0=false", "GIT_CONFIG_KEY_1=maintenance.autoDetach", "GIT_CONFIG_VALUE_1=false", "GIT_CONFIG_COUNT=2"}},
		{"2", []string{"GIT_CONFIG_KEY_2=gc.autoDetach", "GIT_CONFIG_VALUE_2=false", "GIT_CONFIG_KEY_3=maintenance.autoDetach", "GIT_CONFIG_VALUE_3=false", "GIT_CONFIG_COUNT=4"}},
		{"junk", []string{"GIT_CONFIG_KEY_0=gc.autoDetach", "GIT_CONFIG_VALUE_0=false", "GIT_CONFIG_KEY_1=maintenance.autoDetach", "GIT_CONFIG_VALUE_1=false", "GIT_CONFIG_COUNT=2"}},
	} {
		if got := gitForegroundMaintenance(test.inherited); !slices.Equal(got, test.want) {
			t.Errorf("inherited %q: %q, want %q", test.inherited, got, test.want)
		}
	}
}
