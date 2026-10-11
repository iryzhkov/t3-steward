package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func TestCoordinatorBackupDispatchExplicitConfig(t *testing.T) {
	// A valid default configuration deliberately lacks coordinator mode. Reaching
	// the mode check proves both explicit provenance and the selected path survived
	// dispatch, without creating a snapshot or opening a coordinator database.
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	configPath := filepath.Join(dir, "operator.yaml")
	if err := os.WriteFile(configPath, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.LoadFile(configPath); err != nil {
		t.Fatalf("test configuration: %v", err)
	}
	out := filepath.Join(dir, "snapshot")
	for name, args := range map[string][]string{
		"documented":  {"coordinator", "backup", "--config", configPath, "--out", out},
		"before verb": {"coordinator", "--config", configPath, "backup", "--out", out},
		"after out":   {"coordinator", "backup", "--out", out, "--config", configPath},
		"repeated":    {"coordinator", "backup", "--config", filepath.Join(dir, "absent.yaml"), "--config", configPath, "--out", out},
	} {
		t.Run(name, func(t *testing.T) {
			err := dispatch(args)
			if err == nil || err.Error() != "coordinator backup requires local coordinator mode without a remote coordinator client" {
				t.Fatalf("explicit config did not reach mode check: %v", err)
			}
		})
	}
	if err := dispatch([]string{"coordinator", "backup", "--out", out}); err == nil ||
		!strings.Contains(err.Error(), "requires an explicit operator-controlled --config") {
		t.Fatalf("implicit configuration was not refused: %v", err)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("admission test created a snapshot: %v", err)
	}
}

func TestExchangeDispatchExplicitConfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	configPath := filepath.Join(dir, "operator.yaml")
	if err := os.WriteFile(configPath, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"coordinator-exchange", "worker-exchange"} {
		t.Run(command, func(t *testing.T) {
			err := dispatch([]string{command, "--config", configPath})
			mode := "coordinator"
			if command == "worker-exchange" {
				mode = "worker"
			}
			if err == nil || err.Error() != command+" requires backlog_v2.mode="+mode {
				t.Fatalf("explicit config did not reach mode check: %v", err)
			}
		})
	}
}
