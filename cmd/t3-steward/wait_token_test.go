package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// withoutT3CLI hides every t3 binary the lookup would find: PATH and the
// per-user install locations under HOME.
func withoutT3CLI(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv(config.EnvPrefix+"T3_TOKEN", "")
	if err := os.Unsetenv(config.EnvPrefix + "T3_TOKEN"); err != nil {
		t.Fatal(err)
	}
}

// An interactive wait verifies the caller's thread through the T3 API, and
// that needs a token. When the only source was the vanished t3 CLI the error
// must say which operation needs the token and every other way to supply it.
func TestCallerThreadConnectionNamesTheTokenItNeeds(t *testing.T) {
	withoutT3CLI(t)
	cfg := config.Default()
	cfg.T3.URL = "http://127.0.0.1:9"
	cfg.T3.DataDir = t.TempDir()
	_, _, err := connectForCallerThread(cfg, newLogger("error"))
	if err == nil {
		t.Fatal("connected with no token source at all")
	}
	if !errors.Is(err, t3api.ErrT3CLINotFound) {
		t.Fatalf("the cause is not the missing CLI: %v", err)
	}
	for _, want := range []string{
		"resolving the caller's thread needs the T3 API token", "t3.token", "T3_STEWARD_T3_TOKEN", "t3 CLI",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error does not say %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "usual install locations; set t3.t3_binary") {
		t.Fatalf("the error is still the bare CLI lookup failure: %v", err)
	}
	cfg.T3.Token = "configured"
	if _, _, err := connectForCallerThread(cfg, newLogger("error")); err != nil {
		t.Fatalf("a configured token still needs the CLI: %v", err)
	}
}

// The token comes from configuration or the environment before the CLI is
// consulted, so a wait registers on a host whose t3 binary has disappeared.
func TestTokenResolvesFromConfigurationAndEnvironmentWithoutTheCLI(t *testing.T) {
	withoutT3CLI(t)
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "t3-token")
	if err := os.WriteFile(tokenFile, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("t3:\n  url: http://127.0.0.1:9\n  token_file: "+tokenFile+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.T3.Token != "from-file" {
		t.Fatalf("t3.token_file was not read: %q", cfg.T3.Token)
	}
	if _, _, err := connectForCallerThread(cfg, newLogger("error")); err != nil {
		t.Fatalf("a token from t3.token_file still needs the CLI: %v", err)
	}

	t.Setenv(config.EnvPrefix+"T3_TOKEN", "from-env")
	cfg, err = config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.T3.Token != "from-env" {
		t.Fatalf("the environment did not win over the file: %q", cfg.T3.Token)
	}
	if _, _, err := connectForCallerThread(cfg, newLogger("error")); err != nil {
		t.Fatalf("a token from the environment still needs the CLI: %v", err)
	}

	// A token file readable by others is refused rather than read.
	if err := os.Unsetenv(config.EnvPrefix + "T3_TOKEN"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tokenFile, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(configPath); err == nil || !strings.Contains(err.Error(), "0600") {
		t.Fatalf("a world-readable token file was accepted: %v", err)
	}
}
