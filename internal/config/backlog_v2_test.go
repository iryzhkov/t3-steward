package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBacklogV2DefaultsDisabledAndLegacyCompatible(t *testing.T) {
	cfg := Default()
	if cfg.BacklogV2.Mode != "disabled" || cfg.BacklogV2.StartupAdmission != "closed" {
		t.Fatalf("backlog v2 defaults = %+v", cfg.BacklogV2)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("backlog:\n  enabled: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Backlog.Enabled || loaded.BacklogV2.Mode != "disabled" {
		t.Fatalf("legacy config = %+v", loaded)
	}
}

func TestExampleConfigurationLoadsWithStrictDecoder(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BacklogV2.Mode != "disabled" {
		t.Fatalf("example backlog-v2 mode = %q", cfg.BacklogV2.Mode)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	for name, contents := range map[string]string{
		"top-level": "mystery: true\n",
		"nested":    "policy:\n  warn_percent: 80\n  mystery: true\n",
		"v2":        "backlog_v2:\n  mode: disabled\n  mystery: true\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "field mystery not found") {
				t.Fatalf("Load error = %v", err)
			}
		})
	}
}

func TestLoadRejectsMultipleDocuments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("{}\n---\n{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("Load error = %v", err)
	}
}

func TestBacklogV2CoordinatorConfigurationAndReferences(t *testing.T) {
	cfg := validBacklogV2Config(t)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*Config){
		"legacy conflict": func(c *Config) { c.Backlog.Enabled = true },
		"open admission":  func(c *Config) { c.BacklogV2.StartupAdmission = "open" },
		"unknown worker": func(c *Config) {
			c.BacklogV2.Projects["steward"] = V2Project{
				Repository: "repo", DefaultRef: "main", T3Project: "dev", Workers: []string{"missing"},
			}
		},
		"unknown setup": func(c *Config) {
			project := c.BacklogV2.Projects["steward"]
			project.SetupProfile = "missing"
			c.BacklogV2.Projects["steward"] = project
		},
		"unknown quota": func(c *Config) {
			worker := c.BacklogV2.Workers["normandy"]
			worker.Providers["codex"] = V2Provider{Models: []string{"gpt"}, QuotaPool: "missing"}
			c.BacklogV2.Workers["normandy"] = worker
		},
		"unsafe root": func(c *Config) { c.BacklogV2.Storage.Bundles = "/" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := validBacklogV2Config(t)
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func validBacklogV2Config(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	cfg := Default()
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.Storage = V2Storage{
		Bundles: filepath.Join(root, "bundles"), Artifacts: filepath.Join(root, "artifacts"), Workspaces: filepath.Join(root, "workspaces"),
	}
	cfg.BacklogV2.QuotaPools = map[string]V2QuotaPool{"codex-main": {Provider: "codex"}}
	cfg.BacklogV2.Workers = map[string]V2Worker{
		"normandy": {
			Address: "normandy", AcceptBacklog: true, Credential: "ssh:normandy",
			Providers: map[string]V2Provider{"codex": {Models: []string{"gpt"}, QuotaPool: "codex-main"}},
		},
	}
	cfg.BacklogV2.SetupProfiles = map[string]V2SetupProfile{
		"go": {Commands: []string{"go mod download"}, Timeout: Duration(time.Minute)},
	}
	cfg.BacklogV2.Projects = map[string]V2Project{
		"steward": {
			Repository: "git@example/steward", DefaultRef: "main", T3Project: "development",
			SetupProfile: "go", Workers: []string{"normandy"}, Credentials: []string{"git:example"},
		},
	}
	return cfg
}
