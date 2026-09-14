package providercontainment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func settingsSpec() T3Spec {
	return T3Spec{Node: "/runtime/0/bin/node", Entry: "/runtime/0/t3/bin.mjs", Port: 18881, OpenCodeBinary: "/runtime/1/opencode", OpenCodeModel: "opencode/muse"}
}

func TestContainedProviderSettingsIsolation(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-provider", true: "opencode"}[enabled], func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			spec := settingsSpec()
			if !enabled {
				spec.OpenCodeBinary = ""
				spec.OpenCodeModel = ""
			}
			if err := prepareT3Settings(root, spec); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "settings.json")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var cfg struct {
				EnableProviderUpdateChecks bool
				EnableAgentBrowserAccess   bool
				Providers                  map[string]struct {
					Enabled                               bool
					BinaryPath, ServerUrl, ServerPassword string
				}
				TextGenerationModelSelection struct{ InstanceId, Model string }
			}
			if err := json.Unmarshal(data, &cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.EnableProviderUpdateChecks || cfg.EnableAgentBrowserAccess || len(cfg.Providers) != 5 {
				t.Fatal("unexpected provider surface")
			}
			for name, p := range cfg.Providers {
				if p.Enabled != (enabled && name == "opencode") {
					t.Fatalf("unexpected enabled provider: %s", name)
				}
				if p.ServerUrl != "" || p.ServerPassword != "" {
					t.Fatal("external server configuration")
				}
			}
			if enabled && (cfg.Providers["opencode"].BinaryPath != spec.OpenCodeBinary || cfg.TextGenerationModelSelection.InstanceId != "opencode" || cfg.TextGenerationModelSelection.Model != spec.OpenCodeModel) {
				t.Fatal("provider route not preserved")
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("settings not private")
			}
			if err := prepareT3Settings(root, spec); err == nil {
				t.Fatal("replaced prior settings")
			}
			after, _ := os.ReadFile(path)
			if string(after) != string(data) {
				t.Fatal("prior state changed")
			}
		})
	}
}

func TestContainedProviderSettingsRefuseUnapprovedAndSymlink(t *testing.T) {
	for _, change := range []func(*T3Spec){
		func(s *T3Spec) { s.OpenCodeBinary = "/home/igor/bin/opencode" },
		func(s *T3Spec) { s.OpenCodeBinary = "/runtime/1/../opencode" },
		func(s *T3Spec) { s.OpenCodeBinary = "opencode" },
		func(s *T3Spec) { s.OpenCodeModel = "" },
		func(s *T3Spec) { s.OpenCodeModel = "bad model" },
	} {
		spec := settingsSpec()
		change(&spec)
		if err := spec.validate(); err == nil {
			t.Fatal("invalid provider accepted")
		}
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "retained")
	if err := os.WriteFile(outside, []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "settings.json")); err != nil {
		t.Fatal(err)
	}
	if err := prepareT3Settings(root, settingsSpec()); err == nil {
		t.Fatal("settings symlink replaced")
	}
	got, _ := os.ReadFile(outside)
	if string(got) != "retained" {
		t.Fatal("external state changed")
	}
}
