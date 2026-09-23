package config

import (
	"bytes"
	"encoding/json"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// A fleet project that has no backlog_v2.projects entry is loaded with an
// empty local binding rather than failing the whole configuration. One such
// project took every coordinator admin query down on 2026-09-17, because the
// projection is applied before validation and its error failed the load.
func TestFleetProjectWithoutLocalBindingLoadsWithDefaults(t *testing.T) {
	c := reviewConfig()
	f := reviewFleet()
	f.Projects["unbound"] = CoordinatorFleetProject{
		Repository: "ssh://git@example.invalid/unbound.git", DefaultRef: "main",
		SetupProfile: "build", EligibleWorkers: []string{"keep"},
	}
	if err := c.ApplyCoordinatorFleet(f); err != nil {
		t.Fatalf("projection-only project failed the load: %v", err)
	}
	project, ok := c.BacklogV2.Projects["unbound"]
	if !ok {
		t.Fatal("projection-only project is absent after the load")
	}
	if project.Repository != "ssh://git@example.invalid/unbound.git" || project.DefaultRef != "main" ||
		project.SetupProfile != "build" || !reflect.DeepEqual(project.Workers, []string{"keep"}) {
		t.Fatalf("projection fields not applied: %+v", project)
	}
	if project.Type != "" || project.T3Project != "" || len(project.Credentials) != 0 ||
		len(project.ResourceLocks) != 0 || len(project.DirectoryResources) != 0 {
		t.Fatalf("default local binding is not empty: %+v", project)
	}
	if got := c.DefaultedFleetProjects(); !reflect.DeepEqual(got, []string{"unbound"}) {
		t.Fatalf("defaulted projects = %v, want [unbound]", got)
	}
	// The explicit binding beside it is preserved exactly as before.
	kept := c.BacklogV2.Projects["keep"]
	previous := reviewConfig().BacklogV2.Projects["keep"]
	if kept.Type != previous.Type || kept.T3Project != previous.T3Project ||
		!reflect.DeepEqual(kept.Credentials, previous.Credentials) ||
		!reflect.DeepEqual(kept.ResourceLocks, previous.ResourceLocks) {
		t.Fatalf("explicit binding changed: %+v", kept)
	}
	// The list is a copy: a caller cannot alter what the configuration reports.
	c.DefaultedFleetProjects()[0] = "mutated"
	if c.DefaultedFleetProjects()[0] != "unbound" {
		t.Fatal("DefaultedFleetProjects aliases internal state")
	}
}

// A configuration with no defaulted project reports none, so a caller can log
// or annotate exactly the projects that were defaulted and nothing else.
func TestFleetProjectTypeIsOptionalAndFreshIsAuthoritative(t *testing.T) {
	t.Run("absent preserves legacy local type and wire", func(t *testing.T) {
		fleet := reviewFleet()
		raw, err := json.Marshal(fleet)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(`"type"`)) {
			t.Fatalf("legacy projection acquired a type field: %s", raw)
		}
		cfg := reviewConfig()
		if err := cfg.ApplyCoordinatorFleet(fleet); err != nil {
			t.Fatal(err)
		}
		if got := cfg.BacklogV2.Projects["keep"].Type; got != "git" {
			t.Fatalf("absent type changed local binding: %q", got)
		}
	})

	t.Run("fresh projects need no repository or setup profile", func(t *testing.T) {
		fleet := reviewFleet()
		fleet.Projects["scratch"] = CoordinatorFleetProject{Type: "fresh", EligibleWorkers: []string{"keep"}}
		raw, err := json.Marshal(fleet)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeCoordinatorFleet(raw)
		if err != nil {
			t.Fatal(err)
		}
		cfg := reviewConfig()
		if err := cfg.ApplyCoordinatorFleet(decoded); err != nil {
			t.Fatal(err)
		}
		project := cfg.BacklogV2.Projects["scratch"]
		if project.Type != "fresh" || project.Repository != "" || project.DefaultRef != "" || project.SetupProfile != "" ||
			!reflect.DeepEqual(project.Workers, []string{"keep"}) {
			t.Fatalf("fresh project projection = %+v", project)
		}
	})

	t.Run("explicit git overrides local fresh type", func(t *testing.T) {
		cfg := reviewConfig()
		project := cfg.BacklogV2.Projects["keep"]
		project.Type = "fresh"
		project.Repository = ""
		project.DefaultRef = ""
		project.SetupProfile = ""
		cfg.BacklogV2.Projects["keep"] = project
		fleet := reviewFleet()
		desired := fleet.Projects["keep"]
		desired.Type = "git"
		fleet.Projects["keep"] = desired
		if err := cfg.ApplyCoordinatorFleet(fleet); err != nil {
			t.Fatal(err)
		}
		if got := cfg.BacklogV2.Projects["keep"].Type; got != "git" {
			t.Fatalf("explicit git type was not applied: %q", got)
		}
	})

	for _, tc := range []struct {
		name, projectType, repository, ref string
	}{
		{name: "invalid type", projectType: "container"},
		{name: "fresh repository", projectType: "fresh", repository: "https://example.invalid/repo.git"},
		{name: "fresh ref", projectType: "fresh", ref: "main"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fleet := reviewFleet()
			fleet.Projects["keep"] = CoordinatorFleetProject{Type: tc.projectType, Repository: tc.repository, DefaultRef: tc.ref, EligibleWorkers: []string{"keep"}}
			raw, err := json.Marshal(fleet)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeCoordinatorFleet(raw); err == nil {
				t.Fatal("invalid project projection decoded")
			}
		})
	}
}

func TestFleetProjectsAllBoundReportsNoDefaults(t *testing.T) {
	c := reviewConfig()
	if err := c.ApplyCoordinatorFleet(reviewFleet()); err != nil {
		t.Fatal(err)
	}
	if got := c.DefaultedFleetProjects(); len(got) != 0 {
		t.Fatalf("defaulted projects = %v, want none", got)
	}
}

// The full loaders (LoadFile and Load) accept a projection that names a project
// absent from backlog_v2.projects, validate the result, and expose the project.
func TestCoordinatorFleetFullLoaderDefaultsUnboundProject(t *testing.T) {
	home := t.TempDir()
	originalHome := coordinatorClientHome
	coordinatorClientHome = func() string { return home }
	t.Cleanup(func() { coordinatorClientHome = originalHome })
	cfg := validBacklogV2Config(t)
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(home, "config.yaml")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	fleet := CoordinatorFleet{Kind: "steward-coordinator-catalog-input", SchemaVersion: 1, CoordinatorID: "normandy",
		Workers: map[string]CoordinatorFleetWorker{"normandy": {WorkerID: "normandy", CPUClass: "high", ExecutorSlots: 1,
			Capabilities: []string{"git", "huyang"}, ProviderInstances: []string{"codex"}, DesiredModels: map[string][]string{"codex": {"gpt"}}, QuotaPools: []string{"codex-main"}}},
		Projects: map[string]CoordinatorFleetProject{
			"steward":               {Repository: "https://example.invalid/steward.git", DefaultRef: "main", SetupProfile: "go", EligibleWorkers: []string{"normandy"}},
			"home-assistant-config": {Repository: "https://example.invalid/home-assistant-config.git", DefaultRef: "main", SetupProfile: "go", EligibleWorkers: []string{"normandy"}},
		}}
	projection, err := json.Marshal(fleet)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(home, CoordinatorFleetPath)
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, projection, 0600); err != nil {
		t.Fatal(err)
	}
	for _, load := range []func(string) (Config, error){LoadFile, Load} {
		loaded, err := load(path)
		if err != nil {
			t.Fatalf("full loader rejected a projection-only project: %v", err)
		}
		if err := loaded.Validate(); err != nil {
			t.Fatal(err)
		}
		if got := loaded.DefaultedFleetProjects(); !reflect.DeepEqual(got, []string{"home-assistant-config"}) {
			t.Fatalf("defaulted projects = %v", got)
		}
		project := loaded.BacklogV2.Projects["home-assistant-config"]
		if project.Repository == "" || len(project.Workers) != 1 || len(project.Credentials) != 0 {
			t.Fatalf("defaulted project = %+v", project)
		}
		if loaded.BacklogV2.Projects["steward"].T3Project != cfg.BacklogV2.Projects["steward"].T3Project {
			t.Fatal("explicit binding lost its execution metadata")
		}
	}
}
