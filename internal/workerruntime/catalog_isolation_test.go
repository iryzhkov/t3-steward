package workerruntime

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

// isolationSettings is a coordinator with two projects on one worker, one of
// which has a repository the catalog cannot accept.
func isolationSettings(brokenRepository string) config.BacklogV2 {
	return config.BacklogV2{
		Coordinator: config.V2Coordinator{ID: "normandy-coordinator"},
		SetupProfiles: map[string]config.V2SetupProfile{
			"go": {Commands: []string{"go build ./..."}, Timeout: config.Duration(time.Minute)},
		},
		Projects: map[string]config.V2Project{
			"healthy": {
				Repository: "https://github.com/iryzhkov/t3-steward", DefaultRef: "main",
				SetupProfile: "go", T3Project: "t3-steward development",
				Workers: []string{"homelab"},
			},
			"broken": {
				Repository: brokenRepository, DefaultRef: "main",
				SetupProfile: "go", T3Project: "t3-steward development",
				Workers: []string{"homelab"},
			},
		},
		Workers: map[string]config.V2Worker{
			"homelab": {
				AcceptBacklog: true, Address: "homelab", Credential: "secretref:f02-protocol/homelab",
				Connection: "persistent-ssh", CPUClass: "high",
				Executors: config.V2Executors{Slots: 2},
			},
		},
	}
}

// TestOneMalformedProjectDoesNotDisableTheWorker is the fleet-outage
// regression.
//
// A single project whose repository syntax is invalid used to make the whole
// execution-package catalog fail to build. Every reconciliation then failed,
// the coordinator ended up holding no worker snapshot at all, and the operator
// saw workers vanish rather than a message naming the project.
func TestOneMalformedProjectDoesNotDisableTheWorker(t *testing.T) {
	for _, repository := range []string{
		"test",                                  // no scheme at all
		"",                                      // absent
		"ftp://example.invalid/x.git",           // a scheme the catalog refuses
		"https://example.invalid/x.git?token=1", // a query the catalog refuses
	} {
		t.Run(repository, func(t *testing.T) {
			binding, err := BuildWorkerBinding(isolationSettings(repository), "homelab", time.Now())
			if err != nil {
				t.Fatalf("one malformed project disabled the worker: %v", err)
			}
			if binding.Catalog == nil || binding.CatalogRevision == "" {
				t.Fatalf("binding = %+v", binding)
			}
			// The healthy project is still served, and the broken one is not.
			// A worker that accepted work for a project it cannot prepare would
			// fail at preparation, which is what this path exists to move
			// earlier.
			names := binding.Catalog.ProjectNames()
			if len(names) != 1 || names[0] != "healthy" {
				t.Fatalf("catalog holds %v, want only the healthy project", names)
			}
			for _, project := range binding.Inventory.Projects {
				if project.Name == "broken" {
					t.Fatal("a project that cannot be prepared was advertised as available")
				}
			}
			// The operator is told which project is broken and why.
			if len(binding.RejectedProjects) != 1 || binding.RejectedProjects[0].Name != "broken" {
				t.Fatalf("rejections = %+v", binding.RejectedProjects)
			}
			issues := binding.CatalogIssues()
			if len(issues) != 1 || !strings.Contains(issues[0], "project:broken:invalid") {
				t.Fatalf("issues = %+v", issues)
			}
			if !strings.Contains(issues[0], "repository") {
				t.Fatalf("the issue does not name the exact failure: %q", issues[0])
			}
		})
	}
}

// TestPerProjectDefectsAreIsolatedNotFatal covers the defects the worker
// binding used to return on directly, before the catalog was ever partitioned.
//
// These run inside the per-project loop, so one project with no setup profile
// or an ineligible directory still stopped the whole binding from being built.
// On a worker with a persistent connection the binding is built during
// coordinator startup, so that stopped the coordinator itself.
func TestPerProjectDefectsAreIsolatedNotFatal(t *testing.T) {
	tests := []struct {
		name    string
		breakIt func(*config.BacklogV2)
		want    string
	}{
		{
			name: "no setup profile",
			breakIt: func(settings *config.BacklogV2) {
				project := settings.Projects["broken"]
				project.SetupProfile = ""
				settings.Projects["broken"] = project
			},
			want: "has no setup profile",
		},
		{
			name: "a setup profile this coordinator does not have",
			breakIt: func(settings *config.BacklogV2) {
				project := settings.Projects["broken"]
				project.SetupProfile = "absent"
				settings.Projects["broken"] = project
			},
			want: "unknown setup profile",
		},
		{
			name: "the reserved implicit profile is declared",
			breakIt: func(settings *config.BacklogV2) {
				project := settings.Projects["broken"]
				project.SetupProfile = ""
				project.Type = "fresh"
				project.Repository = ""
				project.DefaultRef = ""
				settings.Projects["broken"] = project
				settings.SetupProfiles["steward-fresh-empty"] = config.V2SetupProfile{
					Commands: []string{"true"}, Timeout: config.Duration(time.Minute),
				}
			},
			want: "reserved for implicit fresh setup",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := isolationSettings("https://github.com/iryzhkov/upkeeper")
			test.breakIt(&settings)
			binding, err := BuildWorkerBinding(settings, "homelab", time.Now())
			if err != nil {
				t.Fatalf("one defective project disabled the worker: %v", err)
			}
			names := binding.Catalog.ProjectNames()
			if len(names) != 1 || names[0] != "healthy" {
				t.Fatalf("catalog holds %v, want only the healthy project", names)
			}
			if len(binding.RejectedProjects) != 1 || binding.RejectedProjects[0].Name != "broken" {
				t.Fatalf("rejections = %+v", binding.RejectedProjects)
			}
			if !strings.Contains(binding.RejectedProjects[0].Reason, test.want) {
				t.Fatalf("reason %q does not say %q", binding.RejectedProjects[0].Reason, test.want)
			}
			for _, project := range binding.Inventory.Projects {
				if project.Name == "broken" {
					t.Fatal("a project that cannot be prepared was advertised as available")
				}
			}
		})
	}
}

// TestAHealthyFleetReportsNoCatalogIssues states the quiet case, so the health
// surface cannot become noise that an operator learns to ignore.
func TestAHealthyFleetReportsNoCatalogIssues(t *testing.T) {
	settings := isolationSettings("https://github.com/iryzhkov/upkeeper")
	if issues := FleetCatalogIssues(settings); len(issues) != 0 {
		t.Fatalf("a healthy fleet reported %+v", issues)
	}
	binding, err := BuildWorkerBinding(settings, "homelab", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(binding.RejectedProjects) != 0 {
		t.Fatalf("rejections = %+v", binding.RejectedProjects)
	}
	if len(binding.Inventory.Projects) != 2 {
		t.Fatalf("advertised projects = %+v, want both", binding.Inventory.Projects)
	}
}

// TestFleetCatalogIssuesNamesTheBrokenProject states that the problem is
// announced from configuration alone, without waiting for a worker binding or a
// refused submission to surface it.
func TestFleetCatalogIssuesNamesTheBrokenProject(t *testing.T) {
	issues := FleetCatalogIssues(isolationSettings("test"))
	if len(issues) != 1 {
		t.Fatalf("issues = %+v", issues)
	}
	for _, want := range []string{"project:broken:invalid", "repository", "scheme"} {
		if !strings.Contains(issues[0], want) {
			t.Fatalf("issue %q does not contain %q", issues[0], want)
		}
	}
}

// TestABrokenProjectAssignedToNoWorkerIsStillReported covers the case the
// worker binding cannot see: a project nothing is assigned to is invisible to
// every binding and still has to be announced.
func TestABrokenProjectAssignedToNoWorkerIsStillReported(t *testing.T) {
	settings := isolationSettings("test")
	broken := settings.Projects["broken"]
	broken.Workers = nil
	settings.Projects["broken"] = broken

	if issues := FleetCatalogIssues(settings); len(issues) != 1 ||
		!strings.Contains(issues[0], "project:broken:invalid") {
		t.Fatalf("issues = %+v", issues)
	}
	if _, err := BuildWorkerBinding(settings, "homelab", time.Now()); err != nil {
		t.Fatalf("an unassigned broken project disabled the worker: %v", err)
	}
}

// TestAWorkerWithOnlyBrokenProjectsSaysWhy states the floor: a worker whose
// every project is misconfigured has nothing to do, and the error names the
// projects rather than reporting an empty catalog.
func TestAWorkerWithOnlyBrokenProjectsSaysWhy(t *testing.T) {
	settings := isolationSettings("test")
	delete(settings.Projects, "healthy")
	_, err := BuildWorkerBinding(settings, "homelab", time.Now())
	if err == nil {
		t.Fatal("a worker with no usable project built a binding")
	}
	for _, want := range []string{"homelab", "broken", "misconfigured"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err.Error(), want)
		}
	}
}
