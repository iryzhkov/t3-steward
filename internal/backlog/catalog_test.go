package backlog

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestProjectCatalogResolvesDefaultAndRequestedRefs(t *testing.T) {
	projects, profiles := validCatalogFixture()
	catalog, err := NewProjectCatalog(projects, profiles)
	if err != nil {
		t.Fatalf("new project catalog: %v", err)
	}
	workflow := catalogWorkflow()
	task := catalogTask()

	resolved, err := catalog.Resolve(workflow, task)
	if err != nil {
		t.Fatalf("resolve default environment: %v", err)
	}
	if resolved.ProjectName != "t3-steward" ||
		resolved.Repository != "https://github.com/iryzhkov/t3-steward.git" ||
		resolved.Ref != "main" ||
		resolved.Scope != EnvironmentScopeTask ||
		resolved.T3ProjectTemplate != "t3-steward development" {
		t.Fatalf("resolved environment = %#v", resolved)
	}
	if want := []string{"database", "gpu", "repository"}; !reflect.DeepEqual(resolved.ResourceLocks, want) {
		t.Fatalf("resource locks = %#v, want %#v", resolved.ResourceLocks, want)
	}
	if want := []string{"github-token", "signing-key"}; !reflect.DeepEqual(resolved.RequiredCredentials, want) {
		t.Fatalf("credentials = %#v, want names only %#v", resolved.RequiredCredentials, want)
	}
	if resolved.Setup.Name != "go-project" ||
		!reflect.DeepEqual(resolved.Setup.Commands, []string{"go mod download", "go build ./..."}) ||
		resolved.Setup.Timeout != 5*time.Minute {
		t.Fatalf("setup = %#v", resolved.Setup)
	}

	workflow.Environment.Ref = "release/v2"
	overridden, err := catalog.Resolve(workflow, task)
	if err != nil {
		t.Fatalf("resolve requested ref: %v", err)
	}
	if overridden.Ref != "release/v2" {
		t.Fatalf("requested ref = %q, want release/v2", overridden.Ref)
	}
}

func TestProjectCatalogDetachesConfigurationAndResults(t *testing.T) {
	projects, profiles := validCatalogFixture()
	catalog, err := NewProjectCatalog(projects, profiles)
	if err != nil {
		t.Fatalf("new project catalog: %v", err)
	}
	projects[0].ResourceLocks[0] = "mutated"
	projects[0].RequiredCredentials[0] = "mutated"
	profiles[0].Commands[0] = "mutated"

	first, err := catalog.Resolve(catalogWorkflow(), catalogTask())
	if err != nil {
		t.Fatalf("resolve first: %v", err)
	}
	first.ResourceLocks[0] = "mutated"
	first.RequiredCredentials[0] = "mutated"
	first.Setup.Commands[0] = "mutated"

	second, err := catalog.Resolve(catalogWorkflow(), catalogTask())
	if err != nil {
		t.Fatalf("resolve second: %v", err)
	}
	if !reflect.DeepEqual(second.ResourceLocks, []string{"database", "gpu", "repository"}) ||
		!reflect.DeepEqual(second.RequiredCredentials, []string{"github-token", "signing-key"}) ||
		second.Setup.Commands[0] != "go mod download" {
		t.Fatalf("catalog state was aliased: %#v", second)
	}
}

func TestProjectCatalogAcceptsCanonicalSSHRepositories(t *testing.T) {
	for _, repository := range []string{
		"git@github.com:iryzhkov/t3-steward.git",
		"ssh://git@github.com/iryzhkov/t3-steward.git",
	} {
		t.Run(repository, func(t *testing.T) {
			projects, profiles := validCatalogFixture()
			projects[0].Repository = repository
			if _, err := NewProjectCatalog(projects, profiles); err != nil {
				t.Fatalf("repository %q rejected: %v", repository, err)
			}
		})
	}
}

func TestProjectCatalogRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*[]ProjectDefinition, *[]SetupProfile)
		want   string
	}{
		{
			name: "unknown setup profile",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				(*projects)[0].SetupProfile = "absent"
			},
			want: "unknown setup profile",
		},
		{
			name: "local repository",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				(*projects)[0].Repository = "/home/user/repository"
			},
			want: "scheme",
		},
		{
			name: "insecure repository scheme",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				(*projects)[0].Repository = "http://example.test/repository.git"
			},
			want: "scheme",
		},
		{
			name: "embedded HTTPS credentials",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				(*projects)[0].Repository = "https://token@example.test/repository.git"
			},
			want: "credentials",
		},
		{
			name: "repository query",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				(*projects)[0].Repository = "https://example.test/repository.git?token=value"
			},
			want: "query",
		},
		{
			name: "unsafe default ref",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				(*projects)[0].DefaultRef = "main..other"
			},
			want: "safe Git ref",
		},
		{
			name: "unsafe display name",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				(*projects)[0].T3ProjectTemplate = "project\nother"
			},
			want: "invalid T3 project",
		},
		{
			name: "duplicate lock",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				(*projects)[0].ResourceLocks = []string{"repository", "repository"}
			},
			want: "duplicated",
		},
		{
			name: "setup command newline",
			mutate: func(_ *[]ProjectDefinition, profiles *[]SetupProfile) {
				(*profiles)[0].Commands[0] = "go mod download\ngo build ./..."
			},
			want: "control character",
		},
		{
			name: "setup timeout",
			mutate: func(_ *[]ProjectDefinition, profiles *[]SetupProfile) {
				(*profiles)[0].Timeout = 0
			},
			want: "timeout must be positive",
		},
		{
			name: "duplicate profile",
			mutate: func(_ *[]ProjectDefinition, profiles *[]SetupProfile) {
				*profiles = append(*profiles, (*profiles)[0])
			},
			want: "duplicate setup profile",
		},
		{
			name: "duplicate project",
			mutate: func(projects *[]ProjectDefinition, _ *[]SetupProfile) {
				*projects = append(*projects, (*projects)[0])
			},
			want: "duplicate project",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projects, profiles := validCatalogFixture()
			test.mutate(&projects, &profiles)
			_, err := NewProjectCatalog(projects, profiles)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestProjectCatalogRejectsInvalidResolution(t *testing.T) {
	projects, profiles := validCatalogFixture()
	catalog, err := NewProjectCatalog(projects, profiles)
	if err != nil {
		t.Fatalf("new project catalog: %v", err)
	}
	tests := []struct {
		name     string
		workflow domain.Workflow
		task     domain.Task
		want     string
	}{
		{
			name: "unknown project",
			workflow: func() domain.Workflow {
				workflow := catalogWorkflow()
				workflow.Project = "absent"
				return workflow
			}(),
			task: catalogTask(),
			want: "unknown project",
		},
		{
			name: "unsafe requested ref",
			workflow: func() domain.Workflow {
				workflow := catalogWorkflow()
				workflow.Environment.Ref = "-dangerous"
				return workflow
			}(),
			task: catalogTask(),
			want: "safe Git ref",
		},
		{
			name:     "task from another workflow",
			workflow: catalogWorkflow(),
			task: func() domain.Task {
				task := catalogTask()
				task.WorkflowID = "other"
				return task
			}(),
			want: "belongs to workflow",
		},
		{
			name: "unsupported environment",
			workflow: func() domain.Workflow {
				workflow := catalogWorkflow()
				workflow.Environment.Type = "local"
				return workflow
			}(),
			task: catalogTask(),
			want: "unsupported environment",
		},
		{
			name: "invalid scope",
			workflow: func() domain.Workflow {
				workflow := catalogWorkflow()
				workflow.Environment.Scope = "attempt"
				return workflow
			}(),
			task: catalogTask(),
			want: "invalid environment scope",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := catalog.Resolve(test.workflow, test.task)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestNilProjectCatalogCannotResolve(t *testing.T) {
	var catalog *ProjectCatalog
	if _, err := catalog.Resolve(catalogWorkflow(), catalogTask()); err == nil || !strings.Contains(err.Error(), "catalog is required") {
		t.Fatalf("error = %v", err)
	}
}

func validCatalogFixture() ([]ProjectDefinition, []SetupProfile) {
	// Named values rather than nested literals: gofmt 1.25 and 1.27 disagree on
	// how to indent a composite literal inside a slice literal in a return.
	project := ProjectDefinition{
		Name: "t3-steward", Repository: "https://github.com/iryzhkov/t3-steward.git",
		DefaultRef: "main", T3ProjectTemplate: "t3-steward development",
		SetupProfile:        "go-project",
		ResourceLocks:       []string{"repository", "database"},
		RequiredCredentials: []string{"signing-key", "github-token"},
	}
	profile := SetupProfile{
		Name: "go-project", Commands: []string{"go mod download", "go build ./..."},
		Timeout: 5 * time.Minute,
	}
	return []ProjectDefinition{project}, []SetupProfile{profile}
}

func catalogWorkflow() domain.Workflow {
	return domain.Workflow{
		ID: "workflow-1", Version: ManifestVersion, Name: "workflow",
		Project: "t3-steward",
		Environment: domain.ExecutionEnvironment{
			Type: EnvironmentGit, Scope: EnvironmentScopeTask,
		},
	}
}

func catalogTask() domain.Task {
	return domain.Task{
		ID: "task-1", WorkflowID: "workflow-1", Name: "task",
		ResourceLocks: []string{"gpu", "repository"},
	}
}
