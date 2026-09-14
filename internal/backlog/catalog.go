package backlog

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

var scpGitRepositoryPattern = regexp.MustCompile(`^(?:[A-Za-z0-9._-]+@)?[A-Za-z0-9.-]+:[^[:space:]]+$`)

// SetupProfile is a named, host-independent workspace preparation recipe.
// Credential values are intentionally absent and remain in worker-managed stores.
type SetupProfile struct {
	Name     string
	Commands []string
	Timeout  time.Duration
}

// ProjectDefinition maps one workflow project name to immutable preparation metadata.
type ProjectDefinition struct {
	DirectoryBindings   []directoryresource.Binding `json:"directoryBindings,omitempty"`
	Type                string                      `json:"type,omitempty"`
	Name                string
	Repository          string
	DefaultRef          string
	T3ProjectTemplate   string
	SetupProfile        string
	ResourceLocks       []string
	RequiredCredentials []string
}

// ResolvedEnvironment is the deterministic preparation contract for one task attempt.
type ResolvedEnvironment struct {
	DirectoryBindings   []directoryresource.Binding `json:"directoryBindings,omitempty"`
	Type                string                      `json:"type,omitempty"`
	ProjectName         string
	Repository          string
	Ref                 string
	Scope               string
	T3ProjectTemplate   string
	Setup               SetupProfile
	ResourceLocks       []string
	RequiredCredentials []string
}

// ProjectCatalog is an immutable validated set of projects and named setup profiles.
type ProjectCatalog struct {
	projects map[string]ProjectDefinition
	profiles map[string]SetupProfile
}

// NewProjectCatalog validates and detaches catalog configuration from its caller.
func NewProjectCatalog(projects []ProjectDefinition, profiles []SetupProfile) (*ProjectCatalog, error) {
	catalog := &ProjectCatalog{
		projects: make(map[string]ProjectDefinition, len(projects)),
		profiles: make(map[string]SetupProfile, len(profiles)),
	}
	for _, profile := range profiles {
		if err := validateSetupProfile(profile); err != nil {
			return nil, err
		}
		if _, duplicate := catalog.profiles[profile.Name]; duplicate {
			return nil, fmt.Errorf("project catalog: duplicate setup profile %q", profile.Name)
		}
		catalog.profiles[profile.Name] = cloneSetupProfile(profile)
	}
	for _, project := range projects {
		if err := directoryresource.ValidateCatalog(project.DirectoryBindings); err != nil {
			return nil, err
		}
		if err := validateProjectDefinition(project); err != nil {
			return nil, err
		}
		if _, duplicate := catalog.projects[project.Name]; duplicate {
			return nil, fmt.Errorf("project catalog: duplicate project %q", project.Name)
		}
		if _, exists := catalog.profiles[project.SetupProfile]; !exists {
			return nil, fmt.Errorf("project catalog: project %q references unknown setup profile %q", project.Name, project.SetupProfile)
		}
		catalog.projects[project.Name] = cloneProjectDefinition(project)
	}
	return catalog, nil
}

// Project returns one catalog project by name. It exists so that a caller which
// must distinguish an unknown project from an unknown setup profile can ask
// each question separately, instead of reading the answer out of the wording of
// a Resolve error.
func (c *ProjectCatalog) Project(name string) (ProjectDefinition, bool) {
	if c == nil {
		return ProjectDefinition{}, false
	}
	project, found := c.projects[name]
	if !found {
		return ProjectDefinition{}, false
	}
	return cloneProjectDefinition(project), true
}

// Profile returns one named setup profile.
func (c *ProjectCatalog) Profile(name string) (SetupProfile, bool) {
	if c == nil {
		return SetupProfile{}, false
	}
	profile, found := c.profiles[name]
	if !found {
		return SetupProfile{}, false
	}
	return cloneSetupProfile(profile), true
}

// ValidateRepositorySyntax and ValidateRefSyntax expose the catalog's own
// repository and ref rules so that a live readiness check applies exactly the
// syntax the catalog applies, rather than a second, weaker copy of it.
func ValidateRepositorySyntax(repository string) error { return validateGitRepository(repository) }

// ValidateRefSyntax reports whether ref is a safe Git ref.
func ValidateRefSyntax(ref string) error { return validateGitRef(ref) }

// Resolve constructs the immutable environment for a workflow task. It applies
// the catalog default ref only when the workflow did not request one explicitly.
func (c *ProjectCatalog) Resolve(workflow domain.Workflow, task domain.Task) (ResolvedEnvironment, error) {
	if c == nil {
		return ResolvedEnvironment{}, errors.New("resolve project: catalog is required")
	}
	if workflow.ID == "" || task.ID == "" {
		return ResolvedEnvironment{}, errors.New("resolve project: workflow and task IDs are required")
	}
	if task.WorkflowID != workflow.ID {
		return ResolvedEnvironment{}, fmt.Errorf("resolve project: task %q belongs to workflow %q, want %q", task.ID, task.WorkflowID, workflow.ID)
	}
	if workflow.Environment.Type != EnvironmentGit && workflow.Environment.Type != EnvironmentFresh {
		return ResolvedEnvironment{}, fmt.Errorf("resolve project: unsupported environment type %q", workflow.Environment.Type)
	}
	if workflow.Environment.Scope != EnvironmentScopeTask && workflow.Environment.Scope != EnvironmentScopeWorkflow {
		return ResolvedEnvironment{}, fmt.Errorf("resolve project: invalid environment scope %q", workflow.Environment.Scope)
	}
	project, exists := c.projects[workflow.Project]
	if !exists {
		return ResolvedEnvironment{}, fmt.Errorf("resolve project: unknown project %q", workflow.Project)
	}
	projectType := project.Type
	if projectType == "" {
		projectType = EnvironmentGit
	}
	if projectType != workflow.Environment.Type {
		return ResolvedEnvironment{}, errors.New("resolve project: workspace type does not match catalog")
	}
	if projectType == EnvironmentFresh && (workflow.Environment.Ref != "" || workflow.Environment.Scope != EnvironmentScopeTask) {
		return ResolvedEnvironment{}, errors.New("resolve project: fresh workspace requires task scope and no ref")
	}
	profile, exists := c.profiles[project.SetupProfile]
	if !exists {
		return ResolvedEnvironment{}, fmt.Errorf("resolve project: setup profile %q is unavailable", project.SetupProfile)
	}
	ref := workflow.Environment.Ref
	if ref == "" {
		ref = project.DefaultRef
	}
	if err := validateGitRef(ref); projectType == EnvironmentGit && err != nil {
		return ResolvedEnvironment{}, fmt.Errorf("resolve project %q ref: %w", project.Name, err)
	}

	if err := directoryresource.Authorize(project.DirectoryBindings, task.DirectoryBindings); err != nil {
		return ResolvedEnvironment{}, err
	}
	locks := append([]string(nil), project.ResourceLocks...)
	locks = append(locks, task.ResourceLocks...)
	locks = uniqueSorted(locks)
	return ResolvedEnvironment{
		DirectoryBindings: directoryresource.CloneBindings(task.DirectoryBindings),
		Type:              project.Type, ProjectName: project.Name, Repository: project.Repository, Ref: ref,
		Scope: workflow.Environment.Scope, T3ProjectTemplate: project.T3ProjectTemplate,
		Setup: cloneSetupProfile(profile), ResourceLocks: locks,
		RequiredCredentials: append([]string(nil), project.RequiredCredentials...),
	}, nil
}

func validateSetupProfile(profile SetupProfile) error {
	if !manifestNamePattern.MatchString(profile.Name) {
		return fmt.Errorf("project catalog: invalid setup profile name %q", profile.Name)
	}
	if profile.Timeout <= 0 {
		return fmt.Errorf("project catalog: setup profile %q timeout must be positive", profile.Name)
	}
	if len(profile.Commands) == 0 && profile.Name != "steward-fresh-empty" {
		return fmt.Errorf("project catalog: setup profile %q must declare at least one command", profile.Name)
	}
	for index, command := range profile.Commands {
		if strings.TrimSpace(command) != command || command == "" {
			return fmt.Errorf("project catalog: setup profile %q command %d must be nonempty and trimmed", profile.Name, index+1)
		}
		if strings.ContainsAny(command, "\x00\r\n") {
			return fmt.Errorf("project catalog: setup profile %q command %d contains a forbidden control character", profile.Name, index+1)
		}
	}
	return nil
}

func validateProjectDefinition(project ProjectDefinition) error {
	if !manifestNamePattern.MatchString(project.Name) {
		return fmt.Errorf("project catalog: invalid project name %q", project.Name)
	}
	if project.Type != "" && project.Type != EnvironmentGit && project.Type != EnvironmentFresh {
		return errors.New("project catalog: unsupported workspace type")
	}
	if project.Type == EnvironmentFresh && (project.Repository != "" || project.DefaultRef != "") {
		return errors.New("project catalog: fresh workspace must not declare repository or ref")
	}
	if err := validateGitRepository(project.Repository); project.Type != EnvironmentFresh && err != nil {
		return fmt.Errorf("project catalog: project %q repository: %w", project.Name, err)
	}
	if err := validateGitRef(project.DefaultRef); project.Type != EnvironmentFresh && err != nil {
		return fmt.Errorf("project catalog: project %q default ref: %w", project.Name, err)
	}
	if project.T3ProjectTemplate != "" && !safeDisplayName(project.T3ProjectTemplate) {
		return fmt.Errorf("project catalog: project %q has invalid T3 project template", project.Name)
	}
	if !manifestNamePattern.MatchString(project.SetupProfile) {
		return fmt.Errorf("project catalog: project %q has invalid setup profile %q", project.Name, project.SetupProfile)
	}
	if err := validateIdentifiers("project "+project.Name+" resource lock", project.ResourceLocks); err != nil {
		return fmt.Errorf("project catalog: %w", err)
	}
	if err := validateIdentifiers("project "+project.Name+" required credential", project.RequiredCredentials); err != nil {
		return fmt.Errorf("project catalog: %w", err)
	}
	return nil
}

func validateGitRepository(repository string) error {
	if strings.TrimSpace(repository) != repository || repository == "" || strings.ContainsRune(repository, '\x00') {
		return errors.New("must be nonempty, trimmed, and contain no NUL")
	}
	if !strings.Contains(repository, "://") && scpGitRepositoryPattern.MatchString(repository) {
		parts := strings.SplitN(repository, ":", 2)
		if parts[0] == "" || parts[1] == "" || strings.HasPrefix(parts[1], "-") {
			return errors.New("has invalid SSH repository syntax")
		}
		return nil
	}
	parsed, err := url.Parse(repository)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	if parsed.Scheme != "https" && parsed.Scheme != "ssh" {
		return fmt.Errorf("scheme %q is not allowed", parsed.Scheme)
	}
	if parsed.Host == "" || parsed.Path == "" || parsed.Path == "/" {
		return errors.New("URL must include a host and repository path")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("URL query and fragment are not allowed")
	}
	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword || parsed.Scheme == "https" {
			return errors.New("embedded HTTP credentials or passwords are not allowed")
		}
	}
	return nil
}

func validateGitRef(ref string) error {
	if ref == "" {
		return errors.New("must not be empty")
	}
	if strings.HasPrefix(ref, "-") || strings.HasPrefix(ref, "/") ||
		strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".") ||
		strings.Contains(ref, "..") || strings.Contains(ref, "@{") ||
		strings.Contains(ref, "//") || strings.HasSuffix(ref, ".lock") {
		return fmt.Errorf("%q is not a safe Git ref", ref)
	}
	for _, r := range ref {
		if unicode.IsControl(r) || unicode.IsSpace(r) || strings.ContainsRune(`~^:?*[\`, r) {
			return fmt.Errorf("%q is not a safe Git ref", ref)
		}
	}
	for _, component := range strings.Split(ref, "/") {
		if component == "" || component == "." || component == ".." || strings.HasPrefix(component, ".") {
			return fmt.Errorf("%q is not a safe Git ref", ref)
		}
	}
	return nil
}

func safeDisplayName(value string) bool {
	if strings.TrimSpace(value) != value || value == "" {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func cloneSetupProfile(profile SetupProfile) SetupProfile {
	profile.Commands = append([]string(nil), profile.Commands...)
	return profile
}

func cloneProjectDefinition(project ProjectDefinition) ProjectDefinition {
	project.DirectoryBindings = directoryresource.CloneBindings(project.DirectoryBindings)
	project.ResourceLocks = append([]string(nil), project.ResourceLocks...)
	project.RequiredCredentials = append([]string(nil), project.RequiredCredentials...)
	sort.Strings(project.ResourceLocks)
	sort.Strings(project.RequiredCredentials)
	return project
}
