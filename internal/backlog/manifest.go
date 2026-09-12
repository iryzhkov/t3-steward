package backlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

const ManifestVersion = 2

const (
	EnvironmentGit = "git"

	EnvironmentScopeTask     = "task"
	EnvironmentScopeWorkflow = "workflow"
)

var manifestNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*$`)

// Manifest is the version 2 workflow.yaml submission format. ParseManifest
// applies defaults so callers receive a complete, validated definition.
type Manifest struct {
	Version     int                     `yaml:"version"`
	Name        string                  `yaml:"name"`
	Class       domain.TaskClass        `yaml:"class"`
	Placement   ManifestPlacement       `yaml:"placement"`
	Environment ManifestEnvironment     `yaml:"environment"`
	Inputs      []string                `yaml:"inputs"`
	Routes      []ManifestRoute         `yaml:"routes"`
	Tasks       map[string]ManifestTask `yaml:"tasks"`
}

// ManifestPlacement limits eligible hosts and names capabilities that a worker
// must provide.
type ManifestPlacement struct {
	Hosts    []string `yaml:"hosts"`
	Requires []string `yaml:"requires"`
}

// ManifestEnvironment describes the project workspace prepared for tasks.
type ManifestEnvironment struct {
	Project string `yaml:"project"`
	Type    string `yaml:"type"`
	Scope   string `yaml:"scope"`
	Ref     string `yaml:"ref"`
}

// ManifestRoute is one ordered provider candidate.
type ManifestRoute struct {
	Host      string            `yaml:"host"`
	Instance  string            `yaml:"instance"`
	Model     string            `yaml:"model"`
	Options   map[string]string `yaml:"options"`
	QuotaPool string            `yaml:"quota_pool"`
}

// ManifestTask is one node in a workflow manifest.
type ManifestTask struct {
	Class         domain.TaskClass    `yaml:"class"`
	PromptFile    string              `yaml:"prompt_file"`
	Needs         []string            `yaml:"needs"`
	InputsFrom    map[string][]string `yaml:"inputs_from"`
	Outputs       []string            `yaml:"outputs"`
	Verify        []string            `yaml:"verify"`
	Placement     ManifestPlacement   `yaml:"placement"`
	Routes        []ManifestRoute     `yaml:"routes"`
	ResourceLocks []string            `yaml:"resource_locks"`
	Importance    int                 `yaml:"importance"`
	Difficulty    int                 `yaml:"difficulty"`
	EstimatedCost *float64            `yaml:"estimated_cost"`
	MaxTurns      int                 `yaml:"max_turns"`
	NotBefore     *time.Time          `yaml:"not_before"`
	Deadline      *time.Time          `yaml:"deadline"`
	ExpiresAt     *time.Time          `yaml:"expires_at"`

	placementImpossible bool
}

// ParseManifest strictly decodes, defaults, and validates a version 2
// workflow manifest. Referenced files are checked by LoadManifest.
func ParseManifest(raw []byte) (Manifest, error) {
	var manifest Manifest
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, fmt.Errorf("decode workflow manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return manifest, errors.New("decode workflow manifest: multiple YAML documents are not allowed")
		}
		return manifest, fmt.Errorf("decode workflow manifest: %w", err)
	}

	applyManifestDefaults(&manifest)
	if err := validateManifest(manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

// LoadManifest reads workflow.yaml from a bundle directory, validates all
// referenced files, and rejects paths or symlinks that escape the bundle.
func LoadManifest(bundleDir string) (Manifest, error) {
	var manifest Manifest
	root, err := filepath.Abs(bundleDir)
	if err != nil {
		return manifest, fmt.Errorf("resolve workflow bundle: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return manifest, fmt.Errorf("resolve workflow bundle: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return manifest, fmt.Errorf("stat workflow bundle: %w", err)
	}
	if !info.IsDir() {
		return manifest, errors.New("workflow bundle is not a directory")
	}

	manifestPath, err := safeBundleFile(root, "workflow.yaml")
	if err != nil {
		return manifest, fmt.Errorf("workflow.yaml: %w", err)
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return manifest, fmt.Errorf("read workflow.yaml: %w", err)
	}
	manifest, err = ParseManifest(raw)
	if err != nil {
		return manifest, err
	}
	if err := validateManifestFiles(root, manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func applyManifestDefaults(manifest *Manifest) {
	if manifest.Class == "" {
		manifest.Class = domain.TaskClassSurplus
	}
	if manifest.Environment.Type == "" {
		manifest.Environment.Type = EnvironmentGit
	}
	if manifest.Environment.Scope == "" {
		manifest.Environment.Scope = EnvironmentScopeTask
	}
	for name, task := range manifest.Tasks {
		if task.Class == "" {
			task.Class = manifest.Class
		}
		if task.Importance == 0 {
			task.Importance = 3
		}
		if task.Difficulty == 0 {
			task.Difficulty = 3
		}
		if task.MaxTurns == 0 {
			task.MaxTurns = 3
		}
		if len(task.Routes) == 0 {
			task.Routes = cloneRoutes(manifest.Routes)
		}
		task.placementImpossible = len(manifest.Placement.Hosts) != 0 && len(task.Placement.Hosts) != 0 &&
			len(intersectConstraints(manifest.Placement.Hosts, task.Placement.Hosts)) == 0
		task.Placement = effectivePlacement(manifest.Placement, task.Placement)
		manifest.Tasks[name] = task
	}
}

func cloneRoutes(routes []ManifestRoute) []ManifestRoute {
	if routes == nil {
		return nil
	}
	result := make([]ManifestRoute, len(routes))
	copy(result, routes)
	for i := range result {
		if routes[i].Options != nil {
			result[i].Options = make(map[string]string, len(routes[i].Options))
			for key, value := range routes[i].Options {
				result[i].Options[key] = value
			}
		}
	}
	return result
}

func effectivePlacement(workflow, task ManifestPlacement) ManifestPlacement {
	result := ManifestPlacement{
		Hosts:    intersectConstraints(workflow.Hosts, task.Hosts),
		Requires: uniqueSorted(append(append([]string(nil), workflow.Requires...), task.Requires...)),
	}
	return result
}

func intersectConstraints(left, right []string) []string {
	switch {
	case len(left) == 0:
		return uniqueSorted(right)
	case len(right) == 0:
		return uniqueSorted(left)
	}
	allowed := make(map[string]struct{}, len(right))
	for _, value := range right {
		allowed[value] = struct{}{}
	}
	var result []string
	for _, value := range left {
		if _, ok := allowed[value]; ok {
			result = append(result, value)
		}
	}
	return uniqueSorted(result)
}

func uniqueSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func validateManifest(manifest Manifest) error {
	if manifest.Version != ManifestVersion {
		return fmt.Errorf("version must be %d", ManifestVersion)
	}
	if !manifestNamePattern.MatchString(manifest.Name) {
		return errors.New("name must start with a lowercase letter and contain only lowercase letters, digits, hyphens, or underscores")
	}
	if !validClass(manifest.Class) {
		return fmt.Errorf("invalid workflow class %q", manifest.Class)
	}
	if manifest.Environment.Project == "" {
		return errors.New("environment.project is required")
	}
	if manifest.Environment.Type != EnvironmentGit {
		return fmt.Errorf("unsupported environment.type %q", manifest.Environment.Type)
	}
	if manifest.Environment.Scope != EnvironmentScopeTask && manifest.Environment.Scope != EnvironmentScopeWorkflow {
		return fmt.Errorf("invalid environment.scope %q", manifest.Environment.Scope)
	}
	if len(manifest.Tasks) == 0 {
		return errors.New("at least one task is required")
	}
	if err := validatePlacement("workflow placement", manifest.Placement); err != nil {
		return err
	}
	if err := validateRoutes("workflow routes", manifest.Routes); err != nil {
		return err
	}
	if err := validateUniquePaths("inputs", manifest.Inputs, true); err != nil {
		return err
	}

	taskNames := make([]string, 0, len(manifest.Tasks))
	for name := range manifest.Tasks {
		taskNames = append(taskNames, name)
	}
	sort.Strings(taskNames)
	for _, name := range taskNames {
		if !manifestNamePattern.MatchString(name) {
			return fmt.Errorf("invalid task name %q", name)
		}
		if err := validateManifestTask(name, manifest.Tasks[name], manifest.Tasks); err != nil {
			return err
		}
	}
	if err := validateAcyclic(taskNames, manifest.Tasks); err != nil {
		return err
	}
	if manifest.Environment.Scope == EnvironmentScopeWorkflow {
		if err := validateWorkflowPlacement(taskNames, manifest.Tasks); err != nil {
			return err
		}
	}
	return nil
}

func validClass(class domain.TaskClass) bool {
	return class == domain.TaskClassRequired || class == domain.TaskClassSurplus
}

func validateManifestTask(name string, task ManifestTask, tasks map[string]ManifestTask) error {
	prefix := "task " + name
	if !validClass(task.Class) {
		return fmt.Errorf("%s has invalid class %q", prefix, task.Class)
	}
	if err := validateRelativePath(task.PromptFile, false); err != nil {
		return fmt.Errorf("%s prompt_file: %w", prefix, err)
	}
	if task.Importance < 1 || task.Importance > 5 || task.Difficulty < 1 || task.Difficulty > 5 {
		return fmt.Errorf("%s importance and difficulty must be between 1 and 5", prefix)
	}
	if task.MaxTurns < 1 {
		return fmt.Errorf("%s max_turns must be positive", prefix)
	}
	if task.EstimatedCost != nil && *task.EstimatedCost <= 0 {
		return fmt.Errorf("%s estimated_cost must be positive", prefix)
	}
	if task.Deadline != nil && task.NotBefore != nil && task.Deadline.Before(*task.NotBefore) {
		return fmt.Errorf("%s deadline precedes not_before", prefix)
	}
	if task.ExpiresAt != nil && task.NotBefore != nil && task.ExpiresAt.Before(*task.NotBefore) {
		return fmt.Errorf("%s expires_at precedes not_before", prefix)
	}
	if err := validatePlacement(prefix+" placement", task.Placement); err != nil {
		return err
	}
	if task.placementImpossible {
		return fmt.Errorf("%s has impossible placement", prefix)
	}
	if err := validateRoutes(prefix+" routes", task.Routes); err != nil {
		return err
	}
	if err := validateRoutePlacement(prefix, task); err != nil {
		return err
	}
	if err := validateUniquePaths(prefix+" outputs", task.Outputs, false); err != nil {
		return err
	}
	if err := validateNonEmptyUnique(prefix+" verification command", task.Verify); err != nil {
		return err
	}
	if err := validateNonEmptyUnique(prefix+" resource lock", task.ResourceLocks); err != nil {
		return err
	}

	needs := make(map[string]struct{}, len(task.Needs))
	for _, dependency := range task.Needs {
		if dependency == name {
			return fmt.Errorf("%s cannot depend on itself", prefix)
		}
		if _, ok := tasks[dependency]; !ok {
			return fmt.Errorf("%s needs missing task %q", prefix, dependency)
		}
		if _, duplicate := needs[dependency]; duplicate {
			return fmt.Errorf("%s repeats dependency %q", prefix, dependency)
		}
		needs[dependency] = struct{}{}
	}
	for producer, artifacts := range task.InputsFrom {
		if _, ok := needs[producer]; !ok {
			return fmt.Errorf("%s inputs_from task %q is not a direct dependency", prefix, producer)
		}
		if err := validateNonEmptyUnique(prefix+" inputs_from "+producer, artifacts); err != nil {
			return err
		}
		outputs := make(map[string]struct{}, len(tasks[producer].Outputs))
		for _, output := range tasks[producer].Outputs {
			outputs[output] = struct{}{}
		}
		for _, artifact := range artifacts {
			if err := validateRelativePath(artifact, false); err != nil {
				return fmt.Errorf("%s inputs_from %s artifact %q: %w", prefix, producer, artifact, err)
			}
			if _, ok := outputs[artifact]; !ok {
				return fmt.Errorf("%s references undeclared artifact %q from %s", prefix, artifact, producer)
			}
		}
	}
	return nil
}

// manifestTaskDeclaredHosts distinguishes an empty effective intersection from
// two unconstrained placements after defaults have been applied.

func validatePlacement(label string, placement ManifestPlacement) error {
	if err := validateIdentifiers(label+" host", placement.Hosts); err != nil {
		return err
	}
	return validateIdentifiers(label+" capability", placement.Requires)
}

func validateIdentifiers(label string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !manifestNamePattern.MatchString(value) {
			return fmt.Errorf("%s %q is invalid", label, value)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s %q is duplicated", label, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateRoutes(label string, routes []ManifestRoute) error {
	seen := make(map[string]struct{}, len(routes))
	for i, route := range routes {
		if route.Instance == "" || route.Model == "" {
			return fmt.Errorf("%s[%d] requires instance and model", label, i)
		}
		if route.Host != "" && !manifestNamePattern.MatchString(route.Host) {
			return fmt.Errorf("%s[%d] has invalid host %q", label, i, route.Host)
		}
		key := route.Host + "\x00" + route.Instance + "\x00" + route.Model
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%s[%d] duplicates a provider candidate", label, i)
		}
		seen[key] = struct{}{}
		for option, value := range route.Options {
			if strings.TrimSpace(option) == "" || strings.TrimSpace(value) == "" {
				return fmt.Errorf("%s[%d] has an empty option name or value", label, i)
			}
		}
	}
	return nil
}

func validateRoutePlacement(prefix string, task ManifestTask) error {
	if len(task.Routes) == 0 || len(task.Placement.Hosts) == 0 {
		return nil
	}
	hosts := make(map[string]struct{}, len(task.Placement.Hosts))
	for _, host := range task.Placement.Hosts {
		hosts[host] = struct{}{}
	}
	for _, route := range task.Routes {
		if route.Host == "" {
			return nil
		}
		if _, ok := hosts[route.Host]; ok {
			return nil
		}
	}
	return fmt.Errorf("%s has no provider route on an eligible host", prefix)
}

func validateAcyclic(names []string, tasks map[string]ManifestTask) error {
	const (
		unseen = iota
		visiting
		visited
	)
	state := make(map[string]int, len(tasks))
	var visit func(string) error
	visit = func(name string) error {
		switch state[name] {
		case visiting:
			return fmt.Errorf("workflow contains a dependency cycle at task %q", name)
		case visited:
			return nil
		}
		state[name] = visiting
		for _, dependency := range tasks[name].Needs {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[name] = visited
		return nil
	}
	for _, name := range names {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

func staticTaskHosts(task ManifestTask) []string {
	hosts := append([]string(nil), task.Placement.Hosts...)
	if len(task.Routes) == 0 {
		return hosts
	}
	var routeHosts []string
	for _, route := range task.Routes {
		if route.Host == "" {
			return hosts
		}
		routeHosts = append(routeHosts, route.Host)
	}
	if len(hosts) == 0 {
		return uniqueSorted(routeHosts)
	}
	return intersectConstraints(hosts, routeHosts)
}

func validateWorkflowPlacement(names []string, tasks map[string]ManifestTask) error {
	var common []string
	constrained := false
	for _, name := range names {
		hosts := staticTaskHosts(tasks[name])
		if len(hosts) == 0 {
			continue
		}
		if !constrained {
			common = append([]string(nil), hosts...)
			constrained = true
		} else {
			common = intersectConstraints(common, hosts)
		}
		if len(common) == 0 {
			return errors.New("workflow-scoped environment has no host eligible for every task")
		}
	}
	return nil
}

func validateUniquePaths(label string, paths []string, allowGlob bool) error {
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if err := validateRelativePath(path, allowGlob); err != nil {
			return fmt.Errorf("%s path %q: %w", label, path, err)
		}
		if _, ok := seen[path]; ok {
			return fmt.Errorf("%s path %q is duplicated", label, path)
		}
		seen[path] = struct{}{}
	}
	return nil
}

func validateRelativePath(path string, allowGlob bool) error {
	if path == "" {
		return errors.New("path is empty")
	}
	if filepath.IsAbs(path) || filepath.VolumeName(path) != "" {
		return errors.New("absolute paths are not allowed")
	}
	if strings.ContainsRune(path, '\x00') {
		return errors.New("path contains NUL")
	}
	clean := filepath.Clean(path)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return errors.New("path escapes the workflow bundle")
	}
	if !allowGlob && strings.ContainsAny(path, "*?[") {
		return errors.New("glob characters are not allowed")
	}
	return nil
}

func validateNonEmptyUnique(label string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is empty", label)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s %q is duplicated", label, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateManifestFiles(root string, manifest Manifest) error {
	for name, task := range manifest.Tasks {
		if _, err := safeBundleFile(root, task.PromptFile); err != nil {
			return fmt.Errorf("task %s prompt_file: %w", name, err)
		}
	}
	for _, pattern := range manifest.Inputs {
		matches, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil {
			return fmt.Errorf("input pattern %q: %w", pattern, err)
		}
		if len(matches) == 0 {
			return fmt.Errorf("input pattern %q matches no files", pattern)
		}
		for _, match := range matches {
			relative, err := filepath.Rel(root, match)
			if err != nil {
				return fmt.Errorf("input pattern %q: %w", pattern, err)
			}
			if _, err := safeBundleFile(root, relative); err != nil {
				return fmt.Errorf("input pattern %q: %w", pattern, err)
			}
		}
	}
	return nil
}

func safeBundleFile(root, relative string) (string, error) {
	if err := validateRelativePath(relative, false); err != nil {
		return "", err
	}
	// Resolve the root as well: on macOS the temporary directory itself lives
	// behind a symlink (/var -> /private/var), and comparing a resolved child
	// against an unresolved root would report every file as escaping.
	rootResolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	candidate := filepath.Join(root, relative)
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}
	inside, err := filepath.Rel(rootResolved, resolved)
	if err != nil {
		return "", err
	}
	if inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
		return "", errors.New("symlink escapes the workflow bundle")
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("referenced path is not a regular file")
	}
	return resolved, nil
}
