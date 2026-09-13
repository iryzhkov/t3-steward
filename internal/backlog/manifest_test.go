package backlog

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestParseManifestAppliesDefaults(t *testing.T) {
	manifest := mustParseManifest(t, `
version: 2
name: simple-workflow
environment:
  project: t3-steward
tasks:
  inspect:
    prompt_file: prompts/inspect.md
`)

	if manifest.Class != domain.TaskClassSurplus {
		t.Fatalf("class = %q, want surplus", manifest.Class)
	}
	if manifest.Environment.Type != EnvironmentGit || manifest.Environment.Scope != EnvironmentScopeTask {
		t.Fatalf("environment defaults = %#v", manifest.Environment)
	}
	task := manifest.Tasks["inspect"]
	if task.Class != domain.TaskClassSurplus || task.Importance != 3 || task.Difficulty != 3 || task.MaxTurns != 3 {
		t.Fatalf("task defaults = %#v", task)
	}
}

func TestDocumentedWorkflowBundleStaysValid(t *testing.T) {
	bundle := filepath.Join("..", "..", "docs", "examples", "backlog-v2")
	manifest, err := LoadManifest(bundle)
	if err != nil {
		t.Fatalf("load documented workflow bundle: %v", err)
	}
	if manifest.Version != ManifestVersion || manifest.Name != "review-and-implement" {
		t.Fatalf("documented manifest identity = %#v", manifest)
	}
	implement, ok := manifest.Tasks["implement"]
	if !ok {
		t.Fatal("documented manifest is missing implement task")
	}
	if !reflect.DeepEqual(implement.Needs, ManifestNeeds{"review"}) ||
		!reflect.DeepEqual(implement.InputsFrom["review"], []string{"review.md"}) {
		t.Fatalf("documented dependency contract = %#v", implement)
	}
	if len(implement.Routes) != 1 || implement.Routes[0].QuotaPool != "openai-main" {
		t.Fatalf("documented route inheritance = %#v", implement.Routes)
	}
}

func TestParseManifestEveryFieldAndInheritance(t *testing.T) {
	manifest := mustParseManifest(t, `
version: 2
name: complete-workflow
class: required
placement:
  hosts: [normandy, homelab]
  requires: [internet]
environment:
  project: t3-steward
  type: git
  scope: workflow
  ref: feature/backlog-orchestrator
inputs:
  - inputs/plan.md
  - inputs/findings/*.md
routes:
  - host: normandy
    instance: codex
    model: gpt-5.6-sol
    options:
      effort: high
    quota_pool: codex-main
tasks:
  inspect:
    class: surplus
    prompt_file: prompts/inspect.md
    outputs: [findings.md]
    verify: [go test ./internal/backlog]
    placement:
      hosts: [normandy]
      requires: [docker]
    routes:
      - host: normandy
        instance: claudeAgent
        model: claude-opus-5
        options:
          effort: max
        quota_pool: claude-main
    resource_locks: [repository]
    importance: 5
    difficulty: 4
    estimated_cost: 7.5
    max_turns: 6
    not_before: 2026-09-10T08:00:00Z
    deadline: 2026-09-11T08:00:00Z
    expires_at: 2026-09-12T08:00:00Z
  implement:
    prompt_file: prompts/implement.md
    needs: [inspect]
    inputs_from:
      inspect: [findings.md]
`)

	if manifest.Name != "complete-workflow" || manifest.Class != domain.TaskClassRequired {
		t.Fatalf("workflow identity/class = %#v", manifest)
	}
	if !reflect.DeepEqual(manifest.Placement.Hosts, []string{"normandy", "homelab"}) {
		t.Fatalf("workflow hosts = %#v", manifest.Placement.Hosts)
	}
	if manifest.Environment.Ref != "feature/backlog-orchestrator" || len(manifest.Inputs) != 2 {
		t.Fatalf("environment/inputs = %#v %#v", manifest.Environment, manifest.Inputs)
	}
	inspect := manifest.Tasks["inspect"]
	if inspect.Class != domain.TaskClassSurplus {
		t.Fatalf("inspect class = %q", inspect.Class)
	}
	if !reflect.DeepEqual(inspect.Placement.Hosts, []string{"normandy"}) ||
		!reflect.DeepEqual(inspect.Placement.Requires, []string{"docker", "internet"}) {
		t.Fatalf("inspect placement = %#v", inspect.Placement)
	}
	if len(inspect.Routes) != 1 || inspect.Routes[0].Instance != "claudeAgent" ||
		inspect.Routes[0].Options["effort"] != "max" {
		t.Fatalf("inspect routes = %#v", inspect.Routes)
	}
	if inspect.EstimatedCost == nil || *inspect.EstimatedCost != 7.5 ||
		inspect.Importance != 5 || inspect.Difficulty != 4 || inspect.MaxTurns != 6 {
		t.Fatalf("inspect scheduling fields = %#v", inspect)
	}
	wantNotBefore := time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)
	if inspect.NotBefore == nil || !inspect.NotBefore.Equal(wantNotBefore) ||
		inspect.Deadline == nil || inspect.ExpiresAt == nil {
		t.Fatalf("inspect timing = %#v %#v %#v", inspect.NotBefore, inspect.Deadline, inspect.ExpiresAt)
	}
	if !reflect.DeepEqual(inspect.Outputs, []string{"findings.md"}) ||
		!reflect.DeepEqual(inspect.Verify, []string{"go test ./internal/backlog"}) ||
		!reflect.DeepEqual(inspect.ResourceLocks, []string{"repository"}) {
		t.Fatalf("inspect execution fields = %#v", inspect)
	}

	implement := manifest.Tasks["implement"]
	if implement.Class != domain.TaskClassRequired {
		t.Fatalf("inherited class = %q", implement.Class)
	}
	if !reflect.DeepEqual(implement.Placement.Hosts, []string{"homelab", "normandy"}) ||
		!reflect.DeepEqual(implement.Placement.Requires, []string{"internet"}) {
		t.Fatalf("inherited placement = %#v", implement.Placement)
	}
	if len(implement.Routes) != 1 || implement.Routes[0].Model != "gpt-5.6-sol" {
		t.Fatalf("inherited routes = %#v", implement.Routes)
	}
	implement.Routes[0].Options["effort"] = "changed"
	if manifest.Routes[0].Options["effort"] != "high" {
		t.Fatal("inherited route options alias workflow options")
	}
}

func TestParseManifestRejectsInvalidDefinitions(t *testing.T) {
	base := `
version: 2
name: valid-workflow
environment:
  project: t3-steward
tasks:
  inspect:
    prompt_file: prompts/inspect.md
`
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "unknown field",
			yaml: strings.Replace(base, "name: valid-workflow", "name: valid-workflow\nunknown: true", 1),
			want: "field unknown not found",
		},
		{
			name: "wrong version",
			yaml: strings.Replace(base, "version: 2", "version: 1", 1),
			want: "version must be 2",
		},
		{
			name: "invalid workflow name",
			yaml: strings.Replace(base, "valid-workflow", "Invalid Name", 1),
			want: "name must start",
		},
		{
			name: "missing project",
			yaml: strings.Replace(base, "  project: t3-steward", "  project: \"\"", 1),
			want: "environment.project is required",
		},
		{
			name: "invalid task name",
			yaml: strings.Replace(base, "  inspect:", "  Bad Task:", 1),
			want: "invalid task name",
		},
		{
			name: "empty prompt",
			yaml: strings.Replace(base, "prompts/inspect.md", "\"\"", 1),
			want: "path is empty",
		},
		{
			name: "prompt escape",
			yaml: strings.Replace(base, "prompts/inspect.md", "../inspect.md", 1),
			want: "escapes the workflow bundle",
		},
		{
			name: "prompt glob",
			yaml: strings.Replace(base, "prompts/inspect.md", "prompts/*.md", 1),
			want: "glob characters",
		},
		{
			name: "missing dependency",
			yaml: strings.Replace(base, "    prompt_file:", "    needs: [missing]\n    prompt_file:", 1),
			want: "needs missing task",
		},
		{
			name: "cycle",
			yaml: `
version: 2
name: cycle
environment: {project: t3-steward}
tasks:
  first: {prompt_file: first.md, needs: [second]}
  second: {prompt_file: second.md, needs: [first]}
`,
			want: "dependency cycle",
		},
		{
			name: "undeclared artifact",
			yaml: `
version: 2
name: artifacts
environment: {project: t3-steward}
tasks:
  first: {prompt_file: first.md, outputs: [result.md]}
  second:
    prompt_file: second.md
    needs: [first]
    inputs_from: {first: [other.md]}
`,
			want: "undeclared artifact",
		},
		{
			name: "artifact producer is not dependency",
			yaml: `
version: 2
name: artifacts
environment: {project: t3-steward}
tasks:
  first: {prompt_file: first.md, outputs: [result.md]}
  second:
    prompt_file: second.md
    inputs_from: {first: [result.md]}
`,
			want: "not a direct dependency",
		},
		{
			name: "output escape",
			yaml: strings.Replace(base, "    prompt_file: prompts/inspect.md", "    prompt_file: prompts/inspect.md\n    outputs: [../result.md]", 1),
			want: "escapes the workflow bundle",
		},
		{
			name: "impossible task placement",
			yaml: strings.Replace(base,
				"environment:\n  project: t3-steward",
				"placement: {hosts: [normandy]}\nenvironment:\n  project: t3-steward", 1) +
				"    placement: {hosts: [homelab]}\n",
			want: "impossible placement",
		},
		{
			name: "workflow scope impossible",
			yaml: `
version: 2
name: placement
placement: {hosts: [normandy, homelab]}
environment: {project: t3-steward, scope: workflow}
tasks:
  first:
    prompt_file: first.md
    placement: {hosts: [normandy]}
  second:
    prompt_file: second.md
    placement: {hosts: [homelab]}
`,
			want: "no host eligible for every task",
		},
		{
			name: "route outside placement",
			yaml: strings.Replace(base,
				"    prompt_file: prompts/inspect.md",
				"    prompt_file: prompts/inspect.md\n    placement: {hosts: [normandy]}\n    routes: [{host: homelab, instance: codex, model: gpt-5.6-sol}]", 1),
			want: "no provider route on an eligible host",
		},
		{
			name: "workflow routes require different hosts",
			yaml: `
version: 2
name: route-placement
placement: {hosts: [normandy, homelab]}
environment: {project: t3-steward, scope: workflow}
tasks:
  first:
    prompt_file: first.md
    routes: [{host: normandy, instance: codex, model: gpt-5.6-sol}]
  second:
    prompt_file: second.md
    routes: [{host: homelab, instance: codex, model: gpt-5.6-sol}]
`,
			want: "no host eligible for every task",
		},
		{
			name: "invalid priority",
			yaml: strings.Replace(base, "    prompt_file:", "    importance: 6\n    prompt_file:", 1),
			want: "between 1 and 5",
		},
		{
			name: "multiple documents",
			yaml: base + "\n---\nversion: 2\n",
			want: "multiple YAML documents",
		},
		{
			name: "duplicate task key",
			yaml: base + "  inspect:\n    prompt_file: another.md\n",
			want: "mapping key \"inspect\" already defined",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseManifest([]byte(test.yaml))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadManifestValidatesBundleFiles(t *testing.T) {
	root := t.TempDir()
	writeBundleFile(t, root, "prompts/inspect.md", "inspect")
	writeBundleFile(t, root, "inputs/plan.md", "plan")
	writeBundleFile(t, root, "inputs/findings/one.md", "finding")
	if err := os.Symlink("plan.md", filepath.Join(root, "inputs", "plan-link.md")); err != nil {
		t.Fatal(err)
	}
	writeBundleFile(t, root, "workflow.yaml", `
version: 2
name: bundle
environment: {project: t3-steward}
inputs:
  - inputs/plan-link.md
  - inputs/findings/*.md
tasks:
  inspect: {prompt_file: prompts/inspect.md}
`)

	manifest, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Name != "bundle" {
		t.Fatalf("name = %q", manifest.Name)
	}
}

func TestLoadManifestRejectsMissingAndUnsafeFiles(t *testing.T) {
	t.Run("missing input match", func(t *testing.T) {
		root := validBundle(t)
		rewriteBundleManifest(t, root, strings.Replace(
			readBundleManifest(t, root),
			"tasks:",
			"inputs: [inputs/*.md]\ntasks:", 1))
		_, err := LoadManifest(root)
		assertErrorContains(t, err, "matches no files")
	})

	t.Run("missing prompt", func(t *testing.T) {
		root := validBundle(t)
		rewriteBundleManifest(t, root, strings.Replace(
			readBundleManifest(t, root), "prompts/inspect.md", "prompts/missing.md", 1))
		_, err := LoadManifest(root)
		assertErrorContains(t, err, "no such file")
	})

	t.Run("prompt directory", func(t *testing.T) {
		root := validBundle(t)
		if err := os.MkdirAll(filepath.Join(root, "prompts", "directory"), 0o755); err != nil {
			t.Fatal(err)
		}
		rewriteBundleManifest(t, root, strings.Replace(
			readBundleManifest(t, root), "prompts/inspect.md", "prompts/directory", 1))
		_, err := LoadManifest(root)
		assertErrorContains(t, err, "not a regular file")
	})

	t.Run("prompt symlink escape", func(t *testing.T) {
		root := validBundle(t)
		outside := filepath.Join(t.TempDir(), "outside.md")
		if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(root, "prompts", "escape.md")
		if err := os.Symlink(outside, link); err != nil {
			t.Fatal(err)
		}
		rewriteBundleManifest(t, root, strings.Replace(
			readBundleManifest(t, root), "prompts/inspect.md", "prompts/escape.md", 1))
		_, err := LoadManifest(root)
		assertErrorContains(t, err, "symlink escapes")
	})

	t.Run("glob through escaping symlink", func(t *testing.T) {
		root := validBundle(t)
		outside := t.TempDir()
		if err := os.WriteFile(filepath.Join(outside, "outside.md"), []byte("outside"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "outside")); err != nil {
			t.Fatal(err)
		}
		rewriteBundleManifest(t, root, strings.Replace(
			readBundleManifest(t, root),
			"tasks:",
			"inputs: [outside/*.md]\ntasks:", 1))
		_, err := LoadManifest(root)
		assertErrorContains(t, err, "symlink escapes")
	})

	t.Run("manifest symlink escape", func(t *testing.T) {
		root := validBundle(t)
		outside := filepath.Join(t.TempDir(), "workflow.yaml")
		if err := os.WriteFile(outside, []byte(readBundleManifest(t, root)), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(root, "workflow.yaml")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "workflow.yaml")); err != nil {
			t.Fatal(err)
		}
		_, err := LoadManifest(root)
		assertErrorContains(t, err, "symlink escapes")
	})
}

func mustParseManifest(t *testing.T, raw string) Manifest {
	t.Helper()
	manifest, err := ParseManifest([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func validBundle(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeBundleFile(t, root, "prompts/inspect.md", "inspect")
	writeBundleFile(t, root, "workflow.yaml", `
version: 2
name: bundle
environment: {project: t3-steward}
tasks:
  inspect: {prompt_file: prompts/inspect.md}
`)
	return root
}

func writeBundleFile(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readBundleManifest(t *testing.T, root string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "workflow.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func rewriteBundleManifest(t *testing.T, root, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, "workflow.yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want substring %q", err, want)
	}
}
