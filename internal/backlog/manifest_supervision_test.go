package backlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// supervisedYAML is a minimal supervised campaign: one observed producer, one
// protected successor that already depends on it, and one gate between them.
const supervisedYAML = `
version: 2
name: supervised
environment:
  project: t3-steward
routes:
  - instance: workerInstance
    model: worker-model
supervision:
  route:
    instance: overseerInstance
    model: overseer-model
    quota_pool: overseer-pool
  prompt_file: prompts/overseer.md
  max_activations: 8
  max_turns_per_activation: 2
  activation_deadline: 90m
  idle_escalation_after: 12h
  escalation:
    notify_thread: true
gates:
  review:
    after: [analyze]
    before: [apply]
    rubric_file: rubrics/review.md
tasks:
  analyze:
    prompt_file: prompts/analyze.md
    outputs: [analysis.md]
  apply:
    prompt_file: prompts/apply.md
    needs: [analyze]
    inputs_from:
      analyze: [analysis.md]
`

func TestParseManifestAcceptsSupervision(t *testing.T) {
	manifest, err := ParseManifest([]byte(supervisedYAML))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	config, supervised := manifest.SupervisionConfig()
	if !supervised {
		t.Fatal("a declared supervision block must produce a domain configuration")
	}
	if config.Route.ProviderInstanceID != "overseerInstance" || config.Route.Model != "overseer-model" {
		t.Fatalf("overseer route = %#v", config.Route)
	}
	if config.Route.QuotaPoolID != "overseer-pool" {
		t.Fatalf("overseer quota pool = %q", config.Route.QuotaPoolID)
	}
	// The overseer prompt travels as a bundle path until ingestion retains it.
	if config.PromptArtifactID != "prompts/overseer.md" {
		t.Fatalf("prompt artifact = %q", config.PromptArtifactID)
	}
	if config.ActivationDeadline != 90*time.Minute || config.IdleEscalationAfter != 12*time.Hour {
		t.Fatalf("durations = %s / %s", config.ActivationDeadline, config.IdleEscalationAfter)
	}
	if !config.Escalation.NotifyThread {
		t.Fatal("escalation destination was declared and is not carried")
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("the produced configuration must satisfy the domain: %v", err)
	}

	gates := manifest.GateDefinitions()
	if len(gates) != 1 {
		t.Fatalf("gates = %#v", gates)
	}
	want := domain.GateDefinition{
		ID:               "review",
		Name:             "review",
		ObservedTaskIDs:  []string{"analyze"},
		ProtectedTaskIDs: []string{"apply"},
		RubricArtifactID: "rubrics/review.md",
	}
	if gates[0].ID != want.ID || gates[0].Name != want.Name || gates[0].Final {
		t.Fatalf("gate identity = %#v", gates[0])
	}
	if !gates[0].Observes("analyze") || !gates[0].Protects("apply") {
		t.Fatalf("gate scope = %#v", gates[0])
	}
	if err := gates[0].Validate(); err != nil {
		t.Fatalf("the produced gate must satisfy the domain: %v", err)
	}
}

func TestParseManifestDefaultsSupervisionBounds(t *testing.T) {
	source := strings.Replace(supervisedYAML, `  max_activations: 8
  max_turns_per_activation: 2
  activation_deadline: 90m
  idle_escalation_after: 12h
`, "", 1)
	manifest, err := ParseManifest([]byte(source))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	config, _ := manifest.SupervisionConfig()
	if config.MaxActivations != DefaultSupervisionMaxActivations ||
		config.MaxTurnsPerActivation != DefaultSupervisionMaxTurnsPerActivation ||
		config.ActivationDeadline != DefaultSupervisionActivationDeadline {
		t.Fatalf("defaults = %#v", config)
	}
	// An absent idle escalation is no idle escalation, not a defaulted one.
	if config.IdleEscalationAfter != 0 {
		t.Fatalf("idle escalation = %s", config.IdleEscalationAfter)
	}
}

func TestParseManifestWithoutSupervisionIsUnsupervised(t *testing.T) {
	manifest, err := ParseManifest([]byte(`
version: 2
name: plain
environment:
  project: t3-steward
tasks:
  analyze:
    prompt_file: prompts/analyze.md
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, supervised := manifest.SupervisionConfig(); supervised {
		t.Fatal("an undeclared supervision block must not be invented")
	}
	if manifest.Supervision != nil || len(manifest.GateDefinitions()) != 0 {
		t.Fatal("absence of supervision is the unsupervised case; no empty record is created")
	}
}

// TestParseManifestRejectsInvalidSupervision covers the eleven authoring rules
// of the supervision schema, one refusal each.
func TestParseManifestRejectsInvalidSupervision(t *testing.T) {
	tests := []struct {
		name string
		rule string
		yaml string
		want string
	}{
		{
			name: "route without a model",
			rule: "1: one validated route",
			yaml: strings.Replace(supervisedYAML, "    model: overseer-model\n", "", 1),
			want: "supervision route[0] requires instance and model",
		},
		{
			name: "route as a list of alternatives",
			rule: "1: one route, never a list",
			yaml: strings.Replace(supervisedYAML,
				"  route:\n    instance: overseerInstance\n    model: overseer-model\n    quota_pool: overseer-pool\n",
				"  route: [{instance: overseerInstance, model: overseer-model}]\n", 1),
			want: "cannot unmarshal",
		},
		{
			name: "route pinned off the eligible hosts",
			rule: "1: the one route must be placeable",
			yaml: strings.Replace(
				strings.Replace(supervisedYAML, "environment:", "placement:\n  hosts: [alpha]\nenvironment:", 1),
				"  route:\n    instance: overseerInstance",
				"  route:\n    host: beta\n    instance: overseerInstance", 1),
			want: "is not an eligible workflow host",
		},
		{
			name: "overseer prompt escapes the bundle",
			rule: "2: prompt and rubric are relative bundle paths",
			yaml: strings.Replace(supervisedYAML, "prompt_file: prompts/overseer.md", "prompt_file: ../overseer.md", 1),
			want: "supervision prompt_file: path escapes the workflow bundle",
		},
		{
			name: "rubric is a glob",
			rule: "2: prompt and rubric are relative bundle paths",
			yaml: strings.Replace(supervisedYAML, "rubric_file: rubrics/review.md", "rubric_file: rubrics/*.md", 1),
			want: "gate review rubric_file: glob characters",
		},
		{
			name: "gate name is not a manifest name",
			rule: "3: gate names share the task name space",
			yaml: strings.Replace(supervisedYAML, "  review:", "  Review Gate:", 1),
			want: "invalid gate name",
		},
		{
			name: "gate collides with a task",
			rule: "3: gate names share the task name space",
			yaml: strings.Replace(supervisedYAML, "  review:", "  apply:", 1),
			want: "collides with a task of the same name",
		},
		{
			name: "gate takes the reserved sink name",
			rule: "3: gate names share the task name space",
			yaml: strings.Replace(supervisedYAML, "  review:", "  "+domain.SinkTaskName+":", 1),
			want: "uses the reserved sink name",
		},
		{
			name: "observed task does not exist",
			rule: "4: every named task is declared",
			yaml: strings.Replace(supervisedYAML, "after: [analyze]", "after: [missing]", 1),
			want: "gate review observes missing task",
		},
		{
			name: "protected task does not exist",
			rule: "4: every named task is declared",
			yaml: strings.Replace(supervisedYAML, "before: [apply]", "before: [missing]", 1),
			want: "gate review protects missing task",
		},
		{
			name: "gate observes nothing",
			rule: "5: after is non-empty",
			yaml: strings.Replace(supervisedYAML, "after: [analyze]", "after: []", 1),
			want: "gate needs at least one observed task",
		},
		{
			name: "gate protects nothing and is not final",
			rule: "6: before may be empty only when final",
			yaml: strings.Replace(supervisedYAML, "before: [apply]", "before: []", 1),
			want: "gate protects no task and is not marked final",
		},
		{
			name: "final gate protects a downstream task",
			rule: "6: a final gate guards settlement instead",
			yaml: strings.Replace(supervisedYAML, "    rubric_file: rubrics/review.md", "    final: true", 1),
			want: "a final-settlement gate protects no downstream task",
		},
		{
			name: "gate observes and protects one task",
			rule: "7: no task in both scopes",
			yaml: strings.Replace(supervisedYAML, "before: [apply]", "before: [analyze, apply]", 1),
			want: "observes and protects the same task",
		},
		{
			name: "gate closes a cycle",
			rule: "7: the task-plus-gate graph stays acyclic",
			yaml: strings.Replace(supervisedYAML,
				"    after: [analyze]\n    before: [apply]",
				"    after: [apply]\n    before: [analyze]", 1),
			want: "gates introduce a dependency cycle",
		},
		{
			name: "gate stands in for a missing needs edge",
			rule: "8: a gate cannot replace a dependency",
			yaml: strings.Replace(supervisedYAML,
				"    needs: [analyze]\n    inputs_from:\n      analyze: [analysis.md]\n", "", 1),
			want: "does not depend on observed task",
		},
		{
			name: "gates without supervision",
			rule: "10: a gate with no overseer can never be released",
			yaml: strings.Replace(supervisedYAML,
				"supervision:\n  route:\n    instance: overseerInstance\n    model: overseer-model\n    quota_pool: overseer-pool\n  prompt_file: prompts/overseer.md\n  max_activations: 8\n  max_turns_per_activation: 2\n  activation_deadline: 90m\n  idle_escalation_after: 12h\n  escalation:\n    notify_thread: true\n",
				"", 1),
			want: "gates require supervision",
		},
		{
			name: "activation budget is not positive",
			rule: "11: bounded positive budgets",
			yaml: strings.Replace(supervisedYAML, "max_activations: 8", "max_activations: -1", 1),
			want: "max activations above zero",
		},
		{
			name: "activation budget exceeds its bound",
			rule: "11: bounded positive budgets",
			yaml: strings.Replace(supervisedYAML, "max_activations: 8", "max_activations: 1000", 1),
			want: "at most 100",
		},
		{
			name: "turn budget exceeds its bound",
			rule: "11: bounded positive budgets",
			yaml: strings.Replace(supervisedYAML, "max_turns_per_activation: 2", "max_turns_per_activation: 99", 1),
			want: "max turns per activation above zero and at most 20",
		},
		{
			name: "activation deadline exceeds its bound",
			rule: "11: bounded durations",
			yaml: strings.Replace(supervisedYAML, "activation_deadline: 90m", "activation_deadline: 48h", 1),
			want: "activation deadline above zero and at most 24h0m0s",
		},
		{
			name: "idle escalation exceeds its bound",
			rule: "11: bounded durations",
			yaml: strings.Replace(supervisedYAML, "idle_escalation_after: 12h", "idle_escalation_after: 1000h", 1),
			want: "idle escalation of at most",
		},
		{
			name: "unknown supervision field",
			rule: "strict decoding is the compatibility mechanism",
			yaml: strings.Replace(supervisedYAML, "  prompt_file: prompts/overseer.md", "  prompt_file: prompts/overseer.md\n  retries: 4", 1),
			want: "field retries not found",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseManifest([]byte(test.yaml))
			if err == nil {
				t.Fatalf("rule %s was not enforced", test.rule)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("rule %s: error = %q, want it to contain %q", test.rule, err, test.want)
			}
		})
	}
}

// Rule 9: several gates may protect one task, and all of them must be accepted.
// The manifest layer's part of that is to accept the declaration.
func TestParseManifestAcceptsSeveralGatesOverOneTask(t *testing.T) {
	source := strings.Replace(supervisedYAML,
		"    rubric_file: rubrics/review.md\n",
		"    rubric_file: rubrics/review.md\n  second_review:\n    after: [analyze]\n    before: [apply]\n", 1)
	manifest, err := ParseManifest([]byte(source))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	gates := manifest.GateDefinitions()
	if len(gates) != 2 || gates[0].Name != "review" || gates[1].Name != "second_review" {
		t.Fatalf("gates = %#v", gates)
	}
	for _, gate := range gates {
		if !gate.Protects("apply") {
			t.Fatalf("gate %s does not protect apply", gate.Name)
		}
	}
}

// A final-settlement gate is declared, not simulated with a dummy agent task.
func TestParseManifestAcceptsAFinalSettlementGate(t *testing.T) {
	source := strings.Replace(supervisedYAML,
		"    after: [analyze]\n    before: [apply]\n    rubric_file: rubrics/review.md\n",
		"    after: [apply]\n    final: true\n", 1)
	manifest, err := ParseManifest([]byte(source))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	gates := manifest.GateDefinitions()
	if len(gates) != 1 || !gates[0].Final || len(gates[0].ProtectedTaskIDs) != 0 {
		t.Fatalf("final gate = %#v", gates)
	}
}

// The overseer prompt and every rubric are bundle files, checked by the same
// path policy every other referenced file goes through.
func TestLoadManifestChecksSupervisionFiles(t *testing.T) {
	write := func(t *testing.T, root, relative, content string) {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	bundle := func(t *testing.T, withRubric bool) string {
		t.Helper()
		root := t.TempDir()
		write(t, root, "workflow.yaml", supervisedYAML)
		write(t, root, "prompts/overseer.md", "overseer")
		write(t, root, "prompts/analyze.md", "analyze")
		write(t, root, "prompts/apply.md", "apply")
		if withRubric {
			write(t, root, "rubrics/review.md", "rubric")
		}
		return root
	}

	if _, err := LoadManifest(bundle(t, true)); err != nil {
		t.Fatalf("a complete supervised bundle must load: %v", err)
	}
	_, err := LoadManifest(bundle(t, false))
	if err == nil || !strings.Contains(err.Error(), "rubrics/review.md") {
		t.Fatalf("a missing rubric must be refused by name: %v", err)
	}
}
