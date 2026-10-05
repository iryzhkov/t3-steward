package backlog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"gopkg.in/yaml.v3"
)

func TestReviewExecutionProfileParserIntegerForms(t *testing.T) {
	profile := "effort: medium, quota_pool: review-pool, max_turns: 9, resources: {preset: build, cpu_units: 1.5, memory_mb: 512, scratch_mb: 64}"
	forms := map[string]func(string) string{
		"direct":             func(p string) string { return "execution: {" + p + "}" },
		"execution-merge":    func(p string) string { return "execution: {<<: {" + p + "}}" },
		"execution-sequence": func(p string) string { return "execution: {<<: [{" + p + "}]}" },
		"member-merge":       func(p string) string { return "<<: {execution: {" + p + "}}" },
		"member-sequence":    func(p string) string { return "<<: [{execution: {" + p + "}}]" },
	}
	for field, original := range map[string]string{"max_turns": "9", "memory_mb": "512", "scratch_mb": "64"} {
		for _, bad := range []string{"32.9", "9.0", "!!float 9", "'9'", "true", "null", "[9]", "{value: 9}", "9223372036854775808", strings.Repeat("9", 150)} {
			for form, wrap := range forms {
				t.Run(field+"/"+bad+"/"+form, func(t *testing.T) {
					changed := strings.Replace(profile, field+": "+original, field+": "+bad, 1)
					raw := strings.ReplaceAll(executionManifestYAML(), "execution: {"+profile+"}", wrap(changed))
					if raw == executionManifestYAML() {
						t.Fatal("fixture unchanged")
					}
					if _, err := ParseManifest([]byte(raw)); err == nil {
						t.Fatal("unsafe authored integer accepted")
					}
				})
			}
		}
	}
	for form, wrap := range forms {
		t.Run("valid/"+form, func(t *testing.T) {
			m, err := ParseManifest([]byte(strings.ReplaceAll(executionManifestYAML(), "execution: {"+profile+"}", wrap(profile))))
			if err != nil {
				t.Fatal(err)
			}
			c := compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{})
			if p := c.Members[0].Execution; p.MaxTurns != 9 || p.Resources.MemoryMB != 512 || p.Resources.ScratchMB != 64 {
				t.Fatal("merged profile changed", p)
			}
		})
	}
	for name, body := range map[string]string{
		"minimum":           strings.Replace(profile, "max_turns: 9", "max_turns: 1", 1),
		"direct-boundary":   strings.Replace(profile, "max_turns: 9", "max_turns: 32", 1),
		"hex":               strings.Replace(profile, "max_turns: 9", "max_turns: 0x20", 1),
		"integer-aliases":   strings.Replace(strings.Replace(profile, "max_turns: 9", "max_turns: &turn 9", 1), "memory_mb: 512, scratch_mb: 64", "memory_mb: *turn, scratch_mb: *turn", 1),
		"direct-over-merge": "<<: {max_turns: 32.9}, " + profile,
		"sequence-first":    "<<: [{" + profile + "}, {max_turns: 32.9}]",
		"anchored-merge":    "<<: [&profile {" + profile + "}, *profile]",
		"resource-merge":    strings.Replace(profile, "preset: build, cpu_units: 1.5, memory_mb: 512, scratch_mb: 64", "<<: [{preset: build, cpu_units: 1.5, memory_mb: 512, scratch_mb: 64}, {memory_mb: 512.75}]", 1),
		"resource-direct":   strings.Replace(profile, "preset: build", "<<: {memory_mb: 512.75}, preset: build", 1),
	} {
		t.Run(name, func(t *testing.T) {
			raw := strings.ReplaceAll(executionManifestYAML(), profile, body)
			m, err := ParseManifest([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			c := compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{})
			expected := 9
			if name == "minimum" {
				expected = 1
			}
			if name == "direct-boundary" || name == "hex" {
				expected = 32
			}
			if c.Members[0].Execution.MaxTurns != expected || c.Members[0].Execution.Resources.CPUUnits != 1.5 {
				t.Fatal("effective value changed", c)
			}
		})
	}
	for name, body := range map[string]string{
		"sequence-first-invalid":    "<<: [{max_turns: 32.9}, {" + profile + "}]",
		"alias-fractional":          strings.Replace(strings.Replace(profile, "max_turns: 9", "max_turns: &turn 9.5", 1), "memory_mb: 512", "memory_mb: *turn", 1),
		"resource-alias-fractional": strings.Replace(profile, "memory_mb: 512, scratch_mb: 64", "memory_mb: &memory 512.75, scratch_mb: *memory", 1),
		"merged-unknown":            "<<: {options: unsafe}, " + profile,
		"duplicate-turns":           "max_turns: 32, " + profile,
		"duplicate-resource":        strings.Replace(profile, "memory_mb: 512", "memory_mb: 512, memory_mb: 64", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseManifest([]byte(strings.ReplaceAll(executionManifestYAML(), profile, body))); err == nil {
				t.Fatal("unsafe profile accepted")
			}
		})
	}
}

func TestReviewExecutionProfileParserLegacyMergeBytes(t *testing.T) {
	original, err := ParseManifest([]byte(declaredManifestYAML()))
	if err != nil {
		t.Fatal(err)
	}
	golden := compileTaskReview(original.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{})
	before, err := json.Marshal(golden)
	if err != nil {
		t.Fatal(err)
	}
	for _, fields := range []string{"<<: {required: true}", "<<: [{required: true}]", "<<: [&base {required: true}, *base]"} {
		raw := strings.ReplaceAll(declaredManifestYAML(), "required: true", fields)
		m, err := ParseManifest([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		after, err := json.Marshal(compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{}))
		if err != nil || string(before) != string(after) {
			t.Fatal("legacy canonical declaration changed", err)
		}
	}
	for _, fields := range []string{"<<: {execution: null}", "<<: [{required: true}, {execution: null}]", "<<: [&base {execution: null}, *base]", "execution: null, <<: {required: true}"} {
		raw := strings.ReplaceAll(declaredManifestYAML(), "required: true}", "required: true, "+fields+"}")
		if _, err := ParseManifest([]byte(raw)); err == nil {
			t.Fatal("legacy effective execution accepted", fields)
		}
	}
	// Unrelated legacy scalar conversion remains yaml.v3's historical behavior.
	raw := strings.ReplaceAll(declaredManifestYAML(), "role: independent", "role: 42")
	if _, err := ParseManifest([]byte(raw)); err != nil {
		t.Fatal("unrelated legacy scalar tightened", err)
	}
}

func TestReviewExecutionProfileParserNodeBounds(t *testing.T) {
	scalar := func(value, tag string) *yaml.Node { return &yaml.Node{Kind: yaml.ScalarNode, Value: value, Tag: tag} }
	mapping := func(key string, value *yaml.Node) *yaml.Node {
		return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{scalar(key, "!!str"), value}}
	}
	cycle := mapping("execution", nil)
	cycle.Content[1] = &yaml.Node{Kind: yaml.AliasNode, Alias: cycle}
	deep := mapping("required", scalar("true", "!!bool"))
	for i := 0; i < 70; i++ {
		deep = &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{scalar("<<", "!!merge"), deep}}
	}
	wide := &yaml.Node{Kind: yaml.SequenceNode}
	for i := 0; i < 4100; i++ {
		wide.Content = append(wide.Content, scalar("x", "!!str"))
	}
	for name, node := range map[string]*yaml.Node{"cycle": cycle, "depth": deep, "oversize": wide, "nil": nil, "odd-mapping": {Kind: yaml.MappingNode, Content: []*yaml.Node{scalar("execution", "!!str")}}, "shape": scalar("bad", "!!str"), "nil-alias": {Kind: yaml.AliasNode}} {
		t.Run(name, func(t *testing.T) {
			if _, err := reviewExecutionNodePresence(node); err == nil {
				t.Fatal("unsafe node accepted")
			}
		})
	}
	for _, raw := range []string{
		"{id: one, role: independent, route: codex/org/sol, required: true, <<: &loop {<<: *loop}}",
		"{id: one, role: independent, route: codex/org/sol, required: true, <<: [null]}",
		"{id: one, role: independent, route: codex/org/sol, required: true, <<: {unknown: true}}",
		"{id: one, id: two, role: independent, route: codex/org/sol, required: true}",
	} {
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(raw), &node); err != nil {
			t.Fatal(err)
		}
		var member ManifestReviewMember
		if err := member.UnmarshalYAML(node.Content[0]); err == nil {
			t.Fatal("unsafe member decoded", raw)
		}
	}
}

func TestReviewExecutionProfileParserPermanentAdmissionNoMutation(t *testing.T) {
	f := executionFixture(t)
	db := stagingSQL(t, f)
	for _, change := range []struct{ old, new string }{
		{"max_turns: 9", "max_turns: 32.9"},
		{"memory_mb: 512", "memory_mb: 512.75"},
		{"scratch_mb: 64", "scratch_mb: 64.75"},
		{"max_turns: 9", "<<: {max_turns: 32.9}"},
		{"memory_mb: 512", "<<: [{memory_mb: 512.75}]"},
		{"scratch_mb: 64", "<<: {scratch_mb: 64.75}"},
	} {
		before := independentDeclaredTables(t, db)
		rewriteBundleManifest(t, f.source, strings.ReplaceAll(executionManifestYAML(), change.old, change.new))
		_, err := (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source)
		if err == nil {
			t.Fatal("fractional profile admitted", change)
		}
		if before != independentDeclaredTables(t, db) {
			t.Fatal("refusal mutated any logical SQL/native audit", change)
		}
	}
	for _, fields := range []string{"<<: {execution: null}", "<<: [{execution: null}]", "<<: [&base {execution: null}, *base]"} {
		before := independentDeclaredTables(t, db)
		rewriteBundleManifest(t, f.source, strings.ReplaceAll(declaredManifestYAML(), "required: true}", "required: true, "+fields+"}"))
		_, err := (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source)
		if err == nil || before != independentDeclaredTables(t, db) {
			t.Fatal("legacy merged-null refusal changed SQL/native audit", fields, err)
		}
	}
}
