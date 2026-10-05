package backlog

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestIndependentRepair1DecodedKeyRefusal(t *testing.T) {
	binary := func(s string) string { return "!!binary " + base64.StdEncoding.EncodeToString([]byte(s)) }
	cases := []struct{ name, raw string }{
		{"turn-key", strings.ReplaceAll(executionManifestYAML(), "max_turns: 9", binary("max_turns")+": 32.9")},
		{"memory-key", strings.ReplaceAll(executionManifestYAML(), "memory_mb: 512", binary("memory_mb")+": 512.75")},
		{"scratch-key", strings.ReplaceAll(executionManifestYAML(), "scratch_mb: 64", binary("scratch_mb")+": 64.75")},
		{"execution-key", strings.ReplaceAll(strings.ReplaceAll(executionManifestYAML(), "execution:", binary("execution")+":"), "max_turns: 9", "max_turns: 32.9")},
		{"resources-key", strings.ReplaceAll(strings.ReplaceAll(executionManifestYAML(), "resources:", binary("resources")+":"), "memory_mb: 512", "memory_mb: 512.75")},
		{"legacy-null-key", strings.Replace(declaredManifestYAML(), "required: true}", "required: true, "+binary("execution")+": null}", 1)},
		{"merged-turn-key", strings.ReplaceAll(executionManifestYAML(), "max_turns: 9", "<<: {"+binary("max_turns")+": 32.9}")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, err := ParseManifest([]byte(c.raw))
			if err == nil {
				compiled := compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{})
				b, _ := json.Marshal(compiled.Members[0])
				t.Errorf("unsafe decoded key accepted: %s", b)
			} else {
				t.Logf("parser refusal: %v", err)
			}
			f := executionFixture(t)
			db := stagingSQL(t, f)
			before := independentDeclaredTables(t, db)
			rewriteBundleManifest(t, f.source, c.raw)
			result, err := (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source)
			after := independentDeclaredTables(t, db)
			if err == nil {
				b, _ := json.Marshal(result.Records.Tasks[0].ReviewRequirements.Members[0])
				t.Errorf("permanent admission accepted: %s", b)
			}
			if before != after {
				t.Error("admission changed logical SQL/native audit")
			}
		})
	}
}

func TestIndependentRepair1NestedPrecedenceLegacy(t *testing.T) {
	profile := "effort: medium, quota_pool: review-pool, max_turns: 9, resources: {preset: build, cpu_units: 1.5, memory_mb: 512, scratch_mb: 64}"
	for _, body := range []string{
		"<<: [{<<: {" + profile + "}}, {max_turns: 32.9}]",
		"<<: [{max_turns: 32.9}, {" + profile + "}], max_turns: 9",
		strings.Replace(profile, "resources: {", "resources: {<<: [{memory_mb: 512.75}, {scratch_mb: 64.75}], ", 1),
	} {
		raw := strings.ReplaceAll(executionManifestYAML(), profile, body)
		m, err := ParseManifest([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		p := compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{}).Members[0].Execution
		if p.MaxTurns != 9 || p.Resources.MemoryMB != 512 || p.Resources.ScratchMB != 64 || p.Resources.CPUUnits != 1.5 {
			t.Fatal("precedence changed", p)
		}
	}
	original, err := ParseManifest([]byte(declaredManifestYAML()))
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(compileTaskReview(original.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{}))
	raw := strings.ReplaceAll(declaredManifestYAML(), "required: true", "<<: [{<<: {required: true}}, {required: false}]")
	m, err := ParseManifest([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{}))
	if string(before) != string(after) {
		t.Fatal("nested legacy canonical bytes changed")
	}
	t.Log("nested merge and direct overrides preserve exact profiles and legacy canonical bytes")
}
