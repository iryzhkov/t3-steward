package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"reflect"
	"strings"
	"testing"
)

func TestIndependentM16StrictYAML(t *testing.T) {
	cases := map[string]string{
		"v1-merged-null":    strings.Replace(declaredManifestYAML(), "required: true}", "required: true, <<: {execution: null}}", 1),
		"v1-merged-profile": strings.Replace(declaredManifestYAML(), "required: true}", "required: true, <<: {execution: {effort: medium}}}", 1),
		"v2-merged-unknown": strings.ReplaceAll(executionManifestYAML(), "effort: medium", "effort: medium, <<: {options: {unsafe: true}}"),
		"duplicate-effort":  strings.ReplaceAll(executionManifestYAML(), "effort: medium", "effort: high, effort: medium"),
		"fractional-turn":   strings.ReplaceAll(executionManifestYAML(), "max_turns: 9", "max_turns: 9.5"),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseManifest([]byte(raw)); err == nil {
				t.Fatal("malformed or forbidden execution declaration accepted")
			}
		})
	}
}
func TestIndependentM16SharedProfileAndGrantUnion(t *testing.T) {
	m, err := ParseManifest([]byte(executionManifestYAML()))
	if err != nil {
		t.Fatal(err)
	}
	r := m.Tasks["inspect"].ReviewRequirements
	r.Members[1].Execution = r.Members[0].Execution
	c := compileTaskReview(r, "w", "r", "t", domain.Artifact{})
	original := *c.Members[1].Execution
	c.Members[0].Execution.Resources.MemoryMB++
	if *c.Members[1].Execution != original {
		t.Fatal("compiler retains shared profile")
	}
	detached := cloneManifestReview(r)
	*detached.Members[0].Execution.Resources.MemoryMB++
	if *r.Members[0].Execution.Resources.MemoryMB != 512 || *detached.Members[1].Execution.Resources.MemoryMB != 512 {
		t.Fatal("manifest clone retains numeric alias")
	}
	f := executionFixture(t)
	w := &f.catalog.catalog.AuthoredWorkers[0]
	w.Providers[0].QuotaPoolID = "wrong"
	// Pool exists on project+wrong-instance; exact instance exists on wrong-model.
	f.catalog.catalog.AuthoredWorkers = append(f.catalog.catalog.AuthoredWorkers,
		domain.WorkerInventory{ID: "union-one", Projects: []domain.WorkerProjectInventory{{Name: "t3-steward"}}, Providers: []domain.WorkerProviderInventory{{InstanceID: "wrong", Models: []string{"org/sol"}, QuotaPoolID: "review-pool"}}},
		domain.WorkerInventory{ID: "union-two", Projects: []domain.WorkerProjectInventory{{Name: "t3-steward"}}, Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Models: []string{"wrong"}, QuotaPoolID: "review-pool"}}})
	before, err := f.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.FreezeDeclared(context.Background(), declaredRequest(f)); err == nil {
		t.Fatal("union of inexact grants accepted")
	}
	after, err := f.store.LoadCoordinatorRecords(context.Background())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rejection mutated records", err)
	}
}

func TestIndependentM16FractionalIntegerWitness(t *testing.T) {
	for _, x := range []struct{ old, value string }{
		{"max_turns: 9", "max_turns: 32.9"},
		{"memory_mb: 512", "memory_mb: 512.75"},
		{"scratch_mb: 64", "scratch_mb: 64.75"},
	} {
		t.Run(x.value, func(t *testing.T) {
			m, err := ParseManifest([]byte(strings.ReplaceAll(executionManifestYAML(), x.old, x.value)))
			if err != nil {
				return
			}
			c := compileTaskReview(m.Tasks["inspect"].ReviewRequirements, "w", "r", "t", domain.Artifact{})
			t.Fatalf("fractional integer accepted and compiled: turns=%d memory=%d scratch=%d", c.Members[0].Execution.MaxTurns, c.Members[0].Execution.Resources.MemoryMB, c.Members[0].Execution.Resources.ScratchMB)
		})
	}
}

func TestIndependentM16FractionalAdmissionRejected(t *testing.T) {
	f := executionFixture(t)
	rewriteBundleManifest(t, f.source, strings.ReplaceAll(executionManifestYAML(), "max_turns: 9", "max_turns: 32.9"))
	result, err := (BundleIngester{Store: f.store, StorageRoot: f.service.Artifacts.SubmissionRoot, Permanent: declarationValidator{f.catalog}}).Ingest(context.Background(), f.source)
	if err != nil {
		return
	}
	t.Fatalf("malformed profile admitted and stored with turns=%d", result.Records.Tasks[0].ReviewRequirements.Members[0].Execution.MaxTurns)
}
