package domain

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func validProjectContext() ProjectContext {
	now := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	fresh := now.Add(time.Hour)
	return ProjectContext{
		Version: ProjectContextVersion, Revision: "context-7", Status: ProjectContextAccepted,
		Objective: "complete the cold-start fixture", Authority: []string{"coordinator:run-1"},
		Budget: "one bounded attempt", Outputs: []string{"result.txt"},
		RequiredReferences: []string{"git-source"},
		References: []ProjectContextReference{{
			ID: "git-source", Kind: ContextReferenceGit, URI: "https://example.invalid/repo",
			Revision: strings.Repeat("a", 40), Status: ProjectContextPinned, Authority: "coordinator",
			Topics: []string{"cold", "source"},
			Binding: &ProjectContextArtifactBinding{
				ArtifactID: "artifact-input", Path: "inputs/context.md", SHA256: strings.Repeat("b", 64),
			},
		}},
		Decisions: []ProjectContextDecision{{
			ID: "decision-1", Status: "accepted", Summary: "reuse the accepted branch",
			Authority: "review-gate", Topics: []string{"cold", "resume"},
		}},
		CheckpointDelta: []string{"producer accepted"}, CodeLocations: []ProjectContextLocation{{
			Path: "internal/domain/project_context.go", Revision: strings.Repeat("a", 40), Topics: []string{"cold", "code"},
		}},
		InputLocations: []ProjectContextLocation{{
			Path: ".t3/inputs/context.md", Revision: strings.Repeat("b", 64), Topics: []string{"cold", "input"},
		}},
		Setup: []string{"go version", "go mod download"}, Checks: []string{"go test ./..."},
		CapabilityRefs: []string{"git-source"}, Freshness: ProjectContextFreshness{ObservedAt: now, FreshThrough: &fresh},
	}
}

func TestProjectContextLookupIsBoundedDeterministicAndSupplementary(t *testing.T) {
	index := validProjectContext()
	first, err := LookupProjectContext(index, "cold", 2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LookupProjectContext(index, "cold", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || !reflect.DeepEqual(first, second) {
		t.Fatalf("lookup is not stable and bounded: %#v %#v", first, second)
	}
	if len(index.RequiredReferences) != 1 || index.RequiredReferences[0] != "git-source" {
		t.Fatal("lookup changed explicit required selection")
	}
}

func TestProjectContextRejectsMissingAuthorityUnacceptedDecisionAndStaleShape(t *testing.T) {
	for name, mutate := range map[string]func(*ProjectContext){
		"missing-authority":   func(c *ProjectContext) { c.Authority = nil },
		"unaccepted-decision": func(c *ProjectContext) { c.Decisions[0].Status = "proposed" },
		"missing-required":    func(c *ProjectContext) { c.RequiredReferences[0] = "absent" },
		"unsupported-version": func(c *ProjectContext) { c.Version++ },
	} {
		t.Run(name, func(t *testing.T) {
			index := validProjectContext()
			mutate(&index)
			if err := ValidateProjectContext(&index); err == nil {
				t.Fatal("invalid context accepted")
			}
		})
	}
}
