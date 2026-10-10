package backlog

import (
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const fixLoopManifest = `
version: 2
name: bounded-repair
environment:
  project: t3-steward
fix_loops:
  repair:
    implement: implement
    review: review
    max_rounds: 4
tasks:
  implement:
    prompt_file: implement.md
    commits: [{name: implementation}]
  review:
    prompt_file: review.md
    needs: [implement]
    outputs: [review.md]
    review_output:
      verdict_line: review.md
  publish:
    prompt_file: publish.md
    needs: [review]
    needs_verdict:
      review: accept
`

func TestManifestFixLoopExpansion(t *testing.T) {
	manifest := mustParseManifest(t, fixLoopManifest)
	if len(manifest.Tasks) != 9 {
		t.Fatalf("tasks = %d, want 9", len(manifest.Tasks))
	}
	for round := 1; round <= 4; round++ {
		implementation := fixLoopRoundName("implement", round)
		review := fixLoopRoundName("review", round)
		task := manifest.Tasks[implementation]
		reviewer := manifest.Tasks[review]
		if task.FixLoop.Name != "repair" || task.FixLoop.Round != round || task.FixLoop.MaxRounds != 4 || task.FixLoop.Kind != "implement" {
			t.Fatalf("implementation metadata: %#v", task.FixLoop)
		}
		if !reflect.DeepEqual(reviewer.InputsFrom[implementation], []string{"implementation"}) {
			t.Fatalf("round %d review input = %#v", round, reviewer.InputsFrom)
		}
		if round > 1 {
			previousReview := fixLoopRoundName("review", round-1)
			previousImplementation := fixLoopRoundName("implement", round-1)
			if task.NeedsVerdict[previousReview] != "changes-requested" {
				t.Fatalf("missing round %d condition", round)
			}
			if !reflect.DeepEqual(task.InputsFrom[previousReview], []string{"review.md"}) || !reflect.DeepEqual(task.InputsFrom[previousImplementation], []string{"implementation"}) {
				t.Fatalf("round %d repair inputs = %#v", round, task.InputsFrom)
			}
		}
	}
	// Mutating a round must not change its siblings' condition/input declarations.
	task := manifest.Tasks["implement-round-2"]
	task.InputsFrom["review"][0] = "changed.md"
	if manifest.Tasks["review"].Outputs[0] != "review.md" {
		t.Fatal("round input aliases review outputs")
	}
	task.NeedsVerdict["review"] = "accept"
	if manifest.Tasks["implement-round-3"].NeedsVerdict["review"] != "" {
		t.Fatal("round conditions share a map")
	}
}

func TestManifestFixLoopRoundTrip(t *testing.T) {
	original := mustParseManifest(t, fixLoopManifest)
	raw, err := yaml.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	serialized, err := yaml.Marshal(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if string(serialized) != string(raw) || len(parsed.Tasks) != len(original.Tasks) {
		t.Fatal("round trip changed graph or authored form")
	}
	for name, before := range original.Tasks {
		after := parsed.Tasks[name]
		if !reflect.DeepEqual(before.FixLoop, after.FixLoop) || !reflect.DeepEqual(before.NeedsVerdict, after.NeedsVerdict) || !reflect.DeepEqual(before.InputsFrom, after.InputsFrom) && len(before.InputsFrom) > 0 {
			t.Fatalf("round trip changed task %s", name)
		}
	}
}

func TestManifestFixLoopRefusals(t *testing.T) {
	cases := map[string]struct{ old, new, want string }{
		"unbounded":                 {"max_rounds: 4", "max_rounds: 0", "max_rounds"},
		"excessive":                 {"max_rounds: 4", "max_rounds: 21", "max_rounds"},
		"missing implementation":    {"implement: implement", "implement: absent", "implement task"},
		"same template":             {"review: review\n    max_rounds", "review: implement\n    max_rounds", "distinct"},
		"no commit":                 {"commits: [{name: implementation}]", "outputs: [result.md]", "declare a commit"},
		"no verdict":                {"review_output:\n      verdict_line: review.md", "", "review_output"},
		"shared workspace":          {"project: t3-steward", "project: t3-steward\n  scope: workflow", "fresh"},
		"collision":                 {"  publish:", "  implement-round-2:\n    prompt_file: extra.md\n  publish:", "collides"},
		"missing review dependency": {"needs: [implement]", "needs: []", "review must need implement"},
		"cycle":                     {"prompt_file: implement.md", "prompt_file: implement.md\n    needs: [review]", "cannot depend"},
		"invalid loop name":         {"  repair:", "  BAD:", "invalid loop name"},
		"duplicate template":        {"tasks:\n", "  another:\n    implement: implement\n    review: review\n    max_rounds: 2\ntasks:\n", "multiple loops"},
		"unknown field":             {"max_rounds: 4", "max_rounds: 4\n    forever: true", "forever"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseManifest([]byte(strings.Replace(fixLoopManifest, c.old, c.new, 1)))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error = %v, want %s", err, c.want)
			}
		})
	}
}

func TestManifestVerdictDependencyRefusals(t *testing.T) {
	cases := []struct{ old, new, want string }{
		{"review: accept", "review: failed", "accept or changes-requested"},
		{"review: accept", "implement: accept", "not a direct dependency"},
		{"needs: [review]", "needs: [implement]", "not a direct dependency"},
	}
	for _, c := range cases {
		_, err := ParseManifest([]byte(strings.Replace(fixLoopManifest, c.old, c.new, 1)))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("error = %v, want %s", err, c.want)
		}
	}
}
