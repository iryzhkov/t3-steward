package campaign

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// The campaign shape the declared-commit feature was built for: one task
// produces a commit, the next consumes it by name.
const commitsYAML = headerYAML + `tasks:
  implement:
    prompt_file: prompts/implement.md
    outputs: [notes.md]
    commits:
      - name: implementation
        revision: HEAD
      - name: fixture
  review:
    prompt_file: prompts/review.md
    needs: [implement]
    inputs_from:
      implement: [implementation, notes.md]
`

// A task that declares commits: says so in the plan. Before this the projection
// read Outputs only, so the one field that decides whether a handoff works at
// all was invisible in the surface an author uses to check their manifest.
func TestProjectReportsDeclaredCommits(t *testing.T) {
	plan := project(t, commitsYAML)
	implement := taskByName(t, plan, "implement")
	want := []Commit{{Name: "implementation", Revision: "HEAD"}, {Name: "fixture"}}
	if !reflect.DeepEqual(implement.Commits, want) {
		t.Fatalf("commits = %#v, want %#v", implement.Commits, want)
	}
	if !reflect.DeepEqual(implement.Outputs, []string{"notes.md"}) {
		t.Fatalf("a declared commit was folded into outputs: %v", implement.Outputs)
	}
	if review := taskByName(t, plan, "review"); len(review.Commits) != 0 {
		t.Fatalf("the consumer declares commits: %#v", review.Commits)
	}
	if plan.Totals.Commits != 2 || plan.Totals.Outputs != 1 {
		t.Fatalf("totals = %#v", plan.Totals)
	}
}

func TestRenderTextReportsDeclaredCommits(t *testing.T) {
	text := RenderText(project(t, commitsYAML))
	for _, want := range []string{
		"implementation: Git commit at HEAD, retained as its provenance record",
		"fixture: Git commit at HEAD (default), retained as its provenance record",
		"2 declared commits",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("plan text does not report %q:\n%s", want, text)
		}
	}
}

func TestRenderJSONReportsDeclaredCommits(t *testing.T) {
	raw, err := RenderJSON(project(t, commitsYAML))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Totals struct {
			Commits int `json:"commits"`
		} `json:"totals"`
		Tasks []struct {
			Name    string   `json:"name"`
			Commits []Commit `json:"commits"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("invalid plan document: %v", err)
	}
	if document.Totals.Commits != 2 {
		t.Fatalf("totals.commits = %d", document.Totals.Commits)
	}
	for _, task := range document.Tasks {
		if task.Name != "implement" {
			continue
		}
		want := []Commit{{Name: "implementation", Revision: "HEAD"}, {Name: "fixture"}}
		if !reflect.DeepEqual(task.Commits, want) {
			t.Fatalf("commits = %#v, want %#v", task.Commits, want)
		}
		return
	}
	t.Fatalf("the producing task is missing from the document: %s", raw)
}

// In DOT an artifact is visible on the edge it crosses, so a declared commit is
// visible there too, and marked: what travels is a commit reference and not a
// file the producer wrote.
func TestRenderDOTMarksACarriedCommit(t *testing.T) {
	dot := RenderDOT(project(t, commitsYAML))
	if !strings.Contains(dot, `label="implementation (commit), notes.md"`) {
		t.Fatalf("DOT does not mark the carried commit:\n%s", dot)
	}
}

// The field is documented where an author looks: in the graph-field summary and
// in a topic of its own, because a declared commit is the one output whose
// contract cannot be guessed from its name.
func TestCommitsAreDocumented(t *testing.T) {
	if !strings.Contains(DAGSemanticsHelp, "\ncommits\n") {
		t.Fatal("the graph-field help has no commits section")
	}
	var topic string
	for _, candidate := range HelpTopics() {
		if candidate.Name == "commits" {
			topic = candidate.Body
		}
	}
	if topic == "" {
		t.Fatal("there is no commits help topic")
	}
	for _, want := range []string{
		"commits:", "inputs_from", "provenance record",
		".t3/dependencies/<producer>/<name>",
		"refs/campaigns/<run>/<task>/<name>",
		"--prune", "A rerun pins its source run against retention",
	} {
		if !strings.Contains(topic, want) {
			t.Fatalf("the commits topic does not cover %q", want)
		}
	}
}
