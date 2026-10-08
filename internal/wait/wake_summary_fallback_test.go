package wait

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// fallbackRow builds the summary of one succeeded task of a run and returns
// its row.
func fallbackRow(t *testing.T, contents map[string]string, task SummaryTask) WakeSummaryTask {
	t.Helper()
	s := &fakeSummarySource{contents: contents, openErr: map[string]error{}}
	task.Attempt, task.Progress = "attempt", domain.ProgressSucceeded
	s.run = SummaryRun{ID: fixtureRun, Workflow: "fallback", Tasks: []SummaryTask{task}}
	result := BuildNodeSummary(context.Background(), s, fixtureSinkWait())
	if len(result.Tasks) != 1 {
		t.Fatalf("summary rows = %+v", result)
	}
	return result.Tasks[0]
}

// A declared review.md that was not retained must not stop the verdict
// source list: the retained verdict.json still answers.
func TestNodeSummaryVerdictFallsBackPastAMissingReview(t *testing.T) {
	row := fallbackRow(t, map[string]string{"verdict": `{"verdict":"changes-requested"}`}, SummaryTask{
		ID: "review", Name: "review",
		Outputs:   []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "verdict.json"}},
		Artifacts: []SummaryArtifact{{ID: "verdict", Name: "verdict.json", Kind: domain.ArtifactOutput, Size: 30}},
	})
	if row.Verdict != "CHANGES_REQUESTED" || row.VerdictSource != "verdict.json" {
		t.Fatalf("retained verdict.json must answer when review.md is missing: %+v", row)
	}
}

// A retained review.md that cannot be opened falls through the same way.
func TestNodeSummaryVerdictFallsBackPastAnUnreadableReview(t *testing.T) {
	s := &fakeSummarySource{
		contents: map[string]string{"verdict": `{"verdict":"accept"}`},
		openErr:  map[string]error{"review": &SummaryError{Category: "unreachable"}},
	}
	s.run = SummaryRun{ID: fixtureRun, Workflow: "fallback", Tasks: []SummaryTask{{
		ID: "review", Name: "review", Attempt: "attempt", Progress: domain.ProgressSucceeded,
		Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "verdict.json"}},
		Artifacts: []SummaryArtifact{
			{ID: "review", Name: "review.md", Kind: domain.ArtifactOutput, Size: 30},
			{ID: "verdict", Name: "verdict.json", Kind: domain.ArtifactOutput, Size: 30},
		},
	}}}
	row := BuildNodeSummary(context.Background(), s, fixtureSinkWait()).Tasks[0]
	if row.Verdict != "ACCEPT" || row.VerdictSource != "verdict.json" {
		t.Fatalf("retained verdict.json must answer when review.md is unreadable: %+v", row)
	}
}

// When no source in the list answers, the cell stays unknown and names the
// first source that should have answered.
func TestNodeSummaryVerdictStaysUnknownWhenNoSourceAnswers(t *testing.T) {
	row := fallbackRow(t, map[string]string{}, SummaryTask{
		ID: "review", Name: "review",
		Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "verdict.json"}},
	})
	if row.Verdict != "review output missing" || row.VerdictStatus != "missing" || row.VerdictSource != "review.md" {
		t.Fatalf("no retained verdict source must report missing: %+v", row)
	}
}

// A retained review.md whose first line is not a verdict has answered: the
// answer is unrecognized, and verdict.json is not consulted.
func TestNodeSummaryUnrecognizedReviewStillAnswers(t *testing.T) {
	row := fallbackRow(t, map[string]string{"review": "no verdict here\n", "verdict": `{"verdict":"accept"}`}, SummaryTask{
		ID: "review", Name: "review",
		Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "verdict.json"}},
		Artifacts: []SummaryArtifact{
			{ID: "review", Name: "review.md", Kind: domain.ArtifactOutput, Size: 16},
			{ID: "verdict", Name: "verdict.json", Kind: domain.ArtifactOutput, Size: 20},
		},
	})
	if row.Verdict != verdictUnrecognized || row.VerdictSource != "review.md" {
		t.Fatalf("a readable review.md decides the verdict: %+v", row)
	}
}

// A declared commit whose record is not retained must not stop the head
// source list: the retained bundle still answers.
func TestNodeSummaryHeadFallsBackPastAMissingCommit(t *testing.T) {
	sha := "af7528678d82175d108e11f9aab391a7e74032e6"
	row := fallbackRow(t, map[string]string{"bundle": fixtureBundle(sha)}, SummaryTask{
		ID: "implementation", Name: "implementation",
		Outputs:   []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}, {Name: "unit.bundle"}},
		Artifacts: []SummaryArtifact{{ID: "bundle", Name: "unit.bundle", Kind: domain.ArtifactOutput, Size: 200}},
	})
	if row.Head != sha || row.HeadSource != "unit.bundle" {
		t.Fatalf("retained bundle must answer when the commit record is missing: %+v", row)
	}
}

// A retained commit record that does not parse falls through the same way.
func TestNodeSummaryHeadFallsBackPastAMalformedCommit(t *testing.T) {
	sha := "af7528678d82175d108e11f9aab391a7e74032e6"
	row := fallbackRow(t, map[string]string{"commit": `{"version":"campaign-commit/v1","commit":"zz"}`, "bundle": fixtureBundle(sha)}, SummaryTask{
		ID: "implementation", Name: "implementation",
		Outputs: []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}, {Name: "unit.bundle"}},
		Artifacts: []SummaryArtifact{
			{ID: "commit", Name: "implementation", Kind: domain.ArtifactOutput, Size: 50},
			{ID: "bundle", Name: "unit.bundle", Kind: domain.ArtifactOutput, Size: 200},
		},
	})
	if row.Head != sha || row.HeadSource != "unit.bundle" {
		t.Fatalf("retained bundle must answer when the commit record is malformed: %+v", row)
	}
}

// When neither the commit nor a bundle answers, the head stays unknown and
// names the declared commit.
func TestNodeSummaryHeadStaysUnknownWhenNoSourceAnswers(t *testing.T) {
	row := fallbackRow(t, map[string]string{}, SummaryTask{
		ID: "implementation", Name: "implementation",
		Outputs: []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}, {Name: "unit.bundle"}},
	})
	if row.Head != "head output missing" || row.HeadStatus != "missing" || row.HeadSource != "commit implementation" {
		t.Fatalf("no retained head source must report missing: %+v", row)
	}
}
