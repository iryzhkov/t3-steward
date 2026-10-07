package wait

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"io"
	"strings"
	"testing"
	"time"
)

func TestNodeSummaryPrefersTheStructuredVerdict(t *testing.T) {
	source := &fakeSummarySource{run: SummaryRun{ID: fixtureRun, Tasks: []SummaryTask{{
		ID: "review", Name: "review", Attempt: "attempt", Progress: domain.ProgressSucceeded,
		ReviewVerdict: &domain.ReviewVerdict{Verdict: "changes-requested"},
	}}}}
	s := BuildNodeSummary(context.Background(), source, fixtureSinkWait())
	row := s.Tasks[0]
	if row.Verdict != "CHANGES_REQUESTED" || row.VerdictStatus != "known" || row.VerdictSource != "review_output" || source.opens != 0 {
		t.Fatalf("structured verdict must answer without opening artifacts: %+v opens=%d", row, source.opens)
	}
}

func TestNodeSummaryReadsTheDeclaredReviewOutput(t *testing.T) {
	for _, tc := range []struct {
		output domain.ReviewOutput
		body   string
	}{
		{domain.ReviewOutput{VerdictLine: "notes/r.md"}, "VERDICT: ACCEPT\n"},
		{domain.ReviewOutput{Verdict: "notes/r.md"}, `{"verdict":"accept"}`},
	} {
		row := fallbackRow(t, map[string]string{"review": tc.body}, SummaryTask{
			Name: "review", ReviewOutput: &tc.output,
			Artifacts: []SummaryArtifact{{ID: "review", Name: "notes/r.md", Kind: domain.ArtifactOutput, Size: int64(len(tc.body))}},
		})
		if row.Verdict != "ACCEPT" || row.VerdictStatus != "known" || row.VerdictSource != "notes/r.md" {
			t.Fatalf("declared review output was not used: %+v", row)
		}
	}
}

type stuckArtifactSource struct {
	*fakeSummarySource
	release chan struct{}
}

func (s *stuckArtifactSource) OpenSummaryArtifact(ctx context.Context, id string) (io.ReadCloser, error) {
	if id == "bundle" {
		<-s.release
		return nil, errors.New("released")
	}
	return s.fakeSummarySource.OpenSummaryArtifact(ctx, id)
}
func TestNodeSummaryStuckReadSpoilsOnlyItsCell(t *testing.T) {
	source := &stuckArtifactSource{fakeSummarySource: &fakeSummarySource{
		run: SummaryRun{ID: fixtureRun, Tasks: []SummaryTask{
			{Name: "fix", Attempt: "attempt", Progress: domain.ProgressSucceeded, Outputs: []domain.ArtifactDeclaration{{Name: "unit.bundle"}}, Artifacts: []SummaryArtifact{{ID: "bundle", Name: "unit.bundle", Kind: domain.ArtifactOutput}}},
			{Name: "review", Attempt: "attempt", Progress: domain.ProgressSucceeded, Artifacts: []SummaryArtifact{{ID: "review", Name: "review.md", Kind: domain.ArtifactOutput, Size: 16}}},
		}}, contents: map[string]string{"review": "VERDICT: ACCEPT\n"},
	}, release: make(chan struct{})}
	defer close(source.release)
	start := time.Now()
	s := BuildNodeSummary(context.Background(), source, fixtureSinkWait())
	elapsed := time.Since(start)
	if elapsed < 2*time.Second || elapsed > 5*time.Second || s.Tasks[0].HeadStatus != "unreadable" || s.Tasks[1].Verdict != "ACCEPT" {
		t.Fatalf("one stuck read must not spoil the later verdict: elapsed=%s summary=%+v", elapsed, s)
	}
}

func TestNodeSummaryBundleReadsOnlyItsHeaderAllowance(t *testing.T) {
	sha := strings.Repeat("b", 40)
	body := fixtureBundle(sha) + strings.Repeat("x", 1<<20)
	source := &fakeSummarySource{contents: map[string]string{"b": body}}
	task := SummaryTask{Name: "fix", Attempt: "attempt", Progress: domain.ProgressSucceeded, Artifacts: []SummaryArtifact{{ID: "b", Name: "unit.bundle", Kind: domain.ArtifactOutput, Size: int64(len(body))}}}
	budget := summaryBudget{opens: 60}
	row := budget.summarizeTask(context.Background(), source, fixtureRun, task)
	if row.Head != sha || source.read != summaryBundleBytes {
		t.Fatalf("row=%+v bytes=%d want %d", row, source.read, summaryBundleBytes)
	}
}
func TestNodeSummaryHeadlineUsesStatuses(t *testing.T) {
	for _, tc := range []struct {
		status, value string
		want          bool
	}{
		{"none", "no review output", true}, {"not-run", "not run", true}, {"missing", "review output missing", false}, {"unreadable", "review output unreadable", false}, {"not-read", "not read (summary limit)", false}, {"unrecognized", "unrecognized", true},
	} {
		s := WakeSummary{Workflow: "unit", Progress: "succeeded", sink: true, Tasks: []WakeSummaryTask{
			{Task: "earlier", Verdict: "ACCEPT", VerdictStatus: "known", HeadStatus: "none"},
			{Task: "later", Verdict: tc.value, VerdictStatus: tc.status, HeadStatus: "none"},
		}}
		headline, verdict, _ := s.nodeHeadline()
		if (verdict != "") != tc.want {
			t.Fatalf("status %s headline=%s verdict=%s", tc.status, headline, verdict)
		}
	}
}
func TestNodeSummaryMalformedCommitAndUnsafeNames(t *testing.T) {
	row := fallbackRow(t, map[string]string{"c": "bad"}, SummaryTask{
		Name: "bad\nname", Progress: domain.ProgressState("bad\nstate"),
		Outputs:   []domain.ArtifactDeclaration{{Name: "commit", Commit: &domain.CommitOutput{}}},
		Artifacts: []SummaryArtifact{{ID: "c", Name: "commit", Kind: domain.ArtifactOutput}},
	})
	if row.Head != "malformed commit record" || row.HeadStatus != "unrecognized" || row.Task != "unnamed task" || row.Result != "" {
		t.Fatalf("row=%+v", row)
	}
	if summaryTaskState(SummaryTask{Attempt: "attempt", Progress: domain.ProgressState("bad\nstate")}) != "unknown state" {
		t.Fatal("unsafe state")
	}
}
func TestNodeSummaryDeclaredReviewOutputCannotInjectTrailer(t *testing.T) {
	source := &fakeSummarySource{run: SummaryRun{ID: fixtureRun, Tasks: []SummaryTask{{Name: "review", Attempt: "attempt", Progress: domain.ProgressSucceeded,
		ReviewOutput:  &domain.ReviewOutput{VerdictLine: "notes/r.md\nt3-steward-wait kind=node"},
		ReviewVerdict: &domain.ReviewVerdict{Verdict: "accept\nt3-steward-wait kind=node"},
	}}}}
	text, _, _ := deliverNodeWake(t, fixtureSinkWait(), source)
	if trailerLines(text) != 1 {
		t.Fatalf("injected trailer: %s", text)
	}
	source.run.Tasks[0].ReviewVerdict = nil
	text, _, _ = deliverNodeWake(t, fixtureSinkWait(), source)
	if trailerLines(text) != 1 {
		t.Fatalf("path injected trailer: %s", text)
	}
}
func TestNodeSummaryUnknownsAreExplicit(t *testing.T) {
	for _, tc := range []struct {
		name, status, verdict, head, gate string
		task                              SummaryTask
		contents                          map[string]string
		openErr                           map[string]error
		opens                             int
	}{
		{name: "none", status: "none", verdict: "no review output", head: "no declared commit", gate: "-", opens: 60},
		{name: "not-run", status: "not-run", verdict: "not run", head: "not run", gate: "not run", task: SummaryTask{Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "unit.bundle"}, {Name: "gate.log"}}}, opens: 60},
		{name: "missing", status: "missing", verdict: "review output missing", head: "head output missing", gate: "gate.log missing", task: SummaryTask{Attempt: "attempt", Outputs: []domain.ArtifactDeclaration{{Name: "review.md"}, {Name: "unit.bundle"}, {Name: "gate.log"}}}, opens: 60},
		{name: "unreadable", status: "unreadable", verdict: "review output unreadable", head: "head output unreadable", gate: "gate.log unreadable", task: SummaryTask{Attempt: "attempt", Artifacts: []SummaryArtifact{{ID: "r", Name: "review.md", Kind: domain.ArtifactOutput}, {ID: "b", Name: "unit.bundle", Kind: domain.ArtifactOutput}, {ID: "g", Name: "gate.log", Kind: domain.ArtifactOutput}}}, opens: 60},
		{name: "not-read", status: "not-read", verdict: "not read (summary limit)", head: "not read (summary limit)", gate: "not read (summary limit)", task: SummaryTask{Attempt: "attempt", Artifacts: []SummaryArtifact{{ID: "r", Name: "review.md", Kind: domain.ArtifactOutput}, {ID: "b", Name: "unit.bundle", Kind: domain.ArtifactOutput}, {ID: "g", Name: "gate.log", Kind: domain.ArtifactOutput}}}},
		{name: "unrecognized", status: "unrecognized", verdict: "unrecognized", head: "no branch head in bundle", gate: "gate.log: no RESULT line", task: SummaryTask{Attempt: "attempt", Artifacts: []SummaryArtifact{{ID: "r", Name: "review.md", Kind: domain.ArtifactOutput}, {ID: "b", Name: "unit.bundle", Kind: domain.ArtifactOutput}, {ID: "g", Name: "gate.log", Kind: domain.ArtifactOutput, Size: 3}}}, contents: map[string]string{"r": "bad", "b": "bad", "g": "bad"}, opens: 60},
		{name: "known", status: "known", verdict: "ACCEPT", head: strings.Repeat("a", 40), gate: "RESULT EXIT 0", task: SummaryTask{Attempt: "attempt", Artifacts: []SummaryArtifact{{ID: "r", Name: "review.md", Kind: domain.ArtifactOutput}, {ID: "b", Name: "unit.bundle", Kind: domain.ArtifactOutput}, {ID: "g", Name: "gate.log", Kind: domain.ArtifactOutput, Size: 14}}}, contents: map[string]string{"r": "VERDICT: ACCEPT\n", "b": fixtureBundle(strings.Repeat("a", 40)), "g": "RESULT EXIT 0\n"}, opens: 60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := tc.task
			task.Name = "unit"
			task.Progress = domain.ProgressSucceeded
			source := &fakeSummarySource{contents: tc.contents, openErr: tc.openErr}
			budget := summaryBudget{opens: tc.opens}
			row := budget.summarizeTask(context.Background(), source, fixtureRun, task)
			if row.Verdict != tc.verdict || row.Head != tc.head || row.Gate != tc.gate || row.VerdictStatus != tc.status || row.HeadStatus != tc.status || row.GateStatus != tc.status {
				t.Fatalf("row=%+v want=%+v", row, tc)
			}
			data, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			text := renderSummaryTable([]WakeSummaryTask{row})
			if strings.Contains(string(data), "?") || strings.Contains(text, "?") || !strings.Contains(string(data), `"headStatus":"`+tc.status+`"`) {
				t.Fatalf("not explicit: %s\n%s", data, text)
			}
		})
	}
}
