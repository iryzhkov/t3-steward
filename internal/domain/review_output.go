package domain

import (
	"fmt"
	"path"
	"sort"
	"strings"
)

// ReviewOutput selects one declared output for coordinator verdict parsing.
type ReviewOutput struct {
	Verdict     string `json:"verdict,omitempty" yaml:"verdict,omitempty"`
	VerdictLine string `json:"verdictLine,omitempty" yaml:"verdict_line,omitempty"`
}

func (r ReviewOutput) Path() string {
	if r.Verdict != "" {
		return path.Clean(r.Verdict)
	}
	return path.Clean(r.VerdictLine)
}

// ReviewVerdict is a bounded result, independent of the task's execution success.
type ReviewVerdict struct {
	Verdict          string   `json:"verdict"`
	BlockingFindings int      `json:"blocking_findings"`
	FindingTitles    []string `json:"finding_titles,omitempty"`
}

func (r *ReviewVerdict) Summary() string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("review=%s blocking=%d", r.Verdict, r.BlockingFindings)
}

func (r *ReviewVerdict) Prose() string {
	if r == nil {
		return ""
	}
	out := r.Summary()
	for _, title := range r.FindingTitles {
		out += "\n  - " + fmt.Sprintf("%q", title)
	}
	return out
}

func CloneReviewOutput(r *ReviewOutput) *ReviewOutput {
	if r == nil {
		return nil
	}
	cloned := *r
	return &cloned
}

func CloneReviewVerdict(r *ReviewVerdict) *ReviewVerdict {
	if r == nil {
		return nil
	}
	cloned := *r
	cloned.FindingTitles = append([]string(nil), r.FindingTitles...)
	return &cloned
}

// AggregateReviewVerdicts deterministically summarizes only the latest terminal
// attempt per task. A pending retry hides the previous attempt's verdict.
func AggregateReviewVerdicts(runID, taskID string, attempts []Attempt) *ReviewVerdict {
	latest := map[string]Attempt{}
	for _, a := range attempts {
		if a.WorkflowRunID != runID || (taskID != "" && a.TaskID != taskID) || a.IsSupervisionActivation() {
			continue
		}
		if old, ok := latest[a.TaskID]; !ok || a.Number > old.Number {
			latest[a.TaskID] = a
		}
	}
	var out *ReviewVerdict
	// Titles are sorted to keep run wakes stable across database row order.
	titles := []string{}
	for _, a := range latest {
		if !a.Progress.Terminal() || a.ReviewVerdict == nil {
			continue
		}
		if out == nil {
			out = &ReviewVerdict{Verdict: "accept"}
		}
		if a.ReviewVerdict.Verdict == "changes-requested" {
			out.Verdict = "changes-requested"
		}
		// Saturate instead of wrapping a run aggregate.
		maxInt := int(^uint(0) >> 1)
		if a.ReviewVerdict.BlockingFindings > maxInt-out.BlockingFindings {
			out.BlockingFindings = maxInt
		} else {
			out.BlockingFindings += a.ReviewVerdict.BlockingFindings
		}
		titles = append(titles, a.ReviewVerdict.FindingTitles...)
	}
	if out != nil {
		sort.Strings(titles)
		out.FindingTitles = BoundReviewTitles(titles)
	}
	return out
}

// BoundReviewTitles keeps only five short, single-line titles.
func BoundReviewTitles(titles []string) []string {
	out := []string{}
	for _, title := range titles {
		title = strings.Join(strings.Fields(title), " ")
		runes := []rune(title)
		if len(runes) > 160 {
			runes = runes[:160]
		}
		if len(runes) > 0 {
			out = append(out, string(runes))
		}
		if len(out) == 5 {
			break
		}
	}
	return out
}
