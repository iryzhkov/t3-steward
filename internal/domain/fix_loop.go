package domain

import "sort"

// FixLoopTask records a statically declared round; it never authorizes expansion.
type FixLoopTask struct {
	Name      string `json:"name" yaml:"name"`
	Round     int    `json:"round" yaml:"round"`
	MaxRounds int    `json:"maxRounds" yaml:"max_rounds"`
	Kind      string `json:"kind" yaml:"kind"`
}

type FixLoopSummary struct {
	Name         string `json:"name"`
	Rounds       int    `json:"rounds"`
	MaxRounds    int    `json:"maxRounds"`
	FinalVerdict string `json:"finalVerdict,omitempty"`
	Exhausted    bool   `json:"exhausted,omitempty"`
	Escalation   string `json:"escalation,omitempty"`
}

const VerdictBranchSkipped = "verdict condition not matched"
const FixLoopExhausted = "fix-loop-exhausted"

// SummarizeFixLoops uses latest attempt evidence, including failed reviews.
func SummarizeFixLoops(tasks []Task, attempts []Attempt) []FixLoopSummary {
	latest := map[string]Attempt{}
	for _, a := range attempts {
		if old, ok := latest[a.TaskID]; !ok || a.Number > old.Number {
			latest[a.TaskID] = a
		}
	}
	summaries := map[string]FixLoopSummary{}
	verdictRound := map[string]int{}
	for _, t := range tasks {
		if t.FixLoop == nil {
			continue
		}
		f := t.FixLoop
		s := summaries[f.Name]
		s.Name, s.MaxRounds = f.Name, f.MaxRounds
		a := latest[t.ID]
		if a.StartedAt != nil || a.Progress == ProgressSucceeded || a.Progress == ProgressFailed {
			if f.Round > s.Rounds {
				s.Rounds = f.Round
			}
		}
		if f.Kind == "review" && a.Progress == ProgressSucceeded && a.ReviewVerdict != nil && f.Round > verdictRound[f.Name] {
			s.FinalVerdict = a.ReviewVerdict.Verdict
			verdictRound[f.Name] = f.Round
			s.Exhausted = f.Round == f.MaxRounds && s.FinalVerdict == "changes-requested"
			if s.Exhausted {
				s.Escalation = FixLoopExhausted
			} else {
				s.Escalation = ""
			}
		}
		summaries[f.Name] = s
	}
	names := make([]string, 0, len(summaries))
	for name := range summaries {
		names = append(names, name)
	}
	sort.Strings(names)
	var result []FixLoopSummary
	for _, name := range names {
		result = append(result, summaries[name])
	}
	return result
}
