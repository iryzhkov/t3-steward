package domain

import "fmt"

// FindingLineageKey identifies a finding across fix-loop review rounds.
// ReviewerSlot is stable even when the reviewer route changes.
type FindingLineageKey struct {
	RootUnit     string
	ReviewerSlot string
	ID           string
}

// FixLoopProgress reports whether a fresh review reduced its blocking findings.
type FixLoopProgress struct {
	Progress bool
	Reason   string
}

// CompareFixLoopProgress compares blocking findings from consecutive reviews.
// Inputs must contain unique lineage keys. An empty current review is progress,
// even when the preceding review was empty.
func CompareFixLoopProgress(previous, current []FindingLineageKey) (FixLoopProgress, error) {
	previousSet, err := blockingLineageSet(previous, "previous")
	if err != nil {
		return FixLoopProgress{}, err
	}
	currentSet, err := blockingLineageSet(current, "current")
	if err != nil {
		return FixLoopProgress{}, err
	}
	if len(currentSet) == 0 {
		return FixLoopProgress{Progress: true, Reason: "no current blocking findings"}, nil
	}
	if len(currentSet) == len(previousSet) {
		equal := true
		for key := range currentSet {
			if _, ok := previousSet[key]; !ok {
				equal = false
				break
			}
		}
		if equal {
			return FixLoopProgress{Reason: "blocking lineage key set unchanged"}, nil
		}
	}
	if len(currentSet) >= len(previousSet) {
		return FixLoopProgress{Reason: "blocking finding count did not shrink"}, nil
	}
	return FixLoopProgress{Progress: true, Reason: "blocking finding count shrank"}, nil
}

func blockingLineageSet(keys []FindingLineageKey, input string) (map[FindingLineageKey]struct{}, error) {
	set := make(map[FindingLineageKey]struct{}, len(keys))
	for _, key := range keys {
		if _, exists := set[key]; exists {
			return nil, fmt.Errorf("%s review contains duplicate blocking lineage key (%q, %q, %q)", input, key.RootUnit, key.ReviewerSlot, key.ID)
		}
		set[key] = struct{}{}
	}
	return set, nil
}
