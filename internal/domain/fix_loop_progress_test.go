package domain

import (
	"reflect"
	"strings"
	"testing"
)

func TestCompareFixLoopProgress(t *testing.T) {
	a := FindingLineageKey{RootUnit: "unit", ReviewerSlot: "reviewer-1", ID: "A"}
	b := FindingLineageKey{RootUnit: "unit", ReviewerSlot: "reviewer-1", ID: "B"}
	c := FindingLineageKey{RootUnit: "unit", ReviewerSlot: "reviewer-1", ID: "C"}
	otherUnit := FindingLineageKey{RootUnit: "other-unit", ReviewerSlot: a.ReviewerSlot, ID: a.ID}
	otherSlot := FindingLineageKey{RootUnit: a.RootUnit, ReviewerSlot: "reviewer-2", ID: a.ID}

	tests := []struct {
		name     string
		previous []FindingLineageKey
		current  []FindingLineageKey
		progress bool
		reason   string
		errInput string
	}{
		{name: "same set reordered", previous: []FindingLineageKey{a, b}, current: []FindingLineageKey{b, a}, reason: "blocking lineage key set unchanged"},
		{name: "shrinking subset", previous: []FindingLineageKey{a, b}, current: []FindingLineageKey{b}, progress: true, reason: "blocking finding count shrank"},
		{name: "level count with replaced keys", previous: []FindingLineageKey{a, b}, current: []FindingLineageKey{a, c}, reason: "blocking finding count did not shrink"},
		{name: "growth", previous: []FindingLineageKey{a}, current: []FindingLineageKey{a, b}, reason: "blocking finding count did not shrink"},
		{name: "zero current", previous: []FindingLineageKey{a}, progress: true, reason: "no current blocking findings"},
		{name: "both empty", progress: true, reason: "no current blocking findings"},
		{name: "new blockers after empty", current: []FindingLineageKey{a}, reason: "blocking finding count did not shrink"},
		{name: "shrinking replacement", previous: []FindingLineageKey{a, b}, current: []FindingLineageKey{c}, progress: true, reason: "blocking finding count shrank"},
		{name: "root unit distinguishes keys", previous: []FindingLineageKey{a}, current: []FindingLineageKey{otherUnit}, reason: "blocking finding count did not shrink"},
		{name: "reviewer slot distinguishes keys", previous: []FindingLineageKey{a}, current: []FindingLineageKey{otherSlot}, reason: "blocking finding count did not shrink"},
		{name: "same id in different lineages", previous: []FindingLineageKey{a, otherUnit, otherSlot}, current: []FindingLineageKey{a, otherSlot}, progress: true, reason: "blocking finding count shrank"},
		{name: "duplicate previous", previous: []FindingLineageKey{a, a}, current: []FindingLineageKey{b}, errInput: "previous"},
		{name: "duplicate current", previous: []FindingLineageKey{a, b, c}, current: []FindingLineageKey{a, a}, errInput: "current"},
		{name: "duplicate previous with zero current", previous: []FindingLineageKey{a, a}, errInput: "previous"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previousBefore := append([]FindingLineageKey(nil), tt.previous...)
			currentBefore := append([]FindingLineageKey(nil), tt.current...)
			got, err := CompareFixLoopProgress(tt.previous, tt.current)
			if tt.errInput != "" {
				if err == nil || !strings.Contains(err.Error(), tt.errInput+" review contains duplicate blocking lineage key") {
					t.Fatalf("expected duplicate %s error, got %v", tt.errInput, err)
				}
				if got != (FixLoopProgress{}) {
					t.Fatalf("invalid input produced a result: %+v", got)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if got.Progress != tt.progress || got.Reason != tt.reason {
					t.Fatalf("got %+v, want progress=%v reason=%q", got, tt.progress, tt.reason)
				}
			}
			if !reflect.DeepEqual(tt.previous, previousBefore) || !reflect.DeepEqual(tt.current, currentBefore) {
				t.Fatal("comparator mutated its inputs")
			}
		})
	}
}
