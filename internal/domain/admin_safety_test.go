package domain

import (
	"reflect"
	"testing"
	"time"
)

func TestAdminSafetyFingerprintIsOrderIndependentAndInputPreserving(t *testing.T) {
	now := time.Date(2026, time.September, 10, 20, 0, 0, 0, time.UTC)
	state := AdminSafetyState{
		Tasks:       []Task{{ID: "z"}, {ID: "a"}},
		Attempts:    []Attempt{{ID: "z", Revision: 1}, {ID: "a", Revision: 1}},
		Assignments: []Assignment{{ID: "z"}, {ID: "a"}},
		QuotaPools:  []QuotaPool{{ID: "z"}, {ID: "a"}},
		Workers: []WorkerSnapshot{
			{WorkerID: "z", Sequence: 1, ValidUntil: now},
			{WorkerID: "a", Sequence: 1, ValidUntil: now},
		},
		QuotaAdmissions: []QuotaAdmissionRecord{
			{QuotaPoolID: "z", Revision: 1},
			{QuotaPoolID: "a", Revision: 1},
		},
	}
	original := append([]Task(nil), state.Tasks...)
	first, err := AdminSafetyFingerprint(state)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state.Tasks, original) {
		t.Fatalf("fingerprinting reordered caller input: %#v", state.Tasks)
	}

	reversed := AdminSafetyState{
		Tasks:           reverseSafetySlice(state.Tasks),
		Attempts:        reverseSafetySlice(state.Attempts),
		Assignments:     reverseSafetySlice(state.Assignments),
		QuotaPools:      reverseSafetySlice(state.QuotaPools),
		Workers:         reverseSafetySlice(state.Workers),
		QuotaAdmissions: reverseSafetySlice(state.QuotaAdmissions),
	}
	second, err := AdminSafetyFingerprint(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("order changed fingerprint: %q != %q", first, second)
	}

	reversed.Workers[0].Sequence++
	changed, err := AdminSafetyFingerprint(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatal("worker safety change did not change fingerprint")
	}
}

func reverseSafetySlice[T any](input []T) []T {
	result := append([]T(nil), input...)
	for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result
}
