package backlog

import "testing"

func TestSeedWeeklyCostUsesWeeklyWindowColdStartScale(t *testing.T) {
	want := map[int]float64{1: 0.5, 2: 1, 3: 2, 4: 3.5, 5: 5}
	for difficulty, expected := range want {
		if got := SeedWeeklyCost(difficulty); got != expected {
			t.Fatalf("SeedWeeklyCost(%d) = %v, want %v", difficulty, got, expected)
		}
	}
	if got := SeedWeeklyCost(0); got != want[3] {
		t.Fatalf("fallback SeedWeeklyCost(0) = %v, want %v", got, want[3])
	}
}
