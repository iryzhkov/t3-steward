package testtiming

import (
	"testing"
	"time"
)

// sink keeps the measured loops from being optimised away.
var sink int

func linearWork(n int) func() {
	return func() {
		total := 0
		for i := range n * 1000 {
			total += i ^ total
		}
		sink = total
	}
}

func quadraticWork(n int) func() {
	return func() {
		total := 0
		for i := range n {
			for j := range n {
				total += i ^ j ^ total
			}
		}
		sink = total
	}
}

func TestCheckLinearAcceptsLinearWork(t *testing.T) {
	if err := CheckLinear(64, linearWork); err != nil {
		t.Fatal(err)
	}
}

func TestCheckLinearRejectsQuadraticWork(t *testing.T) {
	if err := CheckLinear(64, quadraticWork); err == nil {
		t.Fatal("a quadratic operation passed as linear")
	}
}

func TestBoundScalesOnlyUnderTheRaceDetector(t *testing.T) {
	want := time.Second
	if RaceEnabled {
		want = RaceFactor * time.Second
	}
	if got := Bound(time.Second); got != want {
		t.Fatalf("Bound(1s) = %s, want %s", got, want)
	}
}
