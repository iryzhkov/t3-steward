//go:build !race

package testtiming

// RaceEnabled reports whether the binary was built with the race detector.
const RaceEnabled = false

// RaceFactor is how many times longer an absolute wall-clock bound is under
// the race detector; without it a bound keeps its own value.
const RaceFactor = 1
