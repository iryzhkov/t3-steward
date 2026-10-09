//go:build race

package testtiming

// RaceEnabled reports whether the binary was built with the race detector.
const RaceEnabled = true

// RaceFactor is how many times longer an absolute wall-clock bound is under
// the race detector. Instrumented code runs several times slower, and `make
// test` runs the whole suite under the detector on hosts that are often
// running other work, so a bound sized for a plain run is scaled by it.
const RaceFactor = 5
