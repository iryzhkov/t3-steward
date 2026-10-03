//go:build !race

package sqlite

// raceEnabled reports whether the tests were built with the race detector.
const raceEnabled = false
