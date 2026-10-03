//go:build race

package main

// raceEnabled reports whether the tests were built with the race detector.
// Under it every write through the pure-Go SQLite driver is several times
// slower, so measurement tests take fewer samples; their numbers are not
// meaningful under the detector anyway.
const raceEnabled = true
